#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""cli.py — CLI đóng gói luồng UPI (cs_ + oaics_) trong upi-checkout/.

Chỉ cần truyền args, không cần sửa file/set env:
  python cli.py oaics <accounts> [--proxy P] [--workers N] [--country CC]
  python cli.py cs    <accounts> [--proxy P] [--workers N] [--promo M] [--country CC]
  python cli.py auto  <accounts> [--proxy P] [--workers N] [--promo M] [--country CC]

accounts         = file chứa 1 JWT/dòng (hoặc --token JWT cho 1 account)
--proxy          = file proxy (hỗ trợ cả dạng host:port:user:pass và http://user:pass@host:port)
--workers        = số account đồng thời (mặc định 4)
--country        = billing country (mặc định IN)
--promo          = cs/auto: cấu hình promo của extract_cs (mặc định off)

Chế độ:
  oaics = tạo link UPI + QR cho account oaics_ (open_ai provider)
  cs    = chạy luồng payment_pages+approve cho account cs_ (stripe provider)
  auto  = tự dò provider (oaics_/cs_) rồi chạy đúng luồng cho từng account
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import queue
import random
import re
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote

import requests

import upi_core as core
import check_upi_eligibility as cu

HERE = Path(__file__).resolve().parent
CHATGPT = "https://chatgpt.com"
SV = "2025-03-31.basil"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")
STATE_TMPL = json.dumps({"seed": {}, "checkout": {}, "promotion": {}, "provider": {}, "pair": {}})
LOCK = threading.Lock()


# ---------- progress hooks ----------
# CLI thuong truyen hook rong (no-op) nen hanh vi dong lenh khong doi.
# web/engine.py truyen hook that de phat su kien SSE len UI.
StepHook = Callable[..., None]
LogHook = Callable[[str], None]
GeoHook = Callable[[dict], None]


def _noop_step(key: str, status: str, **info: Any) -> None:
    """Buoc progress mac dinh — khong lam gi."""


def _noop_log(line: str) -> None:
    """Log hook mac dinh — khong lam gi."""


def _noop_geo(info: dict) -> None:
    """Geo/egress hook mac dinh — khong lam gi."""


# Cac buoc cua tung luong backend. web UI render dung theo danh sach nay.
STEPS_OAICS: list[tuple[str, str]] = [
    ("warmup", "Warmup session"),
    ("checkout", "Create checkout"),
    ("promo", "Apply promotion"),
    ("geo", "Proxy egress / billing"),
    ("taxes", "Update tax region"),
    ("payment_method", "Create payment method"),
    ("confirm", "Confirm checkout"),
    ("pi_confirm", "Confirm intent"),
    ("artifact", "Extract QR artifact"),
]

STEPS_CS: list[tuple[str, str]] = [
    ("runner", "Initialize CS flow"),
    ("checkout", "Create checkout"),
    ("promo", "Apply promotion"),
    ("stripe_init", "Initialize payment page"),
    ("tax", "Update tax region"),
    ("pm", "Create payment method"),
    ("confirm", "Confirm payment"),
    ("approve", "Approve payment"),
    ("poll", "Poll payment artifact"),
    ("artifact", "Extract payment artifact"),
]



RESULT_MARKER = "UPI final pay URL:"
HOSTED_MARKER = "UPI hosted checkout URL:"
# Ảnh QR Stripe trả trong `next_action...qr_code.image_url_png` (host qr.stripe.com).
# extract_cs đã bắt được từ lâu nhưng vứt đi; giờ in marker riêng để đưa lên card.
QR_PNG_MARKER = "UPI QR PNG:"
QR_SVG_MARKER = "UPI QR SVG:"


def extract_marker_url(lines: list[str], marker: str) -> str:
    """Lấy URL ngay sau `marker` (cùng dòng hoặc dòng kế). KHÔNG cắt bớt chuỗi."""
    for i, raw in enumerate(lines):
        line = raw.strip()
        if marker not in line:
            continue
        rest = line.split(marker, 1)[1].strip()
        cand = rest or (lines[i + 1].strip() if i + 1 < len(lines) else "")
        if cand.startswith(("http://", "https://")):
            return cand
    return ""


# Dấu hiệu link QR thật: trang UPI instructions của Stripe mở ra là QR + nút mở app.
# `checkout.stripe.com/c/pay/...` KHÔNG tính — đó là trang checkout hosted.
QR_LINK_MARKER = "/upi/instructions/"


def is_qr_link(url: str) -> bool:
    return QR_LINK_MARKER in str(url or "")

# Marker trong stdout cua extract_cs -> buoc tuong ung.
# (step_key, cac chuoi marker, marker nay nghia la buoc DA XONG)
CS_STEP_MARKERS: list[tuple[str, tuple[str, ...], bool]] = [
    ("checkout", ("Checkout promo: mode=",), True),
    ("promo", ("checkout/update ok",), True),
    ("stripe_init", ("Stripe init ok",), True),
    ("tax", ("checkout/taxes synced", "tax_region submitted"), True),
    ("pm", ("PM created:",), True),
    ("confirm", ("Step 2: first attempt PM=", "confirm extracted"), True),
    ("approve", ("approve attempt",), False),
    ("approve", ("approve ok",), True),
    ("poll", ("poll response summary",), False),
    ("artifact", (RESULT_MARKER,), True),
]


def cs_stage_for_line(line: str) -> tuple[str, bool] | None:
    """Map 1 dong log cua extract_cs -> (step_key, da_xong). None neu khong khop."""
    for key, needles, completed in CS_STEP_MARKERS:
        if any(n in line for n in needles):
            return key, completed
    return None


def steps_for(flow: str) -> list[dict]:
    """Danh sach buoc cua 1 luong. flow='auto' khi chua do duoc provider."""
    if flow == "cs":
        return [{"key": k, "label": lb} for k, lb in STEPS_CS]
    if flow == "oaics":
        return [{"key": k, "label": lb} for k, lb in STEPS_OAICS]
    return [{"key": "warmup", "label": "Warmup session"},
            {"key": "detect", "label": "Detect provider (oaics / cs)"}]





_AMOUNT_RE = re.compile(r"amount=([A-Za-z]{3})\s+([0-9]+(?:\.[0-9]+)?)")


def extract_amount_minor(lines: list[str]) -> int | None:
    """从 extract_cs 的 stdout 里捡出最终金额（单位：分）。

    日志里长这样：`IN Bootstrap Stripe init ok, amount=INR 1999.00`；
    促销生效后同一个 marker 会变成 `amount=INR 0.00`。取**最后一条** ——
    它才是最终值，前面的都是促销前的。

    没有这一步，UI 上的「Số tiền」在 cs 流程里永远是空的：
    `cs_subprocess_one` 的返回值里压根没有 `amount_minor`
    （只有 `oaics_one` 那条路有，见上面 `res.update({"amount_minor": amount})`）。
    """
    for raw in reversed(lines):
        matched = _AMOUNT_RE.search(raw)
        if not matched:
            continue
        try:
            return int(round(float(matched.group(2)) * 100))
        except (TypeError, ValueError):
            continue
    return None


def extract_result_url(lines: list[str]) -> str:
    """Lấy URL kết quả từ output của extract_cs.

    extract_cs in ra:
        ===== RESULT =====
        UPI final pay URL:
        https://...            <- URL nằm ở DÒNG SAU marker

    Nhận MỌI dạng URL chứ không chỉ payments.stripe.com/upi/instructions/ —
    luồng cs_ có thể trả về https://checkout.stripe.com/c/pay/cs_live_... .
    """
    for i, raw in enumerate(lines):
        line = raw.strip()
        if RESULT_MARKER not in line:
            continue
        rest = line.split(RESULT_MARKER, 1)[1].strip()
        cand = rest or (lines[i + 1].strip() if i + 1 < len(lines) else "")
        if cand.startswith(("http://", "https://")):
            # KHÔNG cắt bớt URL. Trước đây là `cand[:300]`, mà URL instructions thật
            # có thể dài hơn — cắt là ra link chết.
            return cand
    # Stripe khong tra QR qua API (next_action = upi_await_notification rong),
    # nhung co stripe_hosted_url -> extract_cs in marker rieng. Day la artifact dung:
    # mo https://checkout.stripe.com/c/pay/cs_live_...#fid... la ra QR.
    for i, raw in enumerate(lines):
        line = raw.strip()
        if HOSTED_MARKER not in line:
            continue
        rest = line.split(HOSTED_MARKER, 1)[1].strip()
        cand = rest or (lines[i + 1].strip() if i + 1 < len(lines) else "")
        if cand.startswith(("http://", "https://")):
            # Trước đây là `cand[:400]`. `stripe_hosted_url` thật dài **501** ký tự,
            # nên cắt 400 là chặt mất 101 ký tự cuối — đúng đoạn `#fid...` mang
            # client secret. Hậu quả: mở link ra chỉ thấy
            # "This link is incomplete. Use the unmodified checkout URL..." (đã đo
            # bằng cách mở thật cả 2 bản). URL là một khối nguyên vẹn, không cắt được.
            return cand

    # fallback cuoi: URL upi/instructions nam lac o dong nao do
    for raw in lines:
        line = raw.strip()
        if line.startswith(("http://", "https://")) and "upi/instructions/" in line:
            return line
    return ""


EGRESS_PROBE_URL = "https://ipwho.is/"


def probe_egress(proxy: str, timeout: int = 20) -> dict:
    """Đo IP thật mà proxy xuất ra + chấm chất lượng, qua ippure.com.

    Vì sao đổi từ findip sang ippure: ippure trả thông tin của CHÍNH IP đang gọi
    nên gộp được "đo exit" và "chấm chất lượng" vào 1 request (bản cũ tốn 2:
    ipwho.is + findip.lookup theo IP). Và bộ điểm của ippure (fraudScore,
    isResidential, isBroadcast) nhắm đúng bài toán IP cho dịch vụ AI.

    Key giữ nguyên tên cũ (risk/verdict/user_type) vì UI egress đang đọc chúng;
    các key mới (fraud_score/is_residential/is_broadcast) thêm vào cho ai cần.

    Trả {} nếu lỗi/timeout.
    """
    if not proxy:
        return {}
    t0 = time.time()
    probe: dict = {}
    try:
        import ippure  # noqa: PLC0415
        probe = ippure.probe_exit(proxy, timeout) or {}
    except Exception:  # noqa: BLE001 — loi mang/module thi roi ve ipwho.is
        probe = {}

    if probe.get("quality_present"):
        fraud = probe.get("fraud_score")
        if not isinstance(fraud, (int, float)):
            verdict = ""
        elif fraud <= 10:
            verdict = "clean"
        elif fraud <= 30:
            verdict = "medium"
        else:
            verdict = "dirty"
        is_res = probe.get("is_residential")
        return {
            "ip": str(probe.get("ip") or ""),
            "country": str(probe.get("country") or ""),
            "city": str(probe.get("city") or ""),
            "region": str(probe.get("region") or ""),
            "ip_type": str(probe.get("family") or ""),      # "IPv4" / "IPv6"
            "asn": probe.get("asn"),
            "org": str(probe.get("isp") or ""),
            "isp": str(probe.get("isp") or ""),
            "domain": "",
            "latency_ms": probe.get("latency_ms") or int((time.time() - t0) * 1000),
            "timezone": str(probe.get("timezone") or ""),
            "ippure": True,
            "fraud_score": int(fraud) if isinstance(fraud, (int, float)) else None,
            "is_residential": is_res,
            "is_broadcast": probe.get("is_broadcast"),
            "is_hosting": probe.get("is_broadcast"),
            # Tên cũ để UI/probe khác không phải sửa
            "risk": int(fraud) if isinstance(fraud, (int, float)) else None,
            "verdict": verdict,
            "user_type": ("residential" if is_res else
                          "non-residential" if is_res is False else ""),
        }

    # ippure không trả được (Cloudflare chặn / IPv6 không có dữ liệu chất lượng)
    # -> rơi về ipwho.is để vẫn đo được exit, chỉ thiếu phần chất lượng.
    try:
        g = requests.get(EGRESS_PROBE_URL,
                         headers={"User-Agent": "Mozilla/5.0 (compatible; momo-checkout/1.0)"},
                         proxies={"http": proxy, "https": proxy}, timeout=timeout).json()
    except Exception:  # noqa: BLE001 — probe loi thi coi nhu khong do duoc
        return {}
    if not isinstance(g, dict) or not g.get("ip"):
        return {}
    conn = g.get("connection") or {}
    return {"ip": str(g.get("ip") or ""),
            "country": str(g.get("country_code") or ""),
            "city": str(g.get("city") or ""),
            "region": str(g.get("region") or ""),
            "ip_type": str(g.get("type") or ""),
            "asn": conn.get("asn"),
            "org": str(conn.get("org") or ""),
            "isp": str(conn.get("isp") or ""),
            "domain": str(conn.get("domain") or ""),
            "latency_ms": int((time.time() - t0) * 1000),
            "risk": None, "verdict": "", "user_type": "",
            "fraud_score": None, "is_residential": None, "is_broadcast": None}


# ---------- cham diem proxy (chon proxy sach / risk thap truoc khi chay) ----------
# Nguon "risk" gom 2 phan:
#   1. chat luong tinh: reachable, dung country, ISP residential, IPv4, do tre
#   2. lich su thuc chien: proxy_state.json do chinh extract_cs ghi lai
DC_KEYWORDS = (
    "amazon", "aws", "google", "microsoft", "azure", "oracle", "alibaba", "tencent",
    "digitalocean", "linode", "akamai", "vultr", "hetzner", "ovh", "contabo", "leaseweb",
    "m247", "choopa", "quadranet", "psychz", "colocrossing", "datacamp", "hosting",
    "datacenter", "data center", "server", "vps", "cloud", "colo", "dedicated",
)

# diem toi da tung nhom
_SC_REACH = 45
_SC_COUNTRY = 25
_SC_RESIDENTIAL = 15
_SC_IPV4 = 8
_SC_LATENCY = 7


def _is_datacenter(info: dict) -> bool:
    blob = " ".join(str(info.get(k) or "") for k in ("org", "isp", "domain")).lower()
    return any(k in blob for k in DC_KEYWORDS)


def proxy_history(proxy: str) -> dict:
    """Lay lich su success/fail cua proxy tu proxy_state.json (moi nhom)."""
    try:
        import extract_cs as ecs  # noqa: PLC0415
        key = ecs.proxy_short(proxy)
        state = ecs.load_proxy_state()
    except Exception:  # noqa: BLE001
        return {"success": 0, "fail": 0, "last_reason": ""}
    ok = fail = 0
    last_reason = ""
    for group in ("seed", "checkout", "promotion", "provider"):
        ent = (state.get(group) or {}).get(key)
        if not isinstance(ent, dict):
            continue
        ok += int(ent.get("success") or 0)
        fail += int(ent.get("fail") or 0)
        last_reason = str(ent.get("last_reason") or last_reason)
    return {"success": ok, "fail": fail, "last_reason": last_reason}


def proxy_persona_timezone() -> str:
    """Timezone persona đang khai (khớp sentinel + Stripe browser_timezone)."""
    try:
        import extract_cs as ecs  # noqa: PLC0415
        return str(ecs.payment_browser_timezone() or "Asia/Kolkata")
    except Exception:  # noqa: BLE001
        return "Asia/Kolkata"


def ippure_verdict(info: dict, target_country: str, hist: dict, timeout: int = 20) -> dict:
    """Chấm proxy bằng ippure (fraudScore + isResidential + isBroadcast).

    Khác bản findip: KHÔNG tra thêm theo IP nữa. ippure trả thông tin của chính
    IP đang gọi, mà `probe_egress()` đã đi qua proxy rồi — nên chất lượng đã nằm
    sẵn trong `info`, không cần request thứ hai (bản cũ tốn 2: ipwho + lookup).

    Trả {} khi không có dữ liệu chất lượng, để rơi về cách chấm theo từ khoá ISP.
    """
    try:
        import ippure  # noqa: PLC0415
    except Exception:  # noqa: BLE001
        return {}
    ip = str(info.get("ip") or "")
    if not ip:
        return {}
    if info.get("fraud_score") is None and info.get("is_residential") is None:
        return {}          # ipwho.is fallback: không có phần chất lượng
    probe = {"ip": ip, "country": info.get("country") or "",
             "latency_ms": info.get("latency_ms"),
             "timezone": info.get("timezone") or "",
             "family": "IPv6" if ":" in ip else "IPv4",
             "asn": info.get("asn"), "isp": info.get("isp") or "",
             "fraud_score": info.get("fraud_score"),
             "is_residential": info.get("is_residential"),
             "is_broadcast": info.get("is_broadcast"),
             "source": "ippure", "quality_present": True}
    return ippure.judge(probe, target_country, proxy_persona_timezone(), hist)


def score_proxy(proxy: str, target_country: str = "IN", timeout: int = 20,
                egress: dict | None = None) -> dict:
    """Cham diem 0..100 cho 1 proxy. `grade` A/B/C/F; F = khong nen dung."""
    info = egress if egress is not None else probe_egress(proxy, timeout)
    hist = proxy_history(proxy)
    try:
        import extract_cs as ecs  # noqa: PLC0415
        label = ecs.proxy_short(proxy)
    except Exception:  # noqa: BLE001
        label = "proxy"

    res = {"proxy": proxy, "label": label, "ok": bool(info.get("ip")),
           "ip": info.get("ip") or "", "country": info.get("country") or "",
           "city": info.get("city") or "", "region": info.get("region") or "",
           "ip_type": info.get("ip_type") or "", "org": info.get("org") or "",
           "isp": info.get("isp") or "", "asn": info.get("asn"),
           "latency_ms": info.get("latency_ms"), "history": hist,
           "flags": [], "score": 0, "grade": "F"}

    judged = ippure_verdict(info, target_country, hist, timeout)
    if judged:
        # ippure là bên chấm điểm; các key đo được vẫn giữ nguyên như bản cũ
        res.update({"ok": bool(judged.get("ok")), "risk": judged.get("risk"),
                    "verdict": judged.get("verdict") or "",
                    "user_type": judged.get("user_type") or "",
                    "connection_type": judged.get("connection_type") or "",
                    "timezone": judged.get("timezone") or "",
                    "exit_family": judged.get("family") or "",
                    "flags": list(judged.get("flags") or []),
                    "reasons": list(judged.get("reasons") or []),
                    "score": int(judged.get("score") or 0),
                    "grade": judged.get("grade") or "F"})
        return res

    flags = res["flags"]
    if not res["ok"]:
        flags.append("unreachable")
        return res

    score = _SC_REACH
    if res["country"].upper() == str(target_country).upper():
        score += _SC_COUNTRY
    else:
        flags.append("wrong-country")

    if _is_datacenter(info):
        flags.append("datacenter-isp")
    else:
        score += _SC_RESIDENTIAL

    if res["ip_type"].upper() == "IPV4":
        score += _SC_IPV4
    else:
        flags.append("ipv6")

    lat = res["latency_ms"] or 99999
    if lat < 2500:
        score += _SC_LATENCY
    elif lat > 6000:
        flags.append("slow")

    if hist["success"]:
        score += min(18, hist["success"] * 6)
    if hist["fail"]:
        score -= min(30, hist["fail"] * 10)
        flags.append("history-fail")

    res["score"] = max(0, min(100, score))
    # sai country = khong dung duoc cho flow nay -> khong the la A/B/C
    if "wrong-country" in flags:
        res["score"] = min(res["score"], 25)
    res["grade"] = ("A" if res["score"] >= 75 else
                    "B" if res["score"] >= 55 else
                    "C" if res["score"] >= 35 else "F")
    return res


def scan_proxies(proxies: list[str], target_country: str = "IN",
                 workers: int = 12, timeout: int = 15) -> list[dict]:
    """Do + cham diem ca pool, tra ve list da sap xep diem cao -> thap."""
    from concurrent.futures import ThreadPoolExecutor

    def one(px: str) -> dict:
        try:
            return score_proxy(px, target_country, timeout=timeout)
        except Exception as exc:  # noqa: BLE001
            return {"proxy": px, "label": "proxy", "ok": False, "score": 0, "grade": "F",
                    "flags": ["probe-error"], "error": str(exc)[:120],
                    "ip": "", "country": "", "ip_type": "", "org": "", "latency_ms": None,
                    "history": {"success": 0, "fail": 0, "last_reason": ""}}

    if not proxies:
        return []
    with ThreadPoolExecutor(max_workers=max(1, min(workers, len(proxies)))) as ex:
        out = list(ex.map(one, proxies))
    out.sort(key=lambda r: (-r.get("score", 0), r.get("latency_ms") or 99999))
    return out


def probe_egress_pair(proxy: str) -> dict:
    """Đo IP exit (proxy nguyên bản) và IP billing (proxy rewrite sang country billing).

    Luồng cs_ rewrite region của proxy theo từng chặng
    (bootstrap → promotion → provider); chặng billing/tax/PM dùng
    UPI_PROVIDER_COUNTRY nên IP có thể KHÁC IP exit.
    """
    exit_info = probe_egress(proxy)
    billing_info: dict = {}
    billing_country = ""
    try:
        import extract_cs as ecs  # noqa: PLC0415 — import muộn, module nặng
        billing_country = ecs.UPI_PROVIDER_COUNTRY
        rewritten = ecs.proxy_for_country(proxy, billing_country)
        if core.normalize_proxy_url(rewritten) != core.normalize_proxy_url(proxy):
            billing_info = probe_egress(rewritten)
    except Exception:  # noqa: BLE001 — proxy khong co selector / import loi
        billing_info = {}
    if not billing_info:
        # cung region hoac khong rewrite duoc -> billing dung chung IP voi exit
        billing_info = dict(exit_info)
    return {"exit": exit_info, "billing": billing_info, "billing_country": billing_country}


def b64d(s: str) -> bytes:
    s += "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s)


def decode_email(token: str) -> str:
    try:
        d = json.loads(b64d(token.split(".")[1]))
        return (d.get("https://api.openai.com/profile") or {}).get("email", "?")
    except Exception:
        return "?"


def setup() -> None:
    cu.configure_pre_proxy("auto")
    core.COUNTRY_CURRENCY["IN"] = "INR"
    core.CHATGPT_TIMEOUT = 25
    core.DEFAULT_TIMEOUT = 25
    if not os.environ.get("UPI_DUMP"):
        core.dump_http = lambda *a, **k: None
    core.log = lambda *a, **k: None


def load_proxies(path: Path) -> list[str]:
    px = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "://" in line:
            px.append(core.normalize_proxy_url(line))
        else:
            parts = line.split(":")
            if len(parts) == 4:
                host, port, user, pw = parts
                px.append(f"http://{quote(user)}:{quote(pw)}@{host}:{port}")
            else:
                px.append(core.normalize_proxy_url(line))
    return px or [""]


def chat_h(path: str, proc: str = "", sid: str = "") -> dict:
    return {"Referer": f"{CHATGPT}/checkout/{proc}/{sid}" if sid else f"{CHATGPT}/",
            "x-openai-target-path": path, "x-openai-target-route": path}


def build_session(token: str, proxy: str, step: StepHook = _noop_step):
    step("warmup", "active")
    device_id = str(uuid.uuid4())
    s = core.build_chatgpt_session(token, device_id, proxy, "")
    cu.warmup_csrf(s, 25)
    sh = cu.sentinel_headers(device_id, proxy, 25)
    if not sh.get("OpenAI-Sentinel-Token"):
        step("warmup", "fail", err="sentinel_missing")
        raise RuntimeError("sentinel_missing")
    s.headers.update(sh)
    step("warmup", "done")
    return s


def checkout(session, country: str, promo: str = "off", promo_id: str = "plus-1-month-free",
             trial_days: int = 30) -> dict:
    body = {"entry_point": "all_plans_pricing_modal", "plan_name": "chatgptplusplan",
            "price_interval": "month", "seat_quantity": 1,
            "billing_details": {"country": country, "currency": "INR"},
            "cancel_url": f"{CHATGPT}/#pricing", "checkout_ui_mode": "custom"}
    if promo in ("trial", "free_trial"):
        body["subscription_data"] = {"trial_period_days": trial_days}
    elif promo in ("campaign", "query"):
        body["promo_campaign"] = {"promo_campaign_id": promo_id,
                                  "is_coupon_from_query_param": promo == "query"}
    elif promo == "coupon":
        body["coupon"] = promo_id
    r = session.post(f"{CHATGPT}/backend-api/payments/checkout", json=body,
                     headers=chat_h("/backend-api/payments/checkout"), timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"checkout_{r.status_code}: {r.text[:200]}")
    d = r.json() or {}
    if not d.get("checkout_session_id"):
        raise RuntimeError(f"no_session: {str(d)[:200]}")
    return d


def kind_of(session_id: str) -> str:
    if str(session_id).startswith("cs_"):
        return "cs"
    if str(session_id).startswith("oaics_"):
        return "oaics"
    return "other"


def out(o: dict) -> None:
    with LOCK:
        print(json.dumps(o, ensure_ascii=False, separators=(",", ":")), flush=True)


# ---------- scan ----------
def detect_one(token: str, proxy: str, country: str, step: StepHook = _noop_step) -> dict:
    res = {"email": decode_email(token)}
    try:
        step("detect", "active")
        s = build_session(token, proxy, step)
        d = checkout(s, country)
        sid = d.get("checkout_session_id") or ""
        methods = [str(m).lower() for m in (d.get("payment_method_types") or [])]
        res.update({"status": "ok", "session": str(sid)[:16], "kind": kind_of(sid),
                    "provider": d.get("checkout_provider") or "",
                    "methods": methods, "has_upi": "upi" in methods})
        step("detect", "done",
             detail=f"{kind_of(sid)} · {d.get('checkout_provider') or '?'}")
    except Exception as exc:  # noqa: BLE001
        res.update({"status": "error", "err": str(exc)[:180]})
        step("detect", "fail", err=str(exc)[:180])
    return res


# ---------- oaics ----------
# ---------- proxy-geo billing ----------



def geo_profile(proxy: str, on_geo: GeoHook = _noop_geo) -> dict:
    """Sinh profile billing 100% tự động theo vị trí địa lý của Proxy qua IP Lookup & OpenStreetMap (Nominatim)."""
    h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    ip = ""
    approx = False
    city = region = postal = ""
    lat = lon = None

    if proxy:
        try:
            g = requests.get("https://ipwho.is/", headers=h,
                             proxies={"http": proxy, "https": proxy}, timeout=20).json()
            if g.get("success") and str(g.get("country_code") or "").upper() == "IN":
                ip = str(g.get("ip") or "")
                city = str(g.get("city") or "")
                region = str(g.get("region") or "")
                postal = str(g.get("postal") or "").strip()
                lat, lon = g.get("latitude"), g.get("longitude")
        except Exception:
            approx = True

    road = rcity = rstate = rpost = ""
    if lat and lon:
        try:
            rev = requests.get(
                f"https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=jsonv2",
                headers={**h, "Accept": "application/json"}, timeout=20,
                proxies={"http": proxy, "https": proxy} if proxy else None).json()
            ad = rev.get("address") or {}
            road = str(ad.get("road") or ad.get("street") or ad.get("suburb") or ad.get("neighbourhood") or "")
            rcity = str(ad.get("city") or ad.get("town") or ad.get("village") or ad.get("county") or "")
            rstate = str(ad.get("state") or "")
            rpost = str(ad.get("postcode") or "").strip()
        except Exception:
            pass

    city_final = rcity or city or "Bengaluru"
    state_final = rstate or region or "Karnataka"
    postal_final = rpost or postal or "560001"
    if not postal_final.isdigit() or len(postal_final) != 6:
        postal_final = "560001"

    road_final = road or "MG Road"
    hn = random.randint(1, 299)
    first_names = ["Aarav", "Aisha", "Ananya", "Arjun", "Dev", "Diya", "Kavya", "Neha", "Priya", "Rahul", "Rohan", "Vikram"]
    last_names = ["Sharma", "Patel", "Singh", "Kumar", "Gupta", "Nair", "Verma", "Rao", "Joshi", "Mehta"]
    first = random.choice(first_names)
    last = random.choice(last_names)

    on_geo({"ip": ip, "city": city_final, "region": state_final, "approx": approx})
    return {
        "name": f"{first} {last}",
        "email": f"{first.lower()}{last.lower()}{random.randint(100, 9999)}@gmail.com",
        "line1": f"{hn} {road_final}",
        "city": city_final,
        "state": state_final,
        "postal_code": postal_final,
    }


# ---------- promo update (/checkout/update) ----------
def update_promo(session, sid: str, proc: str, mode: str, promo_id: str) -> dict:
    """POST /checkout/update với promo_campaign để hạ amount về 0₫ (nếu account hợp lệ)."""
    body = {"checkout_session_id": sid, "processor_entity": proc,
            "plan_name": "chatgptplusplan", "price_interval": "month", "seat_quantity": 1}
    if mode in ("campaign", "query", "coupon"):
        body["promo_campaign"] = {"promo_campaign_id": promo_id,
                                  "is_coupon_from_query_param": mode == "query"}
    r = session.post(f"{CHATGPT}/backend-api/payments/checkout/update", json=body,
                     headers=chat_h("/backend-api/payments/checkout/update", proc, sid), timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"promo_update_{r.status_code}: {r.text[:200]}")
    try:
        return r.json() or {}
    except Exception:
        return {}


def oaics_one(token: str, proxy: str, country: str, promo: str = "campaign",
              promo_id: str = "plus-1-month-free", step: StepHook = _noop_step,
              on_geo: GeoHook = _noop_geo) -> dict:
    res = {"email": decode_email(token)}
    try:
        s = build_session(token, proxy, step)

        step("checkout", "active")
        d = checkout(s, country, promo, promo_id)
        sid = d.get("checkout_session_id") or ""
        pk = d.get("publishable_key") or ""
        proc = d.get("processor_entity") or "openai_llc"
        res.update({"session": str(sid)[:16], "kind": kind_of(sid)})
        step("checkout", "done", detail=str(sid)[:16], provider=proc)

        # Bước update promo (chỉ áp cho campaign/query/coupon; trial/free_trial đã áp lúc init)
        if promo in ("campaign", "query", "coupon"):
            step("promo", "active")
            upd = update_promo(s, sid, proc, promo, promo_id)
            res.update({"promo": promo, "promo_resp": str(upd)[:120]})
            step("promo", "done", detail=promo)
        else:
            step("promo", "skip", detail=f"promo={promo}")

        step("geo", "active")
        billing = geo_profile(proxy, on_geo)
        step("geo", "done", detail=f"{billing['city']}, {billing['state']}")
        billing["country"] = country
        tb = {"checkout_session_id": sid, "checkout_email": billing["email"],
              "billing_country": country, "billing_name": billing["name"], "currency": "INR",
              "tax_id": None, "processor_entity": proc,
              "billing_address": {"line1": billing["line1"], "city": billing["city"],
                                  "country": country, "postal_code": billing["postal_code"],
                                  "state": billing["state"]}}
        step("taxes", "active")
        rt = s.post(f"{CHATGPT}/backend-api/payments/checkout/taxes", json=tb,
                    headers=chat_h("/backend-api/payments/checkout/taxes", proc, sid), timeout=30)
        if rt.status_code >= 400:
            step("taxes", "fail", err=f"taxes_{rt.status_code}")
            res.update({"status": "taxes_fail", "err": rt.text[:200]})
            return res
        step("taxes", "done", detail=f"HTTP {rt.status_code}")

        pm = {
            "payment_method_data[type]": "upi",
            "payment_method_data[billing_details][name]": billing["name"],
            "payment_method_data[billing_details][email]": billing["email"],
            "payment_method_data[billing_details][address][country]": country,
            "payment_method_data[billing_details][address][line1]": billing["line1"],
            "payment_method_data[billing_details][address][city]": billing["city"],
            "payment_method_data[billing_details][address][postal_code]": billing["postal_code"],
            "payment_method_data[billing_details][address][state]": billing["state"],
            "payment_method_data[payment_user_agent]": "stripe.js/b0f5e7abe5; payment-element; deferred-intent",
            "payment_method_data[referrer]": "https://chatgpt.com",
            "setup_future_usage": "off_session",
            "mandate_data[customer_acceptance][type]": "online",
            "mandate_data[customer_acceptance][online][infer_from_client]": "true",
            "client_context[currency]": "inr",
            "client_context[mode]": "subscription",
            "client_context[payment_method_types][0]": "upi",
            "set_as_default_payment_method": "false",
            "_stripe_version": SV,
            "key": pk,
        }
        step("payment_method", "active")
        ct = requests.post("https://api.stripe.com/v1/confirmation_tokens", data=pm,
                           headers={"User-Agent": UA, "Accept": "application/json"}, timeout=30).json()
        ct_id = ct.get("id") or ""
        if not ct_id:
            step("payment_method", "fail", err=str(ct)[:160])
            res.update({"status": "ct_fail", "err": str(ct)[:200]})
            return res
        step("payment_method", "done", detail=ct_id)

        cbody = {"checkout_session_id": sid, "confirm_token": ct_id,
                 "selected_payment_method_type": "upi"}
        step("confirm", "active")
        c3 = s.post(f"{CHATGPT}/backend-api/payments/checkout/confirm", json=cbody,
                    headers=chat_h("/backend-api/payments/checkout/confirm", proc, sid), timeout=30).json()
        cs = c3.get("client_secret") or ""
        if not cs:
            step("confirm", "fail", err=str(c3)[:160])
            res.update({"status": "confirm_fail", "err": str(c3)[:200]})
            return res
        pi_id = cs.split("_secret_")[0]
        # client_secret có thể là PaymentIntent (pi_) hoặc SetupIntent (seti_ = mandate 0₫).
        intent_kind = "seti" if pi_id.startswith("seti_") else "pi"
        confirm_url = (f"https://api.stripe.com/v1/setup_intents/{pi_id}/confirm"
                       if intent_kind == "seti" else
                       f"https://api.stripe.com/v1/payment_intents/{pi_id}/confirm")
        res.update({"intent": intent_kind, "intent_id": pi_id})
        step("confirm", "done", detail=pi_id)
        if promo != "off" and intent_kind != "seti":
            step("pi_confirm", "skip", detail="intent=pi, no ₹0 promo")
            step("artifact", "skip", detail="skipped")
            res.update({"status": "no_promo",
                        "note": "intent=pi (charge), no ₹0 promo", "amount_minor": None})
            return res
        step("pi_confirm", "active")
        c4 = requests.post(
            confirm_url,
            data={"return_url": f"{CHATGPT}/checkout/verify", "confirmation_token": ct_id,
                  "key": pk, "_stripe_version": SV, "client_secret": cs},
            headers={"User-Agent": UA, "Accept": "application/json"}, timeout=30).json()
        step("artifact", "active")
        na = (c4.get("next_action") or {}) if isinstance(c4, dict) else {}
        inner = na.get("upi_handle_redirect_or_display_qr_code") or {}
        link = inner.get("hosted_instructions_url") or ""
        qr = inner.get("qr_code") or {}
        amount = c4.get("amount") if isinstance(c4, dict) else None
        res.update({"amount_minor": amount})
        if link:
            pi_status = c4.get("status")
            step("pi_confirm", "done", detail=f"status={pi_status}")
            step("artifact", "done", detail="link + QR")
            res.update({"status": "LINK", "upi_link": link,
                        "qr_png": qr.get("image_url_png"), "qr_svg": qr.get("image_url_svg"),
                        "pi_status": pi_status})
        else:
            step("pi_confirm", "fail", err=str(c4.get("status") or "")[:160])
            step("artifact", "fail", err=f"next_action={na.get('type') or 'none'}")
            res.update({"status": "no_link", "next_action_type": na.get("type"),
                        "pi_status": c4.get("status"), "err": str(c4)[:220]})
    except Exception as exc:  # noqa: BLE001
        res.update({"status": "error", "err": str(exc)[:180]})
    return res


# ---------- cs (subprocess -> cs_runner.py) ----------
def make_step_advancer(step: StepHook, order: list[str]) -> tuple[StepHook, dict]:
    """Tra ve `(advance, state)` cho luong cs_; `state["reached"]` = chi so buoc xa nhat.

    Tien do DON DIEU: cac vong retry se lap lai marker cu, nhung buoc da di qua
    thi khong bi keo nguoc ve `active` — neu khong se co 2 buoc cung sang.
    """
    state = {"reached": order.index("checkout") if "checkout" in order else 0,
             "skipped": set()}

    def advance(key: str, completed: bool) -> None:
        if key not in order:
            return
        i = order.index(key)
        # Bước đã bị đánh dấu skip thì không được ghi đè thành active/done — nếu không
        # UI lại hiện ✓ cho bước chưa hề chạy (đúng cái gây nhầm "Apply promotion ✓"
        # trong khi thực tế nó bị bỏ qua).
        if key in state["skipped"]:
            return
        if i < state["reached"]:
            # buoc da di qua roi (marker lap lai o vong sau) -> giu nguyen, khong active lai
            if completed:
                step(key, "done")
            return
        if i > state["reached"]:
            for k in order[state["reached"]:i]:
                if k not in state["skipped"]:
                    step(k, "done")
            state["reached"] = i
        step(key, "done" if completed else "active")

    def mark_skip(key: str, detail: str = "") -> None:
        """Bước bị BỎ QUA (không phải chạy xong) -> UI hiện (Skipped) với icon nét đứt."""
        if key not in order:
            return
        state["skipped"].add(key)
        step(key, "skip", detail=detail)

    return advance, state, mark_skip


def is_risk_decline(lines: list[str]) -> bool:
    """Stripe chặn ở tầng ACCOUNT (risk decline) — account đã bị gắn cờ.

    Lỗi này KHÔNG phải lỗi tạm thời: retry sẽ submit lại chính account đó và ăn
    decline y hệt (đo thật: t1 chạy 2 lần, mỗi lần ~2 phút, cùng generic_decline).
    Nên engine dùng hàm này để bỏ retry.
    """
    for l in lines:
        low = l.lower()
        if "risk decline" in low or "account/customer risk" in low:
            return True
    return False


def classify_cs_outcome(lines: list[str], stage: str, err: str | None,
                        approve_ok: bool, promo: str = "off",
                        ) -> tuple[str, str | None, str, bool]:
    """Từ stdout của extract_cs -> (status, err, link, có_QR_không).

    Tách ra thành hàm thuần để test được offline bằng stdout thật đã lưu, thay vì
    phải chạy cả subprocess 200 giây mới biết phân loại đúng hay sai.

    Ở đây chỉ chặn được cái nhìn thấy từ **log**: link có phải link QR
    (`payments.stripe.com/upi/instructions/...`) hay chỉ là trang checkout hosted.
    Còn "link đó có thật là uỷ nhiệm ₹0 hay không" thì log KHÔNG trả lời được —
    phải hỏi chính trang instructions (`intent_state` + `fam`), việc đó làm ở
    `web/engine.py::_verify_link()` (bước 8 của upi-zero-link).

    Trước đây chỗ này còn một luật xấp xỉ "promo + amount >= ₹100 -> FAIL". Đã bỏ:
    nó đoán qua số tiền nên có thể loại oan uỷ nhiệm ₹0, mà `_verify_link()` giờ
    phán bằng dữ liệu gốc rồi.
    """
    link = extract_result_url(lines)
    has_qr_link = is_qr_link(link)

    blocked = False
    if link and not has_qr_link and not err:
        # Bị chặn ở tầng account thì "chỉ có hosted link" là TRIỆU CHỨNG, không phải
        # nguyên nhân — nêu nguyên nhân trước để đọc là biết ngay.
        if is_risk_decline(lines):
            err = ("Stripe risk decline — account flagged, no mandate link "
                   "(" + link[:80] + "…)")
        else:
            err = ("no QR link extracted — only a hosted checkout link "
                   "(" + link[:90] + "…)")
        blocked = True

    if not err and not link and not approve_ok:
        tail = next((l.strip() for l in reversed(lines) if l.strip()), "")
        if not lines:
            err = "extract_cs produced no output"
        else:
            err = f"no link — flow stopped at step '{stage}'"
            if tail:
                err += f" | last log: {tail[:160]}"
        blocked = True

    if has_qr_link and not blocked:
        st = "LINK"
    elif err:
        st = "FAIL"
    elif approve_ok:
        st = "APPROVE_OK_NO_LINK"
    else:
        st = "UNKNOWN"
    return st, err, link, has_qr_link


def cs_subprocess_one(token: str, proxy_path: Path, promo: str, state_dir: Path, idx: int,
                      step: StepHook = _noop_step, on_log: LogHook = _noop_log,
                      retry_limit: int | None = None,
                      should_stop: Callable[[], bool] | None = None) -> dict:
    """跑一轮 cs_ 流程（子进程）。

    `should_stop` 每轮循环问一次：job 停的时候直接把子进程 kill 掉。
    没有它的话「停止」只是个标记，而这里一个子进程要跑 ~200 秒
    （实测 min 199s / 中位 206s / max 232s），再叠上 retry 就是好几分钟 ——
    用户按了停止却看着它继续跑，就是这个原因。
    """
    sf = state_dir / f"cs_{idx}.json"
    sf.write_text(STATE_TMPL)
    env = dict(os.environ)
    env.update({
        "PYTHONUNBUFFERED": "1",
        "UPI_TOKEN": token,
        "UPI_PROXY_SEED_FILE": str(proxy_path.resolve()),
        "UPI_PROXY_STATE_FILE": str(sf),
        "UPI_PROXY_REMOVE_FAILED": "0",
        "UPI_REQUIRE_ZERO": "0",
        "PP_PROMO_MODE": promo,
        "UPI_MAX_RETRY": "1",
        "UPI_CHECKOUT_RETRY_MAX": "1",
        "UPI_PROVIDER_RETRY_MAX": "1",
        "UPI_APPROVE_RETRY_MAX": "1",
        "UPI_MAX_APPROVE_BLOCKED": "1",
    })
    if retry_limit is not None:
        limit = str(max(1, min(5, retry_limit)))
        env.update({
            "UPI_MAX_RETRY": limit,
            "UPI_CHECKOUT_RETRY_MAX": limit,
            "UPI_PROVIDER_RETRY_MAX": limit,
            "UPI_APPROVE_RETRY_MAX": limit,
            "UPI_MAX_APPROVE_BLOCKED": limit,
        })
    t0 = time.time()
    step("runner", "active")
    try:
        p = subprocess.Popen([sys.executable, str(HERE / "extract_cs.py")],
                             env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             text=True, bufsize=1)
    except OSError as exc:  # noqa: BLE001
        step("runner", "fail", err=str(exc)[:160])
        return {"email": decode_email(token), "status": "ERROR", "err": str(exc)[:180]}

    # doc stdout tren thread rieng de van giu duoc timeout 240s nhu subprocess.run
    q: queue.Queue = queue.Queue()

    def _reader() -> None:
        try:
            assert p.stdout is not None
            for raw in p.stdout:
                q.put(raw.rstrip("\n"))
        finally:
            q.put(None)

    threading.Thread(target=_reader, daemon=True).start()
    step("runner", "done")

    # Tien do theo marker trong stdout cua extract_cs.
    # `reached` = chi so buoc xa nhat da toi -> chi tien, khong lui (cac vong retry
    # se lap lai marker nhung buoc da xong khong bi tut ve pending).
    order = [k for k, _ in STEPS_CS]
    advance, adv, mark_skip = make_step_advancer(step, order)

    # Bước chắc chắn không chạy trong cấu hình này -> đánh dấu skip NGAY, để bộ đánh
    # dấu tiến trình không điền ✓ cho nó khi các bước sau xuất hiện.
    _truthy = ("1", "true", "yes", "on")
    if str(os.environ.get("UPI_UPDATE_TAX_REGION", "")).strip().lower() not in _truthy:
        mark_skip("tax", "disabled (UPI_UPDATE_TAX_REGION=0)")
    if str(os.environ.get("UPI_CONFIRM_INLINE_PM", "")).strip().lower() in _truthy:
        mark_skip("pm", "inline PM at confirm (UPI_CONFIRM_INLINE_PM=1)")

    lines: list[str] = []
    err = ""
    deadline = t0 + 240
    timed_out = False
    stopped = False
    while True:
        if should_stop is not None and should_stop():
            stopped = True
            break
        remaining = deadline - time.time()
        if remaining <= 0:
            timed_out = True
            break
        try:
            line = q.get(timeout=min(1.0, remaining))
        except queue.Empty:
            continue
        if line is None:
            break
        lines.append(line)
        on_log(line)
        if "all failed:" in line and not err:
            err = line.split("all failed: ")[-1][:200]
        low = line.lower()
        if "skipping apply promotion" in low:
            mark_skip("promo", "checkout already ₹0 — skipped checkout/update")
        elif "upi confirm inline details:" in low:
            mark_skip("pm", "inline PM at confirm")
        hit = cs_stage_for_line(line)
        if hit:
            advance(hit[0], hit[1])

    if stopped:
        # 主动停止：kill 子进程并回收，别留僵尸进程
        p.kill()
        try:
            p.wait(timeout=5)
        except Exception:  # noqa: BLE001
            pass
        step(order[adv["reached"]], "fail", err="job stopped")
        return {"email": decode_email(token), "status": "STOPPED",
                "err": "job stopped", "secs": round(time.time() - t0, 1)}

    if timed_out:
        p.kill()
        step(order[adv["reached"]], "fail", err="timeout 240s")
        return {"email": decode_email(token), "status": "TIMEOUT",
                "err": "timeout 240s", "secs": round(time.time() - t0, 1)}
    try:
        p.wait(timeout=10)
    except subprocess.TimeoutExpired:  # pragma: no cover
        p.kill()

    text = "\n".join(lines)
    approve_ok = "approve ok" in text
    st, err, link, has_qr_link = classify_cs_outcome(
        lines, order[adv["reached"]], err, approve_ok, promo)

    # Chot trang thai cuoi
    if approve_ok:
        step("approve", "done")
    elif not has_qr_link:
        # buoc xa nhat da toi ma chua xong -> danh dau that bai
        step(order[adv["reached"]], "fail",
             err=err or ("no output" if not lines else "flow stopped at this step"))
    if has_qr_link:
        step("artifact", "done", detail="link QR (instructions)")
    else:
        step("artifact", "fail", err=err or "no UPI QR link")

    # Chỉ trả upi_link khi task thật sự THÀNH CÔNG. Link hỏng (hosted) hay link của
    # task bị chặn vì promo không áp được đều không được lọt vào artifact: engine
    # hễ thấy upi_link là tạo artifact, và UI sẽ hiện nó như thứ giao được cho khách.
    # Lý do + số tiền đã nằm trong `err` để tra cứu.
    return {"email": decode_email(token), "status": st,
            "risk_decline": is_risk_decline(lines),
            # Account không được hưởng promo 0₫ -> engine không retry (giống risk decline).
            "promo_not_eligible": ("promo_not_eligible" in (err or "")
                                   or "promo_not_eligible" in text),
            "upi_link": (link or None) if st == "LINK" else None,
            # Ảnh QR Stripe trả kèm (qr.stripe.com). Chỉ trả khi task thành công —
            # cùng lý do với upi_link ở trên.
            "qr_png": (extract_marker_url(lines, QR_PNG_MARKER) or None) if st == "LINK" else None,
            "qr_svg": (extract_marker_url(lines, QR_SVG_MARKER) or None) if st == "LINK" else None,
            "approve_ok": approve_ok, "err": err or None,
            "amount_minor": extract_amount_minor(lines),
            "secs": round(time.time() - t0, 1)}


def scan_one(token: str, proxy: str, country: str = "IN",
             step_hook: StepHook = _noop_step, on_log: LogHook = _noop_log) -> dict:
    """Quét kiểm tra xem tài khoản/session có hỗ trợ thanh toán UPI hay không."""
    t0 = time.time()
    step = step_hook
    step("warmup", "running")
    email = decode_email(token)
    device_id = str(uuid.uuid4())
    s = core.build_chatgpt_session(token, device_id, proxy, "")
    cu.warmup_csrf(s, 20)
    sh = cu.sentinel_headers(device_id, proxy, 20)
    if sh:
        s.headers.update(sh)
    step("warmup", "done")

    step("checkout", "running")
    ck_res = s.post(
        f"{CHATGPT}/backend-api/payments/checkout",
        json={"plan_id": "plus", "promotion_code": "", "country": country},
        headers=chat_h("/backend-api/payments/checkout"),
        timeout=25,
    )
    if ck_res.status_code != 200:
        step("checkout", "fail", err=f"HTTP {ck_res.status_code}")
        return {"email": email, "status": "FAIL", "err": f"checkout HTTP {ck_res.status_code}", "secs": round(time.time() - t0, 1)}

    try:
        data = ck_res.json()
    except Exception:
        step("checkout", "fail", err="invalid json")
        return {"email": email, "status": "FAIL", "err": "invalid checkout json", "secs": round(time.time() - t0, 1)}

    step("checkout", "done")
    cs = data.get("checkout_session_id") or data.get("id") or ""
    pk = data.get("stripe_publishable_key") or data.get("public_key") or ""

    if not cs or not pk:
        return {"email": email, "status": "FAIL", "err": "missing cs or pk", "secs": round(time.time() - t0, 1)}

    step("stripe_init", "running")
    body = {
        "browser_locale": f"en-{country}",
        "browser_timezone": "Asia/Kolkata",
        "elements_session_client[client_betas][0]": "custom_checkout_server_updates_1",
        "elements_session_client[client_betas][1]": "custom_checkout_manual_approval_1",
        "elements_session_client[elements_init_source]": "custom_checkout",
        "elements_session_client[referrer_host]": "chatgpt.com",
        "elements_session_client[stripe_js_id]": str(uuid.uuid4()),
        "elements_session_client[locale]": "en",
        "elements_options_client[saved_payment_method][enable_save]": "never",
        "elements_options_client[saved_payment_method][enable_redisplay]": "never",
        "key": pk,
        "_stripe_version": SV,
    }
    px = {"http": proxy, "https": proxy} if proxy else None
    r = requests.post(f"https://api.stripe.com/v1/payment_pages/{cs}/init", data=body,
                      headers={"User-Agent": UA, "Accept": "application/json"}, proxies=px, timeout=30)
    try:
        init_data = r.json()
    except Exception:
        init_data = {}

    step("stripe_init", "done")
    methods = []
    for k in ("payment_method_types", "automatic_payment_method_types", "custom_payment_methods"):
        v = init_data.get(k)
        if isinstance(v, list):
            methods = [str(x).lower() for x in v]
            break
    has_upi = "upi" in methods

    return {
        "email": email,
        "cs": cs,
        "status": "UPI_AVAILABLE" if has_upi else "NO_UPI",
        "has_upi": has_upi,
        "methods": methods,
        "secs": round(time.time() - t0, 1)
    }


# ---------- run ----------
def order_pool_by_quality(proxies: list[str], target_country: str = "IN",
                          workers: int = 16, timeout: int = 20,
                          task_count: int = 0) -> list[str]:
    """Xếp pool theo điểm chất lượng trước khi chia cho worker; bỏ proxy grade F.

    Vì sao: `run()` chia proxy theo vòng tròn `proxies[i % len]`, nên chỉ cần
    trong pool có proxy chết / ra sai nước / IP đã bị gắn cờ là có task dùng đúng
    con đó và fail. Ở đây chấm 1 lượt song song (có cache 12h theo IP) rồi:
      - xếp điểm cao trước, để worker nhận proxy tốt trước khi pool cạn
      - bỏ grade F; nếu bỏ hết thì giữ nguyên pool cũ (không để chết vì lọc)
    Tắt bằng UPI_PROXY_QUALITY=0.
    """
    if not proxies:
        return proxies
    if str(os.environ.get("UPI_PROXY_QUALITY") or "1").strip().lower() in ("0", "false", "no", "off"):
        return proxies
    try:
        import ippure  # noqa: PLC0415
    except Exception:  # noqa: BLE001
        return proxies
    if not ippure.enabled():
        return proxies
    t0 = time.time()
    limit = max(1, min(int(os.environ.get("UPI_PROXY_QUALITY_WORKERS") or 16), 32))
    rows = ippure.scan(proxies, target_country, proxy_persona_timezone(),
                       workers=limit, timeout=timeout, history_of=proxy_history)
    good = [r["proxy"] for r in rows if str(r.get("grade")) in ("A", "B", "C")]
    # Sinh bù khi pool không đủ cho số task (xem proxy_pool.py: session id tự bịa vẫn
    # chạy và mỗi session ra một IP khác -> không bị giới hạn ở số dòng dán tay).
    generated = {}
    short = max(0, int(task_count) - len(good))
    if short and proxies and str(os.environ.get("UPI_PROXY_GENERATE") or "1").strip().lower() not in ("0", "false", "no", "off"):
        try:
            import proxy_pool  # noqa: PLC0415
            extra, gen = proxy_pool.harvest(proxies[0], short, "B", target_country,
                                            proxy_persona_timezone(),
                                            workers=max(4, min(16, short * 2)), timeout=timeout,
                                            max_attempts=int(os.environ.get("UPI_PROXY_GENERATE_MAX") or 0) or short * 6,
                                            sess_time=int(os.environ.get("UPI_PROXY_SESS_TIME") or 30),
                                            log=lambda *a: None)
            if extra:
                good += [r["proxy"] for r in extra]
                generated = {k: gen.get(k) for k in ("verified", "attempts", "unique_ips", "seconds")}
        except Exception as exc:  # noqa: BLE001 — sinh loi thi dung pool cu
            generated = {"error": f"{type(exc).__name__}: {str(exc)[:100]}"}
    best = rows[0] if rows else {}
    out({"type": "proxy_quality", "scanned": len(rows), "usable": len(good),
         "seconds": round(time.time() - t0, 1),
         "best": {"grade": best.get("grade"), "score": best.get("score"),
                  "country": best.get("country"), "risk": best.get("risk"),
                  "user_type": best.get("user_type"), "family": best.get("family")},
         "generated": generated,
         "dropped": [{"label": r.get("label"), "grade": r.get("grade"),
                      "flags": r.get("flags")}
                     for r in rows if str(r.get("grade")) not in ("A", "B", "C")][:10]})
    return good or proxies


def run(mode: str, tokens: list[str], proxy_path: str, workers: int,
        promo: str, country: str, retries: int = 1) -> None:
    setup()
    proxy_file = Path(proxy_path) if proxy_path else (HERE / "proxy.txt")
    proxies = load_proxies(proxy_file)
    n = len(tokens)
    # Xếp hạng (và sinh bù nếu thiếu) TRƯỚC khi chia proxy cho worker — cần biết số
    # task mới biết pool có đủ proxy chất lượng không.
    proxies = order_pool_by_quality(proxies, country, workers=max(4, min(workers * 4, 32)),
                                    task_count=n)
    state_dir = HERE / "cs_state"
    state_dir.mkdir(exist_ok=True)
    seed_file = state_dir / f"cli_seed_{os.getpid()}.txt"
    seed_file.write_text("\n".join(p for p in proxies if p) + "\n", encoding="utf-8")
    print(json.dumps({"mode": mode, "accounts": n, "proxies": len(proxies),
                      "workers": min(workers, n), "country": country}), flush=True)
    results: list[dict] = []

    def work(i: int) -> dict:
        token = tokens[i]
        proxy = proxies[i % len(proxies)]
        if mode == "scan":
            r = scan_one(token, proxy, country)
        elif mode in ("qr", "oaics"):
            r = oaics_one(token, proxy, country, promo)
        elif mode == "cs":
            r = cs_subprocess_one(token, seed_file, promo, state_dir, i, retry_limit=retries)
        else:
            # auto / batch: dò kind rồi chạy đúng logic (cs_ → cs, oaics_ → confirmation_tokens)
            d = detect_one(token, proxy, country)
            if d.get("kind") == "oaics":
                r = oaics_one(token, proxy, country, promo)
            else:
                r = cs_subprocess_one(token, seed_file, promo, state_dir, i, retry_limit=retries)
        results.append(r)
        return r

    with ThreadPoolExecutor(max_workers=min(workers, n)) as ex:
        futs = [ex.submit(work, i) for i in range(n)]
        for f in as_completed(futs):
            res = f.result()
            out(res)

    results.sort(key=lambda x: x.get("email", ""))
    (HERE / "cli_results.json").write_text(
        json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"mode": mode, "done": True}), flush=True)


def main() -> int:
    p = argparse.ArgumentParser(description="UPI Checkout Unified CLI")
    p.add_argument("mode", choices=["oaics", "cs", "auto", "scan", "qr", "batch"],
                   help="Chế độ chạy: oaics / cs / auto / scan / qr / batch")
    p.add_argument("accounts", nargs="?", help="File chứa 1 JWT/dòng")
    p.add_argument("--token", help="1 JWT duy nhất (thay cho accounts)")
    p.add_argument("--proxy", default=None,
                   help="File proxy (mặc định dùng proxy.txt)")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--retries", type=int, default=1,
                   help="Số lần thử lại mỗi acc khi bị risk decline (1-5). Mỗi lần thử\n"
                        "dùng device_id + phiên proxy MỚI, chạy tuần tự.")
    p.add_argument("--promo", default="off")
    p.add_argument("--country", default="IN")
    args = p.parse_args()

    if args.token:
        tokens = [args.token.strip()]
    elif args.accounts and Path(args.accounts).exists():
        tokens = [ln.strip() for ln in Path(args.accounts).read_text().splitlines() if ln.strip()]
    else:
        print(json.dumps({"error": "need --token or existing accounts file"}), flush=True)
        return 2
    if not tokens:
        print(json.dumps({"error": "no tokens"}), flush=True)
        return 2
    run(args.mode, tokens, args.proxy, args.workers, args.promo, args.country,
        retries=max(1, min(5, args.retries)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
