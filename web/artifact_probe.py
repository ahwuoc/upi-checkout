# -*- coding: utf-8 -*-
"""抓 Stripe 的 UPI 指引页，解出金额 / 到期时间 / 链接判据。

为什么必须真的去抓一次：`cs` 流程落盘的 artifact 里 `amount_minor`、`intent`、
`qr_png`、`qr_svg` **全是 null**，日志里也没有 expires 字样 —— 实测
`logs/web_*_results.json` 里 8 个成功 task 的 `artifact.amount_minor` 都是 null。
金额和到期时间只存在于指引页本身：

    <meta id="payload" data-message="<base64url>">

解开就是 Stripe 的意图 JSON，含 `amount` / `expires_at` / `mobile_auth_url`。
再配合 UPI URI 里的两个参数就能判断链接类型：

    am  = 授权上限（amrule=MAX），不是本次扣款额
    fam = 本次扣款额
    fam <  am  ->  ₹0 委托（要的就是这个）
    fam == am  ->  ₹1999 付款链（交付出去用户看到的是付款页）

这段判据和 `upi-zero-link` 的 `verify.py` 是同一套，只是搬成 Web 侧使用。
"""
from __future__ import annotations

import base64
import json
import re
import threading
import time
import urllib.parse

import requests

PAYLOAD_META = re.compile(r"<meta\b[^>]*>", re.I)
ATTR = re.compile(r'([A-Za-z-]+)\s*=\s*["\']([^"\']*)["\']')
FAM_RE = re.compile(r"[?&]fam=([0-9.]+)")
AM_RE = re.compile(r"[?&]am=([0-9.]+)")
UPI_HOST = "payments.stripe.com"
UPI_PATH = "/upi/instructions/"

PASSING_STATES = ("requires_action", "processing")
TIMEOUT = 15.0
_CACHE_TTL = 120.0
_CACHE: dict[str, tuple[float, dict]] = {}
_LOCK = threading.Lock()

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36")


def is_upi_instructions_url(url: str) -> bool:
    try:
        parsed = urllib.parse.urlparse(str(url or "").strip())
    except Exception:  # noqa: BLE001
        return False
    return (parsed.scheme in ("http", "https")
            and (parsed.netloc or "").lower() == UPI_HOST
            and (parsed.path or "").lower().startswith(UPI_PATH))


def decode_payload(html: str) -> dict | None:
    """从 `<meta id="payload" data-message="...">` 解出 JSON。

    逐个 attribute 读，**不依赖属性顺序** —— 老实现用一条正则要求 `id` 必须排在
    `data-message` 前面，Stripe 换个顺序就再也解不出来了。
    """
    for tag in PAYLOAD_META.findall(str(html or "")):
        attrs = {key.lower(): value for key, value in ATTR.findall(tag)}
        if attrs.get("id") != "payload":
            continue
        raw = attrs.get("data-message", "").replace("&quot;", '"')
        if not raw:
            continue
        raw = raw.replace("-", "+").replace("_", "/")
        raw += "=" * ((4 - len(raw) % 4) % 4)
        try:
            payload = json.loads(base64.b64decode(raw).decode("utf-8"))
        except Exception:  # noqa: BLE001
            return None
        return payload if isinstance(payload, dict) else None
    return None


# 监视语义，和 zero-link 的「交付前核验」**不是一回事**：
#   zero-link 问的是「这条链还能不能交给客户」，所以 succeeded 视为「已经被用掉」；
#   这里问的是「池子里的链现在什么状态」，succeeded 恰恰是**最好的结果** —— 客户签成了。
# 别把两套判据混在一起，混了就会把成功链报成死链。
_STATUS_MAP = {
    "requires_action": ("waiting", "Waiting for customer to scan"),
    "requires_confirmation": ("waiting", "Waiting for customer to confirm in their UPI app"),
    "processing": ("waiting", "Scanned — Stripe processing"),
    "succeeded": ("succeeded", "Customer approved the mandate"),
    "requires_payment_method": ("failed", "Failed — Stripe declined"),
    "requires_capture": ("waiting", "Mandate set — awaiting capture"),
    "canceled": ("canceled", "Cancelled"),
    "cancelled": ("canceled", "Cancelled"),
}


def classify(payload: dict, now: float | None = None) -> dict:
    """把指引页 payload 归一成「监视状态」。

    返回 status / status_label / kind / am / fam / expires_at / seconds_left。
    `kind` 说的是链的类型：fam < am 才是 ₹0 委托，fam == am 是 ₹1999 付款链。
    两个维度是独立的 —— 一条链可以「还在等待」且「是 ₹0 委托」。
    """
    now = time.time() if now is None else now
    state = str(payload.get("intent_state") or "").strip()
    uri = str(payload.get("mobile_auth_url") or payload.get("upi_uri") or "")
    fam_match, am_match = FAM_RE.search(uri), AM_RE.search(uri)
    fam = fam_match.group(1) if fam_match else ""
    am = am_match.group(1) if am_match else ""

    kind, kind_label = "unknown", "unknown kind"
    try:
        if fam and am:
            if float(fam) < float(am):
                kind, kind_label = "zero_mandate", "₹0 委托 (fam=%s < am=%s)" % (fam, am)
            else:
                kind, kind_label = "payment_chain", "payment chain (fam=%s = am=%s)" % (fam, am)
    except ValueError:
        pass

    status, label = _STATUS_MAP.get(state, ("unknown", "Unknown (state=%s)" % (state or "?")))

    expires_at = payload.get("expires_at")
    try:
        expires_int = int(expires_at) if expires_at is not None else 0
    except (TypeError, ValueError):
        expires_int = 0
    seconds_left = int(expires_int - now) if expires_int else 0

    # 过期只对「还没走完」的状态有意义；已经成功/已取消的不该被过期覆盖
    if status == "waiting" and expires_int and seconds_left <= 0:
        status, label = "expired", "Expired — never scanned"

    return {
        "status": status,
        "status_label": label,
        "intent_state": state,
        "kind": kind,
        "kind_label": kind_label,
        "am": am,
        "fam": fam,
        "expires_at": expires_int,
        "seconds_left": seconds_left,
    }


# ---------------------------------------------------------------------------
# Bước 8 của `upi-zero-link`: link có THẬT hay không, do chính trang instructions
# trả lời — không phải do "flow có trả về URL hay không".
#
# Vì sao bắt buộc: `hosted_instructions_url` **vẫn được Stripe trả về khi
# setup_intent bị từ chối** (`requires_payment_method`). Chỉ nhìn "có URL" rồi
# giao đi thì khách mở ra thấy trang trả ₹1,999 chứ không phải uỷ nhiệm ₹0.
# Đây là bản port nguyên chuẩn từ upi-zero-link/upi_zero_link/verify.py.
# ---------------------------------------------------------------------------
PASSING_STATES = ("requires_action", "processing")
PAYMENT_CHAIN_FAM = ("1999.00", "1999")


def judge(info: dict) -> tuple[bool, str]:
    """`(có phải uỷ nhiệm ₹0 dùng được, nhãn)` — 2 tiêu chí, phải đạt cả hai.

    `info` là kết quả `classify()`/`probe()` (cần `intent_state` + `fam`).

    | tiêu chí | đạt | vứt |
    |---|---|---|
    | `intent_state` | requires_action / processing | requires_payment_method, canceled, succeeded |
    | `fam` (URI UPI) | ≠ 1999.00 (vd 1.00) | 1999.00 = chuỗi thu tiền |

    `am=1999.00` là **hạn mức** (`amrule=MAX`) nên cả hai loại đều giống nhau —
    không được dùng `am` để phán. `succeeded` = khách đã duyệt rồi (link bị dùng).
    """
    state = str(info.get("intent_state") or "")
    fam = str(info.get("fam") or "")
    label = "state=%s fam=%s" % (state or "?", fam or "?")
    if state not in PASSING_STATES:
        return False, label
    if fam in PAYMENT_CHAIN_FAM:
        return False, label
    return True, label


def is_inconclusive(error: str) -> bool:
    """Lỗi này là "chưa đọc được", KHÔNG phải "link chết" -> đừng đánh fail oan.

    4xx thì ngược lại: đó là kết luận chắc (token hết hạn / link bị huỷ).
    """
    err = str(error or "")
    if err.startswith("http_"):
        return err.startswith("http_5")     # 5xx = server lỗi, 4xx = chết thật
    return True                             # unreachable / no_payload / exception mạng


def probe(url: str, fresh: bool = False) -> dict:
    """抓一次指引页并归一成监视状态。任何失败都不抛，只回 ok=False。

    `fresh=True` 绕过缓存 —— 前端每 20 秒轮询时要拿最新状态，
    不能吃 120 秒的缓存。
    """
    target = str(url or "").strip()
    if not is_upi_instructions_url(target):
        return {"ok": False, "error": "not a UPI instructions URL"}

    now = time.monotonic()
    if not fresh:
        with _LOCK:
            hit = _CACHE.get(target)
        if hit and hit[0] > now:
            return dict(hit[1])

    result: dict = {"ok": False, "error": "unreachable"}
    try:
        response = requests.get(
            target, timeout=TIMEOUT,
            headers={"Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                     "User-Agent": _UA,
                     "Cache-Control": "no-cache"},
        )
        if response.status_code >= 400:
            result = {"ok": False, "error": "http_%s" % response.status_code}
        else:
            payload = decode_payload(response.text)
            if payload is None:
                result = {"ok": False, "error": "no_payload"}
            else:
                info = classify(payload)
                # `return_url` = trang checkout Stripe/OpenAI của đúng session này
                # (dạng cs_live_...#fid..., đo được 501–701 ký tự). Mở ra là trang
                # chọn phương thức; chọn UPI thì hiện QR. Đây là link thứ hai, khác
                # với link `upi/instructions` (mở ra là QR luôn).
                client_secret = str(payload.get("client_secret") or "")
                result = dict(
                    info,
                    ok=True,
                    amount_minor=payload.get("amount"),
                    currency=str(payload.get("currency") or "").upper(),
                    livemode=payload.get("livemode"),
                    upi_uri=str(payload.get("mobile_auth_url") or payload.get("upi_uri") or ""),
                    stripe_link=str(payload.get("return_url") or ""),
                    # seti_ = SetupIntent (uỷ nhiệm) | pi_ = PaymentIntent (thu tiền).
                    # Đây là căn cứ chắc nhất để biết loại link — `am` thì cả hai
                    # loại đều 1999.00 nên không dùng được.
                    intent_kind=("seti" if client_secret.startswith("seti_")
                                 else "pi" if client_secret.startswith("pi_") else ""),
                    checked_at=int(time.time()),
                )
                # Kèm luôn phán đoán để nơi gọi không phải tự suy lại
                good, label = judge(result)
                result["zero_mandate"] = good
                result["zero_label"] = label
    except Exception as exc:  # noqa: BLE001
        result = {"ok": False, "error": type(exc).__name__}

    with _LOCK:
        _CACHE[target] = (now + _CACHE_TTL, dict(result))
        if len(_CACHE) > 400:
            for key in list(_CACHE)[:80]:
                _CACHE.pop(key, None)
    return result
