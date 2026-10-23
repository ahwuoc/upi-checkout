#!/usr/bin/env python3
"""Kiểm tra token TRƯỚC khi chạy — chỉ 1 request GET, không tạo checkout, không chạm Stripe.

Endpoint (lấy từ repo tham khảo `gpt-auto-register-main`, hàm `check_plus`):

    GET https://chatgpt.com/backend-api/accounts/check/v4-2023-04-27?timezone_offset_min=-480

Trả về, trong `accounts.<account_id>`:
  * `account.is_deactivated`           -> acc bị khoá (banned)
  * `account.plan_type` / `entitlement.has_active_subscription` -> đã Plus (không cần chạy)
  * `eligible_promo_campaigns`         -> danh sách campaign được hưởng (vd plus-1-month-free)

Vì sao dùng GET này thay vì "thử tạo checkout để xem promo":
  * 1 request/token, ~1-2 giây, KHÔNG tạo checkout nào (checkout là thứ dễ bị risk-flag).
  * Trong pipeline hiện tại, acc không đủ điều kiện promo vẫn tốn ~5 request & ~35 giây
    (create_checkout + stripe_init) mới biết; acc đã Plus còn tốn ~10 request.
  * Nếu prefilter cũng tạo checkout thì mỗi acc ĐỦ điều kiện sẽ bị tạo checkout 2 lần
    (probe + chạy thật) -> tăng bề mặt risk, lợi bất cập hại.

Dùng từ Python:

    python3 web/prefilter.py --tokens-file tokens.txt --workers 8

hoặc qua API: POST /api/prefilter -> GET /api/prefilter/<id>.
"""
from __future__ import annotations

import argparse
import base64
import json
import secrets
import sys
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

CHECK_URL = "https://chatgpt.com/backend-api/accounts/check/v4-2023-04-27"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36")

# Nhãn hiển thị + ý nghĩa cho từng kết luận.
VERDICT_LABELS = {
    "eligible": "Đủ điều kiện promo",
    "plus_active": "Đã Plus (bỏ qua)",
    "active_sub": "Có gói đang hoạt động (bỏ qua)",
    "banned": "Bị khoá / deactivated",
    "token_invalid": "Token chết (401)",
    "rate_limited": "Bị giới hạn (429)",
    "network_error": "Lỗi mạng/proxy (thử lại)",
    "bad_token": "Token không hợp lệ (sai định dạng)",
    "expired": "Token hết hạn (exp)",
}
DROP_VERDICTS = {"plus_active", "active_sub", "banned", "token_invalid", "bad_token", "expired"}


# --------------------------------------------------------------------- token
def decode_jwt_payload(token: str) -> dict[str, Any]:
    """Đọc payload JWT KHÔNG verify chữ ký — chỉ để lấy email/account_id cho dễ đọc."""
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        data = json.loads(base64.urlsafe_b64decode(payload))
        return data if isinstance(data, dict) else {}
    except (IndexError, ValueError, json.JSONDecodeError):
        return {}


def token_email(token: str) -> str:
    claims = decode_jwt_payload(token)
    profile = claims.get("https://api.openai.com/profile")
    if isinstance(profile, dict) and profile.get("email"):
        return str(profile["email"])
    for key in ("email", "preferred_username", "upn"):
        if claims.get(key):
            return str(claims[key])
    return ""


def token_expiry(token: str) -> float:
    return float(decode_jwt_payload(token).get("exp") or 0)


def fingerprint(token: str) -> str:
    """Nhãn ngắn để đối chiếu token mà không in nguyên token ra log/UI."""
    import hashlib

    return hashlib.sha256(token.encode()).hexdigest()[:10]


def _bad_token_reason(token: str) -> str:
    """Lỗi định dạng/hết hạn — phát hiện được mà KHÔNG cần mạng."""
    token = token.strip()
    if token.count(".") != 2 or len(token) < 60:
        return "bad_token"
    exp = token_expiry(token)
    if exp and exp < time.time():
        return "expired"
    return ""


# ------------------------------------------------------------------- headers
def _headers(token: str) -> dict[str, str]:
    claims = decode_jwt_payload(token)
    auth = claims.get("https://api.openai.com/auth")
    auth = auth if isinstance(auth, dict) else {}
    account_id = str(auth.get("chatgpt_account_id") or auth.get("account_id")
                     or claims.get("chatgpt_account_id") or "").strip()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": UA,
        "Origin": "https://chatgpt.com",
        "Referer": "https://chatgpt.com/",
    }
    if account_id:
        headers["ChatGPT-Account-ID"] = account_id
    return headers


def _extract_campaigns(raw: Any) -> list[str]:
    """`eligible_promo_campaigns` là dict {key: {...}}; lấy id/key để so với promo mong muốn."""
    if not isinstance(raw, dict):
        return []
    out: list[str] = []
    for key, item in raw.items():
        item = item if isinstance(item, dict) else {}
        for value in (item.get("id"), item.get("campaign_id"), item.get("promo_campaign_id"),
                      item.get("key"), key):
            text = str(value or "").strip()
            if text:
                out.append(text)
                break
    return out


def classify_payload(payload: dict[str, Any], promo_id: str) -> tuple[str, str, dict[str, Any]]:
    """Trả (verdict, detail, extra) từ body của accounts/check.

    Tách riêng để test được mà không cần mạng.
    """
    accounts = payload.get("accounts") if isinstance(payload, dict) else None
    if not isinstance(accounts, dict) or not accounts:
        return "network_error", "no account data", {}
    info = next(iter(accounts.values()))
    info = info if isinstance(info, dict) else {}
    acct = info.get("account") if isinstance(info.get("account"), dict) else {}
    ent = info.get("entitlement") if isinstance(info.get("entitlement"), dict) else {}

    if acct.get("is_deactivated"):
        return "banned", "account is_deactivated", {}
    plan = str(acct.get("plan_type") or "").strip().lower()
    has_sub = bool(ent.get("has_active_subscription"))
    if plan == "plus":
        return "plus_active", "plan_type=plus", {"plan_type": plan}
    if has_sub:
        return "active_sub", "has_active_subscription", {"plan_type": plan or "sub"}

    campaigns = _extract_campaigns(info.get("eligible_promo_campaigns"))
    extra = {"plan_type": plan or "free", "campaigns": campaigns}
    if not campaigns:
        return "not_eligible", "không có campaign nào", extra
    if promo_id and not any(promo_id in c for c in campaigns):
        return "not_eligible", f"campaign khác: {', '.join(campaigns[:3])}", extra
    return "eligible", campaigns[0] if campaigns else promo_id, extra


# ------------------------------------------------------------------- probing
@dataclass
class ProbeResult:
    fingerprint: str
    email: str
    verdict: str
    detail: str = ""
    ms: int = 0
    extra: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "fingerprint": self.fingerprint,
            "email": self.email,
            "verdict": self.verdict,
            "label": VERDICT_LABELS.get(self.verdict, self.verdict),
            "detail": self.detail,
            "ms": self.ms,
            "dropped": self.verdict in DROP_VERDICTS,
            **self.extra,
        }


def probe_token(token: str, proxy: str = "", promo_id: str = "plus-1-month-free",
                timeout: int = 20) -> ProbeResult:
    """1 token -> 1 GET. Không tạo checkout, không ghi gì ra đĩa."""
    token = token.strip()
    mark = fingerprint(token)
    email = token_email(token)
    reason = _bad_token_reason(token)
    if reason:
        return ProbeResult(mark, email, reason, VERDICT_LABELS.get(reason, reason))

    started = time.time()
    try:
        import requests  # noqa: PLC0415 — chỉ cần khi thật sự gọi mạng
        proxies = {"http": proxy, "https": proxy} if proxy else None
        resp = requests.get(
            f"{CHECK_URL}?timezone_offset_min=-480",
            headers=_headers(token),
            proxies=proxies,
            timeout=timeout,
        )
    except Exception as exc:  # noqa: BLE001 — mạng là ngoại lệ bình thường ở đây
        return ProbeResult(mark, email, "network_error", f"{type(exc).__name__}: {exc}"[:120],
                           int((time.time() - started) * 1000))

    ms = int((time.time() - started) * 1000)
    if resp.status_code == 401:
        return ProbeResult(mark, email, "token_invalid", "HTTP 401", ms)
    if resp.status_code == 429:
        return ProbeResult(mark, email, "rate_limited", "HTTP 429", ms)
    if resp.status_code != 200:
        return ProbeResult(mark, email, "network_error", f"HTTP {resp.status_code}", ms)
    try:
        payload = resp.json()
    except ValueError:
        return ProbeResult(mark, email, "network_error", "invalid JSON", ms)
    verdict, detail, extra = classify_payload(payload, promo_id)
    return ProbeResult(mark, email, verdict, detail, ms, extra)


def pick_proxy(proxy_pool: list[str], index: int) -> str:
    pool = [p for p in proxy_pool if p]
    return pool[index % len(pool)] if pool else ""


# ------------------------------------------------------------- job manager
class _Run:
    def __init__(self, total: int, promo_id: str) -> None:
        self.id = secrets.token_hex(6)
        self.total = total
        self.promo_id = promo_id
        self.started_at = time.time()
        self.finished_at = 0.0
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.results: list[dict[str, Any]] = []
        self.state = "running"

    def add(self, result: dict[str, Any]) -> None:
        with self.lock:
            self.results.append(result)

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            results = list(self.results)
        counts: dict[str, int] = {}
        for row in results:
            counts[row["verdict"]] = counts.get(row["verdict"], 0) + 1
        kept = [r for r in results if r["verdict"] not in DROP_VERDICTS]
        return {
            "id": self.id,
            "state": self.state,
            "total": self.total,
            "checked": len(results),
            "kept": len(kept),
            "dropped": len(results) - len(kept),
            "counts": counts,
            "elapsed_ms": int((time.time() - (self.finished_at or time.time())) * 1000)
            if self.state != "running" else int((time.time() - self.started_at) * 1000),
            "results": results,
        }


_RUNS: dict[str, _Run] = {}
_RUNS_LOCK = threading.Lock()


def start(tokens: Iterable[str], proxy_pool: list[str] | None = None, workers: int = 8,
          promo_id: str = "plus-1-month-free") -> str:
    """Chạy prefilter nền; trả về id để poll `status(id)`."""
    tokens = [t.strip() for t in tokens if t and t.strip()]
    proxy_pool = proxy_pool or []
    run = _Run(len(tokens), promo_id)
    with _RUNS_LOCK:
        _RUNS[run.id] = run

    def worker() -> None:
        from concurrent.futures import ThreadPoolExecutor

        def one(index_token: tuple[int, str]) -> None:
            index, token = index_token
            if run.stop.is_set():
                return
            outcome = probe_token(token, pick_proxy(proxy_pool, index), promo_id)
            if run.stop.is_set():
                return
            row = outcome.as_dict()
            row["index"] = index   # để client map ngược ra token gốc theo vị trí
            run.add(row)

        try:
            with ThreadPoolExecutor(max_workers=max(1, min(workers, 32))) as pool:
                list(pool.map(one, enumerate(tokens)))
        finally:
            run.state = "stopped" if run.stop.is_set() else "done"
            run.finished_at = time.time()

    threading.Thread(target=worker, name=f"prefilter-{run.id}", daemon=True).start()
    return run.id


def status(run_id: str) -> dict[str, Any] | None:
    with _RUNS_LOCK:
        run = _RUNS.get(run_id)
    return run.snapshot() if run else None


def stop(run_id: str) -> bool:
    with _RUNS_LOCK:
        run = _RUNS.get(run_id)
    if not run or run.state != "running":
        return False
    run.stop.set()
    return True


def eligible_tokens(run_id: str, tokens: Iterable[str]) -> list[str]:
    """Lấy lại đúng token gốc của những acc giữ lại (results chỉ lưu fingerprint)."""
    snap = status(run_id) or {}
    keep = {r["fingerprint"] for r in snap.get("results", [])
            if r["verdict"] not in DROP_VERDICTS}
    return [t for t in tokens if fingerprint(t.strip()) in keep]


def prune_stale(max_age_sec: int = 3600) -> None:
    """Dọn run cũ để không giữ kết quả mãi trong RAM."""
    now = time.time()
    with _RUNS_LOCK:
        for run_id, run in list(_RUNS.items()):
            if run.finished_at and now - run.finished_at > max_age_sec:
                _RUNS.pop(run_id, None)


def main() -> int:
    ap = argparse.ArgumentParser(description="Kiểm tra token trước khi chạy (1 GET/token)")
    ap.add_argument("--tokens-file", required=True)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--promo", default="plus-1-month-free")
    ap.add_argument("--proxy-file", default="")
    args = ap.parse_args()

    tokens = [ln.strip() for ln in Path(args.tokens_file).read_text().splitlines() if ln.strip()]
    pool: list[str] = []
    if args.proxy_file:
        pool = [ln.strip() for ln in Path(args.proxy_file).read_text().splitlines() if ln.strip()]

    run_id = start(tokens, pool, args.workers, args.promo)
    while True:
        snap = status(run_id) or {}
        print(f"\r{snap.get('checked', 0)}/{snap.get('total', 0)} "
              f"giữ {snap.get('kept', 0)} · loại {snap.get('dropped', 0)}", end="", flush=True)
        if snap.get("state") != "running":
            break
        time.sleep(0.5)
    print()
    for name, count in sorted((snap.get("counts") or {}).items(), key=lambda kv: -kv[1]):
        print(f"  {name:<14} {count:>6}  {VERDICT_LABELS.get(name, '')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
