#!/usr/bin/env python3
"""Safely probe ChatGPT trial eligibility and Stripe UPI availability.

The probe creates one unconfirmed Checkout Session and, when needed, reads its
Stripe init payload. It never confirms payment, creates a PaymentMethod, or
prints account credentials, email addresses, Checkout Session IDs, or proxies.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import upi_core as core


ROOT = Path(__file__).resolve().parent
DEFAULT_PROXY_FILE = ROOT / "proxy_seeds_in_bestgo.txt"
CHECKOUT_URL = "https://chatgpt.com/backend-api/payments/checkout"
SENTINEL_RUNNER = ROOT / "sentinel_runner.js"
SENTINEL_SDK_URL = "https://chatgpt.com/backend-api/sentinel/sdk.js"
SENTINEL_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)

# Hồ sơ browser cho Sentinel: (screen_width, screen_height, cores, memory, weight).
#
# Vì sao phải khác nhau theo device: token `p` mà SDK sinh ra có chứa
# screen.width+height, hardwareConcurrency, timezone (xem sentinel_runner.js).
# Trước đây mọi task gửi đúng một bộ số (screen mặc định 2400x1080 -> tổng 3480,
# cores 8) nên hàng trăm account khác nhau lại khai cùng một "máy" — chỉ cần nhìn
# trường đó là thấy một farm. Ở đây mỗi device_id bốc một hồ sơ cố định: trong
# cùng task (2–5 lần gọi sentinel) vẫn là một máy, khác task là khác máy.
#
# Trọng số = thị phần THẬT của 6 độ phân giải desktop phổ biến nhất ở Ấn Độ
# (StatCounter "Desktop Screen Resolution Stats — India", 9/2026), đã chuẩn hoá
# về 100%. Lấy bảng Ấn Độ vì persona của flow này là IN (proxy IN + en-IN +
# Asia/Kolkata). 1920x1080 được tách theo số core vì cả laptop 8 core lẫn desktop
# 12–16 core đều dùng độ phân giải này; cores là số *logical processor* vì Chrome
# báo hardwareConcurrency theo logical.
#
# KHÔNG random UA: UA phải khớp TLS impersonate chrome146 + sec-ch-ua của mọi
# request khác trong flow, lệch là mâu thuẫn nặng hơn cả việc trùng fingerprint.
_DEVICE_PROFILES = (
    (1920, 1080, 8, 8, 8.40),
    (1920, 1080, 12, 8, 6.30),
    (1920, 1080, 16, 8, 4.02),
    (1536, 864, 8, 8, 14.46),
    (1366, 768, 4, 4, 16.45),
    (1280, 720, 4, 4, 6.79),
    (1440, 900, 8, 8, 3.83),
    (1600, 900, 8, 8, 3.58),
)
_DEVICE_PROFILE_TOTAL = sum(row[4] for row in _DEVICE_PROFILES)


def sentinel_fingerprint(device_id: str) -> dict[str, Any]:
    """Persona browser ổn định theo device_id (cùng device = cùng máy, khác device = khác máy).

    timezone/locale lấy từ UPI_BROWSER_TIMEZONE / UPI_BROWSER_LOCALE (giống
    extract_cs.payment_browser_timezone) để persona sentinel không lệch với
    phần Stripe/checkout còn lại.
    """
    digest = hashlib.sha256(f"sentinel-fp|{device_id}".encode("utf-8")).digest()
    point = int.from_bytes(digest[:8], "big") / float(1 << 64) * _DEVICE_PROFILE_TOTAL
    width, height, cores, memory = _DEVICE_PROFILES[-1][:4]
    cumulative = 0.0
    for row_width, row_height, row_cores, row_memory, weight in _DEVICE_PROFILES:
        cumulative += weight
        if point < cumulative:
            width, height, cores, memory = row_width, row_height, row_cores, row_memory
            break
    locale = str(os.environ.get("UPI_BROWSER_LOCALE") or "en-IN").strip() or "en-IN"
    timezone = str(os.environ.get("UPI_BROWSER_TIMEZONE") or "Asia/Kolkata").strip() or "Asia/Kolkata"
    languages = [locale]
    for tag in ("en-US", locale.split("-")[0], "en"):
        if tag and tag not in languages:
            languages.append(tag)
    return {
        "user_agent": SENTINEL_UA,
        "language": locale,
        "languages": languages,
        "platform": "Win32",
        "timezone": timezone,
        "screen_width": width,
        "screen_height": height,
        "hardware_concurrency": cores,
        "device_memory": memory,
    }


# Cache sentinel token theo (device_id, proxy) TRONG CÙNG tiến trình.
#
# Vì sao: đo thật — sentinel mất **1,1s khi không qua proxy** nhưng **17,6s khi qua
# proxy residential** (độ trễ proxy, không phải CPU). Một task gọi
# `build_chatgpt_session` 2–5 lần (checkout, promotion, tax, snapshot, mỗi round retry)
# và MỖI lần đều gọi sentinel -> phí 17–70s/task, đúng phần khiến task của mình
# ~100–200s trong khi flow tham khảo chỉ 34s.
#
# device_id là duy nhất cho từng task, và cache sống trong 1 tiến trình con
# (extract_cs chạy mới mỗi task) -> không có chuyện dùng token của task khác.
# Tắt bằng UPI_SENTINEL_REUSE=0 nếu nghi token chỉ dùng được 1 lần.
_SENTINEL_CACHE: dict[str, tuple[float, dict[str, str]]] = {}
# Cap an toàn cho cache khi runner KHÔNG trả được expires_at. Đo thật: server
# /sentinel/req trả expire_after = 540s, nên 600s cũ là ĐỦ để dùng token quá hạn ~60s.
# Luôn ưu tiên expires_at từ runner (min(expires_at, now + _SENTINEL_TTL)).
_SENTINEL_TTL = float(os.environ.get("UPI_SENTINEL_TTL") or 480)
_SENTINEL_REUSE = str(os.environ.get("UPI_SENTINEL_REUSE", "1")).strip().lower() not in (
    "0", "false", "no", "off")


_BOOTSTRAP_VERSION_CACHE = ROOT / ".cache" / "sentinel-bootstrap-version-py.json"


def _resolve_sdk_version_cffi(proxy: str, timeout: int) -> str:
    """Đọc version SDK từ bootstrap bằng curl_cffi (Chrome TLS/h2), cache 6h.

    Vì sao: trước đây node tự GET /backend-api/sentinel/sdk.js bằng node https
    (h1.1) ở lần chạy đầu mỗi 6h — cũng là một request browser thấy được. Chuyển
    sang curl_cffi cho đồng bộ TLS. Trả "" nếu thất bại -> node tự lo (fallback).
    """
    try:
        if _BOOTSTRAP_VERSION_CACHE.exists():
            raw = json.loads(_BOOTSTRAP_VERSION_CACHE.read_text(encoding="utf-8"))
            if isinstance(raw, dict) and raw.get("version") and \
                    time.time() - float(raw.get("ts") or 0) < 6 * 3600:
                return str(raw["version"])
    except Exception:  # noqa: BLE001
        pass
    try:
        from curl_cffi.requests import Session as CffiSession
        session = CffiSession(impersonate="chrome146")
        if proxy:
            session.proxies = {"http": proxy, "https": proxy}
        for url in ("https://chatgpt.com/backend-api/sentinel/sdk.js",
                    "https://sentinel.openai.com/backend-api/sentinel/sdk.js"):
            try:
                resp = session.get(url, headers={"Accept": "*/*", "User-Agent": SENTINEL_UA}, timeout=timeout)
                m = re.search(r"/sentinel/([0-9a-z]+)/sdk\.js", resp.text or "")
                if m and resp.status_code == 200:
                    version = m.group(1)
                    try:
                        _BOOTSTRAP_VERSION_CACHE.parent.mkdir(parents=True, exist_ok=True)
                        _BOOTSTRAP_VERSION_CACHE.write_text(
                            json.dumps({"version": version, "ts": time.time()}), encoding="utf-8")
                    except Exception:  # noqa: BLE001
                        pass
                    return version
            except Exception:  # noqa: BLE001
                continue
    except Exception:  # noqa: BLE001
        pass
    return ""


_CHROME146_CH = (
    "\"Chromium\";v=\"146\", \"Not.A/Brand\";v=\"24\", \"Google Chrome\";v=\"146\""
)


def _run_node(request: dict, timeout: int) -> dict:
    """Chạy sentinel_runner.js 1 lần, trả dict (rỗng nếu node lỗi/timeout)."""
    node = shutil.which("node") or "node"
    try:
        completed = subprocess.run(
            [node, str(SENTINEL_RUNNER)],
            input=json.dumps(request, separators=(",", ":")),
            text=True,
            capture_output=True,
            timeout=max(45, timeout + 10),
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {}
    try:
        payload = json.loads(completed.stdout or "{}")
    except (ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _fetch_challenge_cffi(request_p: str, device_id: str, sdk_version: str,
                          proxy: str, fp: dict, timeout: int,
                          cookies: str | None = None) -> dict | None:
    """POST /sentinel/req bằng curl_cffi impersonate chrome146.

    Vì sao: request này trong browser thật đi bằng TLS của Chrome (JA3) + HTTP/2 +
    client hint; node `https` chỉ làm được h1.1 + JA3 của Node. curl_cffi với
    `impersonate=chrome146` sao lại đúng bộ TLS/h2 của Chrome. Trả None để caller
    rơi về đường node tự fetch (không bao giờ vì vậy mà mất token).
    """
    try:
        from curl_cffi.requests import Session as CffiSession
    except Exception:  # noqa: BLE001 — thiếu curl_cffi thì rơi về node
        return None
    try:
        session = CffiSession(impersonate="chrome146")
        if proxy:
            session.proxies = {"http": proxy, "https": proxy}
        lang = fp.get("languages") or [fp.get("language") or "en-IN"]
        headers = {
            "Accept": "*/*",
            "Accept-Language": ",".join(lang),
            "Content-Type": "text/plain;charset=UTF-8",
            "Origin": "https://chatgpt.com",
            "Referer": f"https://chatgpt.com/backend-api/sentinel/frame.html?sv={sdk_version}",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": fp.get("user_agent") or SENTINEL_UA,
            "sec-ch-ua": _CHROME146_CH,
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-ch-ua-full-version-list":
                "\"Chromium\";v=\"146.0.0.0\", \"Not.A/Brand\";v=\"24\", \"Google Chrome\";v=\"146.0.0.0\"",
            "sec-ch-ua-platform-version": "\"10.0.0\"",
            "sec-ch-ua-arch": "\"x86\"",
            "sec-ch-ua-bitness": "\"64\"",
            "Cookie": cookies or f"oai-did={device_id}",
        }
        resp = session.post(
            "https://chatgpt.com/backend-api/sentinel/req",
            data=json.dumps({"p": request_p, "id": device_id, "flow": "chatgpt_checkout"}),
            headers=headers,
            timeout=timeout,
        )
        if resp.status_code != 200:
            return None
        return resp.json() if isinstance(resp.json(), dict) else None
    except Exception:  # noqa: BLE001
        return None


def sentinel_headers(device_id: str, proxy: str, timeout: int = 45,
                      alt_proxies: tuple[str, ...] = (),
                      cookies: str | None = None) -> dict[str, str]:
    """Lấy OpenAI-Sentinel-Token, request /req đi bằng curl_cffi (giống Chrome thật).

    Chia 2 pha để pha HTTP giữa chừng đi bằng TLS/h2 của Chrome thay vì Node:
      1. node chạy SDK -> `request_p` (offline, không gọi mạng /req).
      2. Python (curl_cffi chrome146) POST /req lấy challenge — có JA3 + h2 + CH.
      3. node giải proof (turnstile + PoW) từ challenge -> token + so_token.
    Nếu curl_cffi thiếu/lỗi thì rơi về đường node tự fetch (hành vi cũ), nên không
    bao giờ mất token vì lý do này.

    Retry 3 lần có backoff; có `alt_proxies` thì lần 2/3 đổi proxy. Cache theo hạn
    THẬT `expires_at` do server trả (~540s), không dùng token quá hạn như bản cũ.
    """
    candidates = (proxy,) + tuple(p for p in alt_proxies if p and p != proxy)
    cache_key = f"{device_id}|{proxy}"
    if _SENTINEL_REUSE:
        hit = _SENTINEL_CACHE.get(cache_key)
        if hit and hit[1] and time.time() < hit[0]:
            return dict(hit[1])

    fp = sentinel_fingerprint(device_id)
    resolved_version = _resolve_sdk_version_cffi(proxy, timeout)
    base = {
        "flow": "chatgpt_checkout",
        "persona": "chatgpt",
        "device_id": device_id,
        "session_id": device_id,
        "sentinel_sdk_url": SENTINEL_SDK_URL,
        "timeout_ms": min(120.0, max(10.0, timeout * 1000)),
        "fingerprint": fp,
    }
    # Python đã resolve version bằng curl_cffi -> node không phải GET bootstrap nữa.
    if resolved_version:
        base["sentinel_sdk_version"] = resolved_version
    for attempt in range(3):
        candidate = candidates[min(attempt, len(candidates) - 1)]
        # Pha 1: request_p (offline qua SDK)
        req = {**base, "proxy": candidate or "", "action": "requirements_only"}
        ph1 = _run_node(req, timeout)
        request_p = str(ph1.get("request_p") or "")
        sdk_version = str(ph1.get("sdk_version") or ph1.get("diagnostics", {}).get("sdk_version") or "")
        if not request_p:
            if attempt + 1 < 3:
                time.sleep(0.5 + attempt * 1.0)
            continue

        # Pha 2: lấy challenge bằng curl_cffi (Chrome TLS/h2). Fallback node nếu thất bại.
        challenge = _fetch_challenge_cffi(request_p, device_id, sdk_version, candidate, fp, timeout,
                                         cookies=cookies)
        if challenge is None:
            payload = _run_node({**base, "proxy": candidate or ""}, timeout)
        else:
            # Pha 3: node giải proof từ challenge Python vừa lấy
            payload = _run_node({**base, "proxy": candidate or "", "action": "solve",
                                 "request_p": request_p, "challenge": json.dumps(challenge)}, timeout)

        token = str(payload.get("token") or "")
        so_token = str(payload.get("so_token") or "")
        if token:
            headers = {"OpenAI-Sentinel-Token": token, "OAI-Telemetry": "[1,null]"}
            if so_token:
                headers["OpenAI-Sentinel-SO-Token"] = so_token
            if _SENTINEL_REUSE:
                expires_at = float(payload.get("expires_at") or 0) or (time.time() + _SENTINEL_TTL)
                expires_at = min(expires_at, time.time() + _SENTINEL_TTL)
                _SENTINEL_CACHE[cache_key] = (expires_at, dict(headers))
            return headers
        if attempt + 1 < 3:
            time.sleep(0.5 + attempt * 1.0)
    return {}


def warmup_csrf(session: Any, timeout: int) -> None:
    """Bước warmup giống gopay-core: lấy CSRF token trước checkout."""
    try:
        session.get(
            "https://chatgpt.com/api/auth/csrf",
            headers={
                "Accept": "application/json",
                "Accept-Language": "en-IN,en;q=0.9",
                "Referer": "https://chatgpt.com/",
                "User-Agent": SENTINEL_UA,
            },
            timeout=timeout,
        )
    except Exception:
        pass

DECISION_TEXT = {
    "ready": "real trial supported and current session supports UPI",
    "account_trial_ineligible": "account has no real trial eligibility",
    "trial_not_applied": "30-day trial not applied by OpenAI backend",
    "upi_not_enabled": "trial applied but current session has no UPI",
    "already_paid": "account already subscribed; cannot test new-subscription eligibility via this flow",
    "credential_invalid": "credentials invalid or expired",
    "credential_parse_failed": "cannot parse credentials from file",
    "checkout_failed": "checkout creation failed; result uncertain",
    "stripe_init_failed": "checkout created but Stripe init failed",
    "payment_methods_unknown": "Stripe init returned no explicit payment-method list",
    "unexpected_mode": "Stripe session is not subscription mode",
    "credential_ready": "credential format valid; not yet checked online",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Detect a ChatGPT account's real trial eligibility and Stripe UPI support.",
    )
    parser.add_argument(
        "token_files",
        nargs="+",
        type=Path,
        metavar="TOKEN_FILE",
        help="Text or JSON file containing the accessToken; multiple allowed.",
    )
    parser.add_argument(
        "--proxy-file",
        type=Path,
        default=DEFAULT_PROXY_FILE,
        help=f"India proxy list (default: {DEFAULT_PROXY_FILE}).",
    )
    parser.add_argument(
        "--direct",
        action="store_true",
        help="Connect directly without a proxy file.",
    )
    parser.add_argument(
        "--pre-proxy",
        default="auto",
        help=(
            "Upstream SOCKS/HTTP proxy; default auto uses 127.0.0.1:7897 when available."
            "Pass off to disable."
        ),
    )
    parser.add_argument(
        "--trial-days",
        type=int,
        default=30,
        help="Trial days to request (default: 30).",
    )
    parser.add_argument(
        "--max-proxies",
        type=int,
        default=1,
        help="Max proxy switches when an explicit Cloudflare block is hit (default: 1, no retry).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="Per-request timeout seconds (default: 20).",
    )
    parser.add_argument(
        "--parse-only",
        action="store_true",
        help="Only validate credential format/expiry; send no network requests.",
    )
    parser.add_argument(
        "--check-methods-anyway",
        action="store_true",
        help="Even when trial eligibility is false, call Stripe init once to check payment methods.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output one JSON line per account, for programmatic consumption.",
    )
    args = parser.parse_args()
    if args.trial_days < 1:
        parser.error("--trial-days must be > 0")
    if args.max_proxies < 1:
        parser.error("--max-proxies must be > 0")
    if args.timeout < 1:
        parser.error("--timeout must be > 0")
    return args


def account_label(index: int) -> str:
    if 0 <= index < 26:
        return chr(ord("A") + index)
    return f"#{index + 1}"


def jwt_expiry(access_token: str) -> tuple[bool | None, float | None]:
    parts = access_token.split(".")
    if len(parts) != 3:
        return None, None
    try:
        payload_raw = base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4))
        payload = json.loads(payload_raw)
        exp = payload.get("exp")
        if exp is None:
            return None, None
        ttl_minutes = round((float(exp) - time.time()) / 60, 1)
        return ttl_minutes <= 0, ttl_minutes
    except (ValueError, TypeError, json.JSONDecodeError):
        return None, None


def parse_credential_text(text: str) -> tuple[str, str, dict[str, Any]]:
    """Parse one credential blob (raw JWT or JSON object) without touching disk."""
    stripped = (text or "").strip()
    candidates = [stripped]
    if stripped.startswith(("{", "[")) and stripped.endswith(","):
        candidates.insert(0, stripped[:-1].rstrip())

    access_token = ""
    session_token = ""
    for candidate in candidates:
        parsed_access, parsed_session = core.normalize_token(candidate)
        # normalize_token returns the entire input when malformed JSON starts
        # with a brace. Never send such a blob as a Bearer token.
        if parsed_access and not parsed_access.lstrip().startswith(("{", "[")):
            access_token, session_token = parsed_access, parsed_session
            break
    if not access_token:
        return "", "", {
            "credential_valid": False,
            "decision": "credential_parse_failed",
        }
    expired, _ttl_minutes = jwt_expiry(access_token)
    return access_token, session_token, {
        "credential_valid": expired is not True,
        "credential_expired": expired,
        "decision": "credential_invalid" if expired is True else "credential_ready",
    }


def load_credentials(path: Path) -> list[tuple[str, str, dict[str, Any]]]:
    """Load one or more credentials from a file.

    A plain token or a single JSON object yields one credential. A JSON array
    (e.g. tokens.json) yields one credential per element, keeping the order.
    """
    try:
        raw = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return [("", "", {"credential_valid": False, "decision": "credential_parse_failed"})]
    stripped = raw.strip()
    if stripped.startswith("["):
        try:
            items = json.loads(stripped)
        except json.JSONDecodeError:
            items = None
        if isinstance(items, list):
            results: list[tuple[str, str, dict[str, Any]]] = []
            for item in items:
                if isinstance(item, dict):
                    text = json.dumps(item, ensure_ascii=False)
                else:
                    text = str(item)
                results.append(parse_credential_text(text))
            return results
    return [parse_credential_text(raw)]


def local_port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.25):
            return True
    except OSError:
        return False


def configure_pre_proxy(value: str) -> None:
    normalized = value.strip()
    if normalized.lower() == "auto":
        normalized = "socks5h://127.0.0.1:7897" if local_port_open("127.0.0.1", 7897) else ""
    elif normalized.lower() in {"", "off", "none", "false", "0"}:
        normalized = ""
    if normalized:
        os.environ["IDEAL_PRE_PROXY"] = normalized
        os.environ["PP_PRE_PROXY"] = normalized
    else:
        os.environ.pop("IDEAL_PRE_PROXY", None)
        os.environ.pop("PP_PRE_PROXY", None)


def load_proxies(path: Path, direct: bool) -> list[str]:
    if direct:
        return [""]
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise RuntimeError("cannot read proxy file") from exc
    proxies = [
        core.normalize_proxy_url(line.strip())
        for line in lines
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if not proxies:
        raise RuntimeError("no usable entries in proxy file")
    return proxies


def proxy_for_vietnam(proxy: str) -> str:
    return proxy  # IN: dùng nguyên trạng, không ép country selector


def classify_checkout_error(response: Any) -> str:
    if core.is_user_already_paid_error(response.text):
        return "already_paid"
    if core.is_cloudflare_response(response):
        return "cloudflare"
    if response.status_code == 401:
        return "credential_invalid"
    if response.status_code == 429:
        return "rate_limited"
    return f"http_{response.status_code}"


def checkout_body(trial_days: int) -> dict[str, Any]:
    return {
        "entry_point": "all_plans_pricing_modal",
        "plan_name": "chatgptplusplan",
        "price_interval": "month",
        "seat_quantity": 1,
        "billing_details": {"country": "IN", "currency": "INR"},
        "checkout_ui_mode": "custom",
        "subscription_data": {"trial_period_days": trial_days},
    }


def create_checkout(
    access_token: str,
    session_token: str,
    proxies: list[str],
    start_index: int,
    max_proxies: int,
    trial_days: int,
    timeout: int,
) -> tuple[dict[str, Any] | None, str, str, str, int, int]:
    headers = {
        "Referer": "https://chatgpt.com/",
        "x-openai-target-path": "/backend-api/payments/checkout",
        "x-openai-target-route": "/backend-api/payments/checkout",
    }
    last_error = "no_attempt"
    attempts = 0
    for offset in range(min(max_proxies, len(proxies))):
        proxy_index = (start_index + offset) % len(proxies)
        proxy = proxy_for_vietnam(proxies[proxy_index])
        attempts += 1
        device_id = str(uuid.uuid4())
        try:
            session = core.build_chatgpt_session(
                access_token,
                device_id,
                proxy,
                session_token,
            )
            warmup_csrf(session, timeout)
            sentinel = sentinel_headers(device_id, proxy, timeout)
            if not sentinel.get("OpenAI-Sentinel-Token"):
                last_error = "sentinel_token_missing"
                continue
            base_headers = {
                "Referer": "https://chatgpt.com/",
                "x-openai-target-path": "/backend-api/payments/checkout",
                "x-openai-target-route": "/backend-api/payments/checkout",
            }
            base_headers.update(sentinel)
            response = session.post(
                CHECKOUT_URL,
                json=checkout_body(trial_days),
                headers=base_headers,
                timeout=timeout,
            )
            if response.status_code >= 400:
                last_error = classify_checkout_error(response)
                if last_error in {"already_paid", "credential_invalid"}:
                    return None, "", "", last_error, attempts, proxy_index + 1
                if last_error == "cloudflare" and offset + 1 < min(max_proxies, len(proxies)):
                    continue
                return None, "", "", last_error, attempts, proxy_index + 1
            data = response.json() or {}
            checkout_id = data.get("checkout_session_id") or data.get("session_id") or data.get("id")
            if not checkout_id or not (
                str(checkout_id).startswith("cs_") or str(checkout_id).startswith("oaics_")
            ):
                last_error = "checkout_missing_session"
                continue
            raw_key = (
                data.get("stripe_publishable_key")
                or data.get("publishable_key")
                or data.get("publishableKey")
                or data.get("stripePublishableKey")
                or data.get("key")
                or ""
            )
            key_match = re.search(r"pk_live_[A-Za-z0-9]+", str(raw_key))
            stripe_key = key_match.group(0) if key_match else core.DEFAULT_STRIPE_PK
            return data, str(checkout_id), stripe_key, proxy, attempts, proxy_index + 1
        except Exception as exc:  # A timeout might have created a Session; don't retry it.
            last_error = f"network_{type(exc).__name__}"
            return None, "", "", last_error, attempts, proxy_index + 1
    return None, "", "", last_error, attempts, start_index


def stripe_init(
    checkout_id: str,
    stripe_key: str,
    selected_proxy: str,
) -> tuple[dict[str, Any] | None, str, int]:
    try:
        return core.stripe_init(checkout_id, stripe_key, selected_proxy), "ok", 1
    except Exception as exc:  # Deliberately do not print response bodies.
        return None, f"network_or_init_{type(exc).__name__}", 1


def extract_methods(payload: dict[str, Any]) -> tuple[list[str] | None, str | None]:
    methods = payload.get("payment_method_types")
    source = "top_level"
    if not isinstance(methods, list):
        elements = payload.get("elements_options")
        methods = elements.get("payment_method_types") if isinstance(elements, dict) else None
        source = "elements_options"
    if not isinstance(methods, list):
        return None, None
    return sorted({str(method).lower() for method in methods}), source


def stripe_field(payload: dict[str, Any], key: str) -> Any:
    elements = payload.get("elements_options")
    if isinstance(elements, dict) and key in elements:
        return elements[key]
    return payload.get(key)


def amount_due(payload: dict[str, Any]) -> int | None:
    summary = payload.get("total_summary")
    if isinstance(summary, dict) and summary.get("due") is not None:
        return int(summary.get("due") or 0)
    if payload.get("amount_total") is not None:
        return int(payload.get("amount_total") or 0)
    amount = stripe_field(payload, "amount")
    if amount is not None:
        return int(amount or 0)
    invoice = payload.get("invoice")
    if isinstance(invoice, dict) and invoice.get("amount_due") is not None:
        return int(invoice.get("amount_due") or 0)
    return None


def trial_marker(payload: dict[str, Any], nested_key: str | None = None) -> tuple[bool, Any, bool]:
    candidates = [payload]
    if nested_key and isinstance(payload.get(nested_key), dict):
        candidates.append(payload[nested_key])
    trial_days = None
    trial_end = None
    for candidate in candidates:
        subscription_data = candidate.get("subscription_data")
        if isinstance(subscription_data, dict):
            trial_days = subscription_data.get("trial_period_days")
            trial_end = subscription_data.get("trial_end")
        if trial_days in (None, "", 0, "0", False):
            trial_days = candidate.get("trial_period_days")
        if trial_end in (None, "", 0, "0", False):
            trial_end = candidate.get("trial_end")
        if trial_days not in (None, "", 0, "0", False) or trial_end not in (
            None,
            "",
            0,
            "0",
            False,
        ):
            break
    try:
        has_days = int(trial_days or 0) > 0
    except (TypeError, ValueError):
        has_days = False
    has_end = trial_end not in (None, "", 0, "0", False)
    return has_days or has_end, trial_days, has_end


def has_actual_trial_in_response(payload: dict[str, Any]) -> bool:
    """Detect an applied trial without treating mere eligibility as success."""
    has_trial, _, _ = trial_marker(payload, "checkout_session")
    return has_trial


def choose_decision(
    one_click_eligible: Any,
    actual_trial: bool,
    stripe_mode: Any,
    has_upi: bool | None,
) -> str:
    if not actual_trial and one_click_eligible is False:
        return "account_trial_ineligible"
    if not actual_trial:
        return "trial_not_applied"
    if stripe_mode != "subscription":
        return "unexpected_mode"
    if has_upi is None:
        return "payment_methods_unknown"
    return "ready" if has_upi else "upi_not_enabled"


def probe_account(
    label: str,
    access_token: str,
    session_token: str,
    credential: dict[str, Any],
    proxies: list[str],
    start_index: int,
    args: argparse.Namespace,
) -> tuple[dict[str, Any], int]:
    result: dict[str, Any] = {"account": label, **credential}
    if not access_token or credential.get("credential_expired") is True:
        result["conclusive"] = True
        result["supported"] = False
        return result, start_index
    if args.parse_only:
        result["conclusive"] = True
        result["supported"] = None
        return result, start_index

    data, checkout_id, stripe_key, proxy, attempts, next_index = create_checkout(
        access_token,
        session_token,
        proxies,
        start_index,
        args.max_proxies,
        args.trial_days,
        args.timeout,
    )
    result["checkout_proxy_attempts"] = attempts
    if data is None:
        failure = proxy
        result["checkout_status"] = failure
        if failure == "already_paid":
            result["credential_valid"] = True
            result["decision"] = "already_paid"
        elif failure == "credential_invalid":
            result["credential_valid"] = False
            result["decision"] = "credential_invalid"
        else:
            result["decision"] = "checkout_failed"
        result["conclusive"] = failure == "credential_invalid"
        result["supported"] = False if result["conclusive"] else None
        return result, next_index

    one_click_eligible = data.get("one_click_trial_eligible")
    is_new_customer = data.get("is_new_stripe_customer")
    result.update(
        {
            "credential_valid": True,
            "checkout_status": "created",
            "one_click_trial_eligible": one_click_eligible,
            "is_new_stripe_customer": is_new_customer,
            "trial_in_openai_response": has_actual_trial_in_response(data),
        }
    )

    if (
        one_click_eligible is False
        and not result["trial_in_openai_response"]
        and not args.check_methods_anyway
    ):
        result.update(
            {
                "stripe_init_status": "skipped_not_trial_eligible",
                "actual_trial": False,
                "decision": "account_trial_ineligible",
                "decision_text": DECISION_TEXT["account_trial_ineligible"],
                "conclusive": True,
                "supported": False,
            }
        )
        return result, next_index

    # Methods có thể nằm ngay trong checkout response (flow mới oaics_ — không cần Stripe init)
    methods, methods_source = None, None
    init_status = "in_checkout_response"
    init_attempts = 0
    init_has_trial = False
    trial_days = None
    has_trial_end = False
    if data.get("payment_method_types") or data.get("custom_payment_methods") or data.get("automatic_payment_method_types"):
        methods, methods_source = extract_methods(data)
        init_has_trial, trial_days, has_trial_end = trial_marker(data)
    else:
        init_payload, init_status, init_attempts = stripe_init(
            checkout_id,
            stripe_key,
            proxy,
        )
        result["stripe_init_status"] = init_status
        result["stripe_init_attempts"] = init_attempts
        if init_payload is None:
            result["decision"] = "stripe_init_failed"
            result["conclusive"] = False
            result["supported"] = None
            return result, next_index
        methods, methods_source = extract_methods(init_payload)
        init_has_trial, trial_days, has_trial_end = trial_marker(init_payload, "elements_options")
    result["stripe_init_status"] = init_status
    result["stripe_init_attempts"] = init_attempts
    actual_trial = bool(result["trial_in_openai_response"] or init_has_trial)
    has_upi = None if methods is None else "upi" in methods
    checkout_src = data if methods is not None else (init_payload if "init_payload" in dir() else {})
    stripe_mode = stripe_field(checkout_src, "mode")
    decision = choose_decision(one_click_eligible, actual_trial, stripe_mode, has_upi)
    conclusive = decision != "payment_methods_unknown"
    result.update(
        {
            "stripe_mode": stripe_mode,
            "payment_method_collection": stripe_field(checkout_src, "payment_method_collection"),
            "amount_due": amount_due(checkout_src),
            "currency": stripe_field(checkout_src, "currency"),
            "methods": methods,
            "methods_source": methods_source,
            "has_upi": has_upi,
            "trial_period_days_in_init": trial_days,
            "trial_end_present_in_init": has_trial_end,
            "actual_trial": actual_trial,
            "decision": decision,
            "decision_text": DECISION_TEXT[decision],
            "conclusive": conclusive,
            "supported": decision == "ready",
        }
    )
    return result, next_index


def print_result(result: dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
        return
    label = result["account"]
    decision = result.get("decision", "checkout_failed")
    decision_text = result.get("decision_text") or DECISION_TEXT.get(decision, decision)
    if "methods" not in result:
        print(f"[{label}] {decision_text}")
        return
    eligible = result.get("one_click_trial_eligible")
    eligible_text = "yes" if eligible is True else "no" if eligible is False else "unknown"
    trial_text = "yes" if result.get("actual_trial") else "no"
    upi_value = result.get("has_upi")
    upi_text = "yes" if upi_value is True else "no" if upi_value is False else "unknown"
    methods = ",".join(result.get("methods") or []) or "none"
    print(
        f"[{label}] trial_eligible={eligible_text} | real_trial={trial_text} | "
        f"mode={result.get('stripe_mode') or 'unknown'} | methods={methods} | "
        f"UPI={upi_text} | result={decision_text}"
    )


def main() -> int:
    args = parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    configure_pre_proxy(args.pre_proxy)
    core.COUNTRY_CURRENCY["IN"] = "INR"
    core.CHATGPT_TIMEOUT = args.timeout
    core.DEFAULT_TIMEOUT = args.timeout

    # Prevent the imported extractor from writing request dumps or verbose logs.
    core.dump_http = lambda *unused_args, **unused_kwargs: None
    core.log = lambda *unused_args, **unused_kwargs: None

    try:
        proxies = [""] if args.parse_only else load_proxies(args.proxy_file, args.direct)
    except RuntimeError as exc:
        print(f"check could not start: {exc}", file=sys.stderr)
        return 2

    results: list[dict[str, Any]] = []
    next_proxy_index = 0
    label_index = 0
    for token_file in args.token_files:
        for access_token, session_token, credential in load_credentials(token_file):
            label = account_label(label_index)
            label_index += 1
            result, next_proxy_index = probe_account(
                label,
                access_token,
                session_token,
                credential,
                proxies,
                next_proxy_index,
                args,
            )
            results.append(result)
            print_result(result, args.json)

    return 2 if any(result.get("conclusive") is False for result in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
