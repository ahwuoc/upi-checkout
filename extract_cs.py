"""
\tUPI payment-instruction link extractor.

Notes:
- On success Stripe returns a payment-instruction link for UPI.
- This script only prints the final `payments.stripe.com/upi/instructions/` link or its upstream redirect.
- By default it reads the project's proxy_seeds.txt.

Usage:
  1) Env var:
     UPI_TOKEN="..." python upi_extract.py

  2) File:
     Put the token in token.txt in the current dir
     python upi_extract.py

Common env vars:
  UPI_CONFIRM_INLINE_PM=0   # default gpthel flow: create PM first, then confirm by PM ref
  UPI_UPDATE_TAX_REGION=1   # IN UPI flow: sync ChatGPT/Stripe tax region
  UPI_BOOTSTRAP_COUNTRY=IN  # Checkout / first Stripe init region
  UPI_PROMOTION_COUNTRY=IN  # checkout/update region (phai trung billing country)
  UPI_PROVIDER_COUNTRY=IN   # Stripe refresh / tax / PM / approve region
  UPI_MAX_RETRY=2
  UPI_PROVIDER_PER_CHECKOUT=1
  UPI_MAX_APPROVE_BLOCKED=2
  UPI_WORKERS=1
  UPI_WORKERS_MAX=1
  UPI_APPROVE_RETRY_MAX=10 # approve stage reuses the current provider proxy for retries
  UPI_APPROVE_STICKY=1     # approve prefers history/current egress, then rotates to next provider egress
  UPI_FOLLOW_REDIRECT=1
  UPI_REQUIRE_ZERO=1        # default: enforce 0-value; do not generate non-0 links
  UPI_DUMP_LIMIT=6000       # capture response save length
  UPI_PROXY_SKIP_FAILED=1   # soft-skip normally-failed proxy next time
  UPI_PROXY_REMOVE_FAILED=1 # explicit proxy failures get removed from proxy_seeds.txt
  UPI_PROXY_DEFAULT_SCHEME=http # bare proxy scheme; Mars SOCKS5 can use socks5h
  UPI_PROXY_FAIL_COOLDOWN=180 # failed-proxy cooldown seconds; 0 = keep old always-skip logic
  UPI_PROXY_REMOVE_AFTER_FAILS=3 # reused-proxy health failures removed after 3; normal proxy removed after 1
  UPI_ZERO_CACHE=1          # record 0-value observations per checkout, for logs/diagnostics
  UPI_ZERO_CACHE_SCHEDULING=0 # set to 1 to filter/schedule by 0-value observations
  PP_PROMO_MODE=campaign      # default: go straight to promo_campaign, avoid coupon->fallback overhead
  PP_TRIAL_DAYS=30            # only used when PP_PROMO_MODE=trial/free_trial
"""

from __future__ import annotations

import json
import base64
import hashlib
import os
import random
import re
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from pathlib import Path
from threading import Event, RLock, local
from typing import Any, Callable, Mapping
from urllib.parse import parse_qsl, quote, urlencode, unquote, urljoin, urlparse, urlsplit, urlunsplit

import requests

try:
    from curl_cffi import CurlOpt
    from curl_cffi.requests import Session as CurlCffiSession
except ImportError:
    CurlOpt = None
    CurlCffiSession = None


SCRIPT_DIR = Path(__file__).resolve().parent
LOG_DIR = SCRIPT_DIR / "logs"
DUMP_DIR = SCRIPT_DIR / "dumps"
LOG_DIR.mkdir(parents=True, exist_ok=True)
DUMP_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_TIMEOUT = 30
CHATGPT_TIMEOUT = 45
UPI_UNAVAILABLE_ERROR = "The current account's payment method does not support UPI"

# The standalone CLI reads the environment. The web backend supplies immutable
# per-call overrides so concurrent accounts never share token/config changes.
_run_context = local()


def env_value(name: str, default: Any = None) -> Any:
    context = getattr(_run_context, "value", None)
    if context is not None and name in context["settings"]:
        return context["settings"][name]
    return os.environ.get(name, default)


class FlowCancelled(BaseException):
    """Cancellation must escape protocol retry handlers catching Exception."""


class FlowTimedOut(BaseException):
    """Task deadline elapsed; do not retry the interrupted HTTP operation."""


def check_cancelled() -> None:
    context = getattr(_run_context, "value", None)
    if context is None:
        return
    if context["should_stop"] is not None and context["should_stop"]():
        raise FlowCancelled("job stopped")
    if context["deadline"] is not None and time.monotonic() >= context["deadline"]:
        raise FlowTimedOut("task deadline exceeded")


@contextmanager
def execution_context(settings: Mapping[str, str], *,
                      on_output: Callable[[str], None] | None = None,
                      on_result: Callable[[dict[str, Any]], None] | None = None,
                      should_stop: Callable[[], bool] | None = None,
                      timeout: float | None = 240.0):
    """Isolate one embedded execution without changing os.environ/stdout."""
    previous = getattr(_run_context, "value", None)
    context = {
        "settings": dict(settings), "on_output": on_output,
        "on_result": on_result, "should_stop": should_stop,
        "deadline": time.monotonic() + timeout if timeout and timeout > 0 else None,
        "proxy_state": None, "redaction_values": set(), "sessions": [],
    }
    _run_context.value = context
    try:
        check_cancelled()
        yield
    finally:
        for session in context["sessions"]:
            try:
                session.close()
            except Exception:
                pass
        _run_context.value = previous


def emit_result(**values: Any) -> None:
    context = getattr(_run_context, "value", None)
    callback = context["on_result"] if context is not None else None
    if callback is not None:
        callback(values)


def emit_output(text: str = "", *, flush: bool = False) -> None:
    context = getattr(_run_context, "value", None)
    callback = context["on_output"] if context is not None else None
    if callback is None:
        print(text, flush=flush)
        return
    for line in str(text).split("\n"):
        callback(_redact_text(line))


def stripe_heavy_timeout() -> int:
    """Timeout cho các request Stripe lớn (confirm/poll/intent-get) — cho phép
    override qua env UPI_STRIPE_HEAVY_TIMEOUT (mặc định 90) vì proxy xoay vòng
    (bestgo/novproxy) thường phản hồi chậm với payload lớn; 30s dễ bị curl kill
    giữa chừng, làm mất response generic_decline cần xác minh."""
    return env_int("UPI_STRIPE_HEAVY_TIMEOUT", 90)


def full_dump_stages() -> set[str]:
    """Các stage dump cần giữ nguyên response ĐẦY ĐỦ (không cắt UPI_DUMP_LIMIT).
    Mặc định là 'all' để lưu trọn vẹn toàn bộ response HTTP."""
    raw = str(env_value("UPI_DUMP_FULL_STAGES", "all") or "").strip()
    if raw.lower() == "all":
        return {"all"}
    stages: set[str] = set()
    for part in raw.split(","):
        part = part.strip()
        if part:
            stages.add(part)
    return stages

STRIPE_VERSION_FULL = (
    "2025-03-31.basil; checkout_server_update_beta=v1; "
    "checkout_manual_approval_preview=v1"
)
DEFAULT_STRIPE_RUNTIME_VERSION = "6f8494a281"
CHATGPT_CLIENT_VERSION = "prod-db390ebea64862bf1899c420a4c736e0cf639747"
CHATGPT_CLIENT_BUILD_NUMBER = "7904904"
DEFAULT_STRIPE_PK = str(env_value("STRIPE_PUBLISHABLE_KEY", "") or "")
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)

# Client Hints cho Chrome 146 (khớp DEFAULT_USER_AGENT + impersonate chrome146).
# Trước đây build_chatgpt_session hardcode sec-ch-ua = Safari 17 trên macOS trong
# khi UA + TLS impersonate đều là Chrome/146 Windows -> Cloudflare/Risk Engine thấy
# mâu thuẫn (đúng bệnh đã đo ở gpt-auto-register: Chrome UA mà sai/thiếu CH -> 403).
CHROME_SEC_CH_UA = '"Chromium";v="146", "Not.A/Brand";v="24", "Google Chrome";v="146"'
CHROME_SEC_CH_UA_FULL = (
    '"Chromium";v="146.0.0.0", "Not.A/Brand";v="24", "Google Chrome";v="146.0.0.0"'
)
CHROME_CLIENT_HINTS = {
    "sec-ch-ua": CHROME_SEC_CH_UA,
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-ch-ua-full-version-list": CHROME_SEC_CH_UA_FULL,
    "sec-ch-ua-platform-version": '"10.0.0"',
    "sec-ch-ua-arch": '"x86"',
    "sec-ch-ua-bitness": '"64"',
}
def configured_country(name: str, default: str) -> str:
    value = str(env_value(name, default) or default).strip().upper()
    if not re.fullmatch(r"[A-Z]{2}", value):
        raise RuntimeError(f"{name} must be a two-letter country code")
    return value


def configured_countries(name: str, default: str) -> list[str]:
    value = str(env_value(name, default) or default)
    countries = [part.strip().upper() for part in value.split(",")]
    if not countries or any(not re.fullmatch(r"[A-Z]{2}", country) for country in countries):
        raise RuntimeError(f"{name} must be a comma-separated list of two-letter country codes")
    return countries


UPI_BOOTSTRAP_COUNTRY = configured_country(
    "UPI_BOOTSTRAP_COUNTRY", env_value("UPI_CHECKOUT_COUNTRY", "IN")
)
UPI_PROMOTION_COUNTRIES = configured_countries("UPI_PROMOTION_COUNTRY", "IN")
UPI_PROMOTION_COUNTRY = UPI_PROMOTION_COUNTRIES[0]
UPI_PROVIDER_COUNTRY = configured_country(
    "UPI_PROVIDER_COUNTRY", env_value("UPI_BILLING_COUNTRY", "IN")
)

COUNTRY_CURRENCY = {
    "BR": "BRL",
    "NL": "EUR",
    "BE": "EUR",
    "DE": "EUR",
    "FR": "EUR",
    "US": "USD",
    "IN": "INR",
    "JP": "JPY",
    "VN": "VND",
}

FIRST_NAMES_IN = ["Aarav", "Aisha", "Ananya", "Arjun", "Dev", "Diya", "Ihaan", "Ishaan", "Kavya", "Neha", "Priya", "Rahul", "Rohan", "Sanya", "Shaurya", "Tanvi", "Vikram", "Vivaan", "Yash", "Zoya"]
LAST_NAMES_IN = ["Sharma", "Patel", "Singh", "Kumar", "Gupta", "Nair", "Verma", "Rao", "Joshi", "Mehta", "Bhasin", "Deshmukh", "Chopra", "Chaudhary", "Reddy", "Iyer", "Pillai"]
CITIES_IN = [
    {"city": "Bengaluru", "state": "KA", "postal_code": "560001", "streets": ["MG Road", "Brigade Road", "Indiranagar 100ft Rd", "Koramangala 80ft Rd"]},
    {"city": "Mumbai", "state": "MH", "postal_code": "400001", "streets": ["Marine Drive", "Linking Road", "Hill Road", "Colaba Causeway"]},
    {"city": "Kolkata", "state": "WB", "postal_code": "700016", "streets": ["Park Street", "Camac Street", "Shakespeare Sarani", "AJC Bose Road"]},
    {"city": "New Delhi", "state": "DL", "postal_code": "110001", "streets": ["Connaught Place", "Barakhamba Road", "Janpath", "Ring Road"]},
    {"city": "Hyderabad", "state": "TG", "postal_code": "500001", "streets": ["Banjara Hills Rd 1", "Jubilee Hills Rd 36", "Abids Road"]},
    {"city": "Chennai", "state": "TN", "postal_code": "600001", "streets": ["Anna Salai", "Mount Road", "Usman Road"]},
]


def generate_dynamic_in_billing() -> dict[str, Any]:
    """Tự động sinh ngẫu nhiên thông tin billing Ấn Độ hợp lệ cho mỗi giao dịch."""
    first = random.choice(FIRST_NAMES_IN)
    last = random.choice(LAST_NAMES_IN)
    city_info = random.choice(CITIES_IN)
    street = random.choice(city_info["streets"])
    num = random.randint(1, 199)
    domain = random.choice(["gmail.com", "outlook.com", "icloud.com", "hotmail.com"])
    email_prefix = f"{first.lower()}{last.lower()}{random.randint(100, 9999)}"
    return {
        "email": f"{email_prefix}@{domain}",
        "name": f"{first} {last}",
        "country": "IN",
        "line1": f"{num} {street}",
        "line2": "",
        "city": city_info["city"],
        "postal_code": city_info["postal_code"],
        "state": city_info["state"],
    }


DEFAULT_UPI_BILLING = generate_dynamic_in_billing()


EMAIL_DOMAINS = ("gmail.com", "outlook.com", "icloud.com", "hotmail.com")

# ---------------------------------------------------------------------------
# Billing suy từ IP EXIT (không dùng list cứng).
#
# Vấn đề của `generate_dynamic_in_billing()`: nó random trong 6 thành phố cứng
# (Bengaluru/Mumbai/Kolkata/New Delhi/Hyderabad/Chennai) nên **địa chỉ chẳng liên
# quan gì tới IP exit**. Đo trên 1.103 lượt điền billing: 632/644 ca (98%) exit
# city KHÁC billing city, chỉ có 6 mã bưu chính cho toàn bộ account, và 6 thành
# phố đó phân bố gần đều (194/186/186/185/183/169) — đúng dấu hiệu random.choice.
# Với hệ thống chống gian lận thì "khách ở Patna nhưng billing Chennai, postal
# 600001 dùng đi dùng lại" là mẫu hình rất dễ nhận ra.
#
# Ở đây làm giống `cli.geo_profile()` (đang dùng cho luồng oaics): hỏi ipwho.is
# QUA CHÍNH PROXY đang dùng, rồi reverse-geocode toạ độ bằng Nominatim -> city /
# state / postal / đường đều khớp nhau VÀ khớp IP mà Stripe nhìn thấy.
GEO_BILLING_TIMEOUT = float(env_value("UPI_GEO_BILLING_TIMEOUT", "10") or 10)

# Mã bang theo ISO 3166-2:IN. Đây là dạng mà luồng này ĐÃ chạy thành công: 3 ca uỷ
# nhiệm ₹0 ở job e0a538cbabcb điền TN/TN/DL, và CITIES_IN cũ cũng dùng mã 2 ký tự.
# Tên đầy đủ ("Kerala") là dạng CHƯA từng chạy nên không đổi tại đây.
STATE_CODE_BY_NAME = {
    "andhra pradesh": "AP", "arunachal pradesh": "AR", "assam": "AS", "bihar": "BR",
    "chhattisgarh": "CG", "goa": "GA", "gujarat": "GJ", "haryana": "HR",
    "himachal pradesh": "HP", "jharkhand": "JH", "karnataka": "KA", "kerala": "KL",
    "madhya pradesh": "MP", "maharashtra": "MH", "manipur": "MN", "meghalaya": "ML",
    "mizoram": "MZ", "nagaland": "NL", "odisha": "OD", "punjab": "PB",
    "rajasthan": "RJ", "sikkim": "SK", "tamil nadu": "TN", "telangana": "TG",
    "tripura": "TR", "uttar pradesh": "UP", "uttarakhand": "UT", "west bengal": "WB",
    "delhi": "DL", "new delhi": "DL", "jammu and kashmir": "JK", "ladakh": "LA",
    "puducherry": "PY", "chandigarh": "CH", "andaman and nicobar islands": "AN",
    "dadra and nagar haveli and daman and diu": "DN", "lakshadweep": "LD",
}


def state_code_in(name: str, iso: str = "") -> str:
    """Tên bang / `IN-XX` -> mã 2 ký tự. Rỗng nếu không map được.

    Ưu tiên `ISO3166-2-lvl4` của Nominatim (vd `IN-KL`) vì đó là dữ liệu gốc;
    bảng tên->mã chỉ là đường lui khi Nominatim không kèm field ISO.
    """
    code = str(iso or "").strip().upper()
    if len(code) == 5 and code.startswith("IN-"):
        return code[3:]
    text = re.sub(r"\s+", " ", str(name or "")).strip().lower().replace("&", "and")
    return STATE_CODE_BY_NAME.get(text, "")


def proxy_url_for_requests(proxy: str) -> str:
    """Chuẩn hoá proxy về `scheme://user:pass@host:port` để `requests` hiểu được.

    `normalize_proxy_url()` trong file này chỉ thêm scheme rồi trả về, nên proxy
    dạng 4 phần `host:port:user:pass` thành `http://host:port:user:pass` —
    urlsplit coi cả cụm là netloc (không có '@') -> username/password = None, và
    mọi thứ dùng sau đó (`proxy_for_country`, `requests`) đều hỏng. Seed do
    web/engine.py ghi đã ở dạng URL nên app chạy được, còn CLI đọc thẳng proxy.txt
    thì không.
    """
    raw = str(proxy or "").strip()
    if not raw:
        return ""
    body = raw.split("://", 1)[1] if "://" in raw else raw
    if "@" not in body and body.count(":") == 3:
        host, port, user, pw = body.split(":")
        return f"{default_proxy_scheme()}://{quote(user)}:{quote(pw)}@{host}:{port}"
    return raw if "://" in raw else f"{default_proxy_scheme()}://{raw}"


def geo_billing_via_module(proxy: str) -> dict[str, Any]:
    """Mức 2: dùng `geobilling.py` (port từ upi-zero-link) — PIN là GỐC.

    Khác mức 1 ở chỗ PIN không phải "postcode mà Nominatim/ipwho tình cờ trả về" mà
    được **xác thực với API bưu chính Ấn Độ** (state/district lấy TỪ PIN), nên city
    / state / postal_code không thể mâu thuẫn nhau. Thêm cache theo exit nên task
    sau gần như không tốn thời gian.

    Trả `{}` để nơi gọi rơi xuống mức 1 / list cứng khi không dựng được địa chỉ.
    """
    try:
        # geobilling đọc UPI_STATE_DIR lúc import -> phải set TRƯỚC khi import, và
        # để riêng khỏi repo upi-zero-link (bản gốc mặc định ~/.upi-zero-link).
        os.environ.setdefault("UPI_STATE_DIR", str(SCRIPT_DIR / "cs_state"))
        import geobilling  # noqa: PLC0415 — import muộn: chỉ nạp khi thật sự dùng
    except Exception as exc:  # noqa: BLE001
        log(f"geobilling failed to load: {str(exc)[:90]}", "[WARN] ")
        return {}

    try:
        addr = geobilling.address_for_proxy(
            proxy_url_for_requests(proxy), country="IN",
            log=lambda m: log(str(m).strip()))
    except Exception as exc:  # noqa: BLE001
        log(f"geobilling error: {str(exc)[:90]}", "[WARN] ")
        return {}
    if not addr:
        return {}

    # `state` của geobilling là cách viết chuẩn cho Stripe = TÊN đầy đủ ("Kerala").
    # Luồng này đã chứng minh chạy được với MÃ 2 ký tự (3 ca uỷ nhiệm ₹0 dùng
    # TN/TN/DL), nên quy về mã; không map được thì bỏ để nơi gọi fallback.
    code = state_code_in(addr.get("state") or "", "")
    if not code:
        log("geobilling returned state %r with no matching state code -> falling back"
            % (addr.get("state") or "?"), "[WARN] ")
        return {}

    first = random.choice(FIRST_NAMES_IN)
    last = random.choice(LAST_NAMES_IN)
    domain = random.choice(EMAIL_DOMAINS)
    return {
        "email": f"{first.lower()}{last.lower()}{random.randint(100, 9999)}@{domain}",
        "name": addr.get("name") or f"{first} {last}",
        "country": "IN",
        "line1": addr.get("line1") or "",
        "line2": addr.get("line2") or "",
        "city": addr.get("city") or "",
        "postal_code": addr.get("postal_code") or "",
        "state": code,
        "_ip": addr.get("exit_ip") or "",
        "_src": addr.get("source") or "geobilling",
    }


def geo_billing_from_proxy(proxy: str) -> dict[str, Any]:
    """Sinh billing theo IP exit thật. Trả `{}` nếu không dựng được (nơi gọi fallback).

    Thứ tự: mức 2 (`geobilling`, PIN xác thực) -> mức 1 (ipwho + Nominatim) -> `{}`
    để nơi gọi dùng list cứng.

    Không raise: mạng/ipwho/Nominatim chết thì luồng phải chạy tiếp bằng list cứng,
    chứ không được làm hỏng cả task.
    """
    if not proxy:
        return {}

    advanced = geo_billing_via_module(proxy)
    if advanced:
        log("billing tier 2 (PIN verified) %s / %s / %s %s · source %s"
            % (advanced.get("_ip") or "?", advanced["city"], advanced["state"],
               advanced["postal_code"], advanced.get("_src") or "?"))
        return advanced

    # Địa chỉ phải khớp IP của chặng sẽ gửi PM/tax/approve, tức chặng provider.
    base = proxy_url_for_requests(proxy)
    try:
        target = proxy_for_country(base, UPI_PROVIDER_COUNTRY)
    except Exception:  # noqa: BLE001 — proxy không có selector country thì dùng nguyên bản
        target = base
    proxies = {"http": target, "https": target}
    headers = {"User-Agent": DEFAULT_USER_AGENT}

    info = requests.get("https://ipwho.is/", headers=headers, proxies=proxies,
                        timeout=GEO_BILLING_TIMEOUT).json()
    if not isinstance(info, dict) or not info.get("success"):
        return {}
    if str(info.get("country_code") or "").upper() != "IN":
        return {}                       # chỉ nhận exit Ấn Độ, y như geo_profile()
    city = str(info.get("city") or "").strip()
    state_name = str(info.get("region") or "").strip()
    postal = str(info.get("postal") or "").strip()
    lat, lon = info.get("latitude"), info.get("longitude")

    road = ""
    state_iso = ""
    try:
        rev = requests.get(
            f"https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=jsonv2",
            headers={**headers, "Accept": "application/json"},
            proxies=proxies, timeout=GEO_BILLING_TIMEOUT).json()
        ad = (rev or {}).get("address") or {}
        road = str(ad.get("road") or ad.get("street") or ad.get("suburb")
                   or ad.get("neighbourhood") or "").strip()
        city = str(ad.get("city") or ad.get("town") or ad.get("village")
                   or ad.get("county") or city).strip()
        state_name = str(ad.get("state") or state_name).strip()
        state_iso = str(ad.get("ISO3166-2-lvl4") or "").strip()
        postal = str(ad.get("postcode") or postal).strip()
    except Exception:  # noqa: BLE001 — reverse geocode là phần làm đẹp, không bắt buộc
        pass

    if not city or not postal:
        return {}                       # thiếu city/postal thì Stripe tax dễ toi -> fallback
    state_code = state_code_in(state_name, state_iso)
    if not state_code:
        # Không map được mã bang -> KHÔNG gửi tên đầy đủ (dạng chưa từng chạy trong
        # luồng này). Thà rơi về list cứng còn hơn gửi state sai định dạng.
        return {}
    first = random.choice(FIRST_NAMES_IN)
    last = random.choice(LAST_NAMES_IN)
    domain = random.choice(EMAIL_DOMAINS)
    street = road or random.choice(random.choice(CITIES_IN)["streets"])
    number = random.randint(1, 199)
    return {
        "email": f"{first.lower()}{last.lower()}{random.randint(100, 9999)}@{domain}",
        "name": f"{first} {last}",
        "country": "IN",
        "line1": f"{number} {street}",
        "line2": "",
        "city": city,
        "postal_code": postal,
        "state": state_code,                   # mã ISO 2 ký tự, không phải tên bang
        "_ip": str(info.get("ip") or ""),      # để log truy được, không gửi lên Stripe
    }


_log_file = LOG_DIR / f"upi_{time.strftime('%Y%m%d-%H%M%S')}.log"
_dump_counter = 0
_proxy_state: dict[str, Any] | None = None
_proxy_state_lock = RLock()
_log_lock = RLock()
_dump_lock = RLock()
_proxy_file_lock = RLock()
_proxy_redaction_lock = RLock()
_proxy_redaction_values: set[str] = set()
_log_context = local()


def redact_log_text(text: str) -> str:
    text = str(text or "")
    with _proxy_redaction_lock:
        context = getattr(_run_context, "value", None)
        source = context["redaction_values"] if context is not None else _proxy_redaction_values
        values = sorted(source, key=len, reverse=True)
    for value in values:
        if value:
            try:
                label = proxy_label(value)
            except (TypeError, ValueError):
                label = f"proxy#{hashlib.sha256(value.encode()).hexdigest()[:10]}"
            if label == "direct":
                label = f"proxy#{hashlib.sha256(value.encode()).hexdigest()[:10]}"
            text = text.replace(value, label)
    return text


def log(message: str, prefix: str = "") -> None:
    context = getattr(_log_context, "prefix", "")
    line = redact_log_text(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {prefix}{context}{message}")
    with _log_lock:
        run_context = getattr(_run_context, "value", None)
        output = run_context["on_output"] if run_context is not None else None
        if output is None:
            print(line, flush=True)
        else:
            output(line)
        with open(_log_file, "a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()


def env_bool(name: str, default: bool = False) -> bool:
    raw = env_value(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = str(env_value(name, "") or "").strip()
    if not raw:
        return max(minimum, default)
    try:
        return max(minimum, int(raw))
    except ValueError:
        return max(minimum, default)


def is_checkout_not_active_error(value: Any) -> bool:
    return "checkout_not_active_session" in str(value)


def is_user_already_paid_error(value: Any) -> bool:
    return "user is already paid" in str(value or "").lower()


def is_upi_unavailable_error(value: Any) -> bool:
    text = str(value or "")
    return UPI_UNAVAILABLE_ERROR in text or "the current checkout does not support UPI" in text


def random_user_agent() -> str:
    return DEFAULT_USER_AGENT


def random_runtime_version() -> str:
    return str(env_value("PP_RUNTIME_VERSION", "") or "").strip() or DEFAULT_STRIPE_RUNTIME_VERSION


def stripe_browser_id() -> str:
    return f"{uuid.uuid4()}{uuid.uuid4().hex[:8]}"


def build_email(first_name: str, last_name: str) -> str:
    first = re.sub(r"[^a-z]", "", first_name.lower())
    last = re.sub(r"[^a-z]", "", last_name.lower())
    suffix = random.randint(10000, 999999)
    domain = random.choice(EMAIL_DOMAINS)
    if random.random() < 0.5:
        local = f"{first}.{last}{suffix}"
    else:
        local = f"{first}{last}{suffix}"
    return f"{local}@{domain}"


def normalize_country(country: str) -> str:
    value = str(country or "").strip().upper()
    return value if value in COUNTRY_CURRENCY else "IN"


def currency_for_country(country: str) -> str:
    return COUNTRY_CURRENCY.get(normalize_country(country), "INR")


def payment_browser_locale() -> str:
    return str(env_value("UPI_BROWSER_LOCALE", "en-IN") or "").strip() or "en-IN"


def payment_elements_locale() -> str:
    return str(env_value("UPI_ELEMENTS_LOCALE", "en") or "").strip() or "en"


def payment_browser_timezone() -> str:
    return str(env_value("UPI_BROWSER_TIMEZONE", "Asia/Kolkata") or "").strip() or "Asia/Kolkata"


def saved_payment_value() -> str:
    return str(env_value("UPI_SAVED_PAYMENT_VALUE", "never") or "").strip() or "never"


def payment_accept_language() -> str:
    locale = payment_browser_locale()
    if locale.lower().startswith("en"):
        return "en-US,en;q=0.9"
    return f"{locale},{locale.split('-', 1)[0]};q=0.9,en;q=0.8"


def normalize_proxy_url(proxy: str) -> str:
    proxy = str(proxy or "").strip()
    if not proxy:
        return ""
    if "://" not in proxy:
        proxy = f"{default_proxy_scheme()}://{proxy}"

    parsed = urlsplit(proxy)
    if parsed.username is None and parsed.password is None:
        return proxy

    hostname = parsed.hostname or ""
    host = f"[{hostname}]" if ":" in hostname and not hostname.startswith("[") else hostname
    if parsed.port:
        host = f"{host}:{parsed.port}"
    username = quote(unquote(parsed.username or ""), safe="-._~")
    auth = username
    if parsed.password is not None:
        auth = f"{auth}:{quote(unquote(parsed.password), safe='-._~')}"
    return urlunsplit((parsed.scheme, f"{auth}@{host}", parsed.path, parsed.query, parsed.fragment))


def register_proxy_for_redaction(proxy: str) -> None:
    raw = str(proxy or "").strip()
    if not raw:
        return
    normalized = normalize_proxy_url(raw)
    values = {raw}
    if normalized:
        values.add(normalized)
        decoded = unquote(normalized)
        values.add(decoded)
        parsed = urlsplit(decoded)
        if parsed.netloc:
            values.add(parsed.netloc)
        if parsed.hostname:
            host = parsed.hostname
            if ":" in host and not host.startswith("["):
                host = f"[{host}]"
            try:
                port = parsed.port
            except ValueError:
                port = None
            values.add(f"{host}:{port}" if port else host)
    with _proxy_redaction_lock:
        context = getattr(_run_context, "value", None)
        target = context["redaction_values"] if context is not None else _proxy_redaction_values
        target.update(values)


def default_proxy_scheme() -> str:
    raw = str(env_value("UPI_PROXY_DEFAULT_SCHEME", "http") or "").strip().lower()
    raw = raw[:-3] if raw.endswith("://") else raw
    if raw in ("socks5", "socks5h"):
        return "socks5h"
    if raw in ("http", "https"):
        return raw
    return "http"


def proxy_short(proxy: str) -> str:
    proxy = normalize_proxy_url(proxy)
    if not proxy:
        return "direct"
    digest = hashlib.sha256(proxy.encode()).hexdigest()[:10]
    return f"proxy#{digest}"


def proxy_label(proxy: str) -> str:
    return proxy_short(proxy)


def proxy_key(proxy: str) -> str:
    proxy = normalize_proxy_url(proxy)
    return hashlib.sha256(proxy.encode()).hexdigest() if proxy else ""


_PROXY_COUNTRY_SELECTOR_RE = re.compile(
    r"(?i)(?P<name>country|region)(?P<separator>[-_=])(?P<value>[a-z]{2}(?:,[a-z]{2})*)"
)


def proxy_chain_key(proxy: str) -> str:
    """Return a redacted identity that stays stable across country rewrites."""
    proxy = unquote(normalize_proxy_url(proxy))
    normalized = _PROXY_COUNTRY_SELECTOR_RE.sub(
        lambda match: f"{match.group('name')}{match.group('separator')}*",
        proxy,
    )
    return hashlib.sha256(normalized.encode()).hexdigest()[:10] if normalized else ""


def proxy_for_country(proxy: str, country: str) -> str:
    """Rewrite only a proxy auth country selector while retaining its sticky session."""
    proxy = normalize_proxy_url(proxy)
    target_country = normalize_country(country).lower()
    if not proxy:
        raise RuntimeError("Proxy is empty; cannot derive region chain")

    parsed = urlsplit(proxy)
    username = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    replacements = 0

    def replace_country(match: re.Match[str]) -> str:
        nonlocal replacements
        replacements += 1
        current = match.group("value")
        value = target_country.upper() if current.isupper() else target_country
        return f"{match.group('name')}{match.group('separator')}{value}"

    username = _PROXY_COUNTRY_SELECTOR_RE.sub(replace_country, username)
    password = _PROXY_COUNTRY_SELECTOR_RE.sub(replace_country, password)
    if not replacements:
        raise RuntimeError(
            f"Proxy has no writable country/region selector: {proxy_label(proxy)}"
        )

    hostname = parsed.hostname or ""
    host = f"[{hostname}]" if ":" in hostname and not hostname.startswith("[") else hostname
    if parsed.port:
        host = f"{host}:{parsed.port}"
    auth = quote(username, safe="-._~")
    if parsed.password is not None:
        auth = f"{auth}:{quote(password, safe='-._~')}"
    derived = urlunsplit((parsed.scheme, f"{auth}@{host}", parsed.path, parsed.query, parsed.fragment))
    register_proxy_for_redaction(derived)
    return derived


def upi_proxy_chain(proxy_seed: str) -> tuple[str, str, str]:
    """Keep one sticky seed across configured checkout, promotion, and provider stages."""
    checkout_proxy = proxy_for_country(proxy_seed, UPI_BOOTSTRAP_COUNTRY)
    promotion_proxies = [
        proxy_for_country(proxy_seed, country) for country in UPI_PROMOTION_COUNTRIES
    ]
    provider_proxy = proxy_for_country(proxy_seed, UPI_PROVIDER_COUNTRY)
    chain_key = proxy_chain_key(proxy_seed)
    if not chain_key or any(
        proxy_chain_key(proxy) != chain_key
        for proxy in (checkout_proxy, *promotion_proxies, provider_proxy)
    ):
        raise RuntimeError("Proxy region rewrite changed the sticky seed; refused to mix proxy chains")
    return checkout_proxy, promotion_proxies[0], provider_proxy


def log_upi_proxy_chain(proxy_seed: str, checkout_proxy: str, promotion_proxy: str, provider_proxy: str) -> None:
    promotion_steps = " -> ".join(
        f"{country} promotion={proxy_label(proxy_for_country(promotion_proxy, country))}"
        for country in UPI_PROMOTION_COUNTRIES
    )
    log(
        "derived proxy chain: "
        f"chain={proxy_chain_key(proxy_seed)}; seed={proxy_label(proxy_seed)}; "
        f"{UPI_BOOTSTRAP_COUNTRY} checkout={proxy_label(checkout_proxy)}; "
        f"{promotion_steps}; "
        f"{UPI_PROVIDER_COUNTRY} provider/approve={proxy_label(provider_proxy)}"
    )


def normalize_pre_proxy_url(proxy: str) -> str:
    proxy = str(proxy or "").strip()
    if not proxy:
        return ""
    if "://" not in proxy:
        proxy = f"socks5h://{proxy}"
    return normalize_proxy_url(proxy)


def proxy_state_path() -> Path:
    raw = str(env_value("UPI_PROXY_STATE_FILE", "") or "").strip()
    return Path(raw) if raw else SCRIPT_DIR / "proxy_state.json"


def load_proxy_state() -> dict[str, Any]:
    global _proxy_state
    with _proxy_state_lock:
        context = getattr(_run_context, "value", None)
        cached = context["proxy_state"] if context is not None else _proxy_state
        if cached is not None:
            return cached
        path = proxy_state_path()
        try:
            data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        except Exception:
            data = {}
        if not isinstance(data, dict):
            data = {}
        data.setdefault("seed", {})
        data.setdefault("checkout", {})
        data.setdefault("promotion", {})
        data.setdefault("provider", {})
        data.setdefault("pair", {})
        if context is not None:
            context["proxy_state"] = data
        else:
            _proxy_state = data
        return data


def save_proxy_state() -> None:
    with _proxy_state_lock:
        context = getattr(_run_context, "value", None)
        state = context["proxy_state"] if context is not None else _proxy_state
        if state is None:
            return
        path = proxy_state_path()
        path.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def proxy_state_key(group: str, proxy: str) -> str:
    if group == "seed":
        return proxy_chain_key(proxy)
    return proxy_key(proxy)


def prune_proxy_seed_state(proxy_seeds: list[str]) -> None:
    with _proxy_state_lock:
        state = load_proxy_state()
        seed_state = state.setdefault("seed", {})
        active_keys = {proxy_chain_key(proxy) for proxy in proxy_seeds if proxy_chain_key(proxy)}
        stale_keys = [key for key in seed_state if key not in active_keys]
        for key in stale_keys:
            del seed_state[key]
        if stale_keys:
            save_proxy_state()
    if stale_keys:
        log(f"Seed proxy state cleaned: {len(stale_keys)}")


def prune_proxy_state(checkout_proxies: list[str], promotion_proxies: list[str], provider_proxies: list[str]) -> None:
    removed_counts: dict[str, int] = {}
    with _proxy_state_lock:
        state = load_proxy_state()
        active_keys_by_group: dict[str, set[str]] = {}
        for group, proxies in (
            ("checkout", checkout_proxies),
            ("promotion", promotion_proxies),
            ("provider", provider_proxies),
        ):
            group_state = state.get(group)
            if not isinstance(group_state, dict):
                continue
            active_keys = {proxy_key(proxy) for proxy in proxies if proxy}
            active_keys_by_group[group] = active_keys
            stale_keys = [key for key in group_state if key not in active_keys]
            for key in stale_keys:
                del group_state[key]
            if stale_keys:
                removed_counts[group] = len(stale_keys)
        pair_state = state.get("pair")
        if isinstance(pair_state, dict):
            active_checkout = active_keys_by_group.get("checkout", set())
            active_provider = active_keys_by_group.get("provider", set())
            stale_pair_keys = [
                key
                for key, record in pair_state.items()
                if not isinstance(record, dict)
                or record.get("checkout") not in active_checkout
                or record.get("provider") not in active_provider
            ]
            for key in stale_pair_keys:
                del pair_state[key]
            if stale_pair_keys:
                removed_counts["pair"] = len(stale_pair_keys)
        if removed_counts:
            save_proxy_state()
    if removed_counts:
        summary = ", ".join(f"{group}={count}" for group, count in removed_counts.items())
        log(f"Proxy state cleaned: {summary}")


def proxy_record(group: str, proxy: str) -> dict[str, Any]:
    with _proxy_state_lock:
        state = load_proxy_state()
        group_state = state.setdefault(group, {})
        key = proxy_state_key(group, proxy)
        if not key:
            return {}
        record = group_state.setdefault(key, {})
        record.setdefault("success", 0)
        record.setdefault("fail", 0)
        return record


def proxy_pair_key(checkout_proxy: str, provider_proxy: str) -> str:
    checkout_key = proxy_key(checkout_proxy)
    provider_key = proxy_key(provider_proxy)
    return f"{checkout_key}:{provider_key}" if checkout_key and provider_key else ""


def record_proxy_result(group: str, proxy: str, success: bool, reason: str = "") -> dict[str, Any]:
    if not proxy or not env_bool("UPI_PROXY_SCORE", True):
        return {}
    record = proxy_record(group, proxy)
    if not record:
        return {}
    now = int(time.time())
    if success:
        record["success"] = int(record.get("success") or 0) + 1
        record["fail"] = 0
        record["last_success"] = now
        record["last_reason"] = "success"
    else:
        record["fail"] = int(record.get("fail") or 0) + 1
        record["last_fail"] = now
        record["last_reason"] = str(reason or "failed")[:160]
    save_proxy_state()
    return record


def proxy_remove_after_fails() -> int:
    return env_int("UPI_PROXY_REMOVE_AFTER_FAILS", 3)


def is_reused_proxy_record(group: str, record: dict[str, Any]) -> bool:
    return int(record.get("success") or 0) > 0


def record_proxy_health_failure(group: str, proxy: str, reason: str) -> None:
    record = record_proxy_result(group, proxy, False, reason)
    fail_count = int(record.get("fail") or 0)
    remove_after = proxy_remove_after_fails() if is_reused_proxy_record(group, record) else 1
    if fail_count >= remove_after:
        remove_failed_proxy(group, proxy, reason)


def checkout_zero_cache_ttl() -> int:
    return env_int("UPI_ZERO_CACHE_TTL", 86400, minimum=0)


def zero_cache_scheduling_enabled() -> bool:
    return env_bool("UPI_ZERO_CACHE_SCHEDULING", False)


def checkout_zero_cache_status(proxy: str, country: str) -> tuple[str, int, int]:
    if not proxy or not env_bool("UPI_ZERO_CACHE", True):
        return "", 0, 0
    record = proxy_record("seed", proxy)
    if not record:
        return "", 0, 0
    checked_at = int(record.get("zero_checked_at") or 0)
    if not checked_at:
        return "", 0, 0
    ttl = checkout_zero_cache_ttl()
    if ttl > 0 and int(time.time()) - checked_at > ttl:
        return "", 0, checked_at
    if normalize_country(str(record.get("zero_country") or country)) != normalize_country(country):
        return "", 0, checked_at
    amount = int(record.get("zero_amount") or 0)
    if record.get("zero_ok") is True:
        return "ok", amount, checked_at
    if record.get("zero_ok") is False:
        return "bad", amount, checked_at
    return "", amount, checked_at


def record_checkout_zero_result(proxy: str, country: str, amount: int) -> None:
    if not proxy or not env_bool("UPI_ZERO_CACHE", True):
        return
    record = proxy_record("seed", proxy)
    if not record:
        return
    amount = int(amount or 0)
    record["zero_ok"] = amount == 0
    record["zero_amount"] = amount
    record["zero_country"] = normalize_country(country)
    record["zero_checked_at"] = int(time.time())
    if amount == 0:
        record["zero_success"] = int(record.get("zero_success") or 0) + 1
    save_proxy_state()


def record_proxy_pair_result(checkout_proxy: str, provider_proxy: str, success: bool, reason: str = "") -> None:
    record_proxy_result("checkout", checkout_proxy, success, reason)
    record_proxy_result("provider", provider_proxy, success, reason)
    if not checkout_proxy or not provider_proxy or not env_bool("UPI_PROXY_SCORE", True):
        return
    key = proxy_pair_key(checkout_proxy, provider_proxy)
    if not key:
        return
    with _proxy_state_lock:
        state = load_proxy_state()
        pair_state = state.setdefault("pair", {})
        record = pair_state.setdefault(
            key,
            {"checkout": proxy_key(checkout_proxy), "provider": proxy_key(provider_proxy)},
        )
        now = int(time.time())
        if success:
            record["success"] = int(record.get("success") or 0) + 1
            record["fail"] = 0
            record["last_success"] = now
            record["last_reason"] = "success"
        else:
            record["fail"] = int(record.get("fail") or 0) + 1
            record["last_fail"] = now
            record["last_reason"] = str(reason or "failed")[:160]
        save_proxy_state()


def record_proxy_pair_approve_success(checkout_proxy: str, provider_proxy: str, approve_proxy: str) -> None:
    if not checkout_proxy or not provider_proxy or not approve_proxy or not env_bool("UPI_PROXY_SCORE", True):
        return
    key = proxy_pair_key(checkout_proxy, provider_proxy)
    approve_key = proxy_key(approve_proxy)
    if not key or not approve_key:
        return
    record_proxy_result("provider", approve_proxy, True, "approve_success")
    with _proxy_state_lock:
        state = load_proxy_state()
        pair_state = state.setdefault("pair", {})
        record = pair_state.setdefault(
            key,
            {"checkout": proxy_key(checkout_proxy), "provider": proxy_key(provider_proxy)},
        )
        now = int(time.time())
        record["approve"] = approve_key
        record["approve_success"] = int(record.get("approve_success") or 0) + 1
        record["approve_last_success"] = now
        record["approve_last_reason"] = "success"
        save_proxy_state()


def successful_approve_preferences(checkout_proxy: str, provider_proxy: str, approve_pool: list[str]) -> list[str]:
    if not env_bool("UPI_PROXY_SCORE", True):
        return []
    pair_state = load_proxy_state().get("pair", {})
    if not isinstance(pair_state, dict):
        return []
    record = pair_state.get(proxy_pair_key(checkout_proxy, provider_proxy))
    if not isinstance(record, dict):
        return []
    approve_key = str(record.get("approve") or "")
    if not approve_key:
        return []
    approve_by_key = {proxy_key(proxy): proxy for proxy in approve_pool}
    approve_proxy = approve_by_key.get(approve_key)
    return [approve_proxy] if approve_proxy else []


def record_failure_by_stage(
    reason: str,
    checkout_proxy: str,
    provider_proxy: str,
    promotion_proxy: str = "",
) -> None:
    def record_seed_failure(proxy: str) -> None:
        if not proxy:
            return
        if is_direct_remove_proxy_error(reason):
            remove_failed_proxy("seed", proxy, reason)
            record_proxy_result("seed", proxy, False, reason)
        elif is_proxy_health_failure(reason):
            record_proxy_health_failure("seed", proxy, reason)
        else:
            record_proxy_result("seed", proxy, False, reason)

    if "checkout stage failed" in reason or "checkout creation failed" in reason:
        record_seed_failure(checkout_proxy)
        return
    if is_upi_unavailable_error(reason):
        return
    if "the 0-value discount did not take effect" in reason:
        return
    if "approve blocked" in reason:
        return
    if "promotion stage failed" in reason or "checkout/update" in reason:
        record_seed_failure(promotion_proxy)
        return
    record_seed_failure(provider_proxy)


def order_proxy_group(group: str, proxies: list[str]) -> list[str]:
    if not env_bool("UPI_PROXY_SCORE", True):
        return proxies
    state = load_proxy_state().get(group, {})
    skip_failed = env_bool("UPI_PROXY_SKIP_FAILED", True)
    fail_threshold = env_int("UPI_PROXY_FAIL_SKIP_AFTER", 1)
    fail_cooldown = env_int("UPI_PROXY_FAIL_COOLDOWN", 180, minimum=0)
    zero_ttl = checkout_zero_cache_ttl()
    zero_scheduling = zero_cache_scheduling_enabled()
    now = int(time.time())
    kept: list[str] = []
    cooldown_skipped = 0
    zero_skipped = 0
    zero_seen = 0
    success_seen = 0
    for proxy in proxies:
        record = state.get(proxy_state_key(group, proxy), {}) if isinstance(state, dict) else {}
        success_count = int(record.get("success") or 0)
        fail_count = int(record.get("fail") or 0)
        last_fail = int(record.get("last_fail") or 0)
        if success_count > 0:
            success_seen += 1
        zero_checked_at = int(record.get("zero_checked_at") or 0)
        zero_cache_valid = zero_checked_at and (zero_ttl <= 0 or now - zero_checked_at <= zero_ttl)
        if group == "checkout" and zero_scheduling and zero_cache_valid and record.get("zero_ok") is True:
            zero_seen += 1
        if (
            group == "checkout"
            and zero_scheduling
            and env_bool("UPI_ZERO_CACHE_SKIP_BAD", True)
            and zero_cache_valid
            and record.get("zero_ok") is False
        ):
            zero_skipped += 1
            continue
        if skip_failed and fail_count >= fail_threshold:
            in_cooldown = fail_cooldown <= 0 or not last_fail or now - last_fail <= fail_cooldown
            if in_cooldown:
                cooldown_skipped += 1
                continue
        kept.append(proxy)

    if not kept and proxies:
        log(f"{group} proxy state filtered to empty; skipped all", "[WARN] ")

    def rank(proxy: str) -> tuple[int, int, int, int, int]:
        record = state.get(proxy_state_key(group, proxy), {}) if isinstance(state, dict) else {}
        zero_checked_at = int(record.get("zero_checked_at") or 0)
        zero_cache_valid = zero_checked_at and (zero_ttl <= 0 or now - zero_checked_at <= zero_ttl)
        zero_rank = 1 if group == "checkout" and zero_scheduling and zero_cache_valid and record.get("zero_ok") is True else 0
        return (
            zero_rank,
            int(record.get("success") or 0),
            int(record.get("last_success") or 0),
            -int(record.get("fail") or 0),
            -int(record.get("last_fail") or 0),
        )

    ordered = sorted(kept, key=rank, reverse=True)
    if cooldown_skipped or success_seen or zero_seen or zero_skipped:
        log(
            f"{group} proxy state: success-first={success_seen}, zero-hit={zero_seen}, "
            f"cooldown-skip={cooldown_skipped}, zero-fail-skip={zero_skipped}"
        )
    return ordered


def set_proxy(session: Any, proxy: str) -> None:
    register_proxy_for_redaction(proxy)
    proxy = normalize_proxy_url(proxy)
    if hasattr(session, "trust_env"):
        session.trust_env = False
    session.proxies = {"http": proxy, "https": proxy} if proxy else {}


def pre_proxy_url() -> str:
    """Local front proxy: local proxy -> file proxy -> target site."""
    for name in ("UPI_PRE_PROXY", "PP_PRE_PROXY", "PP_LOCAL_PROXY"):
        if env_value(name, None) is not None:
            raw = str(env_value(name, "") or "").strip()
            if raw.lower() in {"", "0", "off", "none", "direct", "disabled"}:
                return ""
            proxy = normalize_pre_proxy_url(raw)
            register_proxy_for_redaction(proxy)
            return proxy
    raw = ""
    return normalize_pre_proxy_url(raw) if raw else ""


def load_proxy_file(path: Path) -> list[str]:
    proxies: list[str] = []
    if not path.exists():
        return proxies
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            register_proxy_for_redaction(line)
            proxy = normalize_proxy_url(line)
            if proxy:
                proxies.append(proxy)
    random.shuffle(proxies)
    return proxies


def proxy_seed_file() -> Path:
    raw = (
        str(env_value("UPI_PROXY_SEED_FILE", "") or "").strip()
        or str(env_value("PP_PROXY_SEED_FILE", "") or "").strip()
    )
    return Path(raw).expanduser() if raw else SCRIPT_DIR / "proxy_seeds.txt"


def is_direct_remove_proxy_error(reason: str) -> bool:
    text = str(reason or "").lower()
    return any(
        marker in text
        for marker in (
            "proxy authentication",
            "proxy auth",
            "resolve proxy",
            "could not resolve proxy",
            "invalid proxy",
            "malformed proxy",
            "unsupported proxy",
            "http 407",
            "status 407",
        )
    )


def is_proxy_health_failure(reason: str) -> bool:
    text = str(reason or "").lower()
    return any(
        marker in text
        for marker in (
            "target site unreachable",
            "proxy-server",
            "connection reset",
            "recv failure",
            "timed out",
            "timeout",
            "connect tunnel failed",
            "proxy connect aborted",
            "proxy tunneling",
            "proxy handshake",
            "connection refused",
            "ssl connect",
            "tls connect",
            "curl: (28)",
            "curl: (35)",
            "curl: (56)",
            "http_502",
            "http_503",
            "http_504",
        )
    )


def remove_failed_proxies(group: str, failures: list[tuple[str, str]]) -> int:
    if not failures or not env_bool("UPI_PROXY_REMOVE_FAILED", True):
        return 0
    for proxy, _reason in failures:
        register_proxy_for_redaction(proxy)
    path = proxy_seed_file()
    if not path.is_file():
        return 0
    reasons = {proxy_chain_key(proxy): reason for proxy, reason in failures if proxy_chain_key(proxy)}
    if not reasons:
        return 0
    with _proxy_file_lock:
        lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
        removed = [line for line in lines if proxy_chain_key(line) in reasons]
        if not removed:
            return 0
        kept = [line for line in lines if proxy_chain_key(line) not in reasons]
        quarantine = SCRIPT_DIR / "removed_proxies.jsonl"
        with open(quarantine, "a", encoding="utf-8") as f:
            for line in removed:
                chain_key = proxy_chain_key(line)
                f.write(
                    json.dumps(
                        {
                            "time": int(time.time()),
                            "group": group,
                            "proxy": proxy_label(line.strip()),
                            "reason": redact_log_text(str(reasons.get(chain_key) or ""))[:300],
                            "source": path.name,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
        temp_path = path.with_name(f".{path.name}.tmp")
        temp_path.write_text("".join(kept), encoding="utf-8")
        os.replace(temp_path, path)
        return len(removed)


def remove_failed_proxy(group: str, proxy: str, reason: str) -> bool:
    return remove_failed_proxies(group, [(proxy, reason)]) > 0


def unique_proxy_seeds(proxy_seeds: list[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    duplicates = 0
    for proxy_seed in proxy_seeds:
        chain_key = proxy_chain_key(proxy_seed)
        if not chain_key or chain_key in seen:
            duplicates += 1
            continue
        seen.add(chain_key)
        unique.append(proxy_seed)
    if duplicates:
        log(f"Proxy seed dedup: ignored {duplicates} duplicate sticky sessions", "[WARN] ")
    return unique


def load_proxy_seeds() -> list[str]:
    path = proxy_seed_file()
    if not path.is_file():
        raise RuntimeError("proxy seed file does not exist")
    proxy_seeds = unique_proxy_seeds(load_proxy_file(path))
    if not proxy_seeds:
        raise RuntimeError("proxy seed list is empty")
    prune_proxy_seed_state(proxy_seeds)
    proxy_seeds = order_proxy_group("seed", proxy_seeds)
    if not proxy_seeds:
        raise RuntimeError("all proxy seeds are in failure cooldown")
    log(f"loaded {len(proxy_seeds)} proxy seed(s)")
    log(
        "strict proxy policy: one seed per round; derive "
        f"{UPI_BOOTSTRAP_COUNTRY} Checkout -> {' -> '.join(UPI_PROMOTION_COUNTRIES)} checkout/update -> "
        f"{UPI_PROVIDER_COUNTRY} Stripe/UPI/approve"
    )
    log(f"bare proxy scheme: {default_proxy_scheme()}://")
    log(f"local front proxy: {proxy_short(pre_proxy_url())}")
    return proxy_seeds


def _guard_session_stop(session: Any) -> Any:
    """Cho session biết lệnh Stop: kiểm tra trước MỖI request.

    Vì sao cần: `check_cancelled()` trước đây chỉ chạy đúng một lần lúc task bắt đầu,
    nên task đang nằm trong vòng retry của checkout/Stripe không hề biết đã bấm Stop —
    phải chạy hết vòng đó mới quay về được mốc kiểm tra. Đo thực tế trên job 1000 acc:
    bấm Stop lúc 23:05:56, task t703 vẫn còn thử checkout ở 23:06:12/29/35/42/48 và
    chỉ thoát ở 23:06:49 — chậm 53 giây. Kiểm tra ở đây cắt ngay trước request kế tiếp,
    tức là trong vòng ~1 request thay vì cả một vòng retry.

    `check_cancelled()` ném `FlowCancelled` (BaseException) nên không bị các
    `except Exception` của vòng retry nuốt mất — đúng chủ đích thiết kế của nó.
    """
    original = session.request

    def guarded(*args: Any, **kwargs: Any) -> Any:
        check_cancelled()
        return original(*args, **kwargs)

    try:
        session.request = guarded
    except (AttributeError, TypeError):
        # Kiểu session không cho gán (đổi thư viện chẳng hạn): chạy như cũ, không chết task.
        return session
    return session


def new_session(proxy: str = "", use_pre_proxy: bool = True) -> Any:
    pre_proxy = pre_proxy_url() if use_pre_proxy else ""
    register_proxy_for_redaction(pre_proxy)
    if CurlCffiSession is not None:
        kwargs: dict[str, Any] = {
            "impersonate": str(env_value("CURL_IMPERSONATE", "") or "").strip() or "chrome146"
        }
        if pre_proxy:
            if CurlOpt is None:
                raise RuntimeError("local front proxy requires curl_cffi support")
            kwargs["curl_options"] = {CurlOpt.PRE_PROXY: pre_proxy}
        session = CurlCffiSession(**kwargs)
    else:
        if pre_proxy:
            raise RuntimeError("local front proxy requires curl_cffi: run python3 -m pip install curl_cffi")
        session = requests.Session()
    if hasattr(session, "trust_env"):
        session.trust_env = False
    if proxy:
        set_proxy(session, proxy)
    return _guard_session_stop(session)


def _redact_text(text: str, limit: int | None = None) -> str:
    text = text or ""
    text = re.sub(r"(Bearer\s+)[A-Za-z0-9._=-]+", r"\1***", text)
    text = re.sub(r"(__Secure-next-auth\.session-token=)[^;\\s]+", r"\1***", text)
    text = re.sub(r"(accessToken|access_token|sessionToken|token)(['\"]?\s*[:=]\s*['\"])[^'\"]+", r"\1\2***", text)
    text = redact_log_text(text)
    if limit is None:
        limit = env_int("UPI_DUMP_LIMIT", 0, minimum=0)
    if limit > 0:
        return text[:limit]
    return text


def dump_http(
    response: requests.Response | None,
    stage: str,
    request_body: Any = None,
    request_method: str = "",
    request_url: str = "",
    force: bool = False,
) -> None:
    # MẶC ĐỊNH TẮT: mọi dump (kể cả force=True) đều chỉ ghi khi UPI_DUMP=1.
    # Trước đây 12 chỗ `force=True` (approve/poll/redirect/confirm) ghi không cần
    # cờ -> dumps/ phình tới 833MB chỉ sau vài job. Nay force chỉ còn là ghi chú
    # "quan trọng", không còn vượt được công tắc master.
    if not env_bool("UPI_DUMP", False):
        return
    global _dump_counter
    with _dump_lock:
        _dump_counter += 1
        name = f"{time.strftime('%Y%m%d-%H%M%S')}_{_dump_counter:04d}_{stage}.txt"
    path = DUMP_DIR / re.sub(r"[^A-Za-z0-9_.-]+", "_", name)
    lines = [
        f"stage: {stage}",
        f"request: {request_method} {request_url}",
        "",
        "request_body:",
        _redact_text(json.dumps(request_body, ensure_ascii=False, indent=2) if request_body is not None else ""),
        "",
    ]
    if response is not None:
        lines.extend(
            [
                f"status: {response.status_code}",
                f"url: {response.url}",
                "",
                "response:",
                _redact_text(
                    response.text,
                    limit=sys.maxsize
                    if stage == "poll_risk_blocked" or "all" in full_dump_stages() or stage in full_dump_stages()
                    else env_int("UPI_DUMP_LIMIT", 6000, minimum=500),
                ),
            ]
        )
    path.write_text("\n".join(lines), encoding="utf-8")


def token_key_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def find_named_token(payload: Any, aliases: tuple[str, ...]) -> str:
    wanted = {token_key_name(item) for item in aliases}
    if isinstance(payload, dict):
        cookie_name = token_key_name(payload.get("name") or payload.get("key"))
        if cookie_name in wanted:
            for value_key in ("value", "token", "content"):
                value = str(payload.get(value_key) or "").strip()
                if value:
                    return value
        for key, value in payload.items():
            if token_key_name(key) in wanted and isinstance(value, (str, int, float)):
                found = str(value).strip()
                if found:
                    return found
        for value in payload.values():
            found = find_named_token(value, aliases)
            if found:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = find_named_token(item, aliases)
            if found:
                return found
    return ""


def collect_strings(payload: Any, result: list[str] | None = None) -> list[str]:
    values = result if result is not None else []
    if isinstance(payload, str):
        values.append(payload)
    elif isinstance(payload, dict):
        for value in payload.values():
            collect_strings(value, values)
    elif isinstance(payload, list):
        for item in payload:
            collect_strings(item, values)
    return values


def find_session_cookie(payload: Any) -> str:
    for value in collect_strings(payload):
        match = re.search(r"(?:^|[;\s])__Secure-next-auth\.session-token=([^;\s]+)", value)
        if match:
            return unquote(match.group(1))
    return ""


def normalize_token(raw: str) -> tuple[str, str]:
    text = str(raw or "").strip()
    if not text:
        return "", ""
    session_token = ""
    if text.startswith("{") or text.startswith("["):
        try:
            data = json.loads(text)
            access_token = find_named_token(
                data,
                (
                    "accessToken",
                    "access_token",
                    "token",
                    "bearerToken",
                    "bearer_token",
                    "jwt",
                ),
            )
            session_token = find_named_token(
                data,
                (
                    "sessionToken",
                    "session_token",
                    "nextAuthSessionToken",
                    "next_auth_session_token",
                    "__Secure-next-auth.session-token",
                    "secureNextAuthSessionToken",
                ),
            ) or find_session_cookie(data)
            text = access_token
        except json.JSONDecodeError:
            pass
    return text, session_token


def load_token() -> tuple[str, str]:
    for env_name in ("PP_TOKEN", "UPI_TOKEN"):
        value = str(env_value(env_name, "") or "").strip()
        if value:
            log(f"using env var {env_name}")
            token, session_token = normalize_token(value)
            env_session = str(env_value("PP_SESSION_TOKEN", "") or "").strip()
            if env_session or session_token:
                log("loaded sessionToken cookie")
            return token, env_session or session_token

    candidates = [SCRIPT_DIR / "token.txt"]
    for path in candidates:
        if not path.exists():
            continue
        raw = path.read_bytes()
        for enc in ("utf-8-sig", "utf-16", "utf-8", "ascii"):
            try:
                text = raw.decode(enc).strip()
                break
            except UnicodeError:
                continue
        else:
            text = raw.decode("utf-8", errors="ignore").strip()
        if text:
            log("using token file")
            token, session_token = normalize_token(text)
            env_session = str(env_value("PP_SESSION_TOKEN", "") or "").strip()
            if env_session or session_token:
                log("loaded sessionToken cookie")
            return token, env_session or session_token

    token = input("enter access_token: ").strip()
    session_token = str(env_value("PP_SESSION_TOKEN", "") or "").strip()
    token, parsed_session = normalize_token(token)
    return token, session_token or parsed_session


def build_chatgpt_session(access_token: str, device_id: str, proxy: str, session_token: str = "") -> requests.Session:
    session = new_session(proxy)
    cookie = f"oai-did={device_id}"
    if session_token:
        cookie += f"; __Secure-next-auth.session-token={session_token}"
    session.headers.update(
        {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "*/*",
            "Accept-Language": payment_accept_language(),
            "Authorization": f"Bearer {access_token}",
            "Origin": "https://chatgpt.com",
            "Referer": "https://chatgpt.com/",
            "Content-Type": "application/json",
            "oai-device-id": device_id,
            "oai-language": payment_browser_locale(),
            "oai-session-id": device_id,
            "oai-client-version": CHATGPT_CLIENT_VERSION,
            "oai-client-build-number": CHATGPT_CLIENT_BUILD_NUMBER,
            "sec-ch-ua": CHROME_SEC_CH_UA,
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-ch-ua-full-version-list": CHROME_SEC_CH_UA_FULL,
            "sec-ch-ua-platform-version": '"10.0.0"',
            "sec-ch-ua-arch": '"x86"',
            "sec-ch-ua-bitness": '"64"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "Cookie": cookie,
        }
    )
    # Warmup CSRF & Lấy Sentinel Token qua NodeJS runner để pass Risk Engine của OpenAI
    try:
        import check_upi_eligibility as cu
        cu.warmup_csrf(session, 15)
        # Chia sẻ cookie mà warmup đã thu được (cf_clearance/__cf_bm/...) cho request
        # /sentinel/req, để nó cũng mang bộ cookie như frame thật. oai-did luôn được
        # giữ; nếu lấy jar lỗi thì để cu tự dùng oai-did.
        try:
            jar = session.cookies.get_dict() or {}
        except Exception:  # noqa: BLE001
            jar = {}
        cookie_parts = [f"{k}={v}" for k, v in jar.items() if v]
        if session_token:
            cookie_parts.append(f"__Secure-next-auth.session-token={session_token}")
        if not any(p.startswith("oai-did=") for p in cookie_parts):
            cookie_parts.append(f"oai-did={device_id}")
        cookies_str = "; ".join(cookie_parts) or None
        sh = cu.sentinel_headers(device_id, proxy, 45, cookies=cookies_str)
        if sh:
            session.headers.update(sh)
    except Exception as exc:
        log(f"sentinel warmup notice: {exc}", "[WARN] ")
    return session


def checkout_response_has_promo(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    for key in (
        "scheduled_discount_preview",
        "immediate_discount_settings",
        "promo_campaign",
        "promo_credit_grant",
    ):
        value = payload.get(key)
        if value not in (None, "", [], {}):
            return True
    return False


def checkout_response_has_trial(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    if payload.get("one_click_trial_eligible") is True:
        return True
    subscription_data = payload.get("subscription_data")
    if isinstance(subscription_data, dict) and int(subscription_data.get("trial_period_days") or 0) > 0:
        return True
    for key in ("trial_period_days", "trial_end"):
        value = payload.get(key)
        if value not in (None, "", 0, "0", False):
            return True
    return False


def create_checkout(chatgpt: requests.Session, country: str) -> dict[str, str]:
    country = normalize_country(country)
    promo_mode = str(env_value("PP_PROMO_MODE", "campaign") or "").strip().lower() or "campaign"
    promo_id = str(env_value("PP_PROMO_ID", "plus-1-month-free") or "").strip()
    body: dict[str, Any] = {
        "entry_point": str(env_value("PP_ENTRY_POINT", "all_plans_pricing_modal") or ""),
        "plan_name": "chatgptplusplan",
        "billing_details": {"country": country, "currency": currency_for_country(country)},
        "checkout_ui_mode": "custom",
    }
    if promo_mode in ("trial", "free_trial"):
        trial_days = env_int("PP_TRIAL_DAYS", 30)
        body["subscription_data"] = {"trial_period_days": trial_days}
    elif promo_mode in ("campaign", "query"):
        body["promo_campaign"] = {
            "promo_campaign_id": promo_id,
            "is_coupon_from_query_param": promo_mode == "query",
        }
    elif promo_mode == "coupon":
        body["coupon"] = promo_id
    elif promo_mode == "code":
        body["promotion_code"] = promo_id
    elif promo_mode != "off":
        log(f"unknown PP_PROMO_MODE={promo_mode!r}; ignored", "[WARN] ")
    log(f"Checkout promo: mode={promo_mode}, id={promo_id}")

    headers = {
        "Referer": "https://chatgpt.com/",
        "x-openai-target-path": "/backend-api/payments/checkout",
        "x-openai-target-route": "/backend-api/payments/checkout",
    }
    resp = chatgpt.post(
        "https://chatgpt.com/backend-api/payments/checkout",
        json=body,
        headers=headers,
        timeout=CHATGPT_TIMEOUT,
    )
    dump_http(resp, "checkout", body, "POST", "https://chatgpt.com/backend-api/payments/checkout", force=resp.status_code >= 400)
    if resp.status_code >= 400:
        if is_user_already_paid_error(resp.text):
            raise RuntimeError("User is already paid")
        raise RuntimeError(f"checkout creation failed HTTP {resp.status_code}: {resp.text[:500]}")

    data = resp.json() or {}
    if (
        promo_mode == "coupon"
        and promo_id == "plus-1-month-free"
        and not checkout_response_has_promo(data)
        and env_bool("UPI_COUPON_FALLBACK_PROMO_CAMPAIGN", True)
    ):
        log("coupon response shows no discount; retrying with promo_campaign string", "[PROMO] ")
        fallback_body = dict(body)
        fallback_body.pop("coupon", None)
        fallback_body["promo_campaign"] = promo_id
        resp = chatgpt.post(
            "https://chatgpt.com/backend-api/payments/checkout",
            json=fallback_body,
            headers=headers,
            timeout=CHATGPT_TIMEOUT,
        )
        dump_http(
            resp,
            "checkout_promo_campaign",
            fallback_body,
            "POST",
            "https://chatgpt.com/backend-api/payments/checkout",
            force=True,
        )
        if resp.status_code >= 400:
            if is_user_already_paid_error(resp.text):
                raise RuntimeError("User is already paid")
            raise RuntimeError(f"checkout promo_campaign retry failed HTTP {resp.status_code}: {resp.text[:500]}")
        data = resp.json() or {}
        log(f"after promo_campaign retry promo={checkout_response_has_promo(data)}", "[PROMO] ")

    cs_id = data.get("checkout_session_id") or data.get("session_id") or data.get("id")
    if not cs_id or not (str(cs_id).startswith("cs_") or str(cs_id).startswith("oaics_")):
        raise RuntimeError(f"checkout response missing cs_id: {str(data)[:500]}")

    raw_pk = (
        data.get("stripe_publishable_key")
        or data.get("publishable_key")
        or data.get("publishableKey")
        or data.get("stripePublishableKey")
        or data.get("key")
        or ""
    )
    match = re.search(r"pk_live_[A-Za-z0-9]+", str(raw_pk))
    stripe_pk = match.group(0) if match else DEFAULT_STRIPE_PK
    processor_entity = str(data.get("processor_entity") or data.get("processorEntity") or "")
    log(
        f"checkout created: {cs_id} / {country} / {currency_for_country(country)} / "
        f"mode={promo_mode} / promo={checkout_response_has_promo(data)} / "
        f"trial={checkout_response_has_trial(data)}"
    )
    return {
        "cs_id": str(cs_id),
        "processor_entity": processor_entity,
        "stripe_pk": stripe_pk,
        "billing_country": country,
        "currency": currency_for_country(country),
    }


def checkout_page_url(checkout: dict[str, str]) -> str:
    processor = processor_entity_for_country(
        UPI_BOOTSTRAP_COUNTRY,
        checkout.get("processor_entity") or "",
    )
    return f"https://chatgpt.com/checkout/{processor}/{checkout['cs_id']}"


def update_checkout_promotion(
    chatgpt: requests.Session,
    checkout: dict[str, str],
    promotion_country: str,
) -> None:
    mode = str(env_value("PP_PROMO_MODE", "campaign") or "").strip().lower() or "campaign"
    promo_id = str(env_value("PP_PROMO_ID", "plus-1-month-free") or "").strip() or "plus-1-month-free"
    body: dict[str, Any] = {
        "checkout_session_id": checkout["cs_id"],
        "processor_entity": processor_entity_for_country(
            UPI_BOOTSTRAP_COUNTRY,
            checkout.get("processor_entity") or "",
        ),
        "plan_name": "chatgptplusplan",
        "price_interval": "month",
        "seat_quantity": 1,
    }
    if mode in {"campaign", "query", "coupon"}:
        body["promo_campaign"] = {
            "promo_campaign_id": promo_id,
            "is_coupon_from_query_param": mode == "query",
        }
    url = "https://chatgpt.com/backend-api/payments/checkout/update"
    resp = chatgpt.post(
        url,
        json=body,
        headers={
            "Referer": checkout_page_url(checkout),
            "x-openai-target-path": "/backend-api/payments/checkout/update",
            "x-openai-target-route": "/backend-api/payments/checkout/update",
        },
        timeout=CHATGPT_TIMEOUT,
    )
    dump_http(resp, "checkout_promotion_update", body, "POST", url, force=resp.status_code >= 400)
    if resp.status_code >= 400:
        if is_checkout_not_active_error(resp.text):
            raise RuntimeError("checkout_not_active_session")
        raise RuntimeError(f"checkout/update failed HTTP {resp.status_code}: {resp.text[:500]}")
    try:
        payload = resp.json() or {}
    except Exception:
        payload = {}
    if isinstance(payload, dict) and payload.get("success") is False:
        raise RuntimeError(f"checkout/update rejected: {str(payload)[:500]}")
    log(f"{promotion_country} checkout/update ok: promo={promo_id if 'promo_campaign' in body else 'off'}")


def update_upi_checkout_taxes(
    chatgpt: requests.Session,
    checkout: dict[str, str],
    billing: dict[str, str],
) -> None:
    url = "https://chatgpt.com/backend-api/payments/checkout/taxes"
    body = {
        "checkout_session_id": checkout["cs_id"],
        "checkout_email": billing["email"],
        "billing_country": UPI_PROVIDER_COUNTRY,
        "billing_name": billing["name"],
        "currency": currency_for_country(UPI_PROVIDER_COUNTRY),
        "tax_id": None,
        "processor_entity": processor_entity_for_country(
            UPI_BOOTSTRAP_COUNTRY,
            checkout.get("processor_entity") or "",
        ),
        "billing_address": {
            "line1": billing["line1"],
            "city": billing["city"],
            "country": UPI_PROVIDER_COUNTRY,
            "postal_code": billing["postal_code"],
        },
    }
    if billing.get("state"):
        body["billing_address"]["state"] = billing["state"]
    resp = chatgpt.post(
        url,
        json=body,
        headers={
            "Referer": checkout_page_url(checkout),
            "x-openai-target-path": "/backend-api/payments/checkout/taxes",
            "x-openai-target-route": "/backend-api/payments/checkout/taxes",
        },
        timeout=CHATGPT_TIMEOUT,
    )
    dump_http(resp, "checkout_taxes", body, "POST", url, force=resp.status_code >= 400)
    if resp.status_code >= 400:
        if is_checkout_not_active_error(resp.text):
            raise RuntimeError("checkout_not_active_session")
        raise RuntimeError(f"checkout/taxes failed HTTP {resp.status_code}: {resp.text[:500]}")
    log(f"{UPI_PROVIDER_COUNTRY} checkout/taxes synced")


def stripe_init(cs_id: str, stripe_pk: str, proxy: str) -> dict[str, Any]:
    stripe = new_session(proxy)
    stripe.headers.update({
        "User-Agent": random_user_agent(),
        "Accept-Language": payment_accept_language(),
        **CHROME_CLIENT_HINTS,
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "cross-site",
    })
    stripe_js_id = str(uuid.uuid4())
    body = {
        "browser_locale": payment_browser_locale(),
        "browser_timezone": payment_browser_timezone(),
        "elements_session_client[client_betas][0]": "custom_checkout_server_updates_1",
        "elements_session_client[client_betas][1]": "custom_checkout_manual_approval_1",
        "elements_session_client[elements_init_source]": "custom_checkout",
        "elements_session_client[referrer_host]": "chatgpt.com",
        "elements_session_client[stripe_js_id]": stripe_js_id,
        "elements_session_client[locale]": payment_elements_locale(),
        "elements_session_client[is_aggregation_expected]": "false",
        "elements_options_client[saved_payment_method][enable_save]": saved_payment_value(),
        "elements_options_client[saved_payment_method][enable_redisplay]": saved_payment_value(),
        "key": stripe_pk,
        "_stripe_version": STRIPE_VERSION_FULL,
    }
    url = f"https://api.stripe.com/v1/payment_pages/{cs_id}/init"
    resp = stripe.post(url, data=body, timeout=DEFAULT_TIMEOUT)
    dump_http(resp, "stripe_init", body, "POST", url, force=resp.status_code >= 400)
    if resp.status_code >= 400:
        raise RuntimeError(f"Stripe init failed HTTP {resp.status_code}: {resp.text[:500]}")
    payload = resp.json() or {}
    payload["client_stripe_js_id"] = stripe_js_id
    payload["_client_context"] = {"stripe_js_id": stripe_js_id}
    return payload


def amount_from_payload(payload: Any) -> int:
    if isinstance(payload, dict):
        total_summary = payload.get("total_summary")
        if isinstance(total_summary, dict) and total_summary.get("due") is not None:
            return int(total_summary.get("due") or 0)
        invoice = payload.get("invoice")
        if isinstance(invoice, dict) and invoice.get("amount_due") is not None:
            return int(invoice.get("amount_due") or 0)
        line_items = payload.get("line_items")
        if isinstance(line_items, list):
            total = 0
            found = False
            for item in line_items:
                if isinstance(item, dict) and item.get("amount") is not None:
                    total += int(item.get("amount") or 0)
                    found = True
            if found:
                return total
    text = json.dumps(payload, ensure_ascii=False) if not isinstance(payload, str) else payload
    for pattern in (
        r'"total"\s*:\s*(\d+)',
        r'"amount_total"\s*:\s*(\d+)',
        r'"checkout_amount"\s*:\s*(\d+)',
        r'"amount"\s*:\s*(\d+)',
    ):
        match = re.search(pattern, text)
        if match:
            return int(match.group(1))
    return 0


def build_ctx(init_payload: dict[str, Any], checkout: dict[str, str]) -> dict[str, Any]:
    client_context = init_payload.get("_client_context") if isinstance(init_payload.get("_client_context"), dict) else {}
    return {
        "stripe_js_id": str(client_context.get("stripe_js_id") or init_payload.get("client_stripe_js_id") or uuid.uuid4()),
        "client_session_id": str(uuid.uuid4()),
        "guid": stripe_browser_id(),
        "muid": stripe_browser_id(),
        "sid": stripe_browser_id(),
        "elements_session_id": f"elements_session_{uuid.uuid4().hex[:11]}",
        "elements_session_config_id": str(init_payload.get("config_id") or uuid.uuid4()),
        "config_id": init_payload.get("config_id") or "",
        "init_checksum": init_payload.get("init_checksum") or "",
        "checkout_amount": amount_from_payload(init_payload),
        "locale": payment_elements_locale(),
        "currency": str(init_payload.get("currency") or checkout.get("currency") or "eur").lower(),
        "runtime_version": DEFAULT_STRIPE_RUNTIME_VERSION,
        "stripe_version": STRIPE_VERSION_FULL,
    }


def stripe_elements_session_params(ctx: dict[str, Any]) -> dict[str, str]:
    return {
        "elements_session_client[client_betas][0]": "custom_checkout_server_updates_1",
        "elements_session_client[client_betas][1]": "custom_checkout_manual_approval_1",
        "elements_session_client[elements_init_source]": "custom_checkout",
        "elements_session_client[referrer_host]": "chatgpt.com",
        "elements_session_client[session_id]": str(ctx.get("elements_session_id") or f"elements_session_{uuid.uuid4().hex[:11]}"),
        "elements_session_client[stripe_js_id]": str(ctx.get("stripe_js_id") or uuid.uuid4()),
        "elements_session_client[locale]": str(ctx.get("locale") or payment_elements_locale()),
        "elements_session_client[is_aggregation_expected]": "false",
        "elements_options_client[saved_payment_method][enable_save]": saved_payment_value(),
        "elements_options_client[saved_payment_method][enable_redisplay]": saved_payment_value(),
    }


def upi_billing_profile(proxy: str = "") -> dict[str, str]:
    """Profile billing: ưu tiên suy từ IP exit, không thì mới dùng list cứng.

    Bật/tắt bằng `UPI_GEO_BILLING=0`. ENV (UPI_CITY, UPI_STATE, …) vẫn đè lên tất
    cả — ai đã chỉ định tay thì tôn trọng.
    """
    profile: dict[str, Any] = {}
    if proxy and env_bool("UPI_GEO_BILLING", True):
        try:
            profile = geo_billing_from_proxy(proxy)
        except Exception as exc:  # noqa: BLE001
            log(f"billing by IP failed: {str(exc)[:100]}", "[WARN] ")
            profile = {}
        # KHÔNG log nguồn ở đây nữa: `geo_billing_from_proxy()` đã log rõ từng mức
        # ("mức 2 (PIN xác thực)" / "mức 1 (ipwho+Nominatim)"). Log thêm một dòng
        # chung ở đây từng ghi sai tên mức — kết quả mức 2 nhưng vẫn đề "mức 1".
    if not profile:
        profile = generate_dynamic_in_billing()
        log("billing using the hardcoded list (%s) — neither tier 2 nor tier 1 could build an address"
            % profile["city"], "[WARN] ")
    profile.pop("_ip", None)

    env_map = {
        "email": "UPI_EMAIL",
        "name": "UPI_NAME",
        "country": "UPI_BILLING_COUNTRY",
        "line1": "UPI_LINE1",
        "line2": "UPI_LINE2",
        "city": "UPI_CITY",
        "postal_code": "UPI_POSTAL_CODE",
        "state": "UPI_STATE",
        "vpa": "UPI_VPA",
        "phone": "UPI_PHONE",
    }
    for key, env_name in env_map.items():
        value = str(env_value(env_name, "") or "").strip()
        if value:
            profile[key] = value
    profile["country"] = normalize_country(profile.get("country", "IN"))
    profile["phone"] = profile.get("phone") or default_in_phone()
    # 记下 vpa 是不是「人给的」。见 should_send_vpa()：只有人给的真 VPA 才值得
    # 传给 Stripe，从随机 phone 推出来的那个会让整条流程拿不到链接。
    profile["vpa_explicit"] = "1" if str(profile.get("vpa") or "").strip() else ""
    profile["vpa"] = normalize_vpa(profile.get("vpa", ""), profile.get("phone", ""))
    return profile


def should_send_vpa(billing: dict[str, str]) -> bool:
    """要不要把 `upi[vpa]` 塞进 payment_method。

    默认**不塞**。这是从 upi-zero-link 移过来的结论，dump 里能直接看到证据：

        传 upi[vpa]   -> next_action.type = upi_await_notification
                         （Stripe 向这个 VPA 发收款请求，没有 QR / 没有链接）
        不传 upi[vpa] -> next_action.upi_handle_redirect_or_display_qr_code
                         （hosted_instructions_url + QR，这才是要交付的东西）

    而这里 billing["vpa"] 默认是从**随机生成的 phone** 推出来的（`9xxxxxxxxx@ybl`），
    等于每次都在向一个不存在的 UPI 账号发收款请求 —— 桩桩都拿不到链接，
    连 approve 都过了也没用。只有人工用 `UPI_VPA` 指定了真 VPA，或显式设
    `UPI_SEND_VPA=1` 想走 collect-request 路线时，才把它传回去。
    """
    if env_bool("UPI_SEND_VPA", False):
        return True
    return bool(str(billing.get("vpa_explicit") or "").strip())


def default_in_phone() -> str:
    """Sinh số điện thoại Ấn Độ hợp lệ (10 chữ số, đầu 6-9) dùng làm VPA mặc định."""
    return "9" + "".join(str(random.randint(0, 9)) for _ in range(9))


def normalize_vpa(vpa: str, phone: str = "") -> str:
    """Chuẩn hoá UPI ID (VPA). Nếu không có, sinh từ phone thành dạng 10số@ybl.

    Lưu ý: VPA là địa chỉ do ngân hàng/ứng dụng UPI cấp và phải nằm trên mạng UPI
    (NPCI) thì Stripe mới tạo được mandate autopay. Một VPA hư cấu không có tài
    khoản ngân hàng thật vẫn sẽ bị Stripe từ chối; override bằng UPI_VPA để dùng
    VPA thật.
    """
    vpa = str(vpa or "").strip()
    if vpa:
        return vpa
    digits = re.sub(r"\D", "", phone or "")
    if len(digits) >= 10:
        return f"{digits[:10]}@ybl"
    # fallback: sinh số nếu không có phone hợp lệ
    return "9" + "".join(str(random.randint(0, 9)) for _ in range(9)) + "@ybl"


def stripe_update_customer_data(
    stripe: requests.Session,
    cs_id: str,
    stripe_pk: str,
    ctx: dict[str, Any],
    billing: dict[str, str],
) -> bool:
    body: dict[str, Any] = {
        "key": stripe_pk,
        "_stripe_version": STRIPE_VERSION_FULL,
        "expected_amount": str(ctx.get("checkout_amount") or 0),
        "elements_session_client[session_id]": ctx["elements_session_id"],
        "elements_session_client[stripe_js_id]": ctx["stripe_js_id"],
        "elements_session_client[referrer_host]": "chatgpt.com",
        "elements_session_client[elements_init_source]": "custom_checkout",
        "elements_session_client[locale]": ctx["locale"],
        "elements_session_client[client_betas][0]": "custom_checkout_server_updates_1",
        "elements_session_client[client_betas][1]": "custom_checkout_manual_approval_1",
        "customer_data[email]": billing["email"],
        "customer_data[name]": billing["name"],
        "customer_data[address][country]": billing["country"],
        "customer_data[address][line1]": billing["line1"],
        "customer_data[address][city]": billing["city"],
        "customer_data[address][postal_code]": billing["postal_code"],
    }
    if billing.get("line2"):
        body["customer_data[address][line2]"] = billing["line2"]
    if billing.get("state"):
        body["customer_data[address][state]"] = billing["state"]

    url = f"https://api.stripe.com/v1/payment_pages/{cs_id}"
    try:
        resp = stripe.post(url, data=body, timeout=DEFAULT_TIMEOUT)
        dump_http(resp, "customer_data_update_br", body, "POST", url, force=resp.status_code >= 400)
        if resp.status_code < 400:
            log(
                f"IN customer_data submitted: {billing['name']} / {billing['city']} / {billing['postal_code']}"
            )
            return True
        if is_checkout_not_active_error(resp.text):
            raise RuntimeError("checkout_not_active_session")
        log(f"IN customer_data submit failed HTTP {resp.status_code}: {resp.text[:180]}", "[WARN] ")
    except Exception as exc:
        if is_checkout_not_active_error(exc):
            raise
        log(f"IN customer_data submit error: {exc}", "[WARN] ")
    return False


def stripe_update_tax_region(
    stripe: requests.Session,
    cs_id: str,
    stripe_pk: str,
    ctx: dict[str, Any],
    billing: dict[str, str],
) -> bool:
    body: dict[str, Any] = {
        "key": stripe_pk,
        "_stripe_version": STRIPE_VERSION_FULL,
        "elements_session_client[session_id]": ctx["elements_session_id"],
        "elements_session_client[stripe_js_id]": ctx["stripe_js_id"],
        "elements_session_client[referrer_host]": "chatgpt.com",
        "elements_session_client[elements_init_source]": "custom_checkout",
        "elements_session_client[is_aggregation_expected]": "false",
        "elements_session_client[locale]": ctx["locale"],
        "elements_session_client[client_betas][0]": "custom_checkout_server_updates_1",
        "elements_session_client[client_betas][1]": "custom_checkout_manual_approval_1",
        "elements_options_client[saved_payment_method][enable_save]": saved_payment_value(),
        "elements_options_client[saved_payment_method][enable_redisplay]": saved_payment_value(),
        "client_attribution_metadata[merchant_integration_additional_elements][0]": "expressCheckout",
        "client_attribution_metadata[merchant_integration_additional_elements][1]": "payment",
        "client_attribution_metadata[merchant_integration_additional_elements][2]": "address",
        "tax_region[country]": billing["country"],
        "tax_region[postal_code]": billing["postal_code"],
        "tax_region[line1]": billing["line1"],
        "tax_region[city]": billing["city"],
    }
    if billing.get("state"):
        body["tax_region[state]"] = billing["state"]

    url = f"https://api.stripe.com/v1/payment_pages/{cs_id}"
    try:
        resp = stripe.post(url, data=body, timeout=DEFAULT_TIMEOUT)
        dump_http(resp, "tax_region_update", body, "POST", url, force=resp.status_code >= 400)
        if resp.status_code < 400:
            log(f"tax_region submitted: {billing['country']} / {billing['city']} {billing['postal_code']}")
            return True
        if is_checkout_not_active_error(resp.text):
            raise RuntimeError("checkout_not_active_session")
        log(f"tax_region submit failed HTTP {resp.status_code}: {resp.text[:180]}", "[WARN] ")
    except Exception as exc:
        if is_checkout_not_active_error(exc):
            raise
        log(f"tax_region submit error: {exc}", "[WARN] ")
    return False


def checkout_snapshot(chatgpt: requests.Session, checkout: dict[str, str], billing: dict[str, str]) -> None:
    cs_id = checkout["cs_id"]
    processor = processor_entity_for_country(checkout.get("billing_country", "IN"), checkout.get("processor_entity") or "")
    checkout_page_url = f"https://chatgpt.com/checkout/{processor}/{cs_id}"
    body = {
        "snapshot": {
            "billing_address": {
                "name": billing["name"],
                "address": {
                    "line1": billing["line1"],
                    "city": billing["city"],
                    "country": billing["country"],
                    "postal_code": billing["postal_code"],
                    "state": billing.get("state", ""),
                },
            }
        }
    }
    try:
        resp = chatgpt.post(
            "https://chatgpt.com/backend-api/payments/checkout/snapshot",
            json=body,
            headers={
                "Referer": checkout_page_url,
                "x-openai-target-path": "/backend-api/payments/checkout/snapshot",
                "x-openai-target-route": "/backend-api/payments/checkout/snapshot",
            },
            timeout=CHATGPT_TIMEOUT,
        )
        dump_http(resp, "checkout_snapshot", body, "POST", "https://chatgpt.com/backend-api/payments/checkout/snapshot", force=env_bool("UPI_DUMP_WARMUP", False) or resp.status_code >= 400)
        if resp.status_code >= 400:
            if is_checkout_not_active_error(resp.text):
                raise RuntimeError("checkout_not_active_session")
            log(f"checkout snapshot failed HTTP {resp.status_code}: {resp.text[:180]}", "[WARN] ")
        else:
            log("checkout snapshot submitted")
    except Exception as exc:
        if is_checkout_not_active_error(exc):
            raise
        log(f"checkout snapshot error: {exc}", "[WARN] ")


def stripe_create_upi_pm(stripe: requests.Session, cs_id: str, stripe_pk: str, billing: dict[str, str], ctx: dict[str, Any]) -> str:
    body: dict[str, Any] = {
        "billing_details[name]": billing.get("name") or "Aisha Sharma",
        "billing_details[email]": billing.get("email") or "redacted@example.invalid",
        "billing_details[address][country]": billing.get("country") or "IN",
        "billing_details[address][line1]": billing.get("line1") or "24 Park Street",
        "billing_details[address][city]": billing.get("city") or "Kolkata",
        "billing_details[address][postal_code]": billing.get("postal_code") or "700016",
        "type": "upi",
        "client_attribution_metadata[checkout_session_id]": cs_id,
        "key": stripe_pk,
    }
    if should_send_vpa(billing):
        body["upi[vpa]"] = billing.get("vpa") or ""
    if billing.get("state"):
        body["billing_details[address][state]"] = billing["state"]

    resp = stripe.post("https://api.stripe.com/v1/payment_methods", data=body, timeout=DEFAULT_TIMEOUT)
    dump_http(resp, "upi_pm", body, "POST", "https://api.stripe.com/v1/payment_methods", force=resp.status_code >= 400)
    if resp.status_code >= 400:
        raise RuntimeError(f"UPI PM creation failed HTTP {resp.status_code}: {resp.text[:500]}")
    pm_id = str((resp.json() or {}).get("id") or "")
    if not pm_id.startswith("pm_"):
        raise RuntimeError(f"UPI PM creation returned unexpected response: {resp.text[:300]}")
    return pm_id


def add_inline_upi_payment_method_data(body: dict[str, Any], cs_id: str, billing: dict[str, str], ctx: dict[str, Any]) -> None:
    body.update(
        {
            "payment_method_data[type]": "upi",
            "payment_method_data[allow_redisplay]": "limited",
            "payment_method_data[billing_details][name]": billing["name"],
            "payment_method_data[billing_details][email]": billing["email"],
            "payment_method_data[billing_details][address][country]": billing["country"],
            "payment_method_data[billing_details][address][line1]": billing["line1"],
            "payment_method_data[billing_details][address][city]": billing["city"],
            "payment_method_data[billing_details][address][postal_code]": billing["postal_code"],
            "payment_method_data[payment_user_agent]": f"stripe.js/{random_runtime_version()}; stripe-js-v3/{random_runtime_version()}; payment-element; deferred-intent",
            "payment_method_data[referrer]": "https://chatgpt.com",
            "payment_method_data[time_on_page]": str(random.randint(18000, 55000)),
            "payment_method_data[client_attribution_metadata][checkout_session_id]": cs_id,
            "payment_method_data[client_attribution_metadata][client_session_id]": ctx["stripe_js_id"],
            "payment_method_data[client_attribution_metadata][checkout_config_id]": ctx.get("config_id") or "",
            "payment_method_data[client_attribution_metadata][elements_session_id]": ctx["elements_session_id"],
            "payment_method_data[client_attribution_metadata][elements_session_config_id]": ctx["elements_session_config_id"],
            "payment_method_data[client_attribution_metadata][merchant_integration_source]": "elements",
            "payment_method_data[client_attribution_metadata][merchant_integration_subtype]": "payment-element",
            "payment_method_data[client_attribution_metadata][merchant_integration_version]": "2021",
            "payment_method_data[client_attribution_metadata][payment_intent_creation_flow]": "deferred",
            "payment_method_data[client_attribution_metadata][payment_method_selection_flow]": "automatic",
            "payment_method_data[client_attribution_metadata][merchant_integration_additional_elements][0]": "expressCheckout",
            "payment_method_data[client_attribution_metadata][merchant_integration_additional_elements][1]": "payment",
            "payment_method_data[client_attribution_metadata][merchant_integration_additional_elements][2]": "address",
        }
    )
    if should_send_vpa(billing):
        body["payment_method_data[upi][vpa]"] = billing.get("vpa") or ""
    if billing.get("state"):
        body["payment_method_data[billing_details][address][state]"] = billing["state"]


def processor_entity_for_country(country: str, processor_entity: str = "") -> str:
    if processor_entity:
        return processor_entity
    return "openai_llc" if normalize_country(country) == "US" else "openai_ie"


def stripe_checkout_long_url(cs_id: str, country: str, processor_entity: str) -> str:
    processor = processor_entity_for_country(country, processor_entity)
    success = f"https://chatgpt.com/checkout/verify?stripe_session_id={cs_id}&processor_entity={processor}&plan_type=plus"
    return (
        f"https://checkout.stripe.com/c/pay/{cs_id}"
        f"?returned_from_redirect=true&ui_mode=custom&return_url={quote(success, safe='')}"
    )


def to_openai_pay_url(stripe_hosted_url: str) -> str:
    url = str(stripe_hosted_url or "").strip()
    if not url:
        return ""
    if url.startswith("https://checkout.stripe.com"):
        return "https://pay.openai.com" + url[len("https://checkout.stripe.com") :]
    parsed = urlsplit(url)
    if parsed.netloc.lower() == "checkout.stripe.com":
        return urlunsplit((parsed.scheme or "https", "pay.openai.com", parsed.path, parsed.query, parsed.fragment))
    return url


def stripe_confirm_return_url(cs_id: str, checkout: dict[str, str], stripe_hosted_url: str) -> str:
    country = normalize_country(checkout.get("billing_country") or "IN")
    processor = processor_entity_for_country(country, checkout.get("processor_entity") or "")
    success = f"https://chatgpt.com/checkout/verify?stripe_session_id={cs_id}&processor_entity={processor}&plan_type=plus"
    hosted = to_openai_pay_url(stripe_hosted_url) or stripe_checkout_long_url(cs_id, country, processor)
    if "pay.openai.com/" in hosted or "checkout.stripe.com/" in hosted:
        parsed = urlsplit(hosted)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query.setdefault("success_return_url", success)
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment))
    return hosted


def stripe_confirm_upi(
    stripe: requests.Session,
    cs_id: str,
    pm_id: str,
    stripe_pk: str,
    init_payload: dict[str, Any],
    ctx: dict[str, Any],
    checkout: dict[str, str],
    stripe_hosted_url: str,
    billing: dict[str, str],
) -> dict[str, Any]:
    runtime_version = str(ctx.get("runtime_version") or DEFAULT_STRIPE_RUNTIME_VERSION)
    body = {
        "eid": "NA",
        "expected_amount": str(env_value("PP_EXPECTED_AMOUNT", "") or "").strip() or str(ctx.get("checkout_amount") or amount_from_payload(init_payload)),
        "expected_payment_method_type": "upi",
        "return_url": stripe_confirm_return_url(cs_id, checkout, stripe_hosted_url),
        "_stripe_version": str(ctx.get("stripe_version") or STRIPE_VERSION_FULL),
        "guid": str(ctx.get("guid") or stripe_browser_id()),
        "muid": str(ctx.get("muid") or stripe_browser_id()),
        "sid": str(ctx.get("sid") or stripe_browser_id()),
        "key": stripe_pk,
        "version": runtime_version,
        "init_checksum": str(init_payload.get("init_checksum") or ctx.get("init_checksum") or ""),
        "client_attribution_metadata[client_session_id]": str(ctx.get("client_session_id") or ctx["stripe_js_id"]),
        "client_attribution_metadata[checkout_session_id]": cs_id,
        "client_attribution_metadata[checkout_config_id]": ctx.get("config_id") or "",
        "client_attribution_metadata[merchant_integration_source]": "checkout",
        "client_attribution_metadata[merchant_integration_subtype]": "payment-element",
        "client_attribution_metadata[merchant_integration_version]": "custom_checkout",
        "client_attribution_metadata[payment_intent_creation_flow]": "deferred",
        "client_attribution_metadata[payment_method_selection_flow]": "automatic",
        "client_attribution_metadata[elements_session_id]": ctx["elements_session_id"],
        "client_attribution_metadata[elements_session_config_id]": ctx["elements_session_config_id"],
        "client_attribution_metadata[merchant_integration_additional_elements][0]": "payment",
        "client_attribution_metadata[merchant_integration_additional_elements][1]": "address",
        "consent[terms_of_service]": "accepted",
        "link_brand": "link",
    }
    body.update(stripe_elements_session_params(ctx))
    if env_bool("UPI_CONFIRM_INLINE_PM", False):
        add_inline_upi_payment_method_data(body, cs_id, billing, ctx)
    else:
        body["payment_method"] = pm_id

    # GHI CHU (da thu va that bai): KHONG the gui mandate o day.
    # `payment_pages/{cs}/confirm` tra HTTP 400 parameter_unknown cho ca
    # `mandate_data` lan `setup_future_usage` (top-level). HAR tham chieu
    # (2026-09-03-...json) co 10 lan `setup_future_usage` nhung 0 lan `mandate_data`
    # -> 2 param nay chi hop le o /v1/confirmation_tokens (luong oaics_), khong phai
    # o payment_pages. Neu can mandate cho luong cs_ thi phai tim cho khac.
    url = f"https://api.stripe.com/v1/payment_pages/{cs_id}/confirm"
    resp = stripe.post(url, data=body, timeout=stripe_heavy_timeout())
    dump_http(resp, "upi_confirm", body, "POST", url, force=True)
    if resp.status_code >= 400:
        raise RuntimeError(f"UPI confirm failed HTTP {resp.status_code}: {resp.text[:500]}")
    return resp.json() or {}


def collect_urls(payload: Any, urls: list[str] | None = None) -> list[str]:
    found = urls if urls is not None else []
    if isinstance(payload, str):
        for match in re.findall(r"https?://[^\s\"'<>]+", payload):
            found.append(match.rstrip("),.;]"))
        for match in re.findall(r"data:image/(?:png|svg\+xml|jpeg);base64,[A-Za-z0-9+/=]+", payload):
            found.append(match)
    elif isinstance(payload, dict):
        for value in payload.values():
            collect_urls(value, found)
    elif isinstance(payload, list):
        for item in payload:
            collect_urls(item, found)
    return found


def is_resource_url(url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    if is_known_static_host(url):
        return True
    return path.endswith(
        (
            ".js",
            ".css",
            ".map",
            ".woff",
            ".woff2",
            ".ttf",
            ".otf",
            ".ico",
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".svg",
            ".webp",
        )
    )


def is_known_static_host(url: str) -> bool:
    host = (urlparse(url).netloc or "").lower()
    return host in {
        "stripe-camo.global.ssl.fastly.net",
        "files.stripe.com",
        "js.stripe.com",
        "m.stripe.network",
        "q.stripe.com",
    }


def is_upi_instructions_url(url: str) -> bool:
    parsed = urlparse(str(url or "").strip())
    return (
        parsed.scheme in {"http", "https"}
        and (parsed.netloc or "").lower() == "payments.stripe.com"
        and (parsed.path or "").lower().startswith("/upi/instructions/")
    )


def is_redirect_like_url(url: str, from_action_field: bool = False) -> bool:
    if not isinstance(url, str):
        return False
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        return False
    if is_resource_url(url):
        return False
    if is_upi_instructions_url(url):
        return True
    if from_action_field:
        return True

    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    query = (parsed.query or "").lower()
    text = f"{host}{path}?{query}"
    if host in {"hooks.stripe.com", "payments.stripe.com"}:
        return True
    return any(part in text for part in ("upi", "/redirect/", "redirect_to_url", "authenticate"))


_QR_TOKEN_RE = re.compile(r"(?:^|[/._-])qr(?:code)?(?:[/._-]|$)|qrcode|qr[-_]code", re.I)


def is_qr_candidate(url: str) -> bool:
    """True neu URL trong nhu anh/trang QR.

    Truoc day dung `"qr" in url` -> khop ca chuoi random trong session id
    (vd cs_live_a15gVPqel9QtQrSSw...) nen bao "QR candidate" gia.
    Gio yeu cau 'qr' phai la mot token rieng (dau/cuoi hoac sau dau phan cach).
    """
    lower = url.lower()
    if lower.startswith("data:image/"):
        return True
    return bool(_QR_TOKEN_RE.search(lower))


def extract_qr_candidates(payload: Any) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for url in collect_urls(payload):
        if url in seen:
            continue
        seen.add(url)
        if is_qr_candidate(url) and not is_known_static_host(url):
            result.append(url)
    return result


def find_submission_attempt(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        value = payload.get("submission_attempt")
        if isinstance(value, dict):
            return value
        for item in payload.values():
            nested = find_submission_attempt(item)
            if nested:
                return nested
    elif isinstance(payload, list):
        for item in payload:
            nested = find_submission_attempt(item)
            if nested:
                return nested
    return {}


def extract_intent_info(payload: Any) -> dict[str, str]:
    """Tim setup_intent hoac payment_intent id + client_secret trong payload bat ky.

    Tra ve dict {"id": "seti_...", "client_secret": "seti_..._secret_...", "path": "setup_intents"}
    hoac {} neu khong tim thay.
    """
    if isinstance(payload, dict):
        obj_type = str(payload.get("object") or "").strip()
        pid = str(payload.get("id") or "").strip()
        cs = str(payload.get("client_secret") or "").strip()
        if cs and pid:
            if obj_type == "setup_intent" or pid.startswith("seti_"):
                return {"id": pid, "client_secret": cs, "path": "setup_intents"}
            if obj_type == "payment_intent" or pid.startswith("pi_"):
                return {"id": pid, "client_secret": cs, "path": "payment_intents"}
        for key in ("setup_intent", "payment_intent"):
            value = payload.get(key)
            if isinstance(value, dict):
                found = extract_intent_info(value)
                if found:
                    return found
        for value in payload.values():
            if isinstance(value, (dict, list)):
                found = extract_intent_info(value)
                if found:
                    return found
    elif isinstance(payload, list):
        for item in payload:
            found = extract_intent_info(item)
            if found:
                return found
    return {}


def poll_intent_instructions(
    stripe: requests.Session,
    intent_info: dict[str, str],
    stripe_pk: str,
    timeout_secs: int = 30,
    current_pm_id: str = "",
) -> str:
    """Poll SetupIntent/PaymentIntent API de lay hosted_instructions_url.

    Sau khi approve thanh cong va submission state = processing, SetupIntent
    can thoi gian de chuyen sang requires_action voi
    next_action.upi_handle_redirect_or_display_qr_code.hosted_instructions_url.
    """
    intent_id = intent_info.get("id", "")
    client_secret = intent_info.get("client_secret", "")
    intent_path = intent_info.get("path", "setup_intents")
    if not intent_id or not client_secret:
        return ""
    url = f"https://api.stripe.com/v1/{intent_path}/{intent_id}"
    params = {"key": stripe_pk, "client_secret": client_secret}
    deadline = time.time() + timeout_secs
    log(f"polling {intent_path}/{intent_id[:20]}... for hosted_instructions_url (up to {timeout_secs}s)")
    while time.time() < deadline:
        try:
            resp = stripe.get(url, params=params, timeout=stripe_heavy_timeout())
        except Exception as exc:
            log(f"intent poll error: {str(exc)[:120]}", "[WARN] ")
            time.sleep(2)
            continue
        if resp.status_code != 200:
            time.sleep(2)
            continue
        try:
            payload = resp.json() or {}
        except Exception:
            time.sleep(2)
            continue
        raise_if_setup_intent_blocked(payload, "intent poll", current_pm_id=current_pm_id)
        redirect_url = extract_redirect_url(payload)
        if redirect_url:
            dump_http(resp, "intent_poll_success", params, "GET", url, force=True)
            log(f"intent poll found URL: {redirect_url[:180]}")
            return redirect_url
        status = str(payload.get("status") or "").strip()
        if status in ("succeeded", "canceled", "cancelled"):
            log(f"intent poll: intent status={status}, no instructions URL", "[WARN] ")
            break
        time.sleep(2)
    log("intent poll: timeout, no hosted_instructions_url found", "[WARN] ")
    return ""


def extract_redirect_url(payload: Any, path: tuple[str, ...] = ()) -> str:
    if isinstance(payload, dict):
        next_action = payload.get("next_action")
        if isinstance(next_action, dict):
            hosted_instructions_url = str(next_action.get("hosted_instructions_url") or "").strip()
            if is_upi_instructions_url(hosted_instructions_url):
                return hosted_instructions_url
            redirect = next_action.get("redirect_to_url")
            if isinstance(redirect, dict):
                url = str(redirect.get("url") or "").strip()
                if is_redirect_like_url(url, True):
                    return url
            for key in ("url", "redirect_url", "redirect_to_url", "hosted_url", "hosted_instructions_url"):
                value = next_action.get(key)
                if is_redirect_like_url(value, True):
                    return value

        for key in (
            "hosted_instructions_url",
            "redirect_url",
            "redirect_to_url",
            "authorization_url",
            "authentication_url",
        ):
            value = payload.get(key)
            if is_redirect_like_url(value, True):
                return value

        for key, value in payload.items():
            nested = extract_redirect_url(value, path + (str(key),))
            if nested:
                return nested
    elif isinstance(payload, list):
        for index, item in enumerate(payload):
            nested = extract_redirect_url(item, path + (str(index),))
            if nested:
                return nested

    return ""


def first_value_by_key(payload: Any, key: str) -> Any:
    if isinstance(payload, dict):
        if key in payload:
            return payload[key]
        for value in payload.values():
            found = first_value_by_key(value, key)
            if found not in (None, "", [], {}):
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = first_value_by_key(item, key)
            if found not in (None, "", [], {}):
                return found
    return None


def setup_intent_last_error(payload: Any, current_pm_id: str = "") -> str:
    if isinstance(payload, dict):
        payload_id = str(payload.get("id") or "").strip()
        is_setup_intent = payload.get("object") == "setup_intent" or payload_id.startswith("seti_")
        last_error = payload.get("last_setup_error") if is_setup_intent else None
        setup_intent = payload.get("setup_intent")
        if not last_error and isinstance(setup_intent, dict):
            last_error = setup_intent.get("last_setup_error")
        if last_error:
            if current_pm_id and isinstance(last_error, dict):
                error_pm = last_error.get("payment_method")
                error_pm_id = ""
                if isinstance(error_pm, dict):
                    error_pm_id = str(error_pm.get("id") or "").strip()
                elif isinstance(error_pm, str):
                    error_pm_id = error_pm.strip()
                if error_pm_id and error_pm_id != current_pm_id:
                    last_error = None
            if last_error:
                try:
                    return json.dumps(last_error, ensure_ascii=False)[:700]
                except Exception:
                    return str(last_error)[:700]
        for value in payload.values():
            found = setup_intent_last_error(value, current_pm_id=current_pm_id)
            if found:
                return found
    elif isinstance(payload, list):
        for value in payload:
            found = setup_intent_last_error(value, current_pm_id=current_pm_id)
            if found:
                return found
    return ""


def raise_if_setup_intent_blocked(payload: Any, context: str, current_pm_id: str = "") -> None:
    last_error = setup_intent_last_error(payload, current_pm_id=current_pm_id)
    if not last_error:
        return
    if "generic_decline" in last_error.lower():
        raise RuntimeError(f"Stripe risk decline (generic_decline): {context} SetupIntent creation failed; no redirect_url generated")
    raise RuntimeError(f"{context}: setup_intent.last_setup_error: {last_error}")


def should_retry_second_confirm_after_approve(error: Any) -> bool:
    text = str(error or "").lower()
    return (
        "checkout_upcoming_invoice_mismatch" in text
        or "redirect url resolution timeout" in text
        or "missing_redirect" in text
    )


def stripe_intent_redirect_url(
    stripe: requests.Session,
    intent_payload: Any,
    stripe_pk: str,
    current_pm_id: str = "",
) -> str:
    if not isinstance(intent_payload, dict):
        return ""
    intent_id = str(intent_payload.get("id") or "").strip()
    client_secret = str(intent_payload.get("client_secret") or "").strip()
    if not intent_id or not client_secret:
        return ""
    intent_object = str(intent_payload.get("object") or "").strip()
    intent_path = "setup_intents" if intent_object == "setup_intent" or intent_id.startswith("seti_") else "payment_intents"
    params = {"key": stripe_pk, "client_secret": client_secret}
    url = f"https://api.stripe.com/v1/{intent_path}/{intent_id}"
    # Proxy IN hay dut giua chung; truoc day 1 lan loi la mat ca vong, du PaymentIntent
    # dang requires_action (QR da san). Thu lai vai lan roi moi bo.
    resp = None
    last_exc: Exception | None = None
    for attempt in range(1, 4):
        try:
            resp = stripe.get(url, params=params, timeout=stripe_heavy_timeout())
            break
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < 3:
                log(f"stripe intent get loi (lan {attempt}/3), thu lai: {str(exc)[:110]}", "[WARN] ")
                time.sleep(1.5 * attempt)
    if resp is None:
        log(f"stripe intent get that bai sau 3 lan: {str(last_exc)[:160]}", "[WARN] ")
        return ""
    dump_http(resp, "stripe_intent_get", params, "GET", url, force=resp.status_code >= 400)
    if resp.status_code != 200:
        return ""
    try:
        payload = resp.json() or {}
    except Exception:
        payload = {"_raw_text": resp.text}
    raise_if_setup_intent_blocked(payload, "stripe intent", current_pm_id=current_pm_id)
    redirect_url = extract_redirect_url(payload)
    if redirect_url:
        dump_http(resp, "stripe_intent_redirect", params, "GET", url, force=True)
        log(f"read Stripe intent redirect_url: {redirect_url[:180]}")
    return redirect_url


def stripe_payload_intent_redirect_url(
    stripe: requests.Session,
    payload: Any,
    stripe_pk: str,
    current_pm_id: str = "",
) -> str:
    if not isinstance(payload, dict):
        return ""
    for intent_key in ("setup_intent", "payment_intent"):
        candidates: list[Any] = []
        direct = payload.get(intent_key)
        if isinstance(direct, dict):
            candidates.append(direct)
        nested = first_value_by_key(payload, intent_key)
        if isinstance(nested, dict) and all(nested is not item for item in candidates):
            candidates.append(nested)
        for intent_payload in candidates:
            redirect_url = stripe_intent_redirect_url(stripe, intent_payload, stripe_pk, current_pm_id=current_pm_id)
            if redirect_url:
                return redirect_url
    return ""


def infer_processor_entity(payload: Any) -> str:
    for value in collect_strings(payload):
        match = re.search(r"[?&]processor_entity=([A-Za-z0-9_]+)", value)
        if match:
            return match.group(1)
    return ""


def payment_page_summary(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    elements_options = payload.get("elements_options") if isinstance(payload.get("elements_options"), dict) else {}
    submission = find_submission_attempt(payload)
    next_action = first_value_by_key(payload, "next_action")
    payment_intent = first_value_by_key(payload, "payment_intent")
    setup_intent = first_value_by_key(payload, "setup_intent")
    summary: dict[str, Any] = {
        "object": payload.get("object"),
        "id": payload.get("id"),
        "status": payload.get("status"),
        "payment_status": payload.get("payment_status"),
        "amount": elements_options.get("amount") if elements_options else first_value_by_key(payload, "amount"),
        "currency": payload.get("currency") or (elements_options.get("currency") if elements_options else None),
        "mode": elements_options.get("mode") if elements_options else payload.get("mode"),
        "payment_method_types": elements_options.get("payment_method_types") if elements_options else None,
        "submission_state": submission.get("state") if submission else None,
        "submission_status": submission.get("status") if submission else None,
        "has_next_action": isinstance(next_action, dict) and bool(next_action),
    }
    if isinstance(payment_intent, dict):
        summary["payment_intent_status"] = payment_intent.get("status")
    elif isinstance(payment_intent, str):
        summary["payment_intent"] = payment_intent
    if isinstance(setup_intent, dict):
        summary["setup_intent_status"] = setup_intent.get("status")
    elif isinstance(setup_intent, str):
        summary["setup_intent"] = setup_intent
    return {key: value for key, value in summary.items() if value not in (None, "", [], {})}


def log_payment_page_summary(stage: str, payload: Any) -> None:
    summary = payment_page_summary(payload)
    if not summary:
        return
    compact = format_payment_summary(summary)
    log(f"{stage} response summary: {compact}")


def format_payment_summary(summary: dict[str, Any]) -> str:
    return ", ".join(f"{key}={value}" for key, value in summary.items())


def warmup_approve_context(chatgpt: requests.Session, checkout_page_url: str) -> None:
    try:
        resp = chatgpt.post(
            "https://chatgpt.com/backend-api/sentinel/ping",
            json={},
            headers={
                "Referer": "https://chatgpt.com/",
                "x-openai-target-path": "/backend-api/sentinel/ping",
                "x-openai-target-route": "/backend-api/sentinel/ping",
            },
            timeout=CHATGPT_TIMEOUT,
        )
        dump_http(resp, "sentinel_ping", {}, "POST", "https://chatgpt.com/backend-api/sentinel/ping", force=env_bool("UPI_DUMP_WARMUP", False))
    except Exception as exc:
        log(f"approve sentinel request error: {exc}", "[WARN] ")


def chatgpt_approve(chatgpt: requests.Session, checkout: dict[str, str]) -> None:
    cs_id = checkout["cs_id"]
    processor = processor_entity_for_country(checkout.get("billing_country", "IN"), checkout.get("processor_entity", ""))
    checkout_page_url = f"https://chatgpt.com/checkout/{processor}/{cs_id}"
    if env_bool("UPI_APPROVE_WARMUP", True):
        warmup_approve_context(chatgpt, checkout_page_url)
        time.sleep(random.uniform(0.8, 1.6))

    body = {"checkout_session_id": cs_id, "processor_entity": processor}
    headers = {
        "Referer": checkout_page_url,
        "x-openai-target-path": "/backend-api/payments/checkout/approve",
        "x-openai-target-route": "/backend-api/payments/checkout/approve",
    }
    resp = chatgpt.post(
        "https://chatgpt.com/backend-api/payments/checkout/approve",
        json=body,
        headers=headers,
        timeout=CHATGPT_TIMEOUT,
    )
    dump_http(resp, "approve", body, "POST", "https://chatgpt.com/backend-api/payments/checkout/approve", force=True)
    if resp.status_code >= 400:
        raise RuntimeError(f"ChatGPT approve failed HTTP {resp.status_code}: {resp.text[:300]}")
    result = ""
    try:
        result = str((resp.json() or {}).get("result") or "")
    except Exception:
        pass
    if result != "approved":
        raise RuntimeError(f"ChatGPT approve rejected: {result or resp.text[:200]}")


def approve_attempt(
    access_token: str,
    device_id: str,
    checkout: dict[str, str],
    session_token: str,
    proxy: str,
    index: int,
    attempt_count: int,
) -> None:
    log(f"approve attempt {index}/{attempt_count} / proxy={proxy_label(proxy)}")
    chatgpt = build_chatgpt_session(access_token, device_id, proxy, session_token)
    chatgpt_approve(chatgpt, checkout)


def log_approve_failure(error: str) -> bool:
    log(f"approve failed: {error[:180]}", "[WARN] ")
    if "chatgpt approve rejected: blocked" in error.lower():
        log("approve returned blocked; treating as account/checkout risk; not recording proxy failure", "[WARN] ")
        return True
    return False


def is_approve_failure_error(error: str) -> bool:
    text = str(error or "").lower()
    return "approve" in text or "chatgpt approve" in text


def is_account_risk_error(error: Any) -> bool:
    """Classify account/customer-level risk errors (NOT proxy failures).

    HAR: OpenAI approve returns {"result":"blocked"}, or Stripe declines when
    creating the SetupIntent with `setup_intent.last_setup_error.code=generic_decline`
    after approve already returned 'approved'. Both are the same server-side risk
    gate, not a proxy error. If treated as a proxy error, the tool will retry/rotate
    proxy on the SAME account and burn more attempts, heating the account instead of
    stopping + rotating. This detects them early.
    """
    text = str(error or "").lower()
    if not text:
        return False
    signals = (
        "approve rejected: blocked",
        "approve returned blocked",
        "result\":\"blocked",
        'result":"blocked',
        "generic_decline",
        "setup_intent.last_setup_error",
        "setupintent creation failed",
        "risk decline",
        "risk declined",
        "risk_decline",
        "do_not_honor",
        "approve blocked",
    )
    return any(sig in text for sig in signals)


def is_stripe_risk_decline_error(error: Any) -> bool:
    """Stripe-specific decline error (generic_decline ...) when creating the SetupIntent."""
    text = str(error or "").lower()
    return "generic_decline" in text or "setup_intent" in text or "setupintent creation failed" in text


def approve_with_retry(
    access_token: str,
    device_id: str,
    checkout: dict[str, str],
    proxies: list[str],
    session_token: str,
    proxy_group: str = "provider",
) -> str:
    max_retry = env_int("UPI_APPROVE_RETRY_MAX", 1)
    parallel = env_int("UPI_APPROVE_PARALLEL", 1)
    last_error = ""
    if max_retry <= 0:
        raise RuntimeError("approve retry count must be > 0")
    proxies = [proxy for proxy in dict.fromkeys(proxies) if proxy]
    if not proxies:
        raise RuntimeError("approve proxy is empty")
    sticky = env_bool("UPI_APPROVE_STICKY", True)
    if sticky and parallel > 1:
        log("approve failed; switching proxy and running sequentially in candidate order")
        parallel = 1
    if sticky:
        selected_proxies = proxies[:max_retry]
    else:
        attempt_count = min(max_retry, len(proxies))
        fixed_proxies = proxies[: min(2, attempt_count)]
        selected_proxies = fixed_proxies[:]
        remain_count = attempt_count - len(selected_proxies)
        if remain_count > 0:
            selected_proxies.extend(random.sample(proxies[2:], min(remain_count, len(proxies) - 2)))
    attempt_count = len(selected_proxies)
    log(f"approve proxy policy: {'sticky' if sticky else 'rotate'}")
    if parallel > 1:
        workers = min(parallel, attempt_count)
        log(f"approve concurrency: workers={workers}, attempts={attempt_count}")
        blocked_count = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    approve_attempt,
                    access_token,
                    device_id,
                    checkout,
                    session_token,
                    proxy,
                    index,
                    attempt_count,
                ): proxy
                for index, proxy in enumerate(selected_proxies, start=1)
            }
            for future in as_completed(futures):
                try:
                    future.result()
                    log("approve ok")
                    for pending in futures:
                        pending.cancel()
                    return futures[future]
                except Exception as exc:
                    last_error = str(exc)
                    if is_checkout_not_active_error(last_error):
                        raise RuntimeError("checkout_not_active_session")
                    if log_approve_failure(last_error):
                        blocked_count += 1
        if blocked_count and blocked_count == attempt_count:
            raise RuntimeError("approve blocked")
        raise RuntimeError(f"approve retry failed: {last_error}")

    blocked_count = 0
    for index, proxy in enumerate(selected_proxies, start=1):
        try:
            approve_attempt(access_token, device_id, checkout, session_token, proxy, index, attempt_count)
            log("approve ok")
            return proxy
        except Exception as exc:
            last_error = str(exc)
            if is_checkout_not_active_error(last_error):
                raise RuntimeError("checkout_not_active_session")
            if log_approve_failure(last_error):
                blocked_count += 1
            if index < attempt_count:
                time.sleep(random.uniform(1, 2))
    if blocked_count and blocked_count == attempt_count:
        raise RuntimeError("approve blocked")
    raise RuntimeError(f"approve retry failed: {last_error}")


def poll_payment_page(
    stripe: requests.Session,
    checkout: dict[str, str],
    stripe_pk: str,
    ctx: dict[str, Any],
    current_pm_id: str = "",
    confirm_payload: dict[str, Any] | None = None,
) -> tuple[str, list[str]]:
    cs_id = checkout["cs_id"]
    deadline = time.time() + env_int("UPI_POLL_TIMEOUT", 45)
    params = {
        **stripe_elements_session_params(ctx),
        "key": stripe_pk,
        "_stripe_version": str(ctx.get("stripe_version") or STRIPE_VERSION_FULL),
    }
    url = f"https://api.stripe.com/v1/payment_pages/{cs_id}"
    last_error = ""
    last_payload: dict[str, Any] = {}
    last_summary = ""
    while time.time() < deadline:
        resp = stripe.get(url, params=params, timeout=stripe_heavy_timeout())
        if resp.status_code >= 400:
            dump_http(resp, "poll_error", params, "GET", url, force=True)
            if is_checkout_not_active_error(resp.text):
                raise RuntimeError("checkout_not_active_session")
            last_error = f"HTTP {resp.status_code}"
            time.sleep(1)
            continue
        try:
            payload = resp.json() or {}
        except Exception:
            payload = {"_raw_text": resp.text}
        _last_intent_error = setup_intent_last_error(payload, current_pm_id=current_pm_id)
        if _last_intent_error:
            # Ghi lai raw response truoc khi raise, de xac minh bang mat chuoi
            # generic_decline trong last_setup_error (khong de loi bi nem mat).
            dump_http(resp, "poll_risk_blocked", params, "GET", url, force=True)
        raise_if_setup_intent_blocked(payload, "stripe payment_pages", current_pm_id=current_pm_id)
        last_payload = payload
        summary = payment_page_summary(payload)
        summary_text = format_payment_summary(summary) if summary else ""
        if summary_text and summary_text != last_summary:
            last_summary = summary_text
            log(f"poll response summary: {summary_text}")
        redirect_url = extract_redirect_url(payload)
        qr_urls = extract_qr_candidates(payload)
        if redirect_url or qr_urls:
            dump_http(resp, "poll_success", params, "GET", url, force=True)
            return redirect_url, qr_urls
        intent_redirect = stripe_payload_intent_redirect_url(stripe, payload, stripe_pk, current_pm_id=current_pm_id)
        if intent_redirect:
            dump_http(resp, "poll_success", params, "GET", url, force=True)
            return intent_redirect, qr_urls
        submission = find_submission_attempt(payload)
        if submission.get("state") == "requires_approval":
            last_error = "payment_pages still requires_approval"
            time.sleep(1)
            continue
        if submission.get("state") == "failed":
            dump_http(resp, "poll_failed", params, "GET", url, force=True)
            raise RuntimeError(f"Stripe submission failed: {submission}")
        if submission.get("state") == "processing":
            # payment_pages noi state=processing nhung khong co hosted_instructions_url.
            # Thu poll truc tiep SetupIntent/PaymentIntent API de lay URL.
            intent_info = extract_intent_info(payload) or extract_intent_info(confirm_payload or {})
            if intent_info:
                log(f"state=processing; polling intent {intent_info.get('path')}/{intent_info.get('id', '')[:20]}... directly")
                remaining = max(5, int(deadline - time.time()))
                instructions_url = poll_intent_instructions(stripe, intent_info, stripe_pk, timeout_secs=remaining, current_pm_id=current_pm_id)
                if instructions_url:
                    return instructions_url, qr_urls
        last_error = str(submission or "waiting")
        time.sleep(1)
    if last_payload:
        dump_response = type("DumpResponse", (), {})()
        dump_response.status_code = 200
        dump_response.url = url
        dump_response.text = json.dumps(last_payload, ensure_ascii=False, indent=2)
        dump_http(dump_response, "poll_no_redirect", params, "GET", url, force=True)
    log(f"poll ended; no real UPI redirect/QR found: {last_error}", "[WARN] ")
    raise RuntimeError(f"redirect url resolution timeout: {last_error}")


def fetch_redirect_page(stripe: requests.Session, start_url: str) -> list[str]:
    if not start_url or not env_bool("UPI_FOLLOW_REDIRECT", True):
        return []
    current = start_url
    qr_urls: list[str] = []
    for hop in range(1, 6):
        resp = stripe.get(current, timeout=DEFAULT_TIMEOUT, allow_redirects=False)
        dump_http(resp, f"redirect_hop_{hop}", None, "GET", current, force=True)
        qr_urls.extend(extract_qr_candidates(resp.text))
        location = resp.headers.get("location") or resp.headers.get("Location") or ""
        if not location:
            break
        current = urljoin(current, location)
    return list(dict.fromkeys(qr_urls))


def resolve_external_redirect(stripe: requests.Session, start_url: str) -> str:
    if not start_url or not env_bool("UPI_FOLLOW_REDIRECT", True):
        return start_url
    current = start_url
    for hop in range(1, 6):
        if is_upi_instructions_url(current):
            return current
        try:
            resp = stripe.get(current, timeout=DEFAULT_TIMEOUT, allow_redirects=False)
            dump_http(resp, f"resolve_redirect_hop_{hop}", None, "GET", current, force=True)
        except Exception as exc:
            log(f"following redirect error: {exc}", "[WARN] ")
            return current
        location = resp.headers.get("location") or resp.headers.get("Location") or ""
        if not location:
            return current
        current = urljoin(current, location)
    return current


def approve_proxy_candidates(checkout_proxy: str, provider_proxy: str, approve_pool: list[str]) -> list[str]:
    approve_preferences = successful_approve_preferences(checkout_proxy, provider_proxy, [provider_proxy] + approve_pool)
    if approve_preferences:
        log(f"hitting previously-successful approve proxy first: {proxy_label(approve_preferences[0])}")
    return list(dict.fromkeys(approve_preferences + [provider_proxy] + approve_pool))


def resolve_confirm_payload_upi(
    stripe: requests.Session,
    confirm_payload: dict[str, Any],
    checkout: dict[str, str],
    stripe_pk: str,
    ctx: dict[str, Any],
    pm_id: str,
    access_token: str,
    device_id: str,
    session_token: str,
    checkout_proxy: str,
    provider_proxy: str,
    approve_pool: list[str],
) -> tuple[str, list[str], str]:
    raise_if_setup_intent_blocked(confirm_payload, "stripe confirm", current_pm_id=pm_id)
    redirect_url = extract_redirect_url(confirm_payload)
    if not redirect_url:
        redirect_url = stripe_payload_intent_redirect_url(stripe, confirm_payload, stripe_pk, current_pm_id=pm_id)
    qr_urls = extract_qr_candidates(confirm_payload)
    submission = find_submission_attempt(confirm_payload)

    if redirect_url:
        log(f"confirm extracted final pay URL: {redirect_url[:180]}")
    if qr_urls:
        log(f"confirm extracted {len(qr_urls)} QR candidate(s)")

    approve_proxy = ""
    if not redirect_url and submission.get("state") == "requires_approval":
        log("needs ChatGPT approve...")
        if env_bool("UPI_BREAK_BEFORE_APPROVE", False):
            artifact = {
                "break": "before_approve",
                "created_local": time.strftime("%Y-%m-%d %H:%M:%S"),
                "checkout_session_id": checkout.get("cs_id"),
                "processor_entity": checkout.get("processor_entity") or "openai_llc",
                "submission_state": "requires_approval",
                "amount_minor": ctx.get("checkout_amount") if isinstance(ctx, dict) else None,
                "currency": checkout.get("currency") or "inr",
                "payment_method_id": pm_id,
                "checkout_page_url": checkout_page_url(checkout),
            }
            fp = SCRIPT_DIR / f"upi_before_approve_{time.strftime('%Y%m%d-%H%M%S')}.json"
            fp.write_text(json.dumps(artifact, ensure_ascii=False, indent=2))
            emit_output(f"BEFORE_APPROVE_READY: {fp}", flush=True)
            emit_output(json.dumps(artifact, ensure_ascii=False), flush=True)
            raise SystemExit(0)
        approve_proxies = approve_proxy_candidates(checkout_proxy, provider_proxy, approve_pool)
        log("needs approve: UPI 0-value path; prefer previously-successful/current provider proxy, then rotate to next provider proxy.")
        approve_proxy = approve_with_retry(access_token, device_id, checkout, approve_proxies, session_token, "provider")
        log("following redirect to extract final chain...")
        redirect_url, poll_qr = poll_payment_page(stripe, checkout, stripe_pk, ctx, current_pm_id=pm_id, confirm_payload=confirm_payload)
        qr_urls.extend(poll_qr)
    elif not redirect_url and not qr_urls:
        log("confirm returned no real UPI redirect/QR; polling payment_pages for final confirmation", "[WARN] ")
        redirect_url, poll_qr = poll_payment_page(stripe, checkout, stripe_pk, ctx, current_pm_id=pm_id, confirm_payload=confirm_payload)
        qr_urls.extend(poll_qr)

    return redirect_url, list(dict.fromkeys(qr_urls)), approve_proxy



def emit_hosted_url_marker(hosted_url: str) -> None:
    """In URL checkout hosted de artifact luon lay duoc.

    Stripe KHONG tra QR qua API o luong cs_: payment_intent.next_action chi la
    {"type":"upi_await_notification","upi_await_notification":{}} (rong) va
    setup_intent = null. Nhung response CO `stripe_hosted_url`
    (https://checkout.stripe.com/c/pay/cs_live_...#fid...) — mo URL nay la ra QR.
    In ngay sau confirm de khong bi mat khi cac buoc sau (poll/approve) that bai.
    Markers: `UPI final pay URL:` (uu tien) > `UPI hosted checkout URL:` (fallback).
    """
    if not hosted_url:
        return
    emit_output("\n===== HOSTED CHECKOUT URL =====")
    emit_output(f"UPI hosted checkout URL:\n{hosted_url}")


def emit_qr_marker(qr_urls: list[str]) -> None:
    """In ảnh QR (nếu Stripe có trả) để lớp trên lấy được.

    `extract_qr_candidates()` đã bắt được `qr.stripe.com/....png` từ
    `next_action.upi_handle_redirect_or_display_qr_code.qr_code.image_url_png`,
    nhưng trước đây giá trị này bị **vứt đi**: caller nhận `_qr_urls` rồi không
    dùng, chỉ in mỗi URL trả tiền. Hệ quả: card trên web không có QR nào để hiện
    dù dữ liệu đã nằm trong tay.

    In cả PNG (ưu tiên) và SVG để lớp trên chọn; marker riêng từng dòng.
    """
    if not qr_urls:
        return
    png = next((u for u in qr_urls if str(u).lower().endswith(".png")), "")
    svg = next((u for u in qr_urls if str(u).lower().endswith(".svg")), "")
    if not png and not svg:
        png = qr_urls[0]
    emit_output("\n===== UPI QR IMAGE =====")
    if png:
        emit_output(f"UPI QR PNG:\n{png}")
    if svg:
        emit_output(f"UPI QR SVG:\n{svg}")
    log("QR image: %s" % (png or svg)[:120])


def run_provider_flow(
    access_token: str,
    session_token: str,
    checkout_proxy: str,
    promotion_proxy: str,
    provider_proxy: str,
    approve_pool: list[str],
    device_id: str,
    checkout: dict[str, str],
    billing: dict[str, str],
    stop_event: Event | None = None,
) -> tuple[str, list[str]]:
    checkout_country = normalize_country(env_value("UPI_CHECKOUT_COUNTRY", UPI_BOOTSTRAP_COUNTRY))
    stripe_pk = checkout.get("stripe_pk") or DEFAULT_STRIPE_PK

    def inspect_init(payload: dict[str, Any], stage: str) -> tuple[dict[str, Any], int]:
        current_ctx = build_ctx(payload, checkout)
        current_amount = int(current_ctx.get("checkout_amount") or 0)
        amount_major = current_amount / 100
        log(f"{stage} Stripe init ok, amount={checkout['currency']} {amount_major:.2f}")
        payment_method_types = first_value_by_key(payload, "payment_method_types")
        if isinstance(payment_method_types, list):
            methods = [str(item).lower() for item in payment_method_types]
            log(f"Stripe available payment methods: {methods}")
            if "upi" not in methods:
                raise RuntimeError(
                    f"{UPI_UNAVAILABLE_ERROR}: {stage} amount={current_amount}; "
                    f"payment_method_types={methods}"
                )
        return current_ctx, current_amount

    log(
        f"{UPI_BOOTSTRAP_COUNTRY} Bootstrap Stripe init "
        f"(PM={billing['country']}, proxy={proxy_label(checkout_proxy)})..."
    )
    init_payload = stripe_init(checkout["cs_id"], stripe_pk, checkout_proxy)
    if not checkout.get("processor_entity"):
        processor_entity = infer_processor_entity(init_payload)
        if processor_entity:
            checkout["processor_entity"] = processor_entity
            log(f"inferred processor_entity={processor_entity} from Stripe init")
    ctx, amount = inspect_init(init_payload, f"{UPI_BOOTSTRAP_COUNTRY} Bootstrap")
    # Bootstrap init cũng trả stripe_hosted_url -> giữ luôn, để nhánh bỏ qua vòng
    # promotion vẫn có hosted_url (code cũ chỉ gán nó bên trong vòng lặp).
    hosted_url = str(init_payload.get("stripe_hosted_url") or "")
    if stop_event and stop_event.is_set():
        raise RuntimeError("task stopped; skipping this round")

    # create_checkout đã gắn promo_campaign và Stripe báo 0₫ NGAY từ bootstrap:
    # không cần gọi /checkout/update (Apply promotion) nữa — bỏ hẳn 1 request
    # checkout/update + 1 lần stripe_init cho mỗi task. Ít request tới payment API
    # = ít bề mặt cho risk engine. (Flow của tool khác hiện "Apply promotion
    # (Skipped)" đúng ở trường hợp này.)
    if amount == 0:
        log("checkout already 0 after create_checkout; skipping Apply promotion (checkout/update)")
        record_checkout_zero_result(checkout_proxy, checkout_country, amount)
    for promotion_index, promotion_country in enumerate(
            UPI_PROMOTION_COUNTRIES if amount != 0 else (), start=1):
        current_promotion_proxy = proxy_for_country(promotion_proxy, promotion_country)
        stage_label = f"{promotion_country} checkout/update {promotion_index}/{len(UPI_PROMOTION_COUNTRIES)}"
        log(f"{stage_label}: proxy={proxy_label(current_promotion_proxy)}")
        try:
            promotion_chatgpt = build_chatgpt_session(
                access_token, device_id, current_promotion_proxy, session_token
            )
            update_checkout_promotion(promotion_chatgpt, checkout, promotion_country)
        except Exception as exc:
            if is_checkout_not_active_error(exc):
                raise
            raise RuntimeError(f"promotion stage failed: {exc}") from exc
        record_proxy_result("promotion", current_promotion_proxy, True, "promotion_update_success")

        log(
            f"after {stage_label} refresh Stripe via {UPI_PROVIDER_COUNTRY}: "
            f"proxy={proxy_label(provider_proxy)}"
        )
        init_payload = stripe_init(checkout["cs_id"], stripe_pk, provider_proxy)
        hosted_url = str(init_payload.get("stripe_hosted_url") or hosted_url or "")
        ctx, amount = inspect_init(
            init_payload, f"{promotion_country} updated; {UPI_PROVIDER_COUNTRY} "
        )
        record_checkout_zero_result(checkout_proxy, checkout_country, amount)
        if amount == 0:
            log("amount is 0 after promotion; continuing with UPI 0-value flow to extract final pay URL")
            if env_bool("UPI_BREAK_AT_ZERO", False):
                artifact = {
                    "break": "after_step2_zero",
                    "created_local": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "checkout_session_id": checkout.get("cs_id"),
                    "processor_entity": checkout.get("processor_entity") or "openai_llc",
                    "amount_minor": amount,
                    "amount_display": round((amount or 0) / 100, 2),
                    "currency": checkout.get("currency") or "inr",
                    "payment_method_types": None,
                    "checkout_page_url": checkout_page_url(checkout),
                    "stripe_hosted_url": hosted_url or "",
                }
                fp = SCRIPT_DIR / f"upi_after_step2_{time.strftime('%Y%m%d-%H%M%S')}.json"
                fp.write_text(json.dumps(artifact, ensure_ascii=False, indent=2))
                emit_output(f"ZERO_AFTER_STEP2_READY: {fp}", flush=True)
                emit_output(json.dumps(artifact, ensure_ascii=False), flush=True)
                raise SystemExit(0)
            break
        if promotion_index < len(UPI_PROMOTION_COUNTRIES):
            log(
                f"amount still non-zero after {promotion_country} update; continuing to next checkout/update",
                "[WARN] ",
            )
            continue
        # (#2) account không được hưởng promo 0₫: sau create_checkout + MỌI checkout/update
        # mà amount vẫn != 0 -> chạy tiếp chuỗi ₹1,999 chỉ để bị _verify_link đánh fail
        # sau ~2 phút. Cắt ngay tại đây, báo rõ để engine không retry.
        promo_mode = str(env_value("PP_PROMO_MODE", "campaign") or "").strip().lower() or "campaign"
        if promo_mode not in ("off", ""):
            raise RuntimeError(
                f"promo_not_eligible: 0-value promo did not apply "
                f"(minor amount={amount}); account not eligible, skipping ₹1,999 chain"
            )

    stripe = new_session(provider_proxy)
    stripe.headers.update({
        "User-Agent": random_user_agent(),
        "Accept-Language": payment_accept_language(),
        **CHROME_CLIENT_HINTS,
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "cross-site",
    })

    if env_bool("UPI_UPDATE_TAX_REGION", False):
        log(f"syncing {UPI_PROVIDER_COUNTRY} checkout/taxes with Stripe tax_region...")
        tax_chatgpt = build_chatgpt_session(access_token, device_id, provider_proxy, session_token)
        update_upi_checkout_taxes(tax_chatgpt, checkout, billing)
        stripe_update_tax_region(stripe, checkout["cs_id"], stripe_pk, ctx, billing)
        init_payload = stripe_init(checkout["cs_id"], stripe_pk, provider_proxy)
        hosted_url = str(init_payload.get("stripe_hosted_url") or hosted_url or "")
        ctx, amount = inspect_init(init_payload, f"{UPI_PROVIDER_COUNTRY} tax sync")
        record_checkout_zero_result(checkout_proxy, checkout_country, amount)
        if amount != 0:
            if env_bool("UPI_REQUIRE_ZERO", True):
                raise RuntimeError(f"0-value discount did not take effect; current minor amount={amount}; stopped generating non-0 UPI link")

    pm_id = ""
    if env_bool("UPI_CONFIRM_INLINE_PM", False):
        log(
            f"UPI confirm inline details: {billing['name']} / "
            f"{billing['line1']} / {billing['city']} {billing['postal_code']}"
        )
    else:
        log(f"creating PM (UPI): {billing['country']} {billing['name']} / {billing['city']}")
        pm_id = stripe_create_upi_pm(stripe, checkout["cs_id"], stripe_pk, billing, ctx)
        log(f"PM created: {pm_id}")
        log(
            f"IN details filled into PM billing_details: {billing['name']} / "
            f"{billing['line1']} / {billing['city']} {billing['postal_code']}"
        )

    if env_bool("UPI_UPDATE_CUSTOMER_DATA", False):
        log(
            f"submitting IN details fill: {billing['name']} / {billing['line1']} / "
            f"{billing['city']} {billing['postal_code']} / {billing['email']}"
        )
        stripe_update_customer_data(stripe, checkout["cs_id"], stripe_pk, ctx, billing)

    if env_bool("UPI_CHECKOUT_SNAPSHOT", False):
        snapshot_chatgpt = build_chatgpt_session(access_token, device_id, provider_proxy, session_token)
        checkout_snapshot(snapshot_chatgpt, checkout, billing)

    log("Stripe confirm (expected=UPI)...")
    confirm_payload = stripe_confirm_upi(stripe, checkout["cs_id"], pm_id, stripe_pk, init_payload, ctx, checkout, hosted_url, billing)
    log("Stripe confirm ok; parsing redirect...")
    emit_hosted_url_marker(hosted_url)
    log_payment_page_summary("confirm", confirm_payload)
    if stop_event and stop_event.is_set():
        raise RuntimeError("task stopped; skipping this round")

    approve_proxy = ""
    qr_urls: list[str] = []
    try:
        redirect_url, qr_urls, approve_proxy = resolve_confirm_payload_upi(
            stripe,
            confirm_payload,
            checkout,
            stripe_pk,
            ctx,
            pm_id,
            access_token,
            device_id,
            session_token,
            checkout_proxy,
            provider_proxy,
            approve_pool,
        )
    except Exception as exc:
        if not should_retry_second_confirm_after_approve(exc):
            raise
        log(f"no redirect after approve/confirm; refreshing Stripe init and doing second confirm: {str(exc)[:180]}", "[WARN] ")
        init_payload = stripe_init(checkout["cs_id"], stripe_pk, provider_proxy)
        hosted_url = str(init_payload.get("stripe_hosted_url") or hosted_url or "")
        ctx = build_ctx(init_payload, checkout)
        confirm_payload = stripe_confirm_upi(stripe, checkout["cs_id"], pm_id, stripe_pk, init_payload, ctx, checkout, hosted_url, billing)
        log("second Stripe confirm ok; parsing redirect...")
        log_payment_page_summary("second_confirm", confirm_payload)
        redirect_url, qr_urls, retry_approve_proxy = resolve_confirm_payload_upi(
            stripe,
            confirm_payload,
            checkout,
            stripe_pk,
            ctx,
            pm_id,
            access_token,
            device_id,
            session_token,
            checkout_proxy,
            provider_proxy,
            approve_pool,
        )
        approve_proxy = retry_approve_proxy or approve_proxy

    # In QR ngay khi có — trước khi follow redirect, để không mất nếu bước sau lỗi.
    # (Trước đây `qr_urls` bị vứt hẳn ở đây nên card trên web không có QR để hiện.)
    emit_qr_marker(qr_urls)

    if redirect_url and approve_proxy:
        record_proxy_pair_approve_success(checkout_proxy, provider_proxy, approve_proxy)
        log("done - remembered this checkout/provider/approve combo")

    if redirect_url:
        final_url = resolve_external_redirect(stripe, redirect_url)
        if final_url and final_url != redirect_url:
            log(f"followed redirect to final chain: {final_url[:180]}")
            redirect_url = final_url

    return redirect_url, list(dict.fromkeys(qr_urls))


def new_device_id(access_token: str = "") -> str:
    """Sinh device_id và LOG LUÔN fingerprint tương ứng.

    Vì sao cần log: `sentinel_fingerprint()` chọn profile (screen/cores/memory) bằng
    hash của device_id, mà device_id là UUID ngẫu nhiên mỗi lần chạy. Trước đây không
    có chỗ nào ghi lại, nên dù lấy được link cũng KHÔNG biết ca thắng dùng fingerprint
    nào -> mọi kết luận về fingerprint chỉ là phỏng đoán.

    `acct` là băm 8 ký tự của token: đủ để nối log với acc trong file (băm lại offline
    là ra), nhưng KHÔNG ghi token ra log.
    """
    device_id = str(uuid.uuid4())
    try:
        acct = hashlib.sha256(str(access_token or "").encode()).hexdigest()[:8] if access_token else "-"
        import check_upi_eligibility as _cue  # noqa: PLC0415
        fp = _cue.sentinel_fingerprint(device_id)
        log("fingerprint: acct=%s device=%s screen=%sx%s cores=%s mem=%s locale=%s tz=%s"
            % (acct, device_id[:8], fp.get("screen_width"), fp.get("screen_height"),
               fp.get("hardware_concurrency"), fp.get("device_memory"),
               fp.get("language"), fp.get("timezone")))
    except Exception:  # noqa: BLE001 — log lỗi không được làm chết flow
        pass
    return device_id


def run_once(
    access_token: str,
    session_token: str,
    checkout_proxy: str,
    promotion_proxy: str,
    provider_proxy: str,
    approve_pool: list[str],
    attempt: int,
    max_retry: int,
    stop_event: Event | None = None,
) -> tuple[str, list[str]]:
    if stop_event and stop_event.is_set():
        raise RuntimeError("task stopped; skipping this round")
    device_id = new_device_id(access_token)
    checkout_country = normalize_country(env_value("UPI_CHECKOUT_COUNTRY", UPI_BOOTSTRAP_COUNTRY))
    # Địa chỉ phải khớp IP của chặng gửi PM/tax/approve -> dùng provider proxy
    billing = upi_billing_profile(provider_proxy or checkout_proxy)
    log(f"starting UPI extraction, attempt {attempt}/{max_retry}")
    log(
        "combo test: "
        f"{checkout_country} / {billing['country']} / {currency_for_country(checkout_country)} / "
        f"{payment_browser_locale()} / "
        f"{str(env_value('UPI_PROVIDER_COUNTRY_LABEL', UPI_PROVIDER_COUNTRY) or '').strip() or UPI_PROVIDER_COUNTRY}"
    )
    if stop_event and stop_event.is_set():
        raise RuntimeError("task stopped; skipping this round")

    try:
        proxy_seed = checkout_proxy
        checkout_proxy, promotion_proxy, provider_proxy = upi_proxy_chain(proxy_seed)
        log_upi_proxy_chain(proxy_seed, checkout_proxy, promotion_proxy, provider_proxy)
        log(f"this round proxy: checkout/eligibility={proxy_label(checkout_proxy)}; Stripe/UPI={proxy_label(provider_proxy)}")
        zero_status, zero_amount, _zero_checked_at = checkout_zero_cache_status(checkout_proxy, checkout_country)
        if zero_status == "ok":
            log(f"checkout 0-value eligibility cache hit: amount={zero_amount}")
        elif zero_status == "bad":
            log(f"checkout 0-value failure cache hit: last amount={zero_amount}; re-validating this round", "[WARN] ")
        chatgpt = build_chatgpt_session(access_token, device_id, checkout_proxy, session_token)
        checkout = create_checkout(chatgpt, checkout_country)
    except Exception as exc:
        if is_user_already_paid_error(exc):
            raise RuntimeError("User is already paid") from exc
        if is_checkout_not_active_error(exc):
            raise
        raise RuntimeError(f"checkout stage failed: {exc}") from exc
    return run_provider_flow(
        access_token,
        session_token,
        checkout_proxy,
        promotion_proxy,
        provider_proxy,
        [provider_proxy],
        device_id,
        checkout,
        billing,
        stop_event,
    )


def run_attempt(
    access_token: str,
    session_token: str,
    checkout_proxy: str,
    promotion_proxy: str,
    provider_proxy: str,
    approve_pool: list[str],
    attempt: int,
    max_retry: int,
    stop_event: Event | None = None,
    batch_no: int = 0,
    batch_total: int = 0,
) -> tuple[int, str, list[str], str, str, str]:
    previous_log_context = getattr(_log_context, "prefix", "")
    if batch_no > 0:
        _log_context.prefix = f"[batch {batch_no}/{batch_total or '?'}][round {attempt}/{max_retry}] "
    else:
        _log_context.prefix = f"[round {attempt}/{max_retry}] "
    try:
        redirect_url, qr_urls = run_once(
            access_token,
            session_token,
            checkout_proxy,
            promotion_proxy,
            provider_proxy,
            approve_pool,
            attempt,
            max_retry,
            stop_event,
        )
        has_result = bool(redirect_url)
        if has_result and stop_event:
            stop_event.set()
        if has_result:
            record_proxy_pair_result(checkout_proxy, provider_proxy, True, "success")
        else:
            record_proxy_result("provider", provider_proxy, False, "no_redirect_url")
        _log_context.prefix = previous_log_context
        return attempt, redirect_url, qr_urls, checkout_proxy, provider_proxy, ""
    except Exception as exc:
        error = str(exc)
        if error.startswith("task stopped"):
            _log_context.prefix = previous_log_context
            return attempt, "", [], checkout_proxy, provider_proxy, ""
        if is_user_already_paid_error(error):
            log("detected 'User is already paid'; user already paid; stopping")
            if stop_event:
                stop_event.set()
            _log_context.prefix = previous_log_context
            return attempt, "", [], checkout_proxy, provider_proxy, error
        if is_upi_unavailable_error(error):
            log(f"round {attempt}/{max_retry} checkout has no UPI; keeping proxy and continuing with later combos", "[WARN] ")
            _log_context.prefix = previous_log_context
            return attempt, "", [], checkout_proxy, provider_proxy, error
        if is_checkout_not_active_error(error):
            log(
                f"round {attempt}/{max_retry} session expired; skipping, not recording proxy failure, proxy kept for later",
                "[WARN] ",
            )
            _log_context.prefix = previous_log_context
            return attempt, "", [], checkout_proxy, provider_proxy, error
        record_failure_by_stage(error, checkout_proxy, provider_proxy, promotion_proxy)
        log(f"round {attempt}/{max_retry} failed: {error[:300]}", "[WARN] ")
        _log_context.prefix = previous_log_context
        return attempt, "", [], checkout_proxy, provider_proxy, error


def successful_pair_preferences(checkout_proxies: list[str], provider_proxies: list[str]) -> dict[str, list[str]]:
    if not env_bool("UPI_PROXY_SCORE", True):
        return {}
    checkout_by_key = {proxy_key(proxy): proxy for proxy in checkout_proxies}
    provider_by_key = {proxy_key(proxy): proxy for proxy in provider_proxies}
    pair_state = load_proxy_state().get("pair", {})
    if not isinstance(pair_state, dict):
        return {}

    candidates: list[tuple[int, int, str, str]] = []
    for record in pair_state.values():
        if not isinstance(record, dict):
            continue
        success_count = int(record.get("success") or 0)
        if success_count <= 0:
            continue
        checkout_proxy = checkout_by_key.get(str(record.get("checkout") or ""))
        provider_proxy = provider_by_key.get(str(record.get("provider") or ""))
        if checkout_proxy and provider_proxy:
            candidates.append((success_count, int(record.get("last_success") or 0), checkout_proxy, provider_proxy))

    candidates.sort(reverse=True)
    preferences: dict[str, list[str]] = {}
    for _success_count, _last_success, checkout_proxy, provider_proxy in candidates:
        providers = preferences.setdefault(checkout_proxy, [])
        if provider_proxy not in providers:
            providers.append(provider_proxy)
    return preferences


def build_attempt_batches(checkout_proxies: list[str], provider_proxies: list[str], max_attempts: int) -> list[tuple[str, list[str]]]:
    per_checkout = env_int("UPI_PROVIDER_PER_CHECKOUT", 30)
    provider_pool = provider_proxies[:]
    preferred_pairs = successful_pair_preferences(checkout_proxies, provider_proxies)
    reserved_provider_owner: dict[str, str] = {}
    for checkout_proxy, preferred_providers in preferred_pairs.items():
        for provider_proxy in preferred_providers:
            reserved_provider_owner.setdefault(provider_proxy, checkout_proxy)
    used_providers: set[str] = set()
    batches: list[tuple[str, list[str]]] = []
    provider_index = 0
    attempt_count = 0
    preferred_count = 0
    for checkout_proxy in checkout_proxies:
        batch: list[str] = []
        for provider_proxy in preferred_pairs.get(checkout_proxy, []):
            if len(batch) >= per_checkout or attempt_count >= max_attempts:
                break
            if provider_proxy in used_providers:
                continue
            batch.append(provider_proxy)
            used_providers.add(provider_proxy)
            attempt_count += 1
            preferred_count += 1
        while len(batch) < per_checkout and provider_index < len(provider_pool) and attempt_count < max_attempts:
            provider_proxy = provider_pool[provider_index]
            provider_index += 1
            if provider_proxy in used_providers:
                continue
            reserved_owner = reserved_provider_owner.get(provider_proxy)
            if reserved_owner and reserved_owner != checkout_proxy:
                continue
            batch.append(provider_proxy)
            used_providers.add(provider_proxy)
            attempt_count += 1
        if batch:
            batches.append((checkout_proxy, batch))
        if attempt_count >= max_attempts:
            break
    if preferred_count:
        log(f"scheduling hit successful-combo priority: {preferred_count} group(s)")
    return batches


def is_preferred_proxy(group: str, proxy: str) -> bool:
    if not group or not env_bool("UPI_PROXY_SCORE", True):
        return False
    state = load_proxy_state().get(group, {})
    if not isinstance(state, dict):
        return False
    record = state.get(proxy_key(proxy), {})
    if not isinstance(record, dict):
        return False
    return int(record.get("success") or 0) > 0


def pick_random_proxies(proxies: list[str], limit: int, group: str = "") -> list[str]:
    if group:
        proxies = order_proxy_group(group, proxies)
    preferred = [proxy for proxy in proxies if is_preferred_proxy(group, proxy)]
    preferred_set = set(preferred)
    rest = [proxy for proxy in proxies if proxy not in preferred_set]
    if limit >= len(proxies):
        random.shuffle(rest)
        return preferred + rest
    selected = preferred[:limit]
    remain_count = limit - len(selected)
    if remain_count > 0:
        selected.extend(random.sample(rest, min(remain_count, len(rest))))
    return selected


def run_single_link_attempt(
    access_token: str,
    session_token: str,
    checkout_proxies: list[str],
    promotion_proxies: list[str],
    provider_proxies: list[str],
    attempt: int,
    upi_retry: int,
    checkout_retry: int,
    provider_retry: int,
    checkout_country: str,
    checkout_currency: str,
    stop_event: Event,
) -> tuple[int, str, str, bool]:
    previous_log_context = getattr(_log_context, "prefix", "")
    _log_context.prefix = f"[UPI {attempt}/{upi_retry}] "
    last_error = ""
    approve_blocked = False
    checkout_proxy_used = ""
    try:
        if stop_event.is_set():
            return attempt, "", "task stopped; skipping this round", False
        billing = upi_billing_profile((provider_proxies or checkout_proxies or [""])[0])
        pm_country = billing["country"]
        device_id = new_device_id(access_token)
        checkout_candidates = pick_random_proxies(checkout_proxies, checkout_retry, "checkout")
        checkout: dict[str, str] | None = None
        promotion_proxy = ""
        provider_proxy = ""

        log(f"starting link extraction round {attempt}/{upi_retry}")
        log(
            f"Step 1: creating ChatGPT checkout... checkout billing={checkout_country}/{checkout_currency}, "
            f"round {attempt}/{upi_retry}; randomly picking up to {checkout_retry} nodes each time"
        )
        log(f"first PM country: {pm_country}")

        for checkout_index, proxy_seed in enumerate(checkout_candidates, start=1):
            if stop_event.is_set():
                return attempt, "", "task stopped; skipping this round", False
            _log_context.prefix = f"[UPI {attempt}/{upi_retry}][PM={pm_country}] "
            try:
                checkout_proxy, promotion_proxy, provider_proxy = upi_proxy_chain(proxy_seed)
                log_upi_proxy_chain(proxy_seed, checkout_proxy, promotion_proxy, provider_proxy)
                log(f"Checkout {checkout_index}/{len(checkout_candidates)}: {checkout_country}/{checkout_currency}, proxy={proxy_label(checkout_proxy)}")
                zero_status, zero_amount, _zero_checked_at = checkout_zero_cache_status(checkout_proxy, checkout_country)
                if zero_status == "ok":
                    log(f"checkout 0-value eligibility cache hit: amount={zero_amount}")
                elif zero_status == "bad":
                    log(f"checkout 0-value failure cache hit: last amount={zero_amount}; re-validating this round", "[WARN] ")
                chatgpt = build_chatgpt_session(access_token, device_id, checkout_proxy, session_token)
                checkout = create_checkout(chatgpt, checkout_country)
                checkout_proxy_used = checkout_proxy
                break
            except Exception as exc:
                error = str(exc)
                last_error = error
                if is_user_already_paid_error(error):
                    log("detected 'User is already paid'; user already paid; stopping")
                    stop_event.set()
                    return attempt, "", error, False
                if not is_checkout_not_active_error(error):
                    record_failure_by_stage(f"checkout stage failed: {error}", checkout_proxy, "")
                log(f"checkout {checkout_index}/{len(checkout_candidates)} failed: {error[:220]}", "[WARN] ")

        _log_context.prefix = f"[UPI {attempt}/{upi_retry}] "
        if not checkout or not checkout_proxy_used:
            log(f"link extraction round {attempt}/{upi_retry} failed at checkout stage", "[WARN] ")
            return attempt, "", last_error or "checkout_failed", False

        stripe_pk = checkout.get("stripe_pk") or DEFAULT_STRIPE_PK
        log(f"Stripe PK: {stripe_pk[:18]}...")
        log(f"Step 2: first attempt PM={pm_country}...")

        if stop_event.is_set():
            return attempt, "", "task stopped; skipping this round", False
        _log_context.prefix = f"[UPI {attempt}/{upi_retry}][PM={pm_country}] "
        try:
            redirect_url, qr_urls = run_provider_flow(
                access_token,
                session_token,
                checkout_proxy_used,
                promotion_proxy,
                provider_proxy,
                [provider_proxy],
                device_id,
                checkout,
                billing,
                stop_event,
            )
            if redirect_url:
                record_proxy_pair_result(checkout_proxy_used, provider_proxy, True, "success")
                stop_event.set()
                return attempt, redirect_url, "", False
            last_error = "no_redirect_url"
            record_proxy_result("provider", provider_proxy, False, last_error)
        except Exception as exc:
            error = str(exc)
            last_error = error
            if is_checkout_not_active_error(error):
                log("session expired; current checkout will not rotate provider further", "[WARN] ")
            elif is_upi_unavailable_error(error):
                log("current checkout has no UPI; moving to next checkout combo", "[WARN] ")
                return attempt, "", error, False
            elif is_account_risk_error(error):
                # Account/customer-level risk (approve blocked, or Stripe generic_decline when
                # creating the SetupIntent): NOT a proxy failure. Do not
                # record_failure (avoids punishing/rotating proxy on the same account).
                log(f"account/customer risk, stopping account: {error[:180]}", "[WARN] ")
                approve_blocked = True
                stop_event.set()
            elif "promo_not_eligible" in error:
                # Account không được hưởng promo 0₫ — cũng là account-level, KHÔNG phải
                # lỗi proxy: đừng phạt proxy, và đừng thử round/proxy khác cho account này.
                log(f"account not eligible for 0-value promo, stopping: {error[:180]}", "[WARN] ")
                approve_blocked = True
                stop_event.set()
            else:
                record_failure_by_stage(error, checkout_proxy_used, provider_proxy, promotion_proxy)
                log(f"provider failed: {error[:220]}", "[WARN] ")
                if is_approve_failure_error(error) and "approve blocked" in error:
                    approve_blocked = True

        _log_context.prefix = f"[UPI {attempt}/{upi_retry}] "
        log(f"link extraction round {attempt}/{upi_retry} ended; no final URL", "[WARN] ")
        return attempt, "", last_error, approve_blocked
    finally:
        _log_context.prefix = previous_log_context


def run_single_link_parallel_mode(
    access_token: str,
    session_token: str,
    checkout_proxies: list[str],
    promotion_proxies: list[str],
    provider_proxies: list[str],
) -> int:
    checkout_retry = env_int("UPI_CHECKOUT_RETRY_MAX", 1)
    provider_retry = env_int("UPI_PROVIDER_RETRY_MAX", 1)
    upi_retry = env_int("UPI_MAX_RETRY", 1)
    requested_workers = env_int("UPI_WORKERS", 1)
    worker_limit = env_int("UPI_WORKERS_MAX", requested_workers)
    workers = min(max(1, requested_workers), max(1, worker_limit), upi_retry)
    checkout_country = normalize_country(env_value("UPI_CHECKOUT_COUNTRY", UPI_BOOTSTRAP_COUNTRY))
    checkout_currency = currency_for_country(checkout_country)
    configured_pm_country = normalize_country(
        env_value("UPI_BILLING_COUNTRY", UPI_PROVIDER_COUNTRY)
    )
    max_blocked = env_int("UPI_MAX_APPROVE_BLOCKED", upi_retry)
    approve_blocked_count = 0
    last_error = ""
    stop_event = Event()

    if requested_workers > workers:
        log(f"UPI concurrency limited from {requested_workers} to {workers}", "[WARN] ")
    log(
        "starting UPI link extraction flow: "
        f"checkout={checkout_country}/{checkout_currency}; PM={configured_pm_country}; locale={payment_browser_locale()}; "
        f"checkout-retry={checkout_retry}, provider-retry={provider_retry}, UPI-total-retry={upi_retry}, UPI-concurrency={workers}."
    )

    executor = ThreadPoolExecutor(max_workers=workers)
    futures: dict[Any, int] = {}
    try:
        for attempt in range(1, upi_retry + 1):
            futures[
                executor.submit(
                    run_single_link_attempt,
                    access_token,
                    session_token,
                    checkout_proxies,
                    promotion_proxies,
                    provider_proxies,
                    attempt,
                    upi_retry,
                    checkout_retry,
                    provider_retry,
                    checkout_country,
                    checkout_currency,
                    stop_event,
                )
            ] = attempt

        for future in as_completed(futures):
            try:
                attempt, redirect_url, error, approve_blocked = future.result()
            except Exception as exc:
                attempt = futures.get(future, 0)
                redirect_url = ""
                error = str(exc)
                approve_blocked = False
                log(f"link extraction round {attempt}/{upi_retry} error: {error[:300]}", "[WARN] ")
            if redirect_url:
                stop_event.set()
                for pending in futures:
                    pending.cancel()
                emit_output("\n===== RESULT =====")
                emit_output(f"UPI final pay URL:\n{redirect_url}")
                if env_bool("UPI_SAVE_LINK_ARTIFACT", False):
                    try:
                        artifact = {"url": redirect_url,
                                    "created_local": time.strftime("%Y-%m-%d %H:%M:%S")}
                        rr = requests.get(redirect_url,
                                          headers={"User-Agent": DEFAULT_USER_AGENT}, timeout=30)
                        mm = re.search(r'<meta id="payload" data-message="([^"]+)"', rr.text)
                        if mm:
                            pp = json.loads(base64.b64decode(mm.group(1)))
                            artifact["amount"] = pp.get("amount")
                            artifact["currency"] = pp.get("currency")
                            artifact["upi_mandate_url"] = pp.get("mobile_auth_url")
                            artifact["merchant"] = (pp.get("brand") or {}).get("business_name")
                            fp = SCRIPT_DIR / f"upi_zero_link_{time.strftime('%Y%m%d-%H%M%S')}.json"
                            fp.write_text(json.dumps(artifact, ensure_ascii=False, indent=2))
                            emit_output(f"ZERO_LINK_ARTIFACT_SAVED: {fp}", flush=True)
                        else:
                            fp = SCRIPT_DIR / f"upi_zero_link_{time.strftime('%Y%m%d-%H%M%S')}.json"
                            fp.write_text(json.dumps(artifact, ensure_ascii=False, indent=2))
                            emit_output(f"ZERO_LINK_ARTIFACT_SAVED: {fp}", flush=True)
                    except Exception as ee:
                        emit_output(f"[warn] artifact save fail: {ee}", flush=True)
                return 0
            last_error = error or last_error
            if is_user_already_paid_error(error):
                log("detected 'User is already paid'; user already paid; task finished normally")
                stop_event.set()
                for pending in futures:
                    pending.cancel()
                return 0
            if approve_blocked:
                approve_blocked_count += 1
                log(f"approve blocked count: {approve_blocked_count}/{max_blocked}", "[WARN] ")
            if approve_blocked_count >= max_blocked:
                log("reached the approve-blocked limit for this account; stopped submitting new links", "[WARN] ")
                stop_event.set()
                for pending in futures:
                    pending.cancel()
                return 1
    finally:
        executor.shutdown(wait=True, cancel_futures=stop_event.is_set())

    log(f"all failed: {last_error}", "[ERROR] ")
    return 1


def run_single_link_mode(
    access_token: str,
    session_token: str,
    proxy_seeds: list[str],
    stop_event: Event | None = None,
) -> int:
    upi_workers = env_int("UPI_WORKERS", 1)
    if upi_workers > 1:
        log(f"UPI chain fixed concurrency=1; ignoring UPI_WORKERS={upi_workers}", "[WARN] ")

    checkout_retry = env_int("UPI_CHECKOUT_RETRY_MAX", 1)
    provider_retry = env_int("UPI_PROVIDER_RETRY_MAX", 1)
    upi_retry = env_int("UPI_MAX_RETRY", 1)
    checkout_country = normalize_country(env_value("UPI_CHECKOUT_COUNTRY", UPI_BOOTSTRAP_COUNTRY))
    checkout_currency = currency_for_country(checkout_country)
    configured_pm_country = normalize_country(
        env_value("UPI_BILLING_COUNTRY", UPI_PROVIDER_COUNTRY)
    )
    max_blocked = env_int("UPI_MAX_APPROVE_BLOCKED", upi_retry)
    approve_blocked_count = 0
    last_error = ""
    stop_event = stop_event or Event()
    attempted_seed_keys: set[str] = set()

    log(
        "starting UPI link extraction flow: "
        f"checkout={checkout_country}/{checkout_currency}; PM={configured_pm_country}; locale={payment_browser_locale()}; "
        f"checkout-retry={checkout_retry}, provider-retry={provider_retry}, UPI-total-retry={upi_retry}."
    )

    for attempt in range(1, upi_retry + 1):
        billing = upi_billing_profile(proxy_seeds[0] if proxy_seeds else "")
        pm_country = billing["country"]
        device_id = new_device_id(access_token)
        available_seeds = [
            proxy_seed
            for proxy_seed in proxy_seeds
            if proxy_chain_key(proxy_seed) not in attempted_seed_keys
        ]
        checkout_candidates = pick_random_proxies(available_seeds, checkout_retry, "seed")
        if not checkout_candidates:
            last_error = last_error or "all proxy seeds for this task have been tried"
            log("all proxy seeds for this task have been tried; not retrying failed nodes", "[WARN] ")
            break
        checkout: dict[str, str] | None = None
        checkout_proxy_used = ""
        promotion_proxy = ""
        provider_proxy = ""

        log(f"starting link extraction round {attempt}/{upi_retry}")
        log(
            f"Step 1: creating ChatGPT checkout... checkout billing={checkout_country}/{checkout_currency}, "
            f"round {attempt}/{upi_retry}; randomly picking up to {checkout_retry} nodes each time"
        )
        log(f"  first PM country: {pm_country}")

        for checkout_index, proxy_seed in enumerate(checkout_candidates, start=1):
            previous_log_context = getattr(_log_context, "prefix", "")
            _log_context.prefix = f"  [PM={pm_country}] "
            checkout_proxy = ""
            promotion_proxy = ""
            provider_proxy = ""
            chain_key = proxy_chain_key(proxy_seed)
            attempted_seed_keys.add(chain_key)
            try:
                checkout_proxy, promotion_proxy, provider_proxy = upi_proxy_chain(proxy_seed)
                log_upi_proxy_chain(proxy_seed, checkout_proxy, promotion_proxy, provider_proxy)
                log(
                    f"Checkout {checkout_index}/{len(checkout_candidates)}: "
                    f"{checkout_country}/{checkout_currency}, proxy={proxy_label(checkout_proxy)}; "
                    f"tried Seed={len(attempted_seed_keys)} this round"
                )
                zero_status, zero_amount, _zero_checked_at = checkout_zero_cache_status(checkout_proxy, checkout_country)
                if zero_status == "ok":
                    log(f"checkout 0-value eligibility cache hit: amount={zero_amount}")
                elif zero_status == "bad":
                    log(f"checkout 0-value failure cache hit: last amount={zero_amount}; re-validating this round", "[WARN] ")
                chatgpt = build_chatgpt_session(access_token, device_id, checkout_proxy, session_token)
                checkout = create_checkout(chatgpt, checkout_country)
                checkout_proxy_used = checkout_proxy
                break
            except Exception as exc:
                error = str(exc)
                last_error = error
                if is_user_already_paid_error(error):
                    log("detected 'User is already paid'; user already paid; task finished normally")
                    return 0
                if not is_checkout_not_active_error(error):
                    record_failure_by_stage(
                        f"checkout stage failed: {error}",
                        checkout_proxy or proxy_seed,
                        "",
                    )
                log(f"checkout {checkout_index}/{len(checkout_candidates)} failed: {error[:220]}", "[WARN] ")
            finally:
                _log_context.prefix = previous_log_context

        if not checkout or not checkout_proxy_used:
            log(f"link extraction round {attempt}/{upi_retry} failed at checkout stage; moving to next round", "[WARN] ")
            continue

        stripe_pk = checkout.get("stripe_pk") or DEFAULT_STRIPE_PK
        log(f"Stripe PK: {stripe_pk[:18]}...")
        log(f"Step 2: first attempt PM={pm_country}...")

        previous_log_context = getattr(_log_context, "prefix", "")
        _log_context.prefix = f"  [PM={pm_country}] "
        try:
            # Vì sao đổi `_qr_urls` -> `qr_urls`: khối này chỉ chạy khi CÓ redirect_url
            # (tức là lúc thành công). Trước đây tên bị lệch (`_qr_urls` lúc gán nhưng
            # `qr_urls` lúc dùng) nên mọi lần lấy được link ở mode cs đều ném NameError
            # và bị ghi nhận là FAIL — link có thật nhưng bị vứt đi.
            redirect_url, qr_urls = run_provider_flow(
                access_token,
                session_token,
                checkout_proxy_used,
                promotion_proxy,
                provider_proxy,
                [provider_proxy],
                device_id,
                checkout,
                billing,
                stop_event,
            )
            if redirect_url:
                record_proxy_result("seed", checkout_proxy_used, True, "success")
                emit_result(status="LINK", upi_link=redirect_url, qr_urls=list(qr_urls))
                emit_output("\n===== RESULT =====")
                emit_output(f"UPI final pay URL:\n{redirect_url}")
                if env_bool("UPI_SAVE_LINK_ARTIFACT", False):
                    try:
                        artifact = {"url": redirect_url,
                                    "created_local": time.strftime("%Y-%m-%d %H:%M:%S")}
                        rr = requests.get(redirect_url,
                                          headers={"User-Agent": DEFAULT_USER_AGENT}, timeout=30)
                        mm = re.search(r'<meta id="payload" data-message="([^"]+)"', rr.text)
                        if mm:
                            pp = json.loads(base64.b64decode(mm.group(1)))
                            artifact["amount"] = pp.get("amount")
                            artifact["currency"] = pp.get("currency")
                            artifact["upi_mandate_url"] = pp.get("mobile_auth_url")
                            artifact["merchant"] = (pp.get("brand") or {}).get("business_name")
                            fp = SCRIPT_DIR / f"upi_zero_link_{time.strftime('%Y%m%d-%H%M%S')}.json"
                            fp.write_text(json.dumps(artifact, ensure_ascii=False, indent=2))
                            emit_output(f"ZERO_LINK_ARTIFACT_SAVED: {fp}", flush=True)
                        else:
                            fp = SCRIPT_DIR / f"upi_zero_link_{time.strftime('%Y%m%d-%H%M%S')}.json"
                            fp.write_text(json.dumps(artifact, ensure_ascii=False, indent=2))
                            emit_output(f"ZERO_LINK_ARTIFACT_SAVED: {fp}", flush=True)
                    except Exception as ee:
                        emit_output(f"[warn] artifact save fail: {ee}", flush=True)
                return 0
            last_error = "no_redirect_url"
            record_proxy_result("seed", provider_proxy, False, last_error)
        except Exception as exc:
            error = str(exc)
            last_error = error
            if is_checkout_not_active_error(error):
                log("session expired; current checkout will not rotate provider further", "[WARN] ")
            elif is_upi_unavailable_error(error):
                log("current checkout has no UPI; moving to next round", "[WARN] ")
            elif is_account_risk_error(error):
                log(f"account/customer risk, stopping account: {error[:180]}", "[WARN] ")
                approve_blocked_count += 1
                log(f"approve blocked count: {approve_blocked_count}/{max_blocked}", "[WARN] ")
                if approve_blocked_count >= max_blocked:
                    stop_event.set()
            else:
                record_failure_by_stage(error, checkout_proxy_used, provider_proxy, promotion_proxy)
                log(f"provider failed: {error[:220]}", "[WARN] ")
                if is_approve_failure_error(error) and "approve blocked" in error:
                    approve_blocked_count += 1
                    log(f"approve blocked count: {approve_blocked_count}/{max_blocked}", "[WARN] ")
        finally:
            _log_context.prefix = previous_log_context

        if approve_blocked_count >= max_blocked:
            log("reached the approve-blocked limit for this account; stopped submitting new links", "[WARN] ")
            return 1
        log(f"link extraction round {attempt}/{upi_retry} ended; no final URL", "[WARN] ")

    log(f"all failed: {last_error}", "[ERROR] ")
    return 1


def main() -> int:
    access_token, session_token = load_token()
    if not access_token:
        log("access_token is empty", "[ERROR] ")
        return 1

    proxy_seeds = load_proxy_seeds()
    flow_mode = str(env_value("UPI_FLOW_MODE", "single") or "").strip().lower() or "single"
    if flow_mode != "single":
        log(f"UPI_FLOW_MODE={flow_mode} collapsed to strict single-seed chain", "[WARN] ")
    return run_single_link_mode(access_token, session_token, proxy_seeds)


if __name__ == "__main__":
    sys.exit(main())
