#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""upi_core.py — Module helper và giao thức core cho UPI Checkout & Proxy Management."""

from __future__ import annotations

import hashlib
import json
import os
import random
import re
import sys
import time
import uuid
from pathlib import Path
from threading import RLock, local
from typing import Any
from urllib.parse import parse_qsl, quote, unquote, urlencode, urljoin, urlparse, urlsplit, urlunsplit

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
STRIPE_VERSION_FULL = (
    "2025-03-31.basil; checkout_server_update_beta=v1; "
    "checkout_manual_approval_preview=v1"
)
DEFAULT_STRIPE_RUNTIME_VERSION = "6f8494a281"
CHATGPT_CLIENT_VERSION = "prod-db390ebea64862bf1899c420a4c736e0cf639747"
CHATGPT_CLIENT_BUILD_NUMBER = "7904904"
DEFAULT_STRIPE_PK = (
    "pk_live_51HOrSwC6h1nxGoI3lTAgRjYVrz4dU3fVOabyCcKR3pbEJguCVAlqCxdxCUvoRh1XWwRac"
    "ViovU3kLKvpkjh7IqkW00iXQsjo3n"
)
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)

UPI_BOOTSTRAP_COUNTRY = "IN"
UPI_PROMOTION_COUNTRY = "IN"
UPI_PROVIDER_COUNTRY = "IN"

COUNTRY_CURRENCY = {
    "IN": "INR",
    "US": "USD",
    "EUR": "EUR",
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


def log(message: str, prefix: str = "") -> None:
    context = getattr(_log_context, "prefix", "")
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {prefix}{context}{message}"
    with _log_lock:
        print(line, flush=True)


def dump_http(*args: Any, **kwargs: Any) -> None:
    pass


def normalize_token(raw: str) -> tuple[str, str]:
    raw = (raw or "").strip()
    if not raw:
        return "", ""
    if raw.startswith("{") and raw.endswith("}"):
        try:
            d = json.loads(raw)
            at = d.get("accessToken") or d.get("access_token") or d.get("token") or ""
            st = d.get("sessionToken") or d.get("session_token") or ""
            return str(at).strip(), str(st).strip()
        except Exception:
            pass
    return raw, ""


def normalize_proxy_url(proxy: str) -> str:
    proxy = (proxy or "").strip()
    if not proxy:
        return ""
    if "://" in proxy:
        return proxy
    parts = proxy.split(":")
    if len(parts) == 4:
        host, port, user, pw = parts
        return f"http://{quote(user)}:{quote(pw)}@{host}:{port}"
    if len(parts) == 2:
        return f"http://{proxy}"
    return proxy


def is_cloudflare_response(response: Any) -> bool:
    if response is None:
        return False
    status = getattr(response, "status_code", None)
    text = str(getattr(response, "text", "") or "")
    if status in (403, 503) and ("cloudflare" in text.lower() or "just a moment" in text.lower()):
        return True
    return False


def is_user_already_paid_error(value: Any) -> bool:
    return "user is already paid" in str(value or "").lower()


def build_chatgpt_session(access_token: str, device_id: str, proxy: str = "",
                          user_agent: str = "") -> requests.Session:
    s = requests.Session()
    px = normalize_proxy_url(proxy)
    if px:
        s.proxies = {"http": px, "https": px}
    ua = user_agent or DEFAULT_USER_AGENT
    s.headers.update({
        "Authorization": f"Bearer {access_token}",
        "User-Agent": ua,
        "Accept": "application/json",
        "Accept-Language": "en-IN,en;q=0.9",
        "Oai-Device-Id": device_id,
        "Oai-Language": "en-IN",
    })
    return s


def load_proxy_state() -> dict[str, Any]:
    with _proxy_state_lock:
        state_file = SCRIPT_DIR / "proxy_state.json"
        if state_file.exists():
            try:
                return json.loads(state_file.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {"seed": {}, "checkout": {}, "promotion": {}, "provider": {}, "pair": {}}


def stripe_init(checkout_id: str, stripe_key: str, proxy: str = "") -> dict[str, Any]:
    px = normalize_proxy_url(proxy)
    proxies = {"http": px, "https": px} if px else None
    body = {
        "browser_locale": "en-IN",
        "browser_timezone": "Asia/Kolkata",
        "elements_session_client[client_betas][0]": "custom_checkout_server_updates_1",
        "elements_session_client[client_betas][1]": "custom_checkout_manual_approval_1",
        "elements_session_client[elements_init_source]": "custom_checkout",
        "elements_session_client[referrer_host]": "chatgpt.com",
        "elements_session_client[stripe_js_id]": str(uuid.uuid4()),
        "elements_session_client[locale]": "en",
        "elements_options_client[saved_payment_method][enable_save]": "never",
        "elements_options_client[saved_payment_method][enable_redisplay]": "never",
        "key": stripe_key,
        "_stripe_version": "2025-03-31.basil",
    }
    r = requests.post(
        f"https://api.stripe.com/v1/payment_pages/{checkout_id}/init",
        data=body,
        headers={"User-Agent": DEFAULT_USER_AGENT, "Accept": "application/json"},
        proxies=proxies,
        timeout=30,
    )
    try:
        return r.json()
    except Exception:
        return {"raw": r.text[:400]}
