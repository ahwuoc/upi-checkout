"""Kiểm tra một acc đã lên Plus chưa, dựa vào chính Access Token.

Cách làm bám theo project `gpt-auto-register-main` (`token_401_panel_8812/app.py`):

  * `access_status()` — `GET api.openai.com/v1/me` bằng Bearer AT.
       401 = AT hết hiệu lực. Trong luồng này, sau khi khách duyệt mandate ₹0 và
       trả tiền, OpenAI nâng acc lên Plus và vô hiệu hoá AT cũ -> 401 là dấu hiệu
       THÀNH CÔNG, không phải lỗi.
  * `account_promo()` — `GET chatgpt.com/backend-api/accounts/check/v4-2023-04-27`.
       `trial_status`:
         `active`       = đang trong trial -> đã lên Plus
         `eligible`     = vẫn còn ưu đãi -> chưa dùng
         `not_eligible` = không có ưu đãi

`check()` chạy cả hai, trả dict gọn kèm `plus` là kết luận cuối.

Dùng proxy của chính task (exit IP Ấn Độ như lúc chạy) vì `chatgpt.com` chặn IP
datacenter; thiếu proxy thì vẫn chạy để không mất kết quả.
"""

from __future__ import annotations

import json
import re
from typing import Any

import requests

ACCESS_URL = "https://api.openai.com/v1/me"
ACCOUNTS_URL = ("https://chatgpt.com/backend-api/accounts/check/v4-2023-04-27"
                "?timezone_offset_min=-480")
ACCOUNTS_PATH = "/backend-api/accounts/check/v4-2023-04-27"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36")

# Ưu đãi "plus 1 month free" hiện ra ở nhiều khoá khác nhau tuỳ acc.
_FREE_RE = re.compile(
    r"trial|free[\s_-]?(?:month|week|day|period|subscription)|gratis|complimentary",
    re.I)


def _proxies(proxy: str) -> dict[str, str] | None:
    url = str(proxy or "").strip()
    if not url:
        return None
    if "://" not in url:
        url = "http://" + url
    return {"http": url, "https": url}


def access_status(access_token: str, proxy: str = "", timeout: int = 20) -> int:
    """Mã HTTP của `GET /v1/me`. 401 = AT hết hiệu lực. 0 = không gọi được."""
    token = str(access_token or "").strip()
    if not token:
        return 0
    try:
        resp = requests.get(
            ACCESS_URL,
            headers={"accept": "application/json",
                     "authorization": "Bearer %s" % token},
            timeout=timeout,
            proxies=_proxies(proxy),
        )
        return int(resp.status_code or 0)
    except Exception:  # noqa: BLE001 — mạng lỗi thì coi như không xác định
        return 0


def trial_offer(payload: Any) -> tuple[str, str]:
    """Đọc `accounts/check` -> (active | eligible | not_eligible | unknown, chi tiết)."""
    if not isinstance(payload, dict):
        return "unknown", ""
    accounts = payload.get("accounts")
    if isinstance(accounts, dict) and accounts:
        entry = next(iter(accounts.values()), {})
        if not isinstance(entry, dict):
            entry = {}
        entitlement = entry.get("entitlement") if isinstance(entry.get("entitlement"), dict) else {}
        account_info = entry.get("account") if isinstance(entry.get("account"), dict) else {}
        if entitlement.get("trial") or entitlement.get("is_active_subscription_gratis"):
            return "active", str(entitlement.get("subscription_plan") or "plus")
        promos = entry.get("eligible_promo_campaigns")
        promo_data = account_info.get("promo_data")
        blob = json.dumps({"promos": promos, "promo_data": promo_data}, ensure_ascii=False)
        if promos or promo_data:
            if _FREE_RE.search(blob):
                return "eligible", blob[:300]
            return "unknown", blob[:300]
        return "not_eligible", ""
    blob = json.dumps(payload, ensure_ascii=False)
    if _FREE_RE.search(blob):
        return "eligible", blob[:300]
    return "unknown", ""


def account_promo(access_token: str, proxy: str = "", timeout: int = 25) -> dict:
    """Trạng thái ưu đãi/trial của acc. `ok=False` khi không gọi được."""
    token = str(access_token or "").strip()
    if not token:
        return {"ok": False, "status": 0, "trial_status": "unknown", "detail": ""}
    headers = {
        "authorization": "Bearer %s" % token,
        "accept": "application/json",
        "user-agent": UA,
        "origin": "https://chatgpt.com",
        "referer": "https://chatgpt.com/",
        "oai-language": "en-US",
        "x-openai-target-path": ACCOUNTS_PATH,
        "x-openai-target-route": ACCOUNTS_PATH,
    }
    try:
        resp = requests.get(ACCOUNTS_URL, headers=headers, timeout=timeout,
                            proxies=_proxies(proxy))
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "status": 0, "trial_status": "unknown", "detail": str(exc)[:200]}
    status = int(resp.status_code or 0)
    try:
        data = resp.json()
    except ValueError:
        data = {}
    trial_status, detail = trial_offer(data)
    return {
        "ok": 200 <= status < 300,
        "status": status,
        "trial_status": trial_status,
        "detail": detail,
    }


def check(access_token: str, proxy: str = "", timeout: int = 20) -> dict:
    """Chạy cả hai phép kiểm và kết luận.

    `plus` = True khi AT đã 401 (acc bị nâng cấp/vô hiệu hoá AT cũ) HOẶC acc đang
    trong trial. Trả kèm `note` để UI hiện chi tiết thô khi cần tra.
    """
    at = access_status(access_token, proxy, timeout)
    promo = account_promo(access_token, proxy, max(timeout, 25))
    trial = str(promo.get("trial_status") or "unknown")
    plus = (at == 401) or (trial == "active")
    note = "AT %s · promo %s" % (at or "?", trial)
    return {"plus": plus, "at_status": at, "trial_status": trial,
            "promo_status": int(promo.get("status") or 0), "note": note}
