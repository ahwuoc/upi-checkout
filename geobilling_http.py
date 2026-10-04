# -*- coding: utf-8 -*-
"""Shim HTTP tối thiểu cho `geobilling.py` (bản port từ upi-zero-link).

Bản gốc import `._vendor.http.make_session/request` — cả một module lớn của repo
kia. Ở đây chỉ cần đúng 2 hàm đó, nên viết lại gọn trên `curl_cffi` (đã có sẵn vì
extract_cs dùng nó) thay vì kéo nguyên `_vendor` sang.

Hành vi phải giữ giống bản gốc:
  • session có TLS fingerprint Chrome (`impersonate`) — mấy API geo hay chặn
    client không giống browser;
  • `request()` chỉ thử lại MỘT lần, và chỉ khi lỗi thuộc nhóm chứng chỉ/TLS.
"""
from __future__ import annotations

from urllib.parse import quote, urlsplit, urlunsplit

try:
    from curl_cffi import requests as curl_requests
except Exception:  # noqa: BLE001 — thiếu curl_cffi thì rơi về requests thường
    curl_requests = None
    import requests as _requests

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)


def normalize_proxy(proxy: str) -> str:
    """`host:port:user:pass` -> `http://user:pass@host:port` (curl_cffi cần dạng URL)."""
    raw = str(proxy or "").strip()
    if not raw:
        return ""
    body = raw.split("://", 1)[1] if "://" in raw else raw
    if "@" not in body and body.count(":") == 3:
        host, port, user, pw = body.split(":")
        return "http://%s:%s@%s:%s" % (quote(user), quote(pw), host, port)
    return raw if "://" in raw else "http://" + raw


def make_session(proxy: str = "", *, impersonate: str = "", user_agent: str = "",
                 accept_language: str = ""):
    """Session có TLS fingerprint browser, gắn proxy nếu có."""
    profile = impersonate or "chrome136"
    if curl_requests is not None:
        try:
            session = curl_requests.Session(impersonate=profile)
        except Exception:  # noqa: BLE001 — profile lạ thì lùi về chrome136
            session = curl_requests.Session(impersonate="chrome136")
        session.headers.update({
            "User-Agent": user_agent or DEFAULT_USER_AGENT,
            "Accept-Language": accept_language or "en-US,en;q=0.9",
        })
        if proxy:
            url = normalize_proxy(proxy)
            session.proxies = {"http": url, "https": url}
        return session

    session = _requests.Session()
    session.headers.update({
        "User-Agent": user_agent or DEFAULT_USER_AGENT,
        "Accept-Language": accept_language or "en-US,en;q=0.9",
    })
    if proxy:
        url = normalize_proxy(proxy)
        session.proxies = {"http": url, "https": url}
    return session


def request(session, method: str, url: str, **kwargs):
    """Gửi request; chỉ thử lại khi lỗi thuộc nhóm chứng chỉ/TLS."""
    verify = kwargs.pop("verify", True)
    attempts = [verify, False] if verify and url.startswith("https://") else [verify]
    last_error: Exception | None = None
    for current_verify in attempts:
        try:
            return session.request(method, url, verify=current_verify, **kwargs)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            detail = ("%s:%s" % (type(exc).__name__, exc)).lower()
            if current_verify and any(marker in detail for marker in
                                      ("certificate", "ssl", "curl: (60)")):
                continue
            raise
    if last_error is not None:
        raise last_error
    raise RuntimeError("request_failed")
