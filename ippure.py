"""Chấm chất lượng proxy bằng ippure.com — thay hẳn findip.net.

Vì sao: ippure nhắm đúng bài toán "IP có sạch để dùng cho dịch vụ AI không"
(ChatGPT/Claude), trả `risk_score` + `is_residential` + `is_broadcast`, và tra
được **IP tuỳ ý** nên thay được đúng chỗ findip đã làm (`lookup(ip)` + cache).

Có 2 đường lấy dữ liệu, tự động rơi xuống đường thấp hơn khi đường trên hỏng:

  1. API KÝ (chính) — `https://api.123169.xyz`
     - Bootstrap: request đầu KHÔNG ký -> response trả header `x-k` (khoá) và
       `x-t` (giờ server dạng ms). Client lưu lại rồi gửi lại request ĐÃ KÝ.
     - Chữ ký: header `x-t: "{t}-{HMAC_SHA256(key=x-k, msg='METHOD-URL-BODY-t')}"`,
       trong đó `t = serverTime + (now - localTime)` (giờ đã hiệu chỉnh theo server).
     - Cho IP tuỳ ý: `/api/info/ip-basic/<ip>`, `/api/info/ip-risk/<ip>`,
       `/api/info/asn/botclass/<asn>`.
     - ĐÂY LÀ API RIÊNG, phải reverse-engineer từ JS của site, có ký chống bot ->
       có thể đổi bất cứ lúc nào. Vì vậy mọi hàm ở đây đều phải chịu được việc
       nó trả `{"ok":false}` hoặc đổi shape, và rơi xuống đường 2 chứ không nổ.
  2. Endpoint công khai (dự phòng) — `https://my.ippure.com/v1/info`
     - Có tài liệu, nhưng chỉ trả thông tin của **chính IP đang gọi**, nên phải
       đi QUA proxy. Bẫy đã đo: phải gửi User-Agent giống browser, không thì
       server trả payload rút gọn MẤT fraudScore/isResidential/isBroadcast
       (vẫn HTTP 200, im lặng).
  3. Không có gì (dự phòng cuối) — trace chatgpt.com / ipwho.is, chỉ có geo.

Cache 12h theo IP ở `.cache/ippure.json` (đổi bằng UPI_IPPURE_CACHE_TTL).
Vì sao cần cache: quét lại cùng một pool là chuyện thường, mà mỗi IP tốn 2
request API; không cache thì vừa chậm vừa dễ bị chặn.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

try:
    import requests
except Exception:  # noqa: BLE001
    requests = None  # type: ignore[assignment]

try:  # ưu tiên curl_cffi: API ippure nằm sau Cloudflare
    from curl_cffi import requests as cffi_requests
except Exception:  # noqa: BLE001
    cffi_requests = None  # type: ignore[assignment]


ROOT = Path(__file__).resolve().parent
CACHE_FILE = ROOT / ".cache" / "ippure.json"
API_BASE = "https://api.123169.xyz"
PUBLIC_INFO_URL = "https://my.ippure.com/v1/info"
TRACE_URL = "https://chatgpt.com/cdn-cgi/trace"
IPWHO_URL = "https://ipwho.is/"

# UA Chrome thật: bắt buộc cho endpoint công khai, xem bẫy ở docstring.
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)

_session_lock = threading.Lock()
_sessions: dict[str, Any] = {}
# Trạng thái ký của API riêng (khoá + hiệu chỉnh giờ), dùng chung cả tiến trình.
_sign_state: dict[str, Any] = {"key": None, "server_time": 0, "local_time": 0}
_sign_lock = threading.Lock()

# API ký có HẠN MỨC THEO NGÀY (server trả {"ok":false,"message":"The quota is
# exhausted, come back tomorrow"}). Đo thật: quét vài trăm proxy là hết, sau đó
# /api/info/ip-basic trả rỗng và proxy bị chấm "không có chất lượng" (risk=None)
# mà không rõ lý do. Nên khi gặp là nghỉ hẳn một khoảng, tránh đốt request vô ích.
_quota_lock = threading.Lock()
_quota_until: float = 0.0
_quota_message = ""


def quota_exhausted() -> bool:
    with _quota_lock:
        return time.time() < _quota_until


def quota_note() -> str:
    with _quota_lock:
        if time.time() >= _quota_until:
            return ""
        left = int(_quota_until - time.time())
        return f"ippure hết hạn mức ngày (còn {left // 3600}h{left % 3600 // 60:02d}p): {_quota_message}"


def _mark_quota(msg: str) -> None:
    """Nghỉ tới hết ngày (giờ VN) hoặc 6h, tuỳ cái nào ngắn hơn."""
    global _quota_until, _quota_message
    try:
        hours = int(os.environ.get("UPI_IPPURE_QUOTA_COOLDOWN_H") or 0)
    except Exception:  # noqa: BLE001
        hours = 0
    if hours <= 0:
        lt = time.localtime()
        seconds_left = (23 - lt.tm_hour) * 3600 + (59 - lt.tm_min) * 60 + (60 - lt.tm_sec)
        hours_secs = min(max(seconds_left, 600), 6 * 3600)
    else:
        hours_secs = hours * 3600
    with _quota_lock:
        _quota_until = time.time() + hours_secs
        _quota_message = str(msg)[:120]


def enabled() -> bool:
    """ippure không cần token. Tắt bằng UPI_IPQUALITY=off."""
    if str(os.environ.get("UPI_IPQUALITY", "") or "").strip().lower() in ("off", "0", "false", "no"):
        return False
    return requests is not None or cffi_requests is not None


def proxy_label(proxy: str) -> str:
    """Nhãn không lộ credential."""
    try:
        parsed = urlsplit(proxy if "://" in proxy else f"http://{proxy}")
        return f"{parsed.hostname or ''}#{abs(hash(proxy)) % (16 ** 10):010x}"
    except Exception:  # noqa: BLE001
        return "proxy#?"


# ------------------------------------------------------------------ HTTP gốc

def _session(impersonate: str = "chrome") -> Any:
    key = "cffi" if cffi_requests is not None else "req"
    with _session_lock:
        if key not in _sessions:
            if cffi_requests is not None:
                _sessions[key] = cffi_requests.Session(impersonate=impersonate)
            else:
                _sessions[key] = requests.Session()
        return _sessions[key]


def _http_get(url: str, *, proxy: str = "", timeout: int = 20,
              headers: dict[str, str] | None = None) -> Any:
    """GET 1 lần. Trả response; raise nếu lỗi mạng."""
    h = {"User-Agent": UA, "Accept": "application/json, text/plain, */*",
         "Accept-Language": "en-US,en;q=0.9"}
    if headers:
        h.update(headers)
    s = _session()
    if proxy:
        proxies = {"http": proxy, "https": proxy}
        if cffi_requests is not None:
            return s.get(url, headers=h, proxies=proxies, timeout=timeout)
        return s.get(url, headers=h, proxies=proxies, timeout=timeout)
    return s.get(url, headers=h, timeout=timeout)


# ------------------------------------------------------- API ký (đường chính)

def _absorb(resp: Any) -> None:
    """Lưu `x-k`/`x-t` từ response để dùng cho các request sau."""
    try:
        new_key = resp.headers.get("x-k")
        new_xt = resp.headers.get("x-t")
    except Exception:  # noqa: BLE001
        return
    with _sign_lock:
        if new_key:
            _sign_state["key"] = new_key
        if new_xt:
            try:
                _sign_state["server_time"] = int(new_xt)
                _sign_state["local_time"] = time.time() * 1000
            except Exception:  # noqa: BLE001
                pass


def _clock_now() -> int:
    """Giờ đã hiệu chỉnh theo server: serverTime + thời gian trôi từ lúc nhận."""
    with _sign_lock:
        st = _sign_state.get("server_time") or 0
        lt = _sign_state.get("local_time") or 0
        key = _sign_state.get("key")
    if not st or not lt or not key:
        return int(time.time() * 1000)
    return int(st + (time.time() * 1000 - lt))


def _sign(method: str, url: str, body: str, key: str, t: int) -> str:
    msg = "-".join([method, url, body or "", str(t)])
    return hmac.new(key.encode(), msg.encode(), hashlib.sha256).hexdigest()


def _api_get(path: str, timeout: int = 15) -> dict[str, Any]:
    """GET có ký lên API riêng. Trả {} nếu không lấy được (để caller rơi xuống).

    Vòng lặp: request đầu không ký -> nhận khoá -> gửi lại đã ký. Nếu response
    trả khoá mới thì cập nhật và thử lại (server xoay khoá), tối đa 4 lần.
    """
    url = API_BASE + path
    if quota_exhausted():
        return {}
    for _ in range(4):
        with _sign_lock:
            key = _sign_state.get("key")
        headers: dict[str, str] = {"Origin": "https://ippure.com", "Referer": "https://ippure.com/"}
        signed = bool(key)
        if signed:
            t = _clock_now()
            headers["x-k"] = str(key)
            headers["x-t"] = f"{t}-{_sign('GET', url, '', str(key), t)}"
        before = key
        try:
            resp = _http_get(url, timeout=timeout, headers=headers)
        except Exception:  # noqa: BLE001
            return {}
        _absorb(resp)
        try:
            payload = resp.json()
        except Exception:  # noqa: BLE001
            return {}
        if isinstance(payload, dict) and payload.get("ok"):
            return payload
        # Hết hạn mức ngày -> nghỉ, đừng thử lại (thử nữa cũng false)
        msg = str((payload or {}).get("message") or "") if isinstance(payload, dict) else ""
        if "quota" in msg.lower():
            _mark_quota(msg)
            return {}
        with _sign_lock:
            after = _sign_state.get("key")
        # ok=false: chỉ thử lại khi có khoá MỚI để thử (đã ký mà vẫn false = bó tay)
        if not after or after == before:
            return {}
    return {}


# ------------------------------------------------------------------- cache

def _cache_ttl() -> int:
    try:
        return int(os.environ.get("UPI_IPPURE_CACHE_TTL") or 12 * 3600)
    except Exception:  # noqa: BLE001
        return 12 * 3600


# RLock chứ không phải Lock: cache_put() giữ khoá rồi gọi _load_cache() cũng khoá
# lại -> với Lock thường là deadlock ngay (đã dính thật khi test).
_cache_lock = threading.RLock()
_cache: dict[str, Any] | None = None


def _load_cache() -> dict[str, Any]:
    global _cache
    with _cache_lock:
        if _cache is None:
            try:
                raw = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
                _cache = raw if isinstance(raw, dict) else {}
            except Exception:  # noqa: BLE001
                _cache = {}
        return _cache


def _save_cache() -> None:
    data = _cache or {}
    try:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        CACHE_FILE.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass


def cache_get(ip: str, ttl: int | None = None) -> dict[str, Any] | None:
    ttl = _cache_ttl() if ttl is None else ttl
    hit = _load_cache().get(ip)
    if isinstance(hit, dict) and time.time() - float(hit.get("ts") or 0) < ttl:
        v = hit.get("v")
        return v if isinstance(v, dict) else None
    return None


def cache_put(ip: str, value: dict[str, Any]) -> None:
    with _cache_lock:
        _load_cache()[ip] = {"ts": time.time(), "v": value}
        if len(_cache or {}) > 5000:          # chặn phình vô hạn
            for key in sorted(_cache, key=lambda k: float(_cache[k].get("ts") or 0))[:1000]:
                _cache.pop(key, None)
    _save_cache()


# ------------------------------------------------------- tra chất lượng IP

def _pick_geo(geo: dict[str, Any]) -> dict[str, Any]:
    """Chọn geo: ưu tiên nguồn có country_code + city, thứ tự IP2Location/MaxMind/DB-IP."""
    for name in ("IP2Location", "MaxMind", "DB-IP"):
        entry = geo.get(name) if isinstance(geo, dict) else None
        if isinstance(entry, dict) and entry.get("country_code"):
            return {
                "country": str(entry.get("country_code") or "").upper(),
                "region": str(entry.get("subdivisions") or ""),
                "city": str(entry.get("city") or ""),
                "postal_code": str(entry.get("postal_code") or ""),
                "timezone": str(entry.get("time_zone") or ""),
                "geo_source": name,
            }
    return {}


def lookup(ip: str, timeout: int = 15, use_cache: bool = True,
           with_asn_bot: bool = True) -> dict[str, Any]:
    """Chất lượng 1 IP bất kỳ qua API ký. Trả {} nếu không lấy được.

    Gộp 2 (hoặc 3) endpoint thành 1 dict đã chuẩn hoá, và cache theo IP.
    """
    ip = str(ip or "").strip()
    if not ip:
        return {}
    if use_cache:
        hit = cache_get(ip)
        if hit is not None:
            return hit

    basic = _api_get(f"/api/info/ip-basic/{quote(ip)}", timeout)
    data = basic.get("data") if isinstance(basic, dict) else None
    if not isinstance(data, dict):
        return {}
    risk = _api_get(f"/api/info/ip-risk/{quote(ip)}", timeout)
    risk_score = None
    if isinstance(risk, dict) and isinstance(risk.get("data"), dict):
        rs = risk["data"].get("risk_score")
        if isinstance(rs, (int, float)):
            risk_score = int(rs)

    asn = data.get("asn") if isinstance(data.get("asn"), dict) else {}
    asn_num = asn.get("number")
    bot = human = None
    if with_asn_bot and asn_num:
        bc = _api_get(f"/api/info/asn/botclass/{quote(str(asn_num))}", timeout)
        if isinstance(bc, dict) and isinstance(bc.get("data"), dict):
            b, h = bc["data"].get("bot"), bc["data"].get("human")
            bot = float(b) if isinstance(b, (int, float)) else None
            human = float(h) if isinstance(h, (int, float)) else None

    traits = data.get("traits") if isinstance(data.get("traits"), dict) else {}
    out = {
        "ip": str(data.get("ip") or ip),
        "risk_score": risk_score,
        "asn": asn_num,
        "org": str(asn.get("organization") or ""),
        "asn_type": str(asn.get("type") or ""),
        "is_residential": bool(asn.get("is_residential")) if "is_residential" in asn else None,
        "is_broadcast": bool(traits.get("is_broadcast")) if "is_broadcast" in traits else None,
        "is_warp": bool(traits.get("is_warp")) if "is_warp" in traits else None,
        "asn_bot": bot,
        "asn_human": human,
        "source": "ippure-signed",
        "quality_present": True,
        **_pick_geo(data.get("geo") if isinstance(data.get("geo"), dict) else {}),
    }
    cache_put(ip, out)
    return out


def public_info(proxy: str, timeout: int = 20) -> dict[str, Any]:
    """Endpoint công khai: chất lượng của CHÍNH IP đang gọi -> phải đi qua proxy."""
    try:
        resp = _http_get(PUBLIC_INFO_URL, proxy=proxy, timeout=timeout)
        d = resp.json()
    except Exception:  # noqa: BLE001
        return {}
    if not isinstance(d, dict) or not d.get("ip"):
        return {}
    ip = str(d["ip"])
    qualities = any(k in d for k in ("fraudScore", "isResidential", "isBroadcast"))
    return {
        "ip": ip,
        "country": str(d.get("countryCode") or "").upper(),
        "region": str(d.get("region") or ""),
        "city": str(d.get("city") or ""),
        "timezone": str(d.get("timezone") or ""),
        "asn": d.get("asn"),
        "org": str(d.get("asOrganization") or ""),
        "risk_score": int(d["fraudScore"]) if isinstance(d.get("fraudScore"), (int, float)) else None,
        "is_residential": bool(d.get("isResidential")) if "isResidential" in d else None,
        "is_broadcast": bool(d.get("isBroadcast")) if "isBroadcast" in d else None,
        "source": "ippure-public",
        "quality_present": bool(qualities),
    }


def _parse_trace(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in str(text or "").splitlines():
        if "=" in line:
            k, _, v = line.partition("=")
            out[k.strip()] = v.strip()
    return out


def _exit_ip(proxy: str, timeout: int) -> dict[str, Any]:
    """IP mà proxy xuất ra. Ưu tiên trace chatgpt.com (đúng host flow)."""
    t0 = time.time()
    try:
        resp = _http_get(TRACE_URL, proxy=proxy, timeout=timeout)
        data = _parse_trace(resp.text)
        ip = str(data.get("ip") or "")
        if ip:
            return {"ip": ip, "country": str(data.get("loc") or "").upper(),
                    "colo": str(data.get("colo") or ""), "source": "chatgpt-trace",
                    "family": "IPv6" if ":" in ip else "IPv4",
                    "latency_ms": int((time.time() - t0) * 1000)}
    except Exception:  # noqa: BLE001
        pass
    try:  # dự phòng: ipwho.is
        g = _http_get(IPWHO_URL, proxy=proxy, timeout=timeout).json()
        if isinstance(g, dict) and g.get("ip"):
            conn = g.get("connection") or {}
            return {"ip": str(g["ip"]), "country": str(g.get("country_code") or "").upper(),
                    "colo": "", "source": "ipwho", "asn": conn.get("asn"),
                    "isp": str(conn.get("isp") or ""),
                    "family": "IPv6" if ":" in str(g["ip"]) else "IPv4",
                    "latency_ms": int((time.time() - t0) * 1000)}
    except Exception:  # noqa: BLE001
        pass
    return {}


def probe_exit(proxy: str, timeout: int = 20) -> dict[str, Any]:
    """Đo IP exit của proxy + chất lượng của IP đó.

    Thứ tự cố ý, vì API ký CÓ HẠN MỨC NGÀY còn endpoint công khai thì không:
      1. trace chatgpt.com qua proxy -> IP exit + độ trễ (chứng minh flow tới được)
      2. `my.ippure.com/v1/info` qua proxy (MIỄN PHÍ) -> chất lượng, đủ cho IPv4
      3. chỉ khi (2) không có chất lượng (thường là exit IPv6) mới gọi API ký —
         và bỏ qua hẳn nếu đang hết hạn mức.
    Không có gì thì vẫn trả geo để chấm phần đo được, không chấm oan "unreachable".
    """
    if not proxy or not enabled():
        return {}
    probe = _exit_ip(proxy, timeout)
    pub = public_info(proxy, timeout)
    if not probe.get("ip") and pub.get("ip"):
        probe = dict(pub)
    if not probe.get("ip"):
        return {}

    ip = str(probe["ip"])
    quality: dict[str, Any] = {}
    reason = ""
    if pub.get("quality_present"):
        quality = pub
    elif quota_exhausted():
        reason = quota_note() or "ippure hết hạn mức ngày"
    else:
        signed = lookup(ip, timeout)
        if signed.get("quality_present"):
            quality = signed
        elif quota_exhausted():
            reason = quota_note() or "ippure hết hạn mức ngày"
        else:
            reason = "ippure không trả chất lượng cho IP này"

    if not quality.get("quality_present") and not reason:
        reason = "không lấy được chất lượng từ ippure"

    if quality.get("quality_present"):
        merged = dict(probe)
        for k, v in quality.items():
            if v not in (None, "") or k not in merged:
                merged[k] = v
        # giữ country/family/độ trễ đo được từ chính proxy khi API không trả
        merged["country"] = str(probe.get("country") or quality.get("country") or "").upper()
        merged["family"] = probe.get("family") or ("IPv6" if ":" in ip else "IPv4")
        merged["latency_ms"] = probe.get("latency_ms")
        merged["exit_source"] = probe.get("source")
        return merged

    probe["quality_present"] = False
    probe["quality_reason"] = reason
    return probe


# ------------------------------------------------- tín hiệu mobile (ip-api)

# Vì sao cần nguồn thứ hai: ippure chỉ có `asn.is_residential` (trùng khít với
# asn.type == "isp"), KHÔNG phân biệt được băng rộng với di động. ip-api có field
# `mobile` thật, miễn phí, không cần key.
# ip-api giới hạn 45 request/phút (header X-Rl/X-Ttl), nên khi quét cả pool thì
# PHẢI dùng endpoint /batch (100 IP mỗi lần) chứ không gọi từng IP.
IPAPI_SINGLE = "http://ip-api.com/json/{ip}"
IPAPI_BATCH = "http://ip-api.com/batch"


def _mobile_cache_key(ip: str) -> str:
    return f"mobile:{ip}"


def mobile_of(ip: str) -> bool | None:
    """`mobile` đã cache cho IP này, None nếu chưa biết."""
    hit = cache_get(_mobile_cache_key(ip))
    if isinstance(hit, dict):
        v = hit.get("mobile")
        return v if isinstance(v, bool) else None
    return None


def _mobile_put(ip: str, value: Any) -> None:
    if isinstance(value, bool):
        cache_put(_mobile_cache_key(ip), {"mobile": value})


def mobile_enabled() -> bool:
    return str(os.environ.get("UPI_IPPURE_MOBILE") or "1").strip().lower() not in ("0", "false", "no", "off")


def fetch_mobile_batch(ips: list[str], timeout: int = 15) -> dict[str, bool]:
    """Tra `mobile` cho nhiều IP bằng endpoint batch (100 IP/lần), có cache.

    Trả {ip: bool} cho những IP tra được. Lỗi mạng chỉ làm thiếu dữ liệu, không
    raise — thiếu `mobile` thì chấm như "không rõ", không phạt oan.
    """
    if not mobile_enabled() or not ips:
        return {}
    out: dict[str, bool] = {}
    todo: list[str] = []
    for ip in dict.fromkeys(str(i) for i in ips if i):
        cached = mobile_of(ip)
        if isinstance(cached, bool):
            out[ip] = cached
        else:
            todo.append(ip)
    for start in range(0, len(todo), 100):
        chunk = todo[start:start + 100]
        try:
            resp = _session().post(
                IPAPI_BATCH + "?fields=status,mobile,query",
                json=chunk,
                headers={"User-Agent": UA, "Content-Type": "application/json"},
                timeout=timeout,
            )
            data = resp.json()
        except Exception:  # noqa: BLE001
            continue
        if not isinstance(data, list):
            continue
        for row in data:
            if not isinstance(row, dict) or row.get("status") != "success":
                continue
            ip = str(row.get("query") or "")
            if ip and isinstance(row.get("mobile"), bool):
                out[ip] = row["mobile"]
                _mobile_put(ip, row["mobile"])
    return out


def fetch_mobile(ip: str, timeout: int = 15) -> bool | None:
    """`mobile` cho 1 IP (đường chấm lẻ). Có cache nên gọi lại là free."""
    cached = mobile_of(ip)
    if isinstance(cached, bool):
        return cached
    return fetch_mobile_batch([ip], timeout).get(ip)


# --------------------------------------------------------------- chấm điểm

# Thang vẫn 100 nhưng đổi trọng số theo yêu cầu "ưu tiên dân dụng/di động":
#   dân dụng 12 + di động 8 = 20 (trước là 10), lấy bớt từ tới-được và độ trễ
#   để tổng không đổi -> xếp hạng vẫn so sánh được với các bản trước.
W_REACH = 25
W_COUNTRY = 20
W_QUALITY = 15      # ippure: risk_score thấp
W_RESIDENTIAL = 12  # ippure: asn.is_residential
W_MOBILE = 8        # ip-api: mobile
W_TZ = 5
W_IPV4 = 5
W_LATENCY = 10

# Kẹp điểm theo risk_score — trước đây risk cao chỉ bị trừ 15 điểm nên IP bẩn
# (đo thật: risk=42) vẫn đạt grade A. Giờ chặn hẳn, giống cách xử lý hosting.
RISK_CAP_HIGH = 60   # > ngưỡng này -> coi như không dùng được
RISK_CAP_MID = 30    # > ngưỡng này -> chỉ xếp cuối (grade C)
CAP_HIGH = 30
CAP_MID = 50


def latency_points(latency_ms: Any) -> int:
    if not isinstance(latency_ms, int):
        return W_LATENCY // 2
    if latency_ms < 2500:
        return W_LATENCY
    if latency_ms < 4500:
        return max(1, W_LATENCY * 2 // 3)
    if latency_ms < 6000:
        return max(1, W_LATENCY // 3)
    return 0


def quality_points(risk_score: Any) -> tuple[int, str]:
    """risk_score 0..100 (thấp = sạch). Trả (điểm, mô tả)."""
    if not isinstance(risk_score, (int, float)):
        return W_QUALITY // 2, "ippure không trả risk_score"
    score = int(risk_score)
    if score <= 10:
        return W_QUALITY, f"ippure sạch (risk={score})"
    if score <= 30:
        return W_QUALITY // 2, f"ippure tạm được (risk={score})"
    return 0, f"ippure xấu (risk={score})"


def verdict_of(risk_score: Any) -> str:
    """Nhãn ngắn cho UI (giữ tên `verdict` như bản findip để app.js không phải sửa)."""
    if not isinstance(risk_score, (int, float)):
        return ""
    if risk_score <= 10:
        return "clean"
    if risk_score <= 30:
        return "medium"
    return "dirty"


def grade_of(score: int, flags: list[str]) -> str:
    if {"malicious", "unreachable"} & set(flags):
        return "F"
    return "A" if score >= 85 else "B" if score >= 70 else "C" if score >= 50 else "F"


def _blank(ip: str = "") -> dict[str, Any]:
    return {"ok": False, "ip": ip, "score": 0, "grade": "F", "flags": ["unreachable"],
            "reasons": ["proxy không kết nối được (hoặc auth fail)"], "risk": None,
            "risk_score": None, "is_residential": None, "is_broadcast": None,
            "asn_bot": None, "asn_human": None, "country": "", "timezone": "",
            "family": "", "colo": "", "latency_ms": None, "isp": "", "asn": None,
            "source": ""}


def judge(probe: dict[str, Any], target_country: str = "IN",
          target_tz: str = "Asia/Kolkata",
          history: dict[str, Any] | None = None) -> dict[str, Any]:
    """Chấm 1 lần đo thành điểm 0..100 + grade A/B/C/F + lý do."""
    history = history or {}
    flags: list[str] = []
    detail: list[str] = []
    if not probe or not probe.get("ip"):
        return _blank()
    ip = str(probe["ip"])

    score = W_REACH
    latency_ms = probe.get("latency_ms")
    score += latency_points(latency_ms)
    if isinstance(latency_ms, int) and latency_ms >= 6000:
        flags.append("slow")

    country = str(probe.get("country") or "").upper()
    cc_target = str(target_country or "").upper()
    if cc_target and country and country != cc_target:
        flags.append("wrong-country")
        detail.append(f"exit ở {country}, cần {cc_target}")
    elif cc_target and country == cc_target:
        score += W_COUNTRY
        detail.append(f"exit đúng {cc_target} (nguồn {probe.get('exit_source') or probe.get('source') or '?'})")

    tz = str(probe.get("timezone") or "")
    is_res = probe.get("is_residential")
    is_bcast = probe.get("is_broadcast")
    user_type = ""

    if not probe.get("quality_present"):
        flags.append("no-quality")
        reason_q = str(probe.get("quality_reason") or "")
        if reason_q:
            detail.append(reason_q)
        score += W_QUALITY // 2 + W_RESIDENTIAL // 2 + W_MOBILE // 2
        if probe.get("family") == "IPv4":
            score += W_IPV4
        if tz and tz == target_tz:
            score += W_TZ
        score = max(0, min(100, score))
        return {**probe, "ok": True, "ip": ip, "country": country, "timezone": tz,
                "user_type": "", "risk": None, "verdict": "", "score": score,
                "quality_reason": reason_q,
                "grade": grade_of(score, flags), "flags": flags, "reasons": detail}

    pts, why = quality_points(probe.get("risk_score"))
    score += pts
    detail.append(why)
    rs = probe.get("risk_score")
    if isinstance(rs, (int, float)) and rs > RISK_CAP_MID:
        flags.append("dirty-ip")

    if is_bcast:
        flags.append("hosting")
        detail.append("isBroadcast=true (IP kiểu broadcast/datacenter)")
    if is_res is True:
        user_type = "residential"
        score += W_RESIDENTIAL
        detail.append(f"isResidential=true (asn.type={probe.get('asn_type') or '?'})")
    elif is_res is False:
        user_type = "non-residential"
        detail.append(f"isResidential=false (asn.type={probe.get('asn_type') or '?'})")
    else:
        score += W_RESIDENTIAL // 2

    # Di động (ip-api). Không rõ thì cho nửa điểm, không phạt oan khi ip-api lỗi.
    mob = probe.get("mobile")
    if mob is True:
        score += W_MOBILE
        user_type = (user_type + "+mobile") if user_type else "mobile"
        detail.append("mobile=true (ip-api)")
    elif mob is False:
        detail.append("mobile=false (ip-api)")
    else:
        score += W_MOBILE // 2

    bot, human = probe.get("asn_bot"), probe.get("asn_human")
    if isinstance(bot, (int, float)) and isinstance(human, (int, float)) and human > 0:
        detail.append(f"ASN traffic: human={human:.1f}% bot={bot:.1f}%")

    if tz and tz == target_tz:
        score += W_TZ
    elif tz:
        flags.append("tz-mismatch")
        detail.append(f"timezone {tz} != persona {target_tz}")

    if str(probe.get("family") or "") == "IPv4":
        score += W_IPV4
    elif probe.get("family") == "IPv6":
        flags.append("ipv6")
        detail.append("exit IPv6 (một số anti-fraud chấm khác IPv4)")

    if history.get("success"):
        score += min(10, int(history["success"]) * 4)
    if history.get("fail"):
        score -= min(30, int(history["fail"]) * 10)
        flags.append("history-fail")

    if {"wrong-country", "hosting"} & set(flags):
        score = min(score, 30)

    # Kẹp theo risk_score: IP bẩn bị chặn thật, không chỉ trừ 15 điểm như trước.
    if isinstance(rs, (int, float)):
        if rs > RISK_CAP_HIGH:
            score = min(score, CAP_HIGH)
            flags.append("risk-blocked")
            detail.append(f"risk={int(rs)} > {RISK_CAP_HIGH} -> kẹp {CAP_HIGH} (loại)")
        elif rs > RISK_CAP_MID:
            score = min(score, CAP_MID)
            detail.append(f"risk={int(rs)} > {RISK_CAP_MID} -> kẹp {CAP_MID} (xếp cuối)")

    score = max(0, min(100, score))
    return {"ok": True, "ip": ip, "country": country, "timezone": tz,
            "family": probe.get("family", ""), "colo": probe.get("colo", ""),
            "latency_ms": latency_ms, "isp": probe.get("org") or probe.get("isp", ""),
            "asn": probe.get("asn"), "user_type": user_type,
            "asn_type": probe.get("asn_type", ""), "risk": probe.get("risk_score"),
            "quality_present": True,
            "verdict": verdict_of(probe.get("risk_score")),
            "risk_score": probe.get("risk_score"), "is_residential": is_res,
            "is_broadcast": is_bcast, "mobile": mob,
            "asn_bot": bot, "asn_human": human,
            "source": probe.get("source", ""), "score": score,
            "grade": grade_of(score, flags), "flags": flags, "reasons": detail}


def check(proxy: str, target_country: str = "IN", target_tz: str = "Asia/Kolkata",
          timeout: int = 20, history: dict[str, Any] | None = None) -> dict[str, Any]:
    """Đo + chấm 1 proxy. Luôn trả dict có `label` (không lộ credential)."""
    probe = probe_exit(proxy, timeout)
    if probe.get("ip") and probe.get("quality_present"):
        probe["mobile"] = fetch_mobile(str(probe["ip"]), timeout)
    result = judge(probe, target_country, target_tz, history)
    result["proxy"] = proxy
    result["label"] = proxy_label(proxy)
    return result


def scan(proxies: list[str], target_country: str = "IN", target_tz: str = "Asia/Kolkata",
         workers: int = 16, timeout: int = 20, history_of: Any = None) -> list[dict[str, Any]]:
    """Chấm cả pool song song, trả list đã xếp điểm cao -> thấp."""
    if not proxies:
        return []

    def one(proxy: str) -> dict[str, Any]:
        try:
            hist = history_of(proxy) if callable(history_of) else None
            return check(proxy, target_country, target_tz, timeout, hist)
        except Exception as exc:  # noqa: BLE001
            return {"proxy": proxy, "label": proxy_label(proxy), "ok": False, "ip": "",
                    "score": 0, "grade": "F", "flags": ["probe-error"],
                    "reasons": [f"{type(exc).__name__}: {str(exc)[:80]}"], "risk": None,
                    "risk_score": None, "is_residential": None, "is_broadcast": None,
                    "asn_bot": None, "asn_human": None, "country": "", "timezone": "",
                    "family": "", "colo": "", "latency_ms": None, "isp": "", "asn": None,
                    "source": ""}

    with ThreadPoolExecutor(max_workers=max(1, min(workers, len(proxies)))) as ex:
        rows = list(ex.map(one, proxies))

    # `mobile` tra THEO LÔ (100 IP/request): gọi từng IP sẽ đụng rate limit 45/phút
    # của ip-api ngay khi quét pool lớn. Chấm lại đúng những dòng vừa có dữ liệu.
    if mobile_enabled():
        need = [str(r.get("ip")) for r in rows
                if r.get("ip") and r.get("quality_present") and r.get("mobile") is None]
        if need:
            got = fetch_mobile_batch(need, timeout)
            if got:
                for i, row in enumerate(rows):
                    if row.get("ip") in got and row.get("quality_present"):
                        probe = dict(row)
                        probe["mobile"] = got[row["ip"]]
                        fixed = judge(probe, target_country, target_tz,
                                      history_of(row["proxy"]) if callable(history_of) else None)
                        fixed["proxy"] = row.get("proxy", "")
                        fixed["label"] = row.get("label", "")
                        rows[i] = fixed

    rows.sort(key=lambda r: (-r.get("score", 0), r.get("latency_ms") or 99999))
    return rows


# ---------------------------------------------------------------------- CLI

def _read_proxies(path: str) -> list[str]:
    out: list[str] = []
    for line in Path(path).read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "://" not in line:
            parts = line.split(":")
            if len(parts) == 4:
                host, port, user, pw = parts
                line = f"http://{quote(user)}:{quote(pw)}@{host}:{port}"
            elif len(parts) == 2:
                line = f"http://{line}"
        out.append(line)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Chấm chất lượng proxy bằng ippure.com")
    parser.add_argument("cmd", choices=["scan", "one", "ip"],
                        help="scan: lọc cả file; one: 1 proxy; ip: tra chất lượng 1 IP")
    parser.add_argument("target", help="file proxy / chuỗi proxy / địa chỉ IP")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--country", default="IN")
    parser.add_argument("--tz", default="Asia/Kolkata")
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--min-grade", default="C", choices=["A", "B", "C", "F"])
    parser.add_argument("--out", default="", help="ghi danh sách proxy đạt chuẩn ra file")
    parser.add_argument("--json", default="", help="ghi kết quả đầy đủ ra file JSON")
    args = parser.parse_args(argv)

    if not enabled():
        print(json.dumps({"error": "không có HTTP client (cần requests hoặc curl_cffi)"}), flush=True)
        return 2

    if args.cmd == "ip":                      # tra IP bất kỳ qua API ký
        print(json.dumps(lookup(args.target, args.timeout), ensure_ascii=False, indent=2), flush=True)
        return 0

    proxies = _read_proxies(args.target) if args.cmd == "scan" else [args.target]
    t0 = time.time()
    rows = scan(proxies, args.country, args.tz, args.workers, args.timeout)
    ranking = {"A": 4, "B": 3, "C": 2, "F": 1}
    keep = [r for r in rows if ranking.get(r.get("grade", "F"), 0) >= ranking[args.min_grade]]

    print(f"# chấm {len(rows)} proxy trong {time.time()-t0:.0f}s | đạt >={args.min_grade}: {len(keep)}", flush=True)
    print(f"{'grade':>5} {'điểm':>4} {'exit ip':<40}{'cc':>3} {'risk':>5} {'res':>6}"
          f"{'mob':>6} {'bot%':>6} {'fam':>5} {'ms':>6}  flags", flush=True)
    for row in rows:
        bot = row.get("asn_bot")
        print(f"{row.get('grade','?'):>5} {row.get('score',0):>4} {str(row.get('ip') or '-'):<40}"
              f"{str(row.get('country') or '-'):>3} "
              f"{str(row.get('risk_score') if row.get('risk_score') is not None else '-'):>5} "
              f"{str(row.get('is_residential') if row.get('is_residential') is not None else '-'):>6}"
              f"{str(row.get('mobile') if row.get('mobile') is not None else '-'):>6}"
              f"{(f'{bot:.1f}' if isinstance(bot, (int, float)) else '-'):>6}"
              f"{str(row.get('family') or '-'):>5} {str(row.get('latency_ms') or '-'):>6}  "
              f"{','.join(row.get('flags') or [])}", flush=True)

    if args.json:
        Path(args.json).write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.out:
        Path(args.out).write_text("\n".join(r["proxy"] for r in keep) + "\n", encoding="utf-8")
        print(f"# đã ghi {len(keep)} proxy vào {args.out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
