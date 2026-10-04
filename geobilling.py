# -*- coding: utf-8 -*-
"""按出口代理**实时**生成一致的印度账单地址。

为什么需要它
------------
`data/in_pincodes.json` 是抓取当时的快照，而且是"离线"的：它不知道这一轮的
出口 IP 到底落在哪个邦。Stripe / OpenAI 会把**税区**、**账单地址**和**出口国家**
三者对起来看；地址里的邦和出口所在邦对不上，正是那种最后以
`generic_decline` 收场的错配。

本模块直接问出口"你在哪"，再把那个坐标还原成一条**内部自洽**的印度地址：

    proxy --（走隧道）--> ip-api / ipwho.is / cdn-cgi/trace
          -> 出口 IP + lat/lon + 邦 + 国家
    lat/lon --（直连）--> Nominatim reverse / BigDataCloud
          -> 真实 PIN + 道路 + 街区
    PIN --（直连）--> api.postalpincode.in
          -> 权威 District + State（实时）
    -> Stripe 形状的账单地址

三条硬规则
----------
1. **门牌号是合成的，街区/道路/PIN 是真的。** 不复制任何真实住户的完整地址，
   只借用地理信息，让地址可信且自洽。
2. **运输层必须是 curl_cffi。** `urllib` 的 `ProxyHandler` 根本不支持 SOCKS，
   这正是"对每一条 `socks5h://` 出口都静默降级到硬编码兜底"的根因。
3. **任何一步失败都不往上抛。** 全部失败时 `address_for_proxy` 返回 `{}`，
   调用方保留自己的默认地址。

缓存：出口信息在进程内 TTL，PIN 数据落盘 30 天，已用过的地址进 ledger，
跨进程不重样。

环境变量
--------
    UPI_STATE_DIR        缓存目录（默认 ~/.upi-zero-link）
    UPI_GEO_BILLING      设为 0/false/no 关闭实时地址（回到数据集池）
    UPI_GEO_EXIT_TTL     出口信息缓存秒数（默认 600）
    UPI_GEO_TIMEOUT      单次地理请求超时秒数（默认 8）
"""
from __future__ import annotations

import json
import os
import random
import re
import threading
import time
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import quote_plus

from geobilling_http import make_session, request  # shim nội bộ của upi-checkout
# (bản gốc: from ._vendor.http import make_session, request)

# --------------------------------------------------------------------------
# 常量与持久化
# --------------------------------------------------------------------------

_STATE_DIR = Path(os.environ.get("UPI_STATE_DIR") or (Path.home() / ".upi-checkout"))
_PIN_STORE = _STATE_DIR / "in_pincode_store.json"
_SEED_STORE = _STATE_DIR / "in_state_seeds.json"
_LEDGER_STORE = _STATE_DIR / "in_address_ledger.json"

_PIN_TTL = 30 * 24 * 3600.0
_LEDGER_CAP = 6000
_GEO_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)

_LOCK = threading.RLock()
_STORE: dict[str, Any] = {"pins": {}, "seeds": {}, "ledger": [], "exits": {}}
_STORE_LOADED = False


def enabled() -> bool:
    """实时地址是否开启（默认开，`UPI_GEO_BILLING=0` 关闭）。"""
    value = str(os.environ.get("UPI_GEO_BILLING", "1")).strip().lower()
    return value not in ("0", "false", "no", "off")


def _timeout() -> float:
    try:
        return max(2.0, float(os.environ.get("UPI_GEO_TIMEOUT") or 8))
    except (TypeError, ValueError):
        return 8.0


def _exit_ttl() -> float:
    try:
        return max(0.0, float(os.environ.get("UPI_GEO_EXIT_TTL") or 600))
    except (TypeError, ValueError):
        return 600.0


# --------------------------------------------------------------------------
# 印度邦名 / PIN 参照表
# --------------------------------------------------------------------------

# Stripe 印度邦下拉里的规范写法
_STRIPE_STATES = (
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh",
    "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka",
    "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya",
    "Mizoram", "Nagaland", "Odisha", "Punjab", "Rajasthan", "Sikkim",
    "Tamil Nadu", "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand",
    "West Bengal", "Andaman & Nicobar Islands", "Chandigarh",
    "Dadra & Nagar Haveli & Daman & Diu", "Delhi", "Jammu & Kashmir",
    "Ladakh", "Lakshadweep", "Puducherry",
)

_STATE_ALIASES = {
    "orissa": "Odisha",
    "pondicherry": "Puducherry",
    "uttaranchal": "Uttarakhand",
    "chattisgarh": "Chhattisgarh",
    "chhatisgarh": "Chhattisgarh",
    "jammu and kashmir": "Jammu & Kashmir",
    "andaman and nicobar islands": "Andaman & Nicobar Islands",
    "andaman and nicobar": "Andaman & Nicobar Islands",
    "dadra and nagar haveli": "Dadra & Nagar Haveli & Daman & Diu",
    "daman and diu": "Dadra & Nagar Haveli & Daman & Diu",
    "nct of delhi": "Delhi",
    "national capital territory of delhi": "Delhi",
    "new delhi": "Delhi",
    "telengana": "Telangana",
    "tamilnadu": "Tamil Nadu",
}

# ISO 3166-2:IN 代码 -> 规范邦名
_STATE_CODES = {
    "AP": "Andhra Pradesh", "AR": "Arunachal Pradesh", "AS": "Assam",
    "BR": "Bihar", "CG": "Chhattisgarh", "CT": "Chhattisgarh", "GA": "Goa",
    "GJ": "Gujarat", "HR": "Haryana", "HP": "Himachal Pradesh",
    "JH": "Jharkhand", "KA": "Karnataka", "KL": "Kerala",
    "MP": "Madhya Pradesh", "MH": "Maharashtra", "MN": "Manipur",
    "ML": "Meghalaya", "MZ": "Mizoram", "NL": "Nagaland", "OD": "Odisha",
    "OR": "Odisha", "PB": "Punjab", "RJ": "Rajasthan", "SK": "Sikkim",
    "TN": "Tamil Nadu", "TS": "Telangana", "TG": "Telangana",
    "TR": "Tripura", "UP": "Uttar Pradesh", "UT": "Uttarakhand",
    "UL": "Uttarakhand", "WB": "West Bengal",
    "AN": "Andaman & Nicobar Islands", "CH": "Chandigarh",
    "DN": "Dadra & Nagar Haveli & Daman & Diu",
    "DD": "Dadra & Nagar Haveli & Daman & Diu", "DL": "Delhi",
    "JK": "Jammu & Kashmir", "LA": "Ladakh", "LD": "Lakshadweep",
    "PY": "Puducherry",
}

# 印度邮政按邮区分配 PIN：前两位就锁定投递圈，绝大多数情况下等同于邦。
# 只有反查拿不到邮编时才用它挑候选 PIN，挑出来的 PIN 仍然要实时校验。
_PIN_CIRCLES: dict[str, tuple[str, ...]] = {
    "11": ("Delhi",),
    "12": ("Haryana",), "13": ("Haryana",),
    "14": ("Punjab",), "15": ("Punjab",), "16": ("Punjab",),
    "17": ("Himachal Pradesh",),
    "18": ("Jammu & Kashmir",), "19": ("Jammu & Kashmir", "Ladakh"),
    "20": ("Uttar Pradesh",), "21": ("Uttar Pradesh",), "22": ("Uttar Pradesh",),
    "23": ("Uttar Pradesh",), "24": ("Uttar Pradesh", "Uttarakhand"),
    "25": ("Uttar Pradesh", "Uttarakhand"), "26": ("Uttar Pradesh", "Uttarakhand"),
    "27": ("Uttar Pradesh",), "28": ("Uttar Pradesh",),
    "30": ("Rajasthan",), "31": ("Rajasthan",), "32": ("Rajasthan",),
    "33": ("Rajasthan",), "34": ("Rajasthan",),
    "36": ("Gujarat",), "37": ("Gujarat",), "38": ("Gujarat",), "39": ("Gujarat",),
    "40": ("Maharashtra",), "41": ("Maharashtra",), "42": ("Maharashtra",),
    "43": ("Maharashtra",), "44": ("Maharashtra",),
    "45": ("Madhya Pradesh",), "46": ("Madhya Pradesh",),
    "47": ("Madhya Pradesh",), "48": ("Madhya Pradesh",),
    "49": ("Chhattisgarh",),
    "50": ("Telangana",),
    "51": ("Andhra Pradesh",), "52": ("Andhra Pradesh",), "53": ("Andhra Pradesh",),
    "56": ("Karnataka",), "57": ("Karnataka",), "58": ("Karnataka",),
    "59": ("Karnataka",),
    "60": ("Tamil Nadu",), "61": ("Tamil Nadu",), "62": ("Tamil Nadu",),
    "63": ("Tamil Nadu",), "64": ("Tamil Nadu",),
    "67": ("Kerala",), "68": ("Kerala", "Lakshadweep"), "69": ("Kerala",),
    "70": ("West Bengal",), "71": ("West Bengal",), "72": ("West Bengal",),
    "73": ("West Bengal",), "74": ("West Bengal", "Andaman & Nicobar Islands"),
    "75": ("Odisha",), "76": ("Odisha",), "77": ("Odisha",),
    "78": ("Assam",),
    "79": ("Arunachal Pradesh", "Manipur", "Meghalaya", "Mizoram",
           "Nagaland", "Tripura"),
    "80": ("Bihar",), "81": ("Bihar",),
    "82": ("Jharkhand",), "83": ("Jharkhand",),
    "84": ("Bihar",), "85": ("Bihar",),
}

# 名字按大区分池：喀拉拉邦的地址不该配一个旁遮普名字。
_NAME_REGIONS: dict[str, dict[str, tuple[str, ...]]] = {
    "north": {
        "first": ("Rahul", "Amit", "Vikram", "Ananya", "Rohan", "Neha", "Manish",
                  "Pooja", "Aditya", "Shreya", "Harish", "Rajesh", "Sunita",
                  "Mahesh", "Ritu", "Gaurav", "Preeti", "Sandeep", "Nisha",
                  "Ankit", "Kavita", "Deepak", "Sneha", "Yash", "Ishaan"),
        "last": ("Sharma", "Gupta", "Singh", "Verma", "Chauhan", "Agarwal",
                 "Mishra", "Chopra", "Yadav", "Bansal", "Jain", "Kapoor",
                 "Malhotra", "Saxena", "Rastogi", "Tiwari", "Pandey", "Arora"),
    },
    "west": {
        "first": ("Aarav", "Rohit", "Sagar", "Meera", "Nikhil", "Sanjay",
                  "Priya", "Kunal", "Shruti", "Omkar", "Tejas", "Prasad",
                  "Vaishnavi", "Harshad", "Dinesh", "Snehal", "Rupal", "Ajay"),
        "last": ("Patel", "Desai", "Joshi", "Kulkarni", "Naik", "Shah",
                 "Mehta", "Pawar", "Shinde", "Chavan", "Jadhav", "Gokhale",
                 "Trivedi", "Bhatt", "Solanki", "Rane"),
    },
    "south": {
        "first": ("Karthik", "Divya", "Lakshmi", "Arjun", "Meenakshi", "Suresh",
                  "Anitha", "Vignesh", "Deepa", "Hari", "Ramya", "Naveen",
                  "Sowmya", "Praveen", "Kavya", "Ashwin", "Bhavana", "Girish"),
        "last": ("Iyer", "Reddy", "Nair", "Menon", "Pillai", "Rao", "Naidu",
                 "Shetty", "Krishnan", "Subramanian", "Raghavan", "Hegde",
                 "Gowda", "Varma", "Chandran", "Balakrishnan"),
    },
    "east": {
        "first": ("Sourav", "Anirban", "Debashis", "Priyanka", "Rituparna",
                  "Abhijit", "Sneha", "Subhankar", "Mousumi", "Arindam",
                  "Payel", "Sudipta", "Rajarshi", "Tanushree", "Bikash"),
        "last": ("Banerjee", "Mukherjee", "Chatterjee", "Das", "Bose", "Ghosh",
                 "Sen", "Roy", "Dutta", "Saha", "Mondal", "Pal",
                 "Bhattacharya", "Mahato", "Behera", "Mohanty", "Patnaik",
                 "Sahoo"),
    },
    "northeast": {
        "first": ("Bikash", "Rupam", "Nabanita", "Pranab", "Jyoti", "Dhruba",
                  "Anup", "Rekha", "Tenzin", "Lalthan", "Aosenla", "Imli",
                  "Bendang", "Nongthombam", "Karma"),
        "last": ("Bora", "Saikia", "Das", "Gogoi", "Hazarika", "Baruah",
                 "Deka", "Nath", "Choudhury", "Jamir", "Ao", "Lalthansanga",
                 "Marak", "Sangma", "Debbarma"),
    },
}

_STATE_REGION = {
    "Delhi": "north", "Haryana": "north", "Punjab": "north",
    "Himachal Pradesh": "north", "Jammu & Kashmir": "north", "Ladakh": "north",
    "Chandigarh": "north", "Uttar Pradesh": "north", "Uttarakhand": "north",
    "Rajasthan": "north",
    "Maharashtra": "west", "Gujarat": "west", "Goa": "west",
    "Madhya Pradesh": "west", "Chhattisgarh": "west",
    "Dadra & Nagar Haveli & Daman & Diu": "west",
    "Tamil Nadu": "south", "Kerala": "south", "Karnataka": "south",
    "Telangana": "south", "Andhra Pradesh": "south", "Puducherry": "south",
    "Lakshadweep": "south", "Andaman & Nicobar Islands": "south",
    "West Bengal": "east", "Odisha": "east", "Bihar": "east",
    "Jharkhand": "east",
    "Assam": "northeast", "Arunachal Pradesh": "northeast",
    "Manipur": "northeast", "Meghalaya": "northeast", "Mizoram": "northeast",
    "Nagaland": "northeast", "Sikkim": "northeast", "Tripura": "northeast",
}

_BUILDING_TEMPLATES = (
    "Flat {n}, {name} Apartments", "House No. {n}, {name} Enclave",
    "{n}/{m}, {name} Residency", "Flat {n}{letter}, {name} Heights",
    "Plot {n}, {name} Layout", "Door No. {n}-{m}, {name} Nagar",
    "{ord} Floor, {name} Towers", "Flat {n}, {name} Block",
    "{n}{letter}, {name} CHS", "Shop {n}, {name} Complex",
    "Flat {n}, {name} Society", "H.No. {n}/{m}, {name} Vihar",
    "{n}, {name} Kripa", "Flat {n}, {name} Residency",
)

_BLOCK_NAMES = (
    "Sunrise", "Green", "Lake", "Palm", "Silver", "Royal", "Crystal", "Golden",
    "Prestige", "Brigade", "Sobha", "Godrej", "Puravankara", "Mahindra",
    "Amrapali", "Lodha", "Shree", "Vasant", "Anand", "Sai", "Krishna", "Ganga",
    "Nirmal", "Aakriti", "Vishwa", "Rohan", "Kolte", "Kalpataru", "Om", "Sagar",
    "Meera", "Vrindavan", "Shanti", "Ashoka", "Neelkanth", "Sanskar", "Utsav",
)


# --------------------------------------------------------------------------
# 小工具
# --------------------------------------------------------------------------

def _text(value: Any) -> str:
    return str(value or "").strip()


def canonical_state(value: Any) -> str:
    """把各种写法的邦名 / ISO 代码收敛到 Stripe 的拼法。"""
    text = re.sub(r"\s+", " ", _text(value))
    if not text:
        return ""
    upper = text.upper()
    if len(upper) == 2 and upper in _STATE_CODES:
        return _STATE_CODES[upper]
    lowered = text.lower().replace("&", "and")
    for alias, canonical in _STATE_ALIASES.items():
        if alias.replace("&", "and") == lowered:
            return canonical
    for canonical in _STRIPE_STATES:
        if canonical.lower().replace("&", "and") == lowered:
            return canonical
    return ""


def validate_pin(value: Any) -> str:
    """返回合法的 6 位印度 PIN，否则空串。

    印度 PIN 不以 0 开头，首位只能是 1-8；9 是军邮（APS），不是住宅地址。
    """
    digits = re.sub(r"\D", "", _text(value))
    if len(digits) != 6 or digits[0] not in "12345678":
        return ""
    return digits


def state_for_pin(pin: str) -> str:
    """静态邮区查询：该 PIN 前缀**唯一**确定邦时才返回。"""
    pin = validate_pin(pin)
    if not pin:
        return ""
    options = _PIN_CIRCLES.get(pin[:2]) or ()
    return options[0] if len(options) == 1 else ""


def circles_for_state(state: str) -> list[str]:
    return [circle for circle, states in _PIN_CIRCLES.items() if state in states]


def _clean_area(value: Any) -> str:
    """印度邮政会返回 `Rajbhavan (Bangalore)`、`... S.O.` 这类名字。

    去掉括号后缀和投递局类型后缀，让它读起来像用户在结账页填的街区名。
    """
    text = re.sub(r"\s+", " ", _text(value))
    text = re.sub(r"\s*\([^)]*\)\s*$", "", text).strip()
    text = re.sub(r"\s+(?:S\.?O\.?|B\.?O\.?|H\.?O\.?|G\.?P\.?O\.?|S\.?P\.?O\.?)$",
                  "", text, flags=re.I).strip()
    text = text.strip(" .,-")
    if len(text) < 3 or re.fullmatch(r"[\d\W]+", text):
        return ""
    return text[:120]


def _ordinal(number: int) -> str:
    if 10 <= number % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(number % 10, "th")
    return "%d%s" % (number, suffix)


# --------------------------------------------------------------------------
# 持久化
# --------------------------------------------------------------------------

def _load_store() -> None:
    global _STORE, _STORE_LOADED
    with _LOCK:
        if _STORE_LOADED:
            return
        loaded: dict[str, Any] = {"pins": {}, "seeds": {}, "ledger": [], "exits": {}}
        for path, key in ((_PIN_STORE, "pins"), (_SEED_STORE, "seeds"),
                          (_LEDGER_STORE, "ledger")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001 - 缓存缺失/损坏都按空处理
                continue
            if key == "ledger":
                if isinstance(data, list):
                    loaded[key] = [str(item) for item in data][-_LEDGER_CAP:]
            elif isinstance(data, dict):
                loaded[key] = data
        _STORE = loaded
        _STORE_LOADED = True


def _save(key: str) -> None:
    path = {"pins": _PIN_STORE, "seeds": _SEED_STORE, "ledger": _LEDGER_STORE}[key]
    try:
        _STATE_DIR.mkdir(parents=True, exist_ok=True)
        payload = _STORE[key]
        if key == "ledger":
            payload = payload[-_LEDGER_CAP:]
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:  # noqa: BLE001 - 缓存写不进去也不能影响提链
        pass


def _remember(key: str, seen: set[str]) -> bool:
    """地址没见过才返回 True，并记进 ledger（跨进程去重）。"""
    _load_store()
    with _LOCK:
        if key in seen or key in _STORE["ledger"]:
            return False
        seen.add(key)
        _STORE["ledger"].append(key)
        return True


# --------------------------------------------------------------------------
# 第 1 步：出口在哪
# --------------------------------------------------------------------------

def _get_json(session: Any, url: str, timeout: float) -> Any:
    response = request(session, "GET", url, timeout=timeout,
                       headers={"Accept": "application/json", "User-Agent": _GEO_UA})
    status = int(getattr(response, "status_code", 0) or 0)
    if status >= 400:
        raise RuntimeError("HTTP %s" % status)
    return json.loads(getattr(response, "text", "") or "{}")


def _get_text(session: Any, url: str, timeout: float) -> str:
    response = request(session, "GET", url, timeout=timeout, headers={
        "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-IN,en;q=0.9",
        "User-Agent": _GEO_UA,
    })
    status = int(getattr(response, "status_code", 0) or 0)
    if status >= 400:
        raise RuntimeError("HTTP %s" % status)
    return str(getattr(response, "text", "") or "")


def _probe_ip_api(session: Any, timeout: float) -> dict[str, Any]:
    fields = ("status,message,country,countryCode,region,regionName,city,zip,"
              "lat,lon,timezone,isp,org,as,query")
    data = _get_json(session, "http://ip-api.com/json/?fields=" + fields, timeout)
    if not isinstance(data, dict) or data.get("status") != "success":
        raise RuntimeError("ip-api failed")
    return {
        "ip": _text(data.get("query")),
        "country_code": _text(data.get("countryCode")).upper(),
        "country": _text(data.get("country")),
        "region": _text(data.get("regionName")),
        "region_code": _text(data.get("region")).upper(),
        "city": _text(data.get("city")),
        "postal_code": _text(data.get("zip")),
        "lat": data.get("lat"),
        "lon": data.get("lon"),
        "isp": _text(data.get("isp")),
        "source": "ip-api",
    }


def _probe_ipwho(session: Any, timeout: float) -> dict[str, Any]:
    data = _get_json(session, "https://ipwho.is/", timeout)
    if not isinstance(data, dict) or not data.get("success"):
        raise RuntimeError("ipwho.is failed")
    return {
        "ip": _text(data.get("ip")),
        "country_code": _text(data.get("country_code")).upper(),
        "country": _text(data.get("country")),
        "region": _text(data.get("region")),
        "region_code": _text(data.get("region_code")).upper(),
        "city": _text(data.get("city")),
        "postal_code": _text(data.get("postal")),
        "lat": data.get("latitude"),
        "lon": data.get("longitude"),
        "isp": _text((data.get("connection") or {}).get("isp")),
        "source": "ipwho.is",
    }


def _probe_cf_trace(session: Any, timeout: float) -> dict[str, Any]:
    """兜底：直接问 OpenAI 自己把这个出口认成哪里。"""
    text = _get_text(session, "https://chatgpt.com/cdn-cgi/trace", timeout)
    parsed: dict[str, str] = {}
    for line in text.splitlines():
        if "=" in line:
            key, _, value = line.partition("=")
            parsed[key.strip()] = value.strip()
    if not parsed.get("ip"):
        raise RuntimeError("cdn-cgi/trace returned no ip")
    return {
        "ip": parsed.get("ip", ""),
        "country_code": parsed.get("loc", "").upper(),
        "country": "",
        "region": "",
        "region_code": "",
        "city": parsed.get("colo", ""),
        "postal_code": "",
        "lat": None,
        "lon": None,
        "isp": "",
        "source": "cdn-cgi/trace",
    }


def probe_exit(proxy: str, *, timeout: float = 0.0, use_cache: bool = True) -> dict[str, Any]:
    """走隧道本身问出口"你在哪"。全部失败返回 `{}`。"""
    timeout = timeout or _timeout()
    cache_key = proxy or "direct"
    now = time.monotonic()
    _load_store()
    if use_cache:
        with _LOCK:
            hit = _STORE["exits"].get(cache_key)
        if hit and hit.get("expires", 0) > now:
            result = dict(hit["value"])
            result["cached"] = True
            return result

    result: dict[str, Any] = {}
    try:
        session = make_session(proxy, impersonate="chrome136", user_agent=_GEO_UA)
    except Exception:  # noqa: BLE001
        return {}
    try:
        for probe in (_probe_ip_api, _probe_ipwho, _probe_cf_trace):
            try:
                candidate = probe(session, timeout)
            except Exception:  # noqa: BLE001
                continue
            if candidate.get("ip"):
                result = candidate
                break
    finally:
        try:
            session.close()
        except Exception:  # noqa: BLE001
            pass

    if result:
        result["cached"] = False
        with _LOCK:
            _STORE["exits"][cache_key] = {
                "expires": now + _exit_ttl(), "value": dict(result)
            }
            if len(_STORE["exits"]) > 512:
                for key in list(_STORE["exits"])[:128]:
                    _STORE["exits"].pop(key, None)
    return result


def assert_exit_country(proxy: str, expected: str = "IN", *,
                        timeout: float = 0.0) -> dict[str, Any]:
    """把一轮提链卡在"出口确实在目标国家"这一步。

    出口不在目标国家时抛 `RuntimeError`，调用方换出口重来即可 —— 比跑到
    tax_update 再看到一个语焉不详的 `nonzero_due` 要好得多。
    """
    info = probe_exit(proxy, timeout=timeout)
    if not info:
        return {"ok": True, "verified": False, "reason": "geo_unreachable", "exit": {}}
    actual = _text(info.get("country_code")).upper()
    if expected and actual and actual != expected.upper():
        raise RuntimeError(
            "exit country is %s, expected %s (ip=%s via %s)"
            % (actual, expected.upper(), info.get("ip"), info.get("source"))
        )
    return {"ok": True, "verified": bool(actual), "exit": info}


# --------------------------------------------------------------------------
# 第 2 步：坐标 -> 真实 PIN / 街区
# --------------------------------------------------------------------------

def _reverse_nominatim(session: Any, lat: float, lon: float, timeout: float) -> dict[str, Any]:
    url = ("https://nominatim.openstreetmap.org/reverse?format=jsonv2"
           "&addressdetails=1&accept-language=en&lat=%s&lon=%s"
           % (quote_plus(str(lat)), quote_plus(str(lon))))
    data = _get_json(session, url, timeout)
    address = data.get("address") if isinstance(data, dict) else None
    if not isinstance(address, dict):
        raise RuntimeError("nominatim returned no address")
    return {
        "postal_code": _text(address.get("postcode")),
        "state": _text(address.get("state") or address.get("region")),
        "district": _text(address.get("state_district") or address.get("county")),
        "city": _text(address.get("city") or address.get("town")
                      or address.get("municipality") or address.get("village")),
        "locality": _text(address.get("suburb") or address.get("neighbourhood")
                          or address.get("city_district") or address.get("hamlet")),
        "road": _text(address.get("road") or address.get("pedestrian")
                      or address.get("residential")),
        "country_code": _text(address.get("country_code")).upper(),
        "source": "nominatim",
    }


def _reverse_bigdatacloud(session: Any, lat: float, lon: float, timeout: float) -> dict[str, Any]:
    url = ("https://api.bigdatacloud.net/data/reverse-geocode-client"
           "?latitude=%s&longitude=%s&localityLanguage=en" % (lat, lon))
    data = _get_json(session, url, timeout)
    if not isinstance(data, dict):
        raise RuntimeError("bigdatacloud returned no data")
    locality_info = data.get("localityInfo") if isinstance(data.get("localityInfo"), dict) else {}
    informative = (locality_info.get("informative")
                   if isinstance(locality_info.get("informative"), list) else [])
    road = ""
    for item in informative:
        if isinstance(item, dict) and item.get("description") == "street":
            road = _text(item.get("name"))
            break
    return {
        "postal_code": _text(data.get("postcode")),
        "state": _text(data.get("principalSubdivision")),
        "district": "",
        "city": _text(data.get("city") or data.get("locality")),
        "locality": _text(data.get("locality")),
        "road": road,
        "country_code": _text(data.get("countryCode")).upper(),
        "source": "bigdatacloud",
    }


def _reverse_geocode(lat: Any, lon: Any, *, timeout: float) -> dict[str, Any]:
    try:
        lat_f, lon_f = float(lat), float(lon)
    except (TypeError, ValueError):
        return {}
    if not (-90 <= lat_f <= 90 and -180 <= lon_f <= 180):
        return {}
    session = None
    try:
        session = make_session("", impersonate="chrome136", user_agent=_GEO_UA)
        for reverse in (_reverse_nominatim, _reverse_bigdatacloud):
            try:
                result = reverse(session, lat_f, lon_f, timeout)
            except Exception:  # noqa: BLE001
                continue
            if result.get("country_code") == "IN":
                return result
    except Exception:  # noqa: BLE001
        return {}
    finally:
        if session is not None:
            try:
                session.close()
            except Exception:  # noqa: BLE001
                pass
    return {}


# --------------------------------------------------------------------------
# 第 3 步：PIN -> 权威 District / State（实时 + 落盘缓存）
# --------------------------------------------------------------------------

def _pin_record(pin: str, *, timeout: float) -> dict[str, Any]:
    pin = validate_pin(pin)
    if not pin:
        return {}
    _load_store()
    with _LOCK:
        hit = _STORE["pins"].get(pin)
    if hit and hit.get("expires", 0) > time.time():
        return dict(hit["value"])

    session = None
    value: dict[str, Any] = {}
    try:
        session = make_session("", impersonate="chrome136", user_agent=_GEO_UA)
        data = _get_json(session, "https://api.postalpincode.in/pincode/%s" % pin, timeout)
        entry = data[0] if isinstance(data, list) and data else {}
        offices = entry.get("PostOffice") if isinstance(entry, dict) else None
        if (_text(entry.get("Status")).lower() == "success"
                and isinstance(offices, list) and offices):
            first = offices[0] if isinstance(offices[0], dict) else {}
            value = {
                "pin": pin,
                "state": canonical_state(first.get("State")) or _text(first.get("State")),
                "district": _clean_area(first.get("District")) or _text(first.get("District")),
                "areas": [
                    cleaned for cleaned in (
                        _clean_area(item.get("Name")) for item in offices[:24]
                        if isinstance(item, dict)
                    ) if cleaned
                ],
            }
    except Exception:  # noqa: BLE001
        value = {}
    finally:
        if session is not None:
            try:
                session.close()
            except Exception:  # noqa: BLE001
                pass

    with _LOCK:
        _STORE["pins"][pin] = {
            "expires": time.time() + (_PIN_TTL if value else 3600),
            "value": value,
        }
        if len(_STORE["pins"]) > 4000:
            for key in list(_STORE["pins"])[:512]:
                _STORE["pins"].pop(key, None)
        _save("pins")
    return dict(value)


def _candidate_pins(circle: str) -> list[str]:
    """给出该邮区里"最像真的"候选 PIN，最可能的排前面。

    印度 PIN = 邮区(2) + 分拣区(1) + 投递局(3)，而一个区的总局几乎总是
    ``…001``。所以拿分拣区数字去扫 `001` 结尾，命中率远高于盲扫。
    """
    digits = list("0123456789")
    random.shuffle(digits)
    candidates = ["%s%s001" % (circle, digit) for digit in digits]
    offsets = list(range(1, 80))
    random.shuffle(offsets)
    candidates.extend("%s%04d" % (circle, offset) for offset in offsets)
    return candidates


def _seed_pins(state: str, *, timeout: float, budget: int = 14) -> list[str]:
    """按邮区探出该邦可用的 PIN，只探一次并落盘。

    仅在反查拿不到邮编时使用；探出来的 PIN 仍然是从印度邮政实时取的真实数据。
    """
    _load_store()
    with _LOCK:
        known = _STORE["seeds"].get(state)
    if isinstance(known, list) and known:
        return [pin for pin in known if validate_pin(pin)]

    circles = circles_for_state(state)
    if not circles:
        return []
    random.shuffle(circles)

    found: list[str] = []
    attempts = 0
    for circle in circles:
        for candidate in _candidate_pins(circle):
            if attempts >= budget or len(found) >= 6:
                break
            attempts += 1
            if _pin_record(candidate, timeout=timeout).get("state") == state:
                found.append(candidate)
        if attempts >= budget or len(found) >= 6:
            break

    with _LOCK:
        if found:
            _STORE["seeds"][state] = found
            _save("seeds")
    return found


# --------------------------------------------------------------------------
# 第 4 步：拼地址
# --------------------------------------------------------------------------

def _synthetic_house() -> str:
    return random.choice(_BUILDING_TEMPLATES).format(
        n=random.randint(1, 780),
        m=random.randint(1, 99),
        letter=random.choice("ABCD"),
        name=random.choice(_BLOCK_NAMES),
        ord=_ordinal(random.randint(1, 16)),
    )


def _person_name(state: str) -> str:
    pool = _NAME_REGIONS.get(_STATE_REGION.get(state, "")) or _NAME_REGIONS["north"]
    return "%s %s" % (random.choice(pool["first"]), random.choice(pool["last"]))


def _assemble(*, state: str, district: str, pin: str, locality: str,
              road: str, name: str = "") -> dict[str, str]:
    house = _synthetic_house()
    anchor = _clean_area(locality) or _clean_area(road) or district
    line1 = ("%s, %s" % (house, anchor)).strip(", ") if anchor else house
    return {
        "name": name or _person_name(state),
        "line1": line1[:200],
        "line2": "",
        "city": (district or anchor or state)[:200],
        "state": state,
        "postal_code": pin,
        "country": "IN",
        "locality": anchor,
    }


def vary(address: dict[str, str], *, name: str = "") -> dict[str, str]:
    """同一条地理地址换个门牌号 / 姓名（PIN、邦、城市不变）。

    重试时想换一条地址，但**不能**换掉税区字段，否则 `expected_amount`
    跟 payment method 上的地址就对不上了。
    """
    if not address:
        return {}
    clone = dict(address)
    clone["line1"] = _assemble(
        state=address.get("state", ""), district=address.get("city", ""),
        pin=address.get("postal_code", ""), locality=address.get("locality", ""),
        road="", name=name or address.get("name", ""),
    )["line1"]
    if name:
        clone["name"] = name
    return clone


def address_for_proxy(
    proxy: str,
    *,
    country: str = "IN",
    name: str = "",
    timeout: float = 0.0,
    allow_offshore: bool = False,
    log: Optional[Callable[[str], None]] = None,
) -> dict[str, str]:
    """给 `proxy` 生成一条**实时、与出口一致**的印度账单地址。

    拿不到出口位置、或拼不出自洽的印度地址时返回 `{}`，让调用方保留自己的
    默认地址，而不是发一条坏地址出去。
    """
    emit = log or (lambda _message: None)
    timeout = timeout or _timeout()
    wanted = (country or "IN").upper()

    exit_info = probe_exit(proxy, timeout=timeout)
    if not exit_info:
        emit("  billing geo: exit location unavailable, falling back to the dataset")
        return {}

    exit_country = _text(exit_info.get("country_code")).upper()
    if wanted == "IN" and exit_country and exit_country != "IN" and not allow_offshore:
        emit("  billing geo: exit country is %s, not IN -- falling back to the dataset"
             % exit_country)
        return {}

    exit_state = (canonical_state(exit_info.get("region"))
                  or canonical_state(exit_info.get("region_code")))
    reverse = _reverse_geocode(exit_info.get("lat"), exit_info.get("lon"), timeout=timeout)

    # 优先信坐标：IP 定位通常只到城市/运营商枢纽，反查出来的才是真实所在邦。
    state = (canonical_state(reverse.get("state"))
             or state_for_pin(reverse.get("postal_code"))
             or exit_state)
    if not state:  # Stripe 不认识的邦比没有地址更糟
        emit("  billing geo: cannot canonicalise state %r, falling back to the dataset"
             % (reverse.get("state") or exit_info.get("region") or "?"))
        return {}

    pin = validate_pin(reverse.get("postal_code"))
    record: dict[str, Any] = {}
    if pin:
        record = _pin_record(pin, timeout=timeout)
        if record.get("state"):
            # 反查和邮政数据不一致时，以印度邮政为准
            if record["state"] != state:
                emit("  billing geo: PIN %s belongs to %s, not %s -- following India Post"
                     % (pin, record["state"], state))
            state = record["state"]
        else:
            pin = ""

    if not pin or not record:
        seeds = _seed_pins(state, timeout=timeout)
        if not seeds:
            emit("  billing geo: no live PIN found for %s, falling back to the dataset" % state)
            return {}
        pin = random.choice(seeds)
        record = _pin_record(pin, timeout=timeout)
        if not record:
            emit("  billing geo: PIN %s lookup failed, falling back to the dataset" % pin)
            return {}

    district = _text(record.get("district")) or _text(reverse.get("district"))
    areas = [area for area in (record.get("areas") or []) if area]
    locality = random.choice(areas) if areas else _text(reverse.get("locality"))

    seen: set[str] = set()
    for _ in range(24):
        candidate = _assemble(
            state=state, district=district, pin=pin, locality=locality,
            road=_text(reverse.get("road")), name=name,
        )
        key = "|".join((candidate["line1"], candidate["city"], candidate["state"],
                        candidate["postal_code"])).lower()
        if not _remember(key, seen):
            continue
        candidate["key"] = key
        candidate["source"] = "geo_exit:%s" % (
            reverse.get("source") or exit_info.get("source") or "?")
        candidate["exit_ip"] = _text(exit_info.get("ip"))
        candidate["exit_state"] = exit_state
        with _LOCK:
            _save("ledger")
        emit("  billing geo: %s / PIN %s / exit %s (%s) via %s"
             % (state, pin, exit_info.get("ip"), exit_country or "?", candidate["source"]))
        return candidate
    return {}


# --------------------------------------------------------------------------
# 离线自测
# --------------------------------------------------------------------------

def run_self_test() -> int:
    ok = True

    def check(label: str, condition: bool) -> None:
        nonlocal ok
        print("%-46s %s" % (label, "OK" if condition else "FAIL"))
        ok = ok and bool(condition)

    check("Orissa normalises to Odisha", canonical_state("Orissa") == "Odisha")
    check("ISO code MH -> Maharashtra", canonical_state("MH") == "Maharashtra")
    check("correctly spelled state passes through", canonical_state("Karnataka") == "Karnataka")
    check("unknown state is rejected", canonical_state("Atlantis") == "")
    check("valid PIN", validate_pin("560064") == "560064")
    check("leading-zero PIN is rejected", validate_pin("060064") == "")
    check("Army Postal PIN is rejected", validate_pin("900001") == "")
    check("short PIN is rejected", validate_pin("56006") == "")
    check("circle 56 -> Karnataka", state_for_pin("560064") == "Karnataka")
    check("circle 79 spans states -> empty", state_for_pin("790001") == "")
    check("Karnataka circles", "56" in circles_for_state("Karnataka"))
    check("Kerala circles", set(circles_for_state("Kerala")) >= {"67", "68", "69"})

    names = [_person_name("Kerala") for _ in range(60)]
    check("Kerala names come from the south pool",
          all(name.split()[-1] in _NAME_REGIONS["south"]["last"] for name in names))

    addr = _assemble(state="Karnataka", district="Bangalore", pin="560064",
                     locality="Jakkur", road="Jakkur Main Road")
    check("assembled address is complete",
          all(addr.get(field) for field in
              ("name", "line1", "city", "state", "postal_code", "country")))
    check("country is always IN", addr["country"] == "IN")
    check("PIN survives assembly", addr["postal_code"] == "560064")
    check("locality lands in line1", "Jakkur" in addr["line1"])

    varied = vary(addr)
    check("vary only changes house and name",
          varied["postal_code"] == addr["postal_code"]
          and varied["state"] == addr["state"]
          and varied["city"] == addr["city"]
          and varied["line1"] != addr["line1"])

    check("unreachable exit returns {} without raising",
          probe_exit("socks5h://user:pass@127.0.0.1:1", timeout=1.0) == {})
    check("address_for_proxy degrades to {}",
          address_for_proxy("socks5h://user:pass@127.0.0.1:1", timeout=1.0) == {})

    print("result: %s" % ("all passed" if ok else "FAILURES"))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(run_self_test())
