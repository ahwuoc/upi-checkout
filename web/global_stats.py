"""Thống kê global: mỗi ngày trích xuất được bao nhiêu mã **₹0.00** (và bao nhiêu đã thanh toán).

Chỉ đếm mã ₹0: mã giá gốc (₹1999, ₹1694…) không phải thứ dùng được nên đưa vào
thống kê chỉ làm loãng con số. Căn cứ là `amount_minor` — số tiền thật của checkout
(đơn vị paise, 0 = ₹0.00):
  * web  -> `task.artifact.amount_minor` trong `logs/web_*_results.json`
  * CLI  -> `links[].amount_minor` trong `logs/links_cli_*.json`
Link không có `amount_minor` (chưa probe được) thì KHÔNG tính là ₹0 — đếm riêng vào
`unknown_amount` để con số không âm thầm sai.

Nguồn là file trên đĩa, không phải RAM: số liệu sống qua restart và không nhảy theo
job đang mở. Cache ở `.cache/global_stats.json` khoá theo (mtime, size) từng file —
quét thật ~2s, lần sau ~0ms.
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path

_CACHE_VERSION = 3     # đổi khi schema cache thay đổi -> cache cũ bị bỏ


def _log_dir() -> Path:
    """Lấy LOG_DIR từ engine (tôn trọng UPI_WEB_LOG_DIR) mà không gây vòng import."""
    from engine import LOG_DIR  # noqa: PLC0415 — import muộn là chủ ý
    return LOG_DIR


def _cache_path(log_dir: Path) -> Path:
    return log_dir.parent / ".cache" / "global_stats.json"


def _day_from_epoch(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")


def _amount_of(link: dict) -> int | None:
    """Số tiền của 1 mã, đơn vị paise. None = không xác định được."""
    raw = link.get("amount_minor")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _tally(items) -> tuple[int, int, int]:
    """(số mã ₹0, số mã ₹0 đã thanh toán, số mã không xác định được giá).

    `succeeded` trong `link_state` là mốc duy nhất nghĩa "khách đã duyệt mandate"
    (xem app.js + artifact_probe). `waiting`/`failed`/`expired`/`canceled`/rỗng đều
    KHÔNG tính là đã thanh toán.
    """
    zero = paid = unknown = 0
    for amount, state in items:
        if amount is None:
            unknown += 1
        elif amount == 0:
            zero += 1
            if state == "succeeded":
                paid += 1
    return zero, paid, unknown


def _parse_web(path: Path, fallback_day: str) -> dict:
    """File kết quả của 1 job web."""
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("not a job snapshot")
    day = (data.get("finished_at") or data.get("started_at") or "")[:10] or fallback_day
    items = []
    for task in data.get("tasks") or []:
        if not isinstance(task, dict):
            continue
        artifact = task.get("artifact") or {}
        if isinstance(artifact, dict) and artifact.get("upi_link"):
            items.append((_amount_of(artifact),
                          str(task.get("link_state") or "").lower()))
    zero, paid, unknown = _tally(items)
    return {"day": day, "zero": zero, "paid": paid, "unknown": unknown,
            "jobs": 1 if zero else 0, "kind": "web"}


def _parse_cli(path: Path, fallback_day: str) -> dict:
    """File links_cli_*.json.

    Không đếm `paid`: bản export của CLI không mang trạng thái thanh toán
    (`link_state`), nên ở đây chỉ có số mã ₹0.
    """
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("not a link export")
    # `generated` dạng YYYYMMDD-HHMMSS (giờ máy lúc chạy)
    gen = str(data.get("generated") or "")
    day = fallback_day
    if len(gen) >= 8 and gen[:8].isdigit():
        day = "%s-%s-%s" % (gen[0:4], gen[4:6], gen[6:8])
    items = [(_amount_of(item), "") for item in (data.get("links") or [])
             if isinstance(item, dict) and item.get("upi_link")]
    zero, paid, unknown = _tally(items)
    return {"day": day, "zero": zero, "paid": paid, "unknown": unknown,
            "jobs": 1 if zero else 0, "kind": "cli"}


def _iter_source_files(log_dir: Path):
    for path in sorted(log_dir.glob("web_*_results.json")):
        yield path, _parse_web
    for path in sorted(log_dir.glob("links_cli_*.json")):
        yield path, _parse_cli


def _load_cache(path: Path) -> dict:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(raw, dict) or raw.get("version") != _CACHE_VERSION:
        return {}
    files = raw.get("files")
    return files if isinstance(files, dict) else {}


def _save_cache(path: Path, files: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"version": _CACHE_VERSION, "files": files},
                                  ensure_ascii=False),
                       encoding="utf-8")
        tmp.replace(path)          # ghi nguyên tử: không để lại cache cụt
    except OSError:
        pass


def daily(log_dir: Path | None = None) -> dict:
    """Số mã ₹0.00 trích xuất được theo từng ngày, mới nhất trước."""
    log_dir = log_dir or _log_dir()
    cache_file = _cache_path(log_dir)
    cache = _load_cache(cache_file)

    fresh: dict = {}
    parsed = failed = 0
    for path, parser in _iter_source_files(log_dir):
        try:
            stat = path.stat()
        except OSError:
            continue
        key = str(path)
        size, mtime = stat.st_size, round(stat.st_mtime, 3)
        hit = cache.get(key)
        if hit and hit.get("size") == size and hit.get("mtime") == mtime:
            fresh[key] = hit
            continue
        try:
            entry = parser(path, _day_from_epoch(mtime))
        except (OSError, ValueError, json.JSONDecodeError):
            # File đang ghi dở hoặc hỏng: bỏ qua lần này, KHÔNG cache -> lần sau thử lại.
            failed += 1
            continue
        entry["size"] = size
        entry["mtime"] = mtime
        fresh[key] = entry
        parsed += 1

    if parsed or len(fresh) != len(cache):
        _save_cache(cache_file, fresh)

    per_day: dict = defaultdict(lambda: {"zero": 0, "paid": 0, "unknown": 0, "jobs": 0})
    total = paid_total = unknown_total = 0
    for entry in fresh.values():
        bucket = per_day[entry["day"]]
        bucket["zero"] += int(entry.get("zero") or 0)
        bucket["paid"] += int(entry.get("paid") or 0)
        bucket["unknown"] += int(entry.get("unknown") or 0)
        bucket["jobs"] += int(entry.get("jobs") or 0)
        total += int(entry.get("zero") or 0)
        paid_total += int(entry.get("paid") or 0)
        unknown_total += int(entry.get("unknown") or 0)

    days = []
    for day in sorted(per_day, reverse=True):
        bucket = per_day[day]
        days.append({"date": day, "zero": bucket["zero"], "paid": bucket["paid"],
                     "unknown": bucket["unknown"], "jobs": bucket["jobs"]})
    best = max(days, key=lambda d: d["zero"], default=None)

    return {
        "total": total,
        "total_paid": paid_total,
        "total_days": len(days),
        "total_jobs": sum(d["jobs"] for d in days),
        "unknown_amount": unknown_total,
        "days": days,
        "best": best,
        "scanned_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "parsed_files": parsed,
        "unreadable_files": failed,
    }
