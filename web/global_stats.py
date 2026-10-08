"""Thống kê global: mỗi ngày trích xuất được bao nhiêu mã (UPI link).

Nguồn số liệu là file kết quả trên đĩa (`logs/web_*_results.json` của web desk và
`logs/links_cli_*.json` của CLI), không phải RAM. Nhờ vậy con số là "đã chốt":
job xong mới vào thống kê, và restart desk không làm mất lịch sử.

Vì sao có cache: quét 150 file (~500 task mỗi file) tốn ~2 giây — mở trang mà chờ
từng đó thì không được. Cache nằm ở `.cache/global_stats.json`, khoá theo
(mtime, size) của từng file, nên lần sau chỉ file mới/đổi mới phải parse lại.

Một "mã" = một `upi_link` lấy được từ một task. Task chạy lại cùng acc ra cùng link
thì vẫn là 2 lần trích xuất nhưng 1 mã phân biệt — nên trả về cả `codes` (số lần)
và `unique` (số mã phân biệt).
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

_CACHE_VERSION = 1


def _log_dir() -> Path:
    """Lấy LOG_DIR từ engine (tôn trọng UPI_WEB_LOG_DIR) mà không gây vòng import."""
    from engine import LOG_DIR  # noqa: PLC0415 — import muộn là chủ ý
    return LOG_DIR


def _cache_path(log_dir: Path) -> Path:
    return log_dir.parent / ".cache" / "global_stats.json"


def _day_from_epoch(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")


def _hash_link(link: str) -> str:
    return hashlib.md5(link.encode("utf-8", "replace")).hexdigest()[:10]


def _parse_web(path: Path, fallback_day: str) -> dict:
    """File kết quả của 1 job web -> {day, codes, unique[]}."""
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("not a job snapshot")
    day = (data.get("finished_at") or data.get("started_at") or "")[:10] or fallback_day
    codes: list[str] = []
    for task in data.get("tasks") or []:
        if not isinstance(task, dict):
            continue
        artifact = task.get("artifact") or {}
        if isinstance(artifact, dict):
            link = artifact.get("upi_link") or ""
            if link:
                codes.append(str(link))
    return {"day": day, "codes": len(codes),
            "unique": sorted({_hash_link(c) for c in codes}), "kind": "web"}


def _parse_cli(path: Path, fallback_day: str) -> dict:
    """File links_cli_*.json -> {day, codes, unique[]}."""
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("not a link export")
    # `generated` dạng YYYYMMDD-HHMMSS (giờ máy lúc chạy)
    gen = str(data.get("generated") or "")
    day = fallback_day
    if len(gen) >= 8 and gen[:8].isdigit():
        day = "%s-%s-%s" % (gen[0:4], gen[4:6], gen[6:8])
    codes: list[str] = []
    for item in data.get("links") or []:
        if isinstance(item, dict) and item.get("upi_link"):
            codes.append(str(item["upi_link"]))
    return {"day": day, "codes": len(codes),
            "unique": sorted({_hash_link(c) for c in codes}), "kind": "cli"}


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
    """Số mã trích xuất được theo từng ngày, mới nhất trước."""
    log_dir = log_dir or _log_dir()
    cache_file = _cache_path(log_dir)
    cache = _load_cache(cache_file)

    fresh: dict = {}
    parsed = 0
    failed = 0
    for path, parser in _iter_source_files(log_dir):
        try:
            stat = path.stat()
        except OSError:
            continue
        key = str(path)
        size = stat.st_size
        mtime = round(stat.st_mtime, 3)
        hit = cache.get(key)
        if hit and hit.get("size") == size and hit.get("mtime") == mtime:
            fresh[key] = hit
            continue
        fallback_day = _day_from_epoch(mtime)
        try:
            entry = parser(path, fallback_day)
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

    per_day: dict = defaultdict(lambda: {"codes": 0, "unique": set(), "jobs": 0,
                                         "web": 0, "cli": 0})
    all_unique: set = set()
    total_codes = 0
    for entry in fresh.values():
        bucket = per_day[entry["day"]]
        codes = int(entry.get("codes") or 0)
        uniq = set(entry.get("unique") or ())
        bucket["codes"] += codes
        bucket["unique"] |= uniq
        bucket["jobs"] += 1
        bucket["web" if entry.get("kind") == "web" else "cli"] += codes
        total_codes += codes
        all_unique |= uniq

    days = []
    for day in sorted(per_day, reverse=True):
        bucket = per_day[day]
        days.append({"date": day, "codes": bucket["codes"],
                     "unique": len(bucket["unique"]), "jobs": bucket["jobs"],
                     "web": bucket["web"], "cli": bucket["cli"]})
    best = max(days, key=lambda d: d["codes"], default=None)

    return {
        "total_codes": total_codes,
        "total_unique": len(all_unique),
        "total_days": len(days),
        "total_jobs": sum(d["jobs"] for d in days),
        "days": days,
        "best": best,
        "scanned_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "parsed_files": parsed,
        "unreadable_files": failed,
    }
