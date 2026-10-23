#!/usr/bin/env python3
"""Phân tích lịch sử chạy: tỉ lệ thành công, phân bố lỗi, và tác động của các lần sửa.

Đọc snapshot đã lưu (`logs/web_<job>_results.json`) — cùng nguồn mà desk dùng để hiện lịch sử,
nên số liệu đây khớp với những gì bạn thấy trên giao diện. Chỉ đọc, không sửa gì.

    python3 analyze_history.py                 # 7 ngày gần nhất
    python3 analyze_history.py --days 14
    python3 analyze_history.py --errors        # kèm top câu lỗi thô

Vì sao cần: sau mỗi lần sửa (proxy filter, link monitor, pre-register link…) muốn biết có cải
thiện thật hay không — nhìn % success và phân bố lỗi theo ngày là cách nhanh nhất.
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import glob
import json
import os
import re
import sys

# Nhóm lỗi — giữ đúng thứ tự ưu tiên như web/fe/src/lib/taskView.ts để số liệu khớp UI.
ERROR_RULES: list[tuple[str, str]] = [
    ("risk decline", r"risk decline|account flagged"),
    ("cloudflare 403", r"Cloudflare|cf-mitigated"),
    ("token invalid 401", r"\b401\b"),
    ("openai blocked", r"OpenAI blocked"),
    ("promo not eligible", r"promo_not_eligible"),
    ("billing country", r"Billing country must match"),
    ("proxy/TLS", r"ProxyError|SSLError|TLS|transport|ConnectError|Tunnel"),
    ("timeout", r"timeout|timed out"),
    ("link unverified", r"unverified"),
    ("no billing address", r"no billing address"),
    ("other", r"."),
]


def classify(err: str) -> str:
    for name, pat in ERROR_RULES:
        if re.search(pat, err, re.I):
            return name
    return "other"


def load_jobs(logs_dir: str, days: int) -> list[dict]:
    cutoff = dt.datetime.now().timestamp() - days * 86400
    files = [
        f for f in glob.glob(os.path.join(logs_dir, "web_*_results.json"))
        if os.path.getsize(f) > 4096 and os.path.getmtime(f) >= cutoff
    ]
    files.sort(key=os.path.getmtime)
    jobs = []
    for path in files:
        jid = os.path.basename(path).split("web_")[1].split("_results")[0]
        try:
            data = json.load(open(path))
        except (OSError, json.JSONDecodeError) as exc:
            print(f"  ! bỏ qua {jid}: {exc}", file=sys.stderr)
            continue
        tasks = data.get("tasks") or []
        jobs.append({
            "job_id": jid,
            "mtime": dt.datetime.fromtimestamp(os.path.getmtime(path)),
            "ok": int(data.get("ok") or 0),
            "fail": int(data.get("fail") or 0),
            "total": int(data.get("total") or len(tasks)),
            "tasks": tasks,
        })
    return jobs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--logs", default="logs")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--errors", action="store_true", help="in top câu lỗi thô")
    args = ap.parse_args()

    jobs = load_jobs(args.logs, args.days)
    if not jobs:
        print("Không có snapshot nào trong khoảng thời gian này.")
        return 1

    # ---- theo ngày ---------------------------------------------------------
    per_day: dict[str, dict] = collections.defaultdict(
        lambda: {"jobs": 0, "tasks": 0, "ok": 0, "fail": 0, "links": 0, "paid": 0, "errors": collections.Counter()}
    )
    for job in jobs:
        day = job["mtime"].strftime("%Y-%m-%d")
        bucket = per_day[day]
        bucket["jobs"] += 1
        for task in job["tasks"]:
            bucket["tasks"] += 1
            status = (task.get("status") or "").lower()
            if status == "done":
                bucket["ok"] += 1 if task.get("link_state") in ("succeeded", "waiting") else 0
                if task.get("artifact"):
                    bucket["links"] += 1
            elif status in ("fail", "stopped"):
                bucket["fail"] += 1
                if status == "fail":
                    bucket["errors"][classify(str(task.get("error") or ""))] += 1
            if task.get("link_state") == "succeeded":
                bucket["paid"] += 1

    print(f"\n== Tỉ lệ theo ngày ({len(jobs)} snapshot, {args.days} ngày) ==")
    header = f"{'ngày':<11}{'job':>4}{'task':>7}{'link OK':>9}{'paid':>6}{'fail':>7}{'% link':>8}"
    print(header)
    print("-" * len(header))
    for day in sorted(per_day):
        b = per_day[day]
        rate = (b["ok"] / b["tasks"] * 100) if b["tasks"] else 0
        print(f"{day:<11}{b['jobs']:>4}{b['tasks']:>7}{b['ok']:>9}{b['paid']:>6}{b['fail']:>7}{rate:>7.1f}%")

    # ---- tỉ lệ từng loại lỗi theo ngày (% trên số task chạy hôm đó) --------
    cats = ["risk decline", "promo not eligible", "openai blocked", "cloudflare 403",
            "proxy/TLS", "timeout", "token invalid 401"]
    shares: dict[str, dict[str, float]] = {}
    for day, b in per_day.items():
        total = b["tasks"] or 1
        shares[day] = {c: b["errors"].get(c, 0) / total * 100 for c in cats}
    print("\n== Tỉ lệ lỗi theo ngày (% số task) ==")
    head = f"{'ngày':<11}" + "".join(f"{c[:11]:>12}" for c in cats) + f"{'link/1k':>9}"
    print(head)
    print("-" * len(head))
    for day in sorted(per_day):
        b = per_day[day]
        link_per_k = b["ok"] / (b["tasks"] or 1) * 1000
        print(f"{day:<11}" + "".join(f"{shares[day][c]:>11.1f}%" for c in cats) + f"{link_per_k:>8.2f}")

    # ---- theo job (chỉ job có >=50 task) ----------------------------------
    print("\n== 12 job lớn gần nhất ==")
    header = f"{'job':<14}{'lúc':<12}{'task':>6}{'ok':>6}{'fail':>6}{'%ok':>7}  lỗi chính"
    print(header)
    print("-" * len(header))
    big = [j for j in jobs if j["total"] >= 50]
    for job in big[-12:]:
        counts: collections.Counter = collections.Counter()
        for task in job["tasks"]:
            if (task.get("status") or "") == "fail":
                counts[classify(str(task.get("error") or ""))] += 1
        total = job["total"] or 1
        rate = job["ok"] / total * 100
        top = ", ".join(f"{k} {v}" for k, v in counts.most_common(2)) or "-"
        print(f"{job['job_id']:<14}{job['mtime'].strftime('%m-%d %H:%M'):<12}"
              f"{job['total']:>6}{job['ok']:>6}{job['fail']:>6}{rate:>6.1f}%  {top}")

    # ---- trước/sau các mốc sửa quan trọng --------------------------------
    marks = [
        ("CF: co chan 403 theo IP proxy", dt.datetime(2026, 10, 18, 22, 34)),
        ("link: so theo doi ben vung", dt.datetime(2026, 10, 16, 19, 41)),
        ("monitor: pre-register link", dt.datetime(2026, 10, 22, 14, 1)),
        ("disk: chan khi dia gan day", dt.datetime(2026, 10, 19, 23, 29)),
    ]
    print("\n== Trước/sau mốc sửa (share lỗi trên tổng task) ==")
    for label, cut in marks:
        ts = cut.timestamp()
        before = collections.Counter()
        after = collections.Counter()
        n_before = n_after = 0
        for job in jobs:
            target = after if job["mtime"].timestamp() >= ts else before
            n = n_after if job["mtime"].timestamp() >= ts else n_before
            for task in job["tasks"]:
                if (task.get("status") or "") != "fail":
                    continue
                target[classify(str(task.get("error") or ""))] += 1
        n_before = sum(before.values()) or 1
        n_after = sum(after.values()) or 1
        diff = []
        for cat in ("cloudflare 403", "proxy/TLS", "risk decline", "promo not eligible"):
            b_share = before.get(cat, 0) / n_before * 100
            a_share = after.get(cat, 0) / n_after * 100
            if before.get(cat) or after.get(cat):
                diff.append(f"{cat}: {b_share:.1f}% → {a_share:.1f}%")
        print(f"  {label} ({cut:%m-%d %H:%M}): " + " | ".join(diff))

    # ---- phân bố lỗi toàn kỳ ---------------------------------------------
    all_errors: collections.Counter = collections.Counter()
    for job in jobs:
        for task in job["tasks"]:
            if (task.get("status") or "") == "fail":
                all_errors[classify(str(task.get("error") or ""))] += 1
    total_err = sum(all_errors.values()) or 1
    print("\n== Phân bố lỗi ==")
    for name, count in all_errors.most_common():
        bar = "█" * max(1, round(count / total_err * 40))
        print(f"{name:<20}{count:>7}  {count / total_err * 100:5.1f}%  {bar}")

    if args.errors:
        raw: collections.Counter = collections.Counter()
        for job in jobs:
            for task in job["tasks"]:
                err = str(task.get("error") or "").strip().split("\n")[0]
                if err:
                    raw[re.sub(r"\d{2,}", "N", err)[:110]] += 1
        print("\n== Câu lỗi thô (top 12) ==")
        for msg, count in raw.most_common(12):
            print(f"{count:>6}  {msg}")

    # ---- sổ theo dõi link -------------------------------------------------
    watch_path = "cs_state/link_watch.json"
    if os.path.exists(watch_path):
        watch = json.load(open(watch_path))
        states = collections.Counter(v.get("state", "") for v in watch.values())
        print("\n== Sổ theo dõi link (cs_state/link_watch.json) ==")
        print(f"  {len(watch)} link đã đăng ký: " + ", ".join(f"{k or '?'}={v}" for k, v in states.most_common()))
        never = sum(1 for v in watch.values() if not v.get("checked_at"))
        print(f"  chưa từng được dò trạng thái: {never}")

        # link đăng ký sau mốc sửa pre-register (23/10 19:30) -> có được dò không?
        cut = dt.datetime(2026, 10, 23, 19, 30).timestamp()
        after = [v for v in watch.values() if float(v.get("registered_at") or 0) >= cut]
        if after:
            checked = sum(1 for v in after if v.get("checked_at"))
            print(f"  đăng ký sau 23/10 19:30: {len(after)}, đã dò: {checked} "
                  f"({checked / len(after) * 100:.0f}%)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
