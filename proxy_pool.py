#!/usr/bin/env python3
"""proxy_pool.py — tự sinh thêm proxy từ 1 dòng mẫu (sticky session).

Vì sao cần: pool dán tay chỉ có bao nhiêu dòng thì dùng được bấy nhiêu task, mà
sau khi lọc chất lượng thì còn ít hơn (đo được: pool 100 dòng -> 97 dùng được).
Với provider dạng
    us.rrp.bestgo.work:10000:USER316226-zone-custom-region-IN-session-47201384-sessTime-5-sessAuto-1:a30ba0
thì phần biến thiên DUY NHẤT là session id, và đo thật cho thấy:
  - session id tự bịa (không có trong danh sách mua) vẫn chạy: 6/6
  - mỗi session ra một IP khác nhau: 6/6 unique
  - chất lượng ngang pool dán tay: 30 session sinh tự động -> 29/30 dùng được,
    22 grade A, tất cả đều exit ở Ấn Độ
Nên thay vì bị giới hạn ở số dòng có sẵn, ở đây làm kiểu "sinh tới khi đủ":
sinh -> đo bằng ippure -> giữ con đạt -> sinh tiếp, dừng khi đủ `want` hoặc hết
ngân sách `max_attempts`.

Nhận dạng chỗ cần thay (không đoán bừa): quét phần username theo các mẫu sticky
session phổ biến của provider (session-/sid-/sessid-/sessionid-) và thay bằng id
mới CÙNG DẠNG (toàn số -> toàn số cùng độ dài, chữ+số -> cùng kiểu). Không thấy
mẫu nào thì báo rõ là không sinh được, không tự bịa chỗ khác trong chuỗi.

Vòng đời session: `sessTime-N` = N phút cho 1 sticky session (đo: IP không đổi
trong suốt thời gian đó), nên sinh theo lô cho 1 job là đủ, không cần sinh lại
giữa task.

CLI:
    python3 proxy_pool.py harvest --from-file proxy.txt --want 60 --min-grade B
    python3 proxy_pool.py harvest --template "<dòng mẫu>" --want 60 --out pool_gen.txt
    python3 proxy_pool.py generate --from-file proxy.txt --count 20      # chỉ in, không đo
"""

from __future__ import annotations

import argparse
import json
import random
import re
import string
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).resolve().parent))

import ippure  # noqa: E402 — module cùng thư mục, không import repo khác

# Các mẫu sticky-session thường gặp. Bắt cả dấu phân cách `-` và `_`.
SESSION_PATTERNS = (
    re.compile(r"(?i)\b(session|sessid|sessionid|session_id|sid)[-_]([A-Za-z0-9]{4,})"),
)

# Thời lượng sticky session trong username. Đo thật: `sessTime-1` đổi IP đúng sau
# ~1 phút, `sessTime-5` giữ nguyên qua mốc 1 phút -> N là số PHÚT giữ IP.
# Vì sao phải chỉnh được: một task chạy 100-200s, CS flow 10 bước có thể lâu hơn;
# nếu IP đổi giữa task thì sentinel token và request checkout đi từ 2 IP khác nhau.
SESS_TIME_PATTERNS = (
    re.compile(r"(?i)(sess(?:ion)?time[-_])(\d{1,3})"),   # bestgo: sessTime-5
    re.compile(r"(?i)([-_]t[-_])(\d{1,3})(?=[-_]|$)"),     # cliproxy: -t-5
)


def set_session_duration(line: str, minutes: int) -> str:
    """Đổi thời lượng sticky session trong username (0 = giữ nguyên)."""
    if not minutes:
        return line
    try:
        host, port, user, pw = split_proxy(line)
    except ValueError:
        return line
    for pattern in SESS_TIME_PATTERNS:
        if pattern.search(user):
            user = pattern.sub(lambda m: f"{m.group(1)}{int(minutes)}", user, count=1)
            break
    return build_proxy(host, port, user, pw)


def session_duration(line: str) -> int | None:
    """Đọc thời lượng session (phút) trong username, None nếu không khai."""
    try:
        _, _, user, _ = split_proxy(line)
    except ValueError:
        return None
    for pattern in SESS_TIME_PATTERNS:
        hit = pattern.search(user)
        if hit:
            return int(hit.group(2))
    return None


def split_proxy(line: str) -> tuple[str, str, str, str]:
    """Tách 1 dòng proxy thành (host, port, user, password).

    Nhận cả `host:port:user:pass` lẫn `scheme://user:pass@host:port`.
    """
    raw = str(line or "").strip()
    if not raw or raw.startswith("#"):
        raise ValueError("empty")
    if "://" in raw:
        scheme, _, rest = raw.partition("://")
        creds, _, hostport = rest.rpartition("@")
        user, _, pw = creds.partition(":")
        from urllib.parse import unquote
        host, _, port = hostport.partition(":")
        return host, port, unquote(user), unquote(pw)
    parts = raw.split(":")
    if len(parts) != 4:
        raise ValueError(f"cần host:port:user:pass, nhận được {len(parts)} phần")
    return parts[0], parts[1], parts[2], parts[3]


def build_proxy(host: str, port: str, user: str, password: str) -> str:
    return f"http://{quote(user, safe='-_.~')}:{quote(password, safe='-_.~')}@{host}:{port}"


def session_slot(line: str) -> tuple[str, str] | None:
    """Tìm (giá trị session id, cả token khớp) trong phần username. None nếu không có."""
    try:
        _, _, user, _ = split_proxy(line)
    except ValueError:
        return None
    for pattern in SESSION_PATTERNS:
        hit = pattern.search(user)
        if hit:
            return hit.group(2), hit.group(0)
    return None


def _new_id(like: str, rng: random.Random) -> str:
    """Sinh id mới CÙNG DẠNG với id cũ (giữ kiểu ký tự + độ dài)."""
    if like.isdigit():
        return "".join(rng.choice(string.digits) for _ in range(len(like)))
    if like.isalpha() and like.isupper():
        return "".join(rng.choice(string.ascii_uppercase) for _ in range(len(like)))
    alphabet = string.ascii_letters + string.digits
    out = "".join(rng.choice(alphabet) for _ in range(len(like)))
    return out


def generate(line: str, count: int, rng: random.Random | None = None,
             avoid: set[str] | None = None) -> list[str]:
    """Sinh `count` proxy mới từ 1 dòng mẫu. Ném ValueError nếu mẫu không có session id."""
    slot = session_slot(line)
    if not slot:
        raise ValueError("không thấy mẫu session/sid trong username -> không sinh được proxy mới")
    old_id, token = slot
    host, port, user, pw = split_proxy(line)
    rng = rng or random.Random()
    avoid = avoid if avoid is not None else set()
    out: list[str] = []
    guard = 0
    while len(out) < count and guard < count * 50:
        guard += 1
        new_id = _new_id(old_id, rng)
        if new_id in avoid or new_id == old_id:
            continue
        avoid.add(new_id)
        out.append(build_proxy(host, port, user.replace(token, f"{token[:len(token) - len(old_id)]}{new_id}", 1), pw))
    return out


def template_from_file(path: str | Path) -> str:
    for line in Path(path).read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            return line
    raise ValueError(f"{path} không có dòng proxy nào")


def harvest(template: str, want: int, min_grade: str = "B", target_country: str = "IN",
            target_tz: str = "Asia/Kolkata", workers: int = 16, max_attempts: int = 0,
            timeout: int = 20, batch: int = 0, sess_time: int = 0,
            log: Any = print) -> tuple[list[dict], dict]:
    """Sinh + đo cho tới khi đủ `want` proxy đạt `min_grade`.

    Trả (danh sách row đạt, báo cáo). `max_attempts` mặc định = want * 6 (đo được
    tỉ lệ đạt khoảng 70-90% nên hệ số 6 là dư, chặn trường hợp provider trả toàn
    IP xấu mà vòng lặp chạy mãi).
    """
    rank = {"A": 4, "B": 3, "C": 2, "F": 1}
    need = rank.get(str(min_grade).upper(), 3)
    template = set_session_duration(template, sess_time)
    max_attempts = int(max_attempts or max(want * 6, want + 8))
    batch = int(batch or max(workers * 2, 8))
    rng = random.Random()
    avoid: set[str] = set()
    good: list[dict] = []
    seen_ips: set[str] = set()
    attempts = 0
    import time as _time
    started = _time.time()

    while len(good) < want and attempts < max_attempts:
        size = min(batch, max_attempts - attempts, max(want - len(good), 4) * 2)
        try:
            candidates = generate(template, size, rng, avoid)
        except ValueError as exc:
            return good, {"error": str(exc), "attempts": attempts, "usable": len(good)}
        rows = ippure.scan(candidates, target_country, target_tz, workers=workers, timeout=timeout)
        attempts += len(candidates)
        for row in rows:
            # cùng một IP có thể lặp lại giữa các session -> bỏ để không phí slot worker
            if not row.get("ok") or row.get("ip") in seen_ips:
                continue
            if rank.get(str(row.get("grade")), 0) < need:
                continue
            seen_ips.add(row["ip"])
            good.append(row)
        log(f"  sinh {attempts} session -> đạt {len(good)}/{want} "
            f"(vòng này {sum(1 for r in rows if rank.get(str(r.get('grade')), 0) >= need)}/{len(rows)})")
    good.sort(key=lambda r: (-r.get("score", 0), r.get("latency_ms") or 99999))
    good = good[:want]   # vòng cuối sinh dư -> chỉ giữ `want` con điểm cao nhất
    report = {"verified": len(good), "attempts": attempts, "unique_ips": len(seen_ips),
              "min_grade": min_grade, "seconds": round(_time.time() - started, 1),
              "sess_minutes": session_duration(template),
              "template_host": split_proxy(template)[0],
              "ids": [session_slot(r["proxy"])[0] for r in good if session_slot(r["proxy"])]}
    return good, report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sinh proxy mới từ 1 dòng mẫu + đo chất lượng")
    parser.add_argument("cmd", choices=["harvest", "generate", "detect"])
    parser.add_argument("--template", default="", help="dòng proxy mẫu (host:port:user:pass)")
    parser.add_argument("--from-file", default="", help="lấy dòng đầu của file làm mẫu")
    parser.add_argument("--want", type=int, default=20, help="cần bao nhiêu proxy đạt chuẩn")
    parser.add_argument("--count", type=int, default=10, help="generate: sinh bao nhiêu dòng")
    parser.add_argument("--min-grade", default="B", choices=["A", "B", "C", "F"])
    parser.add_argument("--country", default="IN")
    parser.add_argument("--tz", default="Asia/Kolkata")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--max-attempts", type=int, default=0)
    parser.add_argument("--sess-time", type=int, default=0,
                        help="đổi thời lượng sticky session (phút) khi sinh, 0 = giữ như mẫu")
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--out", default="", help="ghi proxy đạt chuẩn ra file")
    parser.add_argument("--json", default="", help="ghi kết quả đầy đủ ra file JSON")
    args = parser.parse_args(argv)

    template = args.template or (template_from_file(args.from_file) if args.from_file else "")
    if not template:
        print(json.dumps({"error": "cần --template hoặc --from-file"}), flush=True)
        return 2

    if args.cmd == "detect":
        slot = session_slot(template)
        _, _, user, _ = split_proxy(template)
        print(json.dumps({"generatable": bool(slot), "session_id": slot[0] if slot else None,
                          "token": slot[1] if slot else None, "user": user}, ensure_ascii=False))
        return 0

    if args.cmd == "generate":
        for line in generate(template, args.count):
            print(line)
        return 0

    if not ippure.enabled():
        print(json.dumps({"error": "không có HTTP client cho ippure (cần requests/curl_cffi)"}), flush=True)
        return 2

    rows, report = harvest(template, args.want, args.min_grade, args.country, args.tz,
                           args.workers, args.max_attempts, args.timeout,
                           sess_time=args.sess_time)
    print("# " + json.dumps(report, ensure_ascii=False), flush=True)
    print(f"{'grade':>5} {'điểm':>4} {'exit ip':<42}{'cc':>3} {'risk':>4} {'user_type':<11}{'fam':>5} {'ms':>6}", flush=True)
    for row in rows:
        print(f"{row.get('grade'):>5} {row.get('score'):>4} {str(row.get('ip')):<42}"
              f"{str(row.get('country')):>3} {str(row.get('risk')):>4} "
              f"{str(row.get('user_type')):<11}{str(row.get('family')):>5} "
              f"{str(row.get('latency_ms')):>6}", flush=True)
    if args.out:
        Path(args.out).write_text("\n".join(r["proxy"] for r in rows) + "\n", encoding="utf-8")
    if args.json:
        Path(args.json).write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
