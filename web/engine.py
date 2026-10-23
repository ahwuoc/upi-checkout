#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""engine.py — queue/SSE orchestration for the web UI.

Protocol work lives behind the root `backend.py` seam. This module owns task
lifecycle, retry/stop policy, persistence and progress events only.

Mô hình:
    Job  -> nhiều Task (1 task = 1 access token)
    Task -> nhiều Step (key, label, status) do cli.steps_for(flow) quyết định

Sự kiện phát ra được app.py đẩy qua SSE.
"""

from __future__ import annotations

import json
import os
import queue
import random
import re
import secrets
import shutil
import sys
import threading
import time
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent  # upi-checkout/
sys.path.insert(0, str(ROOT))

import cli  # noqa: E402
import artifact_probe  # noqa: E402
import backend  # noqa: E402

DEFAULT_PROXY_FILE = ROOT / "proxy.txt"
STATE_DIR = ROOT / "cs_state"
# Cho phép đổi thư mục log bằng env: smoke_ui.py trỏ vào thư mục tạm để job test
# KHÔNG rơi vào logs/ thật (trước đây mỗi lần chạy test lại để lại một job 0 task,
# và trang tự attach vào cái mới nhất -> mở lên thấy bảng trống).
_LOG_DIR_ENV = os.environ.get("UPI_WEB_LOG_DIR", "").strip()
LOG_DIR = Path(_LOG_DIR_ENV) if _LOG_DIR_ENV else ROOT / "logs"
MAX_JOBS = 20
# Snapshot của job 5.000 acc là 11–24 MB; ghi lại mỗi 2 giây = write amplification rất lớn trên
# btrfs (và từng góp phần làm FS hết metadata). 10 giây vẫn đủ mịn cho UI, mất tối đa 10 giây
# tiến độ nếu process chết.
CHECKPOINT_SECS = float(os.environ.get("UPI_CHECKPOINT_SECS") or 10)
MAX_LOGS_PER_TASK = 200
# Trần số access token mỗi batch: MẶC ĐỊNH KHÔNG GIỚI HẠN (0).
# Task đang chờ chỉ là 1 record nhỏ (chỉ task đang chạy mới tốn ~26 MB), nhưng
# snapshot/results.json phình ~4 KB mỗi task -> đặt UPI_MAX_BATCH=<n> nếu muốn chặn.
MAX_TOKENS_PER_BATCH = int(os.environ.get("UPI_MAX_BATCH") or 0)
MAX_WORKERS = 300
SUBSCRIBER_QUEUE = 5000

# trang thai coi la thanh cong.
# CHỈ "LINK" — tức đã trích xuất được link QR (`payments.stripe.com/upi/instructions/`).
# "APPROVE_OK_NO_LINK" là approve xong mà **không có link nào** -> không có gì để
# giao cho khách, không thể tính thành công.
OK_STATUS = {"LINK"}

FLOW_LABEL = {
    "oaics": "FLOW OAICS",
    "cs": "FLOW CS",
    None: "DETECTING FLOW",
}


def _configure_core() -> None:
    """Áp cấu hình giống `cli.setup()` để web chạy CÙNG thông số với CLI.

    Khác `cli.setup()` ở chỗ KHÔNG tắt `core.log` — log của core vẫn được ghi
    ra `logs/ideal_<ts>.log` để còn tra khi job lỗi. Riêng dump HTTP (sinh rất
    nhiều file) thì tắt mặc định, bật bằng `UPI_WEB_DUMP_HTTP=1`.
    """
    try:
        cli.cu.configure_pre_proxy("auto")
    except Exception:  # noqa: BLE001 — pre-proxy chi la toi uu, khong bat buoc
        pass
    cli.core.COUNTRY_CURRENCY["IN"] = "INR"
    cli.core.CHATGPT_TIMEOUT = 25
    cli.core.DEFAULT_TIMEOUT = 25
    if os.environ.get("UPI_WEB_DUMP_HTTP", "").strip().lower() not in ("1", "true", "yes", "on"):
        cli.core.dump_http = lambda *a, **k: None


_configure_core()


def _now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------- task / job
@dataclass
class Task:
    task_id: str
    index: int
    token: str
    email: str
    flow: str | None = None
    session: str = ""
    provider: str = ""
    steps: list[dict] = field(default_factory=list)
    status: str = "pending"          # pending | running | done | fail | stopped
    raw_status: str = ""
    error: str | None = None
    duration_ms: int | None = None
    run: int = 0
    egress: dict | None = None
    artifact: dict | None = None
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)
    logs: list[str] = field(default_factory=list)
    # Theo dõi link ở PHÍA SERVER (xem _link_monitor_loop): trạng thái mới nhất của
    # trang instructions, mốc đổi trạng thái, và nhãn zero/chuỗi. Trước đây việc này
    # chỉ có setInterval trong browser -> đóng tab là không ai theo dõi nữa.
    link_state: str = ""             # waiting | succeeded | failed | canceled | expired | ""
    link_checked_at: int = 0         # epoch lần dò gần nhất
    link_first_seen: int = 0         # epoch lần đầu thấy link này
    link_flip_at: int = 0            # epoch lúc trạng thái đổi lần cuối
    link_fam: str = ""
    link_zero: bool = False
    link_note: str = ""
    # Acc đã lên Plus chưa (xem at_check.py). Bám mốc "khách đã duyệt mandate"
    # (link_state == succeeded) rồi dò lại vài lần: duyệt mandate mới chỉ là đồng ý
    # cho phép trừ tiền, khách còn phải trả rồi OpenAI mới nâng cấp — dò ngay lúc
    # duyệt thì gần như luôn ra "chưa".
    plus_state: str = ""             # "" | checking | plus | not_plus
    plus_checked_at: int = 0         # epoch lần dò gần nhất
    plus_note: str = ""              # chi tiết thô: "AT 401 · promo active"
    # Vùng hoá đơn của acc này. Flow in ra log, engine BẮT LẠI ngay lúc nhận để lưu
    # vào kết quả — trước đây thông tin này chỉ nằm trong log và log chỉ giữ 50 dòng
    # cuối, nên 85% task thành công mất dữ liệu vùng -> không thống kê được vùng nào
    # ra mã ₹0 nhiều hơn.
    # Hai NGUỒN khác nhau, không trộn:
    #  • geo_*  = vị trí IP exit (dòng "billing geo:" / "tier 2") -> vùng của PROXY
    #  • billing_* = địa chỉ THẬT trên hoá đơn (dòng PM billing_details) -> có thể
    #    khác geo vì flow có list địa chỉ dự phòng theo thành phố lớn.
    geo_state: str = ""
    geo_city: str = ""
    geo_pin: str = ""
    billing_tier: str = ""           # tier2 (PIN xác thực) | tier1 (IP+Nominatim) | hardcoded-list
    billing_check: str = ""           # ok | fixed | mismatch-exit | bad-pin | unknown
    billing_name: str = ""           # tên trên hoá đơn (để đo trùng lặp tên)
    billing_city: str = ""
    billing_pin: str = ""
    # Dung lượng proxy đã dùng cho acc này (byte vào/ra + số request). Đo trong
    # extract_cs._count_net; giúp trả lời "mỗi acc tốn bao nhiêu MB proxy".
    net_out: int = 0
    net_in: int = 0
    net_requests: int = 0
    # Proxy đã dùng cho acc này. Giữ lại trên Task vì link monitor chạy SAU khi task xong
    # (lease đã trả về pool) mà vẫn phải đi qua proxy — không được lộ IP thật khi dò link.
    proxy: str = ""
    no_retry: bool = False           # lỗi cấp account -> đừng retry (risk decline)
    no_retry_reason: str = ""
    done_at: int = 0                 # epoch lúc task xong -> UI sắp tab Success theo mốc này

    def step(self, key: str) -> dict | None:
        for s in self.steps:
            if s["key"] == key:
                return s
        return None

    def set_steps(self, steps: list[dict]) -> None:
        """Thay danh sach buoc; giu lai trang thai cua cac buoc trung key."""
        old = {s["key"]: s for s in self.steps}
        self.steps = [
            {**s, "status": old.get(s["key"], {}).get("status", "pending"),
             "detail": old.get(s["key"], {}).get("detail", ""),
             "err": old.get(s["key"], {}).get("err", "")}
            for s in steps
        ]

    def to_dict(self, log_tail: int = 50) -> dict:
        if self.flow:
            flow_label = FLOW_LABEL[self.flow]
        else:
            # chua xac dinh duoc flow: dang do thi bao "dang do",
            # da ket thuc thi de trong cho trung thuc
            flow_label = "DETECTING FLOW" if self.status in ("pending", "running") else "—"
        return {
            "task_id": self.task_id,
            "index": self.index,
            "email": self.email,
            "flow": self.flow,
            "flow_label": flow_label,
            "session": self.session,
            "provider": self.provider,
            "steps": self.steps,
            "status": self.status,
            "raw_status": self.raw_status,
            "error": self.error,
            "duration_ms": self.duration_ms,
            "run": self.run,
            "egress": self.egress,
            "artifact": self.artifact,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "done_at": self.done_at,
            "link_state": self.link_state,
            "link_checked_at": self.link_checked_at,
            "link_flip_at": self.link_flip_at,
            "link_fam": self.link_fam,
            "link_zero": self.link_zero,
            "link_note": self.link_note,
            "plus_state": self.plus_state,
            "plus_checked_at": self.plus_checked_at,
            "plus_note": self.plus_note,
            "geo_state": self.geo_state,
            "geo_city": self.geo_city,
            "geo_pin": self.geo_pin,
            "net_out": self.net_out,
            "net_in": self.net_in,
            "net_requests": self.net_requests,
            "billing_tier": self.billing_tier,
            "billing_check": self.billing_check,
            "billing_name": self.billing_name,
            "billing_city": self.billing_city,
            "billing_pin": self.billing_pin,
            "logs": self.logs[-max(0, log_tail):] if log_tail else [],
        }


class Job:
    def __init__(self, job_id: str, mode: str, tokens: list[str], country: str,
                 promo: str, workers: int, proxy_source: str, retries: int = 1,
                 filter_proxies: bool = True, stripe_telemetry: bool = False,
                 dump_traffic: bool = False) -> None:
        self.job_id = job_id
        self.mode = mode
        self.country = country
        self.promo = promo
        self.workers = workers
        self.retries = retries
        self.proxy_source = proxy_source
        # False = dùng pool NGUYÊN TRẠNG: không chấm điểm, không loại proxy chết/sai
        # nước, không sinh session mới — để đo xem bộ lọc có giúp gì không.
        self.filter_proxies = filter_proxies
        # True = đăng ký phiên Stripe tại m.stripe.com rồi dùng muid/guid/sid thật
        # (thử nghiệm giảm generic_decline; xem extract_cs.stripe_telemetry_ids).
        self.stripe_telemetry = stripe_telemetry
        # True = ghi MỌI call HTTP (request+response, kèm header) của task vào
        # dumps/<task>/<task>.jsonl dạng HAR entry; xem traffic_dump.py.
        self.dump_traffic = dump_traffic
        self.total = len(tokens)
        self.tasks: dict[str, Task] = {}
        self.order: list[str] = []
        # Lý do dừng: chỉ do người dùng bấm Stop (đã bỏ circuit breaker tự dừng).
        self.stop_reason = ""
        # Lỗi ghi results.json (đĩa đầy/quyền) — hiện lên UI thay vì im lặng.
        self.write_error = ""
        self._last_write_warn = 0.0
        self.risk_decline_count = 0
        self.promo_not_eligible_count = 0
        self.status = "running"           # running | done | stopped
        self.stop_requested = False
        self.started_at = _now()
        self.finished_at: str | None = None
        self.duration_ms: int | None = None
        self._t0 = time.time()
        self._lock = threading.RLock()
        # One executor owns initial tasks, appends and retries. Queue entries, not
        # waiting threads, reserve work; a task stays reserved through all retries.
        self._executor: ThreadPoolExecutor | None = None
        self._pending: set[str] = set()
        self._proxy_file: Path | None = None
        self._proxies: list[str] = []
        self._next_index = len(tokens)
        self._last_checkpoint = 0.0
        self.state_dir = STATE_DIR / job_id
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self._subs: list[queue.Queue] = []

        # Log ben vung: moi job 1 file, khong mat khi restart server.
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        self.log_path = LOG_DIR / f"web_{job_id}.log"
        self.results_path = LOG_DIR / f"web_{job_id}_results.json"
        self._log_lock = threading.Lock()
        self._log_fh = None
        try:
            self._log_fh = self.log_path.open("a", encoding="utf-8")
        except OSError:
            self._log_fh = None

        for i, tok in enumerate(tokens):
            tid = f"t{i + 1}"
            t = Task(task_id=tid, index=i, token=tok, email=cli.decode_email(tok))
            t.set_steps(cli.steps_for(mode))
            self.tasks[tid] = t
            self.order.append(tid)

    # -- log file ----------------------------------------------------------
    def log_line(self, line: str) -> None:
        with self._log_lock:
            if self._log_fh is None:
                return
            try:
                self._log_fh.write(f"[{_now()}] {line}\n")
                self._log_fh.flush()
            except (OSError, ValueError):
                pass

    def _log_event(self, event: dict) -> None:
        t = event.get("type")
        tid = event.get("task_id")
        if t == "job_start":
            self.log_line(
                f"=== JOB START {event.get('job_id')} | mode={event.get('mode')} "
                f"total={event.get('total')} workers={event.get('workers')} "
                f"country={event.get('country')} promo={event.get('promo')} "
                f"proxy={event.get('proxy_source')}")
        elif t == "task_init":
            self.log_line(f"[{tid}] task_init {event.get('email')}")
        elif t == "task_start":
            self.log_line(f"[{tid}] --- run #{event.get('run')} bat dau ---")
        elif t == "task_flow":
            self.log_line(f"[{tid}] flow={event.get('flow')} "
                          f"session={event.get('session') or '-'} "
                          f"provider={event.get('provider') or '-'}")
        elif t == "step":
            extra = event.get("detail") or event.get("err") or ""
            self.log_line(f"[{tid}]   step {event.get('key')} -> {event.get('status')}"
                          + (f" | {extra}" if extra else ""))
        elif t == "egress":
            self.log_line(f"[{tid}]   egress ip={event.get('ip') or '-'} "
                          f"loc={event.get('location') or '-'}"
                          + (" (approx)" if event.get("approx") else ""))
        elif t == "log":
            self.log_line(f"[{tid}]   | {event.get('line', '')}")
        elif t == "task_artifact":
            self.log_line(f"[{tid}]   ARTIFACT link={event.get('upi_link') or '-'} "
                          f"qr={event.get('qr_png') or '-'} amount={event.get('amount_minor')}")
        elif t == "task_done":
            self.log_line(f"[{tid}] RESULT ok={event.get('ok')} status={event.get('status')} "
                          f"{event.get('duration_ms')}ms"
                          + (f" err={event.get('error')}" if event.get("error") else ""))
        elif t == "job_done":
            self.log_line(f"=== JOB DONE ok={event.get('ok')} fail={event.get('fail')} "
                          f"total={event.get('total')} {event.get('duration_ms')}ms")

    def write_results(self, *, checkpoint: bool = False) -> None:
        """Ghi snapshot cuoi cung ra file JSON (doc lap voi RAM)."""
        # Serialize writers and atomically replace: history readers never see
        # half a JSON document, including concurrent completion/removal.
        with self._lock:
            now = time.monotonic()
            if checkpoint and now - self._last_checkpoint < CHECKPOINT_SECS:
                return
            try:
                temp = self.results_path.with_suffix(".json.tmp")
                temp.write_text(json.dumps(self.snapshot(), ensure_ascii=False,
                                           separators=(",", ":")), encoding="utf-8")
                temp.replace(self.results_path)
                _write_headline({"job_id": self.job_id, "status": self.status,
                                 "total": self.total, "done": self.done, "ok": self.ok,
                                 "fail": self.fail, "started_at": self.started_at})
                self._last_checkpoint = now
            except OSError as exc:
                # TRƯỚC ĐÂY: `pass` -> đĩa đầy mà không ai biết, desk chạy tiếp và TOÀN BỘ kết
                # quả sau checkpoint cuối chỉ nằm trong RAM (đo thật 19/10: job 1de13e494e07
                # đĩa đầy từ 22:46, flow log ghi 2.717 task xong nhưng results.json kẹt ở 1.372).
                self.write_error = f"{type(exc).__name__}: {exc}"
                now_s = time.monotonic()
                if now_s - self._last_write_warn > 30:
                    self._last_write_warn = now_s
                    msg = (f"could not write {self.results_path.name} ({self.write_error}) — "
                           f"results are in RAM only, free disk space (./clean_logs.sh --apply)")
                    self.log_line(f"[{self.job_id}] {msg}")
                    self.emit({"type": "log", "task_id": "*", "line": msg})
                    try:
                        print(f"[write-results] {msg}", flush=True)
                    except Exception:  # noqa: BLE001
                        pass

    def close_log(self) -> None:
        with self._log_lock:
            if self._log_fh is not None:
                try:
                    self._log_fh.close()
                except OSError:
                    pass
                self._log_fh = None

    # -- pub/sub -----------------------------------------------------------
    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=SUBSCRIBER_QUEUE)
        with self._lock:
            self._subs.append(q)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self._lock:
            if q in self._subs:
                self._subs.remove(q)

    def emit(self, event: dict) -> None:
        self._log_event(event)
        with self._lock:
            for q in self._subs:
                try:
                    q.put_nowait(event)
                except queue.Full:
                    # Losing a lifecycle event requires a fresh snapshot, not
                    # an arbitrary drop that leaves the browser stale forever.
                    while True:
                        try:
                            q.get_nowait()
                        except queue.Empty:
                            break
                    q.put_nowait({"type": "resync"})

    # -- thong ke ----------------------------------------------------------
    @property
    def done(self) -> int:
        with self._lock:
            return sum(1 for t in self.tasks.values() if t.status in ("done", "fail", "stopped"))

    @property
    def ok(self) -> int:
        with self._lock:
            return sum(1 for t in self.tasks.values() if t.status == "done" and t.raw_status in OK_STATUS)

    @property
    def fail(self) -> int:
        with self._lock:
            return sum(1 for t in self.tasks.values() if t.status == "fail")

    def snapshot(self) -> dict:
        with self._lock:
            return self._snapshot_locked()

    def _snapshot_locked(self) -> dict:
        tasks = [deepcopy(self.tasks[tid].to_dict(log_tail=self._log_tail()))
                 for tid in self.order]
        return {
            "job_id": self.job_id,
            "mode": self.mode,
            "country": self.country,
            "promo": self.promo,
            "workers": self.workers,
            "retries": self.retries,
            "total": len(tasks),
            "done": sum(t["status"] in ("done", "fail", "stopped") for t in tasks),
            "ok": sum(t["status"] == "done" and t["raw_status"] in OK_STATUS for t in tasks),
            "fail": sum(t["status"] == "fail" for t in tasks),
            "running": sum(t["status"] == "running" for t in tasks),
            "queued": sum(t["status"] == "pending" for t in tasks),
            "write_error": self.write_error,
            "elapsed_ms": int((time.time() - self._t0) * 1000) if self.status == "running" else self.duration_ms,
            "status": self.status,
            "stop_requested": self.stop_requested,
            "proxy_source": self.proxy_source,
            "stop_reason": self.stop_reason,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_ms": self.duration_ms,
            "log_path": str(self.log_path),
            "results_path": str(self.results_path),
            "tasks": tasks,
        }

    def _log_tail(self) -> int:
        """Số dòng log mang theo mỗi task trong snapshot.

        Mỗi dòng log ~120 ký tự, 50 dòng/task => 100 task là 579 KB riêng phần này.
        Batch 1000 token thì 5 MB cho một lần `/api/state` (và cho cả event `state`
        đầu tiên của SSE) — nặng cho browser mà chẳng ai đọc hết 50.000 dòng log.
        Batch lớn thì cắt bớt; log đầy đủ vẫn nằm trong `web_<id>.log` trên đĩa.
        """
        n = len(self.order)
        if n <= 300:
            return 50
        if n <= 600:
            return 20
        return 10


# ------------------------------------------------------------------ registry
_JOBS: dict[str, Job] = {}
_JOB_ORDER: list[str] = []
_JOBS_LOCK = threading.Lock()


_SNAPSHOT_CACHE: dict[str, tuple[float, dict]] = {}
_SNAPSHOT_CACHE_LOCK = threading.RLock()
SNAPSHOT_CACHE_SECS = float(os.environ.get("UPI_SNAPSHOT_CACHE_SECS") or 2.0)


def get_job_overlay(job_id: str) -> dict | None:
    """Snapshot của job đang trong RAM (đã trộn trạng thái link), CACHE 2 giây.

    Vì sao cache: UI poll `/api/state` vài giây một lần; mỗi lần dựng snapshot cho 5.400 task
    (deepcopy + to_dict) ngốn CPU trong khi 300 worker đang chạy -> API timeout, UI hiện "0 job".
    2 giây trễ không ảnh hưởng gì với dữ liệu này (task chạy hàng chục giây).
    """
    now = time.monotonic()
    with _SNAPSHOT_CACHE_LOCK:
        hit = _SNAPSHOT_CACHE.get(job_id)
        if hit and (now - hit[0]) < SNAPSHOT_CACHE_SECS:
            return deepcopy(hit[1])
    job = _JOBS.get(job_id)
    if job is None:
        return None
    payload = job.snapshot()
    # Tự chữa lành: link nào của job đang chạy mà chưa có trong sổ thì nạp vào (chỉ ghi khi có
    # thêm), để vòng dò bắt đầu theo dõi kể cả khi lần verify đầu bị lỗi.
    backfill_link_watch(payload)
    payload = link_watch_overlay(payload)
    with _SNAPSHOT_CACHE_LOCK:
        _SNAPSHOT_CACHE[job_id] = (now, payload)
        if len(_SNAPSHOT_CACHE) > 64:
            for key in list(_SNAPSHOT_CACHE)[:16]:
                _SNAPSHOT_CACHE.pop(key, None)
    return deepcopy(payload)



    """Snapshot của job đang trong RAM, đã trộn trạng thái link từ sổ."""
    job = _JOBS.get(job_id)
    if job is None:
        return None
    return link_watch_overlay(job.snapshot())


def get_job(job_id: str) -> Job | None:
    return _JOBS.get(job_id)


# --------------------------------------------------- theo dõi link (nền, server)
# Trước đây việc dò link chỉ có `setInterval` 20s trong browser: đóng tab là mất
# theo dõi, mà link chỉ sống ~4–5 phút nên gần như phải ngồi canh. Giờ server tự
# dò và ghi lại mốc đổi trạng thái — "khách quét lúc nào", "chết sau bao lâu" —
# không phụ thuộc việc có ai mở browser hay không.
LINK_TERMINAL = ("succeeded", "failed", "canceled", "expired")
MONITOR_SECS = float(os.environ.get("UPI_MONITOR_SECS", "30") or 30)
MONITOR_CONC = int(os.environ.get("UPI_MONITOR_CONC", "8") or 8)
# Job xong thì KHÔNG ngừng dò ngay: uỷ nhiệm ₹0 có thể được khách duyệt muộn (nhiều giờ sau),
# và nếu thôi dò thì chip "Awaiting payment" đứng im mãi — không ai biết là khách đã quét hay
# uỷ nhiệm đã hết hạn. Nên: dò dày lúc job còn nóng, dò thưa (mặc định 5 phút/lần) sau đó, và
# chỉ bỏ hẳn sau UPI_MONITOR_MAX_IDLE_MIN phút (mặc định 24 giờ).
MONITOR_HOT_IDLE_MIN = float(os.environ.get("UPI_MONITOR_HOT_IDLE_MIN", "5") or 5)
MONITOR_COLD_SECS = float(os.environ.get("UPI_MONITOR_COLD_SECS", "300") or 300)
MONITOR_MAX_IDLE_MIN = float(os.environ.get("UPI_MONITOR_MAX_IDLE_MIN", "1440") or 1440)


# ---------------------------------------------------------------------------
# Sổ theo dõi link (BỀN VỮNG) — vì sao cần: job chỉ sống trong RAM, restart desk là mất,
# mà uỷ nhiệm ₹0 thì khách có thể duyệt MUỘN (nhiều giờ sau). Trước đây vòng dò chỉ nhìn
# `_JOBS` trong RAM nên sau restart/30 phút là chip "Awaiting payment" đứng im vĩnh viễn —
# đo thật trên job 3dcd91d4f58a: 2 link đã được khách duyệt (`intent_state=succeeded`) mà UI
# vẫn ghi "Awaiting payment".
# ---------------------------------------------------------------------------
LINK_LEDGER_PATH = STATE_DIR / "link_watch.json"
_LINK_LEDGER: dict[str, dict] = {}
_LINK_LEDGER_LOCK = threading.RLock()
_LINK_LEDGER_LOADED = False
LINK_LEDGER_CAP = 20000


def _ledger_key(job_id: str, task_id: str) -> str:
    return f"{job_id}|{task_id}"


def _ledger_load() -> None:
    global _LINK_LEDGER_LOADED
    with _LINK_LEDGER_LOCK:
        if _LINK_LEDGER_LOADED:
            return
        _LINK_LEDGER_LOADED = True
        try:
            data = json.loads(LINK_LEDGER_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                _LINK_LEDGER.update({k: v for k, v in data.items() if isinstance(v, dict)})
        except (OSError, json.JSONDecodeError):
            pass


def _ledger_save() -> None:
    with _LINK_LEDGER_LOCK:
        if len(_LINK_LEDGER) > LINK_LEDGER_CAP:
            # Bỏ những entry cũ nhất (theo lần dò) để file không phình vô hạn.
            for key, _ in sorted(_LINK_LEDGER.items(),
                                 key=lambda kv: kv[1].get("checked_at") or 0)[:2000]:
                _LINK_LEDGER.pop(key, None)
        try:
            STATE_DIR.mkdir(parents=True, exist_ok=True)
            temp = LINK_LEDGER_PATH.with_suffix(".json.tmp")
            temp.write_text(json.dumps(_LINK_LEDGER, ensure_ascii=False,
                                       separators=(",", ":")), encoding="utf-8")
            temp.replace(LINK_LEDGER_PATH)
        except OSError:
            pass


def link_watch_upsert(job_id: str, task_id: str, link: str, proxy: str = "",
                      **fields) -> dict:
    """Ghi/ cập nhật một link vào sổ theo dõi."""
    _ledger_load()
    with _LINK_LEDGER_LOCK:
        entry = _LINK_LEDGER.setdefault(_ledger_key(job_id, task_id), {})
        if link:
            entry["link"] = link
        if proxy:
            entry["proxy"] = proxy
        entry["job_id"] = job_id
        entry["task_id"] = task_id
        entry.update({k: v for k, v in fields.items() if v is not None})
        entry["updated_at"] = int(time.time())
        return dict(entry)


def link_watch_get(job_id: str, task_id: str) -> dict:
    _ledger_load()
    with _LINK_LEDGER_LOCK:
        return dict(_LINK_LEDGER.get(_ledger_key(job_id, task_id)) or {})


def backfill_link_watch(snapshot: dict) -> int:
    """Nạp link của một snapshot cũ vào sổ theo dõi. Trả số entry mới.

    Vì sao cần: sổ chỉ được ghi từ lúc tính năng này có mặt, nên link của các job CŨ (như
    `3dcd91d4f58a` — 2 link đã được khách duyệt mà UI vẫn ghi "Awaiting payment") không tự
    nhiên có trong sổ. Hàm này chạy mỗi lần đọc snapshot từ đĩa + một lần lúc khởi động.
    """
    if not isinstance(snapshot, dict):
        return 0
    job_id = str(snapshot.get("job_id") or "")
    tasks = snapshot.get("tasks")
    if not job_id or not isinstance(tasks, list):
        return 0
    _ledger_load()
    added = 0
    for task in tasks:
        if not isinstance(task, dict):
            continue
        link = str((task.get("artifact") or {}).get("upi_link") or "")
        if not link or not artifact_probe.is_upi_instructions_url(link):
            continue
        if str(task.get("link_state") or "") in LINK_TERMINAL:
            continue
        task_id = str(task.get("task_id") or "")
        if not task_id or link_watch_get(job_id, task_id):
            continue
        link_watch_upsert(job_id, task_id, link,
                          str((task.get("egress") or {}).get("proxy")
                              or task.get("proxy") or ""),
                          state=str(task.get("link_state") or "waiting"),
                          checked_at=int(task.get("link_checked_at") or 0) or int(time.time()),
                          first_seen=int(task.get("link_first_seen") or 0) or int(time.time()),
                          flip_at=int(task.get("link_flip_at") or 0),
                          fam=str(task.get("link_fam") or ""),
                          zero=bool(task.get("link_zero")),
                          note=str(task.get("link_note") or ""))
        added += 1
    if added:
        _ledger_save()
    return added


def backfill_link_watch_from_disk(max_age_hours: float = 48.0) -> int:
    """Quét các snapshot gần đây trên đĩa và nạp link vào sổ (gọi lúc khởi động desk)."""
    cutoff = time.time() - max_age_hours * 3600
    total = 0
    try:
        files = sorted(LOG_DIR.glob("web_*_results.json"))
    except OSError:
        return 0
    # Giới hạn: snapshot lớn nhất đo được 24 MB, 354 file = 1,67 GB -> parse hết lúc khởi động
    # là treo process (GIL) vài phút. Chỉ đọc file nhỏ/mới, tối đa 10 file, nhường GIL giữa các
    # file. Link của job lớn vẫn được nạp bù khi mở job đó (`get_saved_snapshot`).
    max_files = int(os.environ.get("UPI_LINK_BACKFILL_FILES") or 10)
    max_bytes = int(os.environ.get("UPI_LINK_BACKFILL_MAX_BYTES") or 4 * 1024 * 1024)
    done = 0
    for path in files:
        if done >= max_files:
            break
        try:
            stat = path.stat()
            if stat.st_mtime < cutoff or stat.st_size > max_bytes:
                continue
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        total += backfill_link_watch(data)
        done += 1
        time.sleep(0)
    return total


def link_watch_overlay(snapshot: dict) -> dict:
    """Trộn trạng thái link mới nhất từ sổ vào snapshot đọc từ đĩa.

    Nhờ vậy UI (`/api/state`) vẫn thấy trạng thái đúng cho job cũ — không phải ghi lại file
    snapshot 18 MB mỗi lần dò được một link.
    """
    if not isinstance(snapshot, dict):
        return snapshot
    job_id = str(snapshot.get("job_id") or "")
    tasks = snapshot.get("tasks")
    if not job_id or not isinstance(tasks, list):
        return snapshot
    _ledger_load()
    with _LINK_LEDGER_LOCK:
        for task in tasks:
            if not isinstance(task, dict):
                continue
            entry = _LINK_LEDGER.get(_ledger_key(job_id, str(task.get("task_id") or "")))
            if not entry:
                continue
            for src, dst in (("state", "link_state"), ("checked_at", "link_checked_at"),
                             ("flip_at", "link_flip_at"), ("first_seen", "link_first_seen"),
                             ("fam", "link_fam"), ("zero", "link_zero"), ("note", "link_note"),
                             ("plus_state", "plus_state"), ("plus_checked_at", "plus_checked_at"),
                             ("plus_note", "plus_note")):
                if entry.get(src) is not None:
                    task[dst] = entry[src]
    return snapshot


def _job_idle_minutes(job: Job, now: float) -> float:
    """Số phút kể từ lúc job xong (0 nếu còn đang chạy)."""
    if not job.finished_at:
        return 0.0
    try:
        end = time.mktime(time.strptime(job.finished_at, "%Y-%m-%d %H:%M:%S"))
    except ValueError:
        return 0.0
    return max(0.0, (now - end) / 60.0)


def _link_monitor_targets() -> tuple[list[dict], list[dict]]:
    """Link cần dò, chia `(hot, cold)` — lấy từ SỔ BỀN VỮNG, không phải từ RAM.

    hot = vừa có link (dò mỗi MONITOR_SECS), cold = link đã cũ (dò mỗi MONITOR_COLD_SECS),
    bỏ hẳn sau MONITOR_MAX_IDLE_MIN phút kể từ lần dò cuối.
    """
    now = time.time()
    hot: list[dict] = []
    cold: list[dict] = []
    _ledger_load()
    with _LINK_LEDGER_LOCK:
        entries = [dict(v) for v in _LINK_LEDGER.values()]
    for entry in entries:
        link = str(entry.get("link") or "")
        if not link or not artifact_probe.is_upi_instructions_url(link):
            continue
        if str(entry.get("state") or "") in LINK_TERMINAL:
            continue
        last = float(entry.get("checked_at") or entry.get("first_seen") or 0)
        if last and (now - last) / 60.0 > MONITOR_MAX_IDLE_MIN:
            continue
        entry["_key"] = _ledger_key(str(entry.get("job_id") or ""),
                                    str(entry.get("task_id") or ""))
        (hot if (now - last) <= MONITOR_HOT_IDLE_MIN * 60 else cold).append(entry)
    return hot, cold


def _task_proxy(task: Task) -> str:
    """Proxy để dò link: ưu tiên `task.proxy`, không có thì lấy từ egress đã lưu.

    Task cũ (lưu trước khi có field `proxy`) chỉ có proxy trong `egress` — vẫn phải dùng nó,
    nếu không việc dò link sẽ đi thẳng bằng IP thật của máy.
    """
    return str(task.proxy or (task.egress or {}).get("proxy") or "")


def _working_proxies(exclude: str = "", limit: int = 2) -> list[str]:
    """Proxy khác trong sổ ĐÃ TỪNG đọc được trang instructions (để dò hộ khi proxy chính lỗi).

    Vì sao cần: sổ ghim 1 proxy cho mỗi link, mà một proxy có thể hỏng TLS tới
    `payments.stripe.com` (đo thật 19/10: link của t4077 đọc được `succeeded` qua 3 proxy khác
    và đọc trực tiếp, nhưng SSLError qua đúng proxy đã ghim -> chip đứng ở "Not verified yet").
    """
    _ledger_load()
    now = time.time()
    with _LINK_LEDGER_LOCK:
        entries = [dict(v) for v in _LINK_LEDGER.values()]
    out: list[str] = []
    for entry in entries:
        proxy = str(entry.get("proxy") or "")
        ok_at = float(entry.get("proxy_ok_at") or 0)
        if not proxy or proxy == exclude or (now - ok_at) > 6 * 3600:
            continue
        if proxy not in out:
            out.append(proxy)
        if len(out) >= limit:
            break
    return out


def _monitor_one(entry: dict) -> None:
    """Dò 1 link trong sổ: cập nhật sổ + (nếu job còn trong RAM) cập nhật Task và báo UI."""
    link = str(entry.get("link") or "")
    job_id = str(entry.get("job_id") or "")
    task_id = str(entry.get("task_id") or "")
    proxy = str(entry.get("proxy") or "")
    old = str(entry.get("state") or "")
    first_seen = int(entry.get("first_seen") or 0)
    probe = artifact_probe.probe(link, fresh=True, proxy=proxy)
    now = int(time.time())
    fields: dict = {"checked_at": now, "first_seen": first_seen or now}
    used_proxy = proxy
    if not probe.get("ok") and artifact_probe.is_inconclusive(str(probe.get("error") or "")):
        # Proxy ghim có thể hỏng TLS riêng với payments.stripe.com -> thử 2 proxy khác đã từng
        # đọc được. Thành công thì đổi luôn proxy của entry (lần sau khỏi lỗi lại).
        for alt in _working_proxies(exclude=proxy, limit=2):
            alt_probe = artifact_probe.probe(link, fresh=True, proxy=alt)
            if alt_probe.get("ok"):
                probe, used_proxy = alt_probe, alt
                break
    if probe.get("ok"):
        fields["proxy_ok_at"] = now
    if not probe.get("ok"):
        err = str(probe.get("error") or "")
        if artifact_probe.is_inconclusive(err):
            link_watch_upsert(job_id, task_id, link, proxy, **fields)
            _ledger_save()
            return                      # chưa đọc được -> giữ nguyên trạng thái cũ
        new_state, fam, zero = "failed", str(entry.get("fam") or ""), False
        note = "link unreadable: %s" % err
    else:
        new_state = str(probe.get("status") or "")
        fam = str(probe.get("fam") or "") or str(entry.get("fam") or "")
        zero = bool(probe.get("zero_mandate"))
        note = str(probe.get("zero_label") or "")

    fields.update({"state": new_state, "fam": fam, "zero": zero, "note": note,
                   "flip_at": int(entry.get("flip_at") or 0) or now})
    link_watch_upsert(job_id, task_id, link, used_proxy, **fields)
    _ledger_save()

    job = _JOBS.get(job_id)
    if job is None:
        return                          # job cũ (chỉ còn trên đĩa) -> sổ là nguồn sự thật
    with job._lock:
        task = job.tasks.get(task_id)
    if task is None:
        return
    task.link_first_seen = fields["first_seen"]
    task.link_checked_at = now
    task.link_fam = fam or task.link_fam
    task.link_zero = zero
    task.link_note = note
    if new_state == old:
        # Trạng thái không đổi vẫn phải báo mốc vừa dò, để UI hiện "checked Xm ago" thay vì
        # một chip "Awaiting payment" đứng im mà không biết còn được theo dõi hay không.
        job.emit({"type": "task_link_checked", "task_id": task_id, "link_checked_at": now})
        return
    task.link_state = new_state
    task.link_flip_at = now
    age = now - fields["first_seen"]
    label = {
        "succeeded": "CUSTOMER SCANNED — mandate succeeded",
        "failed": "link dead (Stripe declined / QR expired)",
        "canceled": "link cancelled",
        "expired": "link expired, never scanned",
    }.get(new_state, "state=%s" % (new_state or "?"))
    job.log_line("[%s] link: %s -> %s sau %ds (%s; zero=%s fam=%s)"
                 % (task_id, old or "unknown", new_state or "?", age,
                    label, zero, fam or "?"))
    job.emit({"type": "task_link", "task_id": task_id, "link_state": new_state,
              "link_checked_at": now, "link_flip_at": now, "link_fam": task.link_fam,
              "link_zero": zero, "link_note": note, "age": age, "probe": probe})
    if new_state == "succeeded":
        # Khách vừa duyệt mandate -> bắt đầu dò xem acc có lên Plus không.
        _start_plus_check(job, task)


# Nhịp dò Plus sau khi khách duyệt mandate: 30 giây một lần, tối đa 30 phút.
# Dò thưa hơn thì phát hiện muộn (khách thường trả tiền trong vài phút); dò dày hơn
# thì tốn proxy vô ích. Đổi được bằng env mà không phải sửa code:
#   UPI_PLUS_CHECK_SECS=30  UPI_PLUS_CHECK_MAX_SECS=1800
PLUS_CHECK_EVERY_SECS = max(1, int(float(os.environ.get("UPI_PLUS_CHECK_SECS", "30") or 30)))
PLUS_CHECK_MAX_SECS = max(0, int(float(os.environ.get("UPI_PLUS_CHECK_MAX_SECS",
                                                      str(30 * 60)) or 1800)))


def _start_plus_check(job: Job, task: Task) -> None:
    """Bắt đầu dò xem acc đã lên Plus chưa. Mỗi task chỉ dò một lần."""
    with job._lock:
        if task.plus_state:
            return                      # đang dò hoặc đã có kết luận
        task.plus_state = "checking"
    job.emit({"type": "task_plus", "task_id": task.task_id, "plus_state": "checking"})
    threading.Thread(target=_plus_check_loop,
                     args=(job, task, task.token,
                           str((task.egress or {}).get("proxy") or "")),
                     name="plus-%s" % task.task_id, daemon=True).start()


def _plus_check_loop(job: Job, task: Task, token: str, proxy: str) -> None:
    try:
        import at_check
    except ImportError as exc:
        job.log_line("[%s] plus: khong import duoc at_check (%s)" % (task.task_id, exc))
        with job._lock:
            task.plus_state = "not_plus"
        return
    # Dò NGAY lần đầu, rồi cách nhau đúng PLUS_CHECK_EVERY_SECS cho tới khi hết hạn
    # dò. (Trước đây lặp qua danh sách mốc rồi `sleep(mốc)` -> các mốc bị CỘNG DỒN,
    # nhịp thật thưa dần chứ không phải cách đều.)
    deadline = time.monotonic() + PLUS_CHECK_MAX_SECS
    attempt = 0
    while True:
        attempt += 1
        try:
            res = at_check.check(token, proxy)
        except Exception as exc:  # noqa: BLE001 — vòng nền không được chết
            job.log_line("[%s] plus: loi khi kiem tra (%s)" % (task.task_id, str(exc)[:120]))
            res = None
        if res is not None:
            plus = bool(res.get("plus"))
            with job._lock:
                task.plus_checked_at = int(time.time())
                task.plus_note = str(res.get("note") or "")
                task.plus_state = "plus" if plus else "checking"
            job.emit({"type": "task_plus", "task_id": task.task_id,
                      "plus_state": task.plus_state, "plus_note": task.plus_note,
                      "plus_checked_at": task.plus_checked_at})
            if plus:
                job.log_line("[%s] plus: DA LEN PLUS sau %d lan do (%s)"
                             % (task.task_id, attempt, task.plus_note))
                return
        left = deadline - time.monotonic()
        if left <= 0:
            break
        time.sleep(min(PLUS_CHECK_EVERY_SECS, left))
    with job._lock:
        if task.plus_state == "checking":
            task.plus_state = "not_plus"
    job.emit({"type": "task_plus", "task_id": task.task_id, "plus_state": "not_plus",
              "plus_note": task.plus_note, "plus_checked_at": task.plus_checked_at})
    job.log_line("[%s] plus: chua len Plus sau %d lan do trong %ds (%s)"
                 % (task.task_id, attempt, PLUS_CHECK_MAX_SECS, task.plus_note))


def _link_monitor_loop() -> None:
    last_cold = 0.0
    while True:
        try:
            time.sleep(MONITOR_SECS)
            hot, cold = _link_monitor_targets()
            now = time.time()
            due_cold = bool(cold) and (now - last_cold) >= MONITOR_COLD_SECS
            targets = hot + (cold if due_cold else [])
            if not targets:
                continue
            if due_cold:
                last_cold = now
            with ThreadPoolExecutor(max_workers=max(1, MONITOR_CONC)) as ex:
                list(ex.map(_monitor_one, targets))
        except Exception as exc:  # noqa: BLE001 — vòng nền không được chết
            try:
                print("[link-monitor] poll loop error: %s" % str(exc)[:160], flush=True)
            except Exception:  # noqa: BLE001
                pass


_MONITOR_STARTED = threading.Event()


def start_link_monitor() -> bool:
    """Bật vòng dò link nền. Gọi từ app.py lúc khởi động (không tự chạy khi import
    để test/CLI không bị dính tác dụng phụ)."""
    if _MONITOR_STARTED.is_set():
        return False
    _MONITOR_STARTED.set()
    threading.Thread(target=_link_monitor_loop, name="link-monitor", daemon=True).start()
    return True


def _bucket(task: dict) -> str:
    """把一个 task 归到 UI 的某个 tab。

    必须和 app.js 的 normalizeStatus() 用同一套判据，否则「Xoá tab Thất bại」
    会漏掉一批 task —— 两边不一致比没有这个按钮更糟。
    """
    ts = str(task.get("status") or "").lower()
    rs = str(task.get("raw_status") or "").strip().upper()
    if ts == "running":
        return "running"
    if rs == "LINK":
        return "success"
    # Dừng theo yêu cầu KHÔNG phải thất bại: task bị kill giữa chừng (raw STOPPED)
    # hoặc chưa hề chạy (status stopped) đều vào tab "Đã dừng". Gộp chúng vào "fail"
    # làm 92 task chưa chạy hiện ra như 92 task lỗi.
    if ts == "stopped" or rs == "STOPPED":
        return "stopped"
    if ts in ("done", "fail"):
        return "fail"
    if rs:
        return "fail"
    return "queued"


def clear_tasks(job_id: str, scope: str = "all") -> dict:
    """Xoá task thuộc một tab khỏi job. Trả về số lượng đã xoá.

    scope khớp tên tab: all | running | queued | success | fail。

    **必须同时改内存里的 Job 和落盘的 JSON。** `/api/state` 优先读内存对象
    （app.py: `job = engine.get_job(job_id) or get_saved_snapshot(...)`），
    只改文件的话界面照样把旧列表读回来 —— 踩过一次。

    安全边界：job 正在跑的时候不允许清 running / queued / all —— 那些 task
    归工作线程所有，从列表里抽掉它们，之后 task_done 会找不到对象。
    已结束的 success / fail 随时可以清。
    """
    if scope not in ("all", "running", "queued", "success", "fail", "stopped"):
        return {"ok": False, "error": "invalid scope: %s" % scope, "removed": 0}

    job = _JOBS.get(job_id)
    running = job is not None and job.status == "running"
    if running and scope in ("all", "running", "queued"):
        return {"ok": False, "removed": 0,
                "error": "job is running — only the Success / Failed tabs can be cleared"}

    removed = 0
    if job is not None:
        with job._lock:
            doomed = [tid for tid, task in job.tasks.items()
                      if tid not in job._pending and (scope == "all" or _bucket({
                          "status": task.status, "raw_status": getattr(task, "raw_status", ""),
                      }) == scope)]
            for tid in doomed:
                job.tasks.pop(tid, None)
                if tid in job.order:
                    job.order.remove(tid)
            removed = len(doomed)
            job.total = len(job.order)
        if removed:
            # total 是普通属性可以改；done / ok / fail 是 @property（281~289 行）
            # 由 tasks 自己算出来的，**赋值会抛 AttributeError**。删完 task 它们
            # 自动就对了，别手动设。
            job.total = len(job.order)
            job.emit({"type": "tasks_cleared", "scope": scope, "removed": removed})
            job.write_results()
        return {"ok": True, "removed": removed, "scope": scope, "total": job.total}

    # 不在内存里（重启过 / 已被 LRU 挤掉）-> 直接改文件
    snapshot = get_saved_snapshot(job_id)
    if snapshot is None:
        return {"ok": False, "error": "job does not exist", "removed": 0}
    tasks = snapshot.get("tasks") or []
    kept = [t for t in tasks if not (scope == "all" or _bucket(t) == scope)]
    removed = len(tasks) - len(kept)
    snapshot["tasks"] = kept
    snapshot["total"] = len(kept)
    snapshot["done"] = sum(1 for t in kept if _bucket(t) in ("success", "fail", "stopped"))
    snapshot["ok"] = sum(1 for t in kept if _bucket(t) == "success")
    snapshot["fail"] = sum(1 for t in kept if _bucket(t) == "fail")
    path = LOG_DIR / f"web_{job_id}_results.json"
    try:
        path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=1), encoding="utf-8")
    except OSError as exc:
        return {"ok": False, "error": str(exc), "removed": 0}
    return {"ok": True, "removed": removed, "scope": scope, "total": len(kept)}


def remove_task(job_id: str, task_id: str) -> dict:
    """Xoá MỘT task khỏi job (cả RAM lẫn file đã lưu).

    Cần endpoint riêng vì `clear_tasks` chỉ xoá theo tab: muốn bỏ 1 card lẻ ra khỏi
    tab Thành công thì trước đây UI chỉ `.remove()` phần tử DOM — F5 là nó quay lại
    nguyên vẹn, vì server chưa hề biết.

    Không cho xoá task đang chạy / đang chờ của job đang chạy: chúng thuộc về
    worker thread, rút khỏi danh sách thì `task_done` sau đó sẽ mất đối tượng.
    """
    job = _JOBS.get(job_id)
    if job is not None:
        with job._lock:
            task = job.tasks.get(task_id)
            if task is None:
                return {"ok": False, "error": "task not found in job", "removed": 0}
            if task_id in job._pending or task.status in ("running", "pending"):
                return {"ok": False, "removed": 0,
                        "error": "task is running/queued — stop the job before removing"}
            job.tasks.pop(task_id, None)
            if task_id in job.order:
                job.order.remove(task_id)
            job.total = len(job.order)
        job.emit({"type": "task_removed", "task_id": task_id})
        job.write_results()
        return {"ok": True, "removed": 1, "total": job.total}

    # job không còn trong RAM -> sửa thẳng file
    snapshot = get_saved_snapshot(job_id)
    if snapshot is None:
        return {"ok": False, "error": "job does not exist", "removed": 0}
    tasks = snapshot.get("tasks") or []
    kept = [t for t in tasks if t.get("task_id") != task_id]
    if len(kept) == len(tasks):
        return {"ok": False, "error": "task does not exist", "removed": 0}
    snapshot["tasks"] = kept
    snapshot["total"] = len(kept)
    snapshot["done"] = sum(1 for t in kept if _bucket(t) in ("success", "fail", "stopped"))
    snapshot["ok"] = sum(1 for t in kept if _bucket(t) == "success")
    snapshot["fail"] = sum(1 for t in kept if _bucket(t) == "fail")
    path = LOG_DIR / f"web_{job_id}_results.json"
    try:
        path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=1), encoding="utf-8")
    except OSError as exc:
        return {"ok": False, "error": str(exc), "removed": 0}
    return {"ok": True, "removed": 1, "total": len(kept)}


def get_saved_snapshot(job_id: str, *, backfill: bool = True) -> dict | None:
    if len(job_id) != 12 or any(c not in "0123456789abcdef" for c in job_id.lower()):
        return None
    path = LOG_DIR / f"web_{job_id}_results.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else None
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    # Trạng thái link mới nhất nằm ở sổ theo dõi (xem link_watch_overlay) — job cũ vẫn phải
    # hiện đúng "khách đã duyệt / hết hạn" chứ không đứng im ở "Awaiting payment".
    link_watch_overlay(data)
    if backfill:
        backfill_link_watch(data)
    # A saved running job without an owner was interrupted by server exit.
    # Never silently resume payment work or show phantom running workers.
    if job_id not in _JOBS and data.get("status") == "running":
        data.update(status="stopped", stop_requested=True,
                    stop_reason="server restarted — unfinished tasks were not resumed")
        tasks = data.get("tasks") or []
        for task in tasks:
            if task.get("status") in ("running", "pending"):
                task.update(status="stopped", raw_status="STOPPED", error=data["stop_reason"])
        data["done"] = sum(t.get("status") in ("done", "fail", "stopped") for t in tasks)
        data["running"] = data["queued"] = 0
    return data


HEADLINE_SUFFIX = "_headline.json"
HEADLINE_KEYS = ("job_id", "status", "total", "done", "ok", "fail", "started_at")
# Snapshot lớn nhất đo được 24 MB; `/api/jobs` cũ parse tới 20 file như vậy MỖI lần gọi
# (1,67 GB trong logs/) -> >65s, UI hiện "0 job" và "Loading...". Nay đọc headline ~300 byte.
HEADLINE_MAX_BYTES = 4096


def _headline_path(job_id: str) -> Path:
    return LOG_DIR / f"web_{job_id}{HEADLINE_SUFFIX}"


def _write_headline(headline: dict) -> None:
    try:
        job_id = str(headline.get("job_id") or "")
        if not job_id:
            return
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        temp = _headline_path(job_id).with_suffix(".json.tmp")
        temp.write_text(json.dumps({k: headline.get(k) for k in HEADLINE_KEYS},
                                   ensure_ascii=False), encoding="utf-8")
        temp.replace(_headline_path(job_id))
    except OSError:
        pass


def _read_headline(path: Path) -> dict | None:
    try:
        if path.stat().st_size > HEADLINE_MAX_BYTES:
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) and data.get("job_id") else None


def list_jobs() -> list[dict]:
    out = []
    seen = set()
    with _JOBS_LOCK:
        jobs = [_JOBS.get(jid) for jid in reversed(_JOB_ORDER)]
    for j in jobs:
        if j:
            out.append({"job_id": j.job_id, "status": j.status, "total": j.total,
                        "done": j.done, "ok": j.ok, "fail": j.fail,
                        "started_at": j.started_at})
            seen.add(j.job_id)
    try:
        headline_files = sorted(LOG_DIR.glob(f"web_*{HEADLINE_SUFFIX}"),
                                key=lambda p: p.stat().st_mtime, reverse=True)
    except OSError:
        headline_files = []
    # Sắp theo `started_at` (KHÔNG theo mtime file headline: file headline vừa được tạo hàng loạt
    # nên mtime không nói gì về thứ tự job).
    disk: list[dict] = []
    for path in headline_files:
        headline = _read_headline(path)
        if not headline or headline.get("job_id") in seen:
            continue
        disk.append({key: headline.get(key) for key in HEADLINE_KEYS})
        seen.add(headline.get("job_id"))
    disk.sort(key=lambda job: str(job.get("started_at") or ""), reverse=True)
    for job in disk[:max(0, MAX_JOBS - len(out))]:
        # Job "running" mà không còn trong RAM = server đã restart -> thực tế đã dừng. Trước đây
        # `/api/state` sửa trạng thái này còn danh sách thì không, nên history hiện job chạy mãi.
        out.append(dict(job, status="stopped") if job.get("status") == "running" else job)
    if len(out) < MAX_JOBS:
        # Job cũ chưa có headline (tạo trước bản này): chỉ khi đó mới parse snapshot, và chỉ
        # để LẤY HEADER rồi ghi luôn headline cho lần sau — không backfill link ở đây.
        try:
            result_files = sorted(LOG_DIR.glob("web_*_results.json"),
                                  key=lambda p: p.stat().st_mtime, reverse=True)
        except OSError:
            result_files = []
        for path in result_files:
            if len(out) >= MAX_JOBS:
                break
            job_id = path.name[4:-13]
            if job_id in seen:
                continue
            snapshot = get_saved_snapshot(job_id, backfill=False)
            if not snapshot or snapshot.get("job_id") in seen:
                continue
            headline = {key: snapshot.get(key) for key in HEADLINE_KEYS}
            _write_headline(headline)
            out.append(headline)
            seen.add(snapshot.get("job_id"))
    return out[:MAX_JOBS]


# ------------------------------------------------------------- proxy helpers
def parse_proxy_lines(text: str) -> list[str]:
    """Chuẩn hoá pool proxy về URL form `http://user:pass@host:port`.

    extract_cs.py reads the seed file and needs the username after `@` to
    rewrite `region-XX`. Compact
    `host:port:user:pass` không có `@` -> urlsplit().username = None -> báo
    "Proxy has no writable country/region selector". Nên phải ghi ra URL form.
    """
    from urllib.parse import quote

    px: list[str] = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "://" in line:
            px.append(cli.core.normalize_proxy_url(line))
            continue
        parts = line.split(":")
        if len(parts) == 4:
            host, port, user, pw = parts
            px.append(f"http://{quote(user)}:{quote(pw)}@{host}:{port}")
        else:
            px.append(cli.core.normalize_proxy_url(line))
    return px


# giu ten cu cho tuong thich
_parse_proxies = parse_proxy_lines


def mix_pool(proxies: list[str], job_id: str = "") -> list[str]:
    """Trộn pool để các task rải đều mọi nhóm vùng thay vì ăn theo thứ tự dán vào.

    `_pick_proxy` chia proxy theo THỨ TỰ trong pool. Pool dán tay thường được xếp
    theo từng khối vùng (`-st-Maharashtra` 200 dòng, rồi `-st-Bihar` 200 dòng...),
    nên 200 task đầu sẽ chỉ nhận đúng 1 vùng. Trộn bằng RNG gieo theo job_id ->
    mỗi job một thứ tự khác nhau nhưng vẫn tái lập được, và pool đã trộn vẫn được
    ghi ra `cs_state/web_proxies_<job>.txt` để đối chiếu sau.
    """
    if len(proxies) < 2:
        return list(proxies)
    out = list(proxies)
    random.Random(f"mix:{job_id}").shuffle(out)
    return out


def _prepare_proxies(job_id: str, proxies: str | None,
                     task_count: int = 0,
                     mix: bool = False,
                     filter_proxies: bool = True) -> tuple[Path, list[str], str, dict]:
    """Tra ve (proxy_file, danh sach proxy, nhan hien thi, bao cao chat luong).

    KHONG cham proxy o day nua (mac dinh `UPI_PROXY_QUALITY=lazy`). Vi sao: cham
    global nam trong POST /api/run nen UI dung o "Creating job... 1m08s" — do that
    30 proxy mat 13-45s, 100 proxy mat 44-77s, trong khi task dau tien chua chay.
    Gio moi worker tu cham lay proxy cua minh trong `_acquire_proxy` (song song),
    nen job bat dau ngay va chi nhung proxy THUC SU dung moi bi cham.

    `UPI_PROXY_QUALITY=pre` -> quay lai cham global truoc khi chay (nhu ban cu).
    """
    if proxies and proxies.strip():
        px = parse_proxy_lines(proxies)
        label = f"custom proxy pool ({len(px)} proxies)"
    else:
        px = cli.load_proxies(DEFAULT_PROXY_FILE)
        label = f"{DEFAULT_PROXY_FILE.name} ({len(px)} proxy)"
    if mix and px:
        px = mix_pool(px, job_id)
        label += " · mixed"
    if not filter_proxies:
        label += " · no filter"

    report: dict = {"mode": proxy_quality_mode() if filter_proxies else "off"}
    if filter_proxies and proxy_quality_mode() == "pre" and px:
        try:
            import ippure  # noqa: PLC0415
            if ippure.enabled():
                budget = int(os.environ.get("UPI_PROXY_QUALITY_MAX") or 0) or len(px)
                budget = max(8, min(budget, len(px)))
                t0 = time.time()
                rows = ippure.scan(px[:budget], "IN", cli.proxy_persona_timezone(),
                                   workers=max(4, min(32, budget)),
                                   history_of=cli.proxy_history)
                usable = [r["proxy"] for r in rows if r.get("ok")]
                dead = [r["proxy"] for r in rows if not r.get("ok")]
                if usable:
                    px = [r["proxy"] for r in rows if r.get("ok")] + \
                         [p for p in px if p not in set(px[:budget]) and p not in set(dead)]
                best = rows[0] if rows else {}
                report.update({"scanned": len(rows), "usable": len(usable), "budget": budget,
                               "seconds": round(time.time() - t0, 1), "excluded": len(dead),
                               "best": {"grade": best.get("grade"), "score": best.get("score"),
                                        "country": best.get("country"), "risk": best.get("risk"),
                                        "user_type": best.get("user_type"), "isp": best.get("isp"),
                                        "family": best.get("family"),
                                        "latency_ms": best.get("latency_ms")},
                               "flags": best.get("flags") or []})
        except Exception as exc:  # noqa: BLE001 — cham diem loi thi chay nhu cu
            report["error"] = f"{type(exc).__name__}: {str(exc)[:120]}"

    path = STATE_DIR / f"web_proxies_{job_id}.txt"
    # Ghi URL form (khong phai nguyen van textarea) — xem parse_proxy_lines()
    path.write_text("\n".join(px) + "\n", encoding="utf-8")
    return path, px or [""], label, report


_PROXY_PICK_LOCK = threading.Lock()
_PROXY_PICK_INDEX: dict[str, int] = {}


def _pick_proxy(job_id: str, proxies: list[str], task_id: str = "") -> str:
    """Chia proxy theo THU TU da xep hang -> moi task mot con khac nhau.

    Ban cu dung secrets.choice(proxies): 100 task tren 100 proxy thi trung nhau
    nhieu (chi ~63% proxy duoc dung) va task nao cung co the nhan proxy F.
    Dat UPI_PROXY_QUALITY=0 de quay lai random nhu truoc.
    """
    if not proxies:
        return ""
    if len(proxies) == 1:
        return proxies[0]
    if str(os.environ.get("UPI_PROXY_QUALITY") or "1").strip().lower() in ("0", "false", "no", "off"):
        return secrets.choice(proxies)
    with _PROXY_PICK_LOCK:
        index = _PROXY_PICK_INDEX.get(job_id, 0)
        _PROXY_PICK_INDEX[job_id] = index + 1
    picked = proxies[index % len(proxies)]
    if task_id:
        _lease_proxy(job_id, picked, task_id)
    return picked


# -------------------------------------------------------------- step helpers
# Ba dạng dòng log flow in ra về hoá đơn. Bắt NGAY lúc nhận log để lưu vào kết quả,
# vì log của task chỉ giữ 50 dòng cuối -> 85% task thành công mất dòng billing.
# Ghi theo từng khoá, mỗi khoá chỉ lấy lần đầu -> nhiều dòng không ghi đè nhau.
_BILLING_PATTERNS: tuple[tuple["re.Pattern[str]", tuple[str, ...]], ...] = (
    # billing geo: Karnataka / PIN 575001 / exit 157.45.192.254 (IN) via geo_exit:nominatim
    (re.compile(r"billing geo:\s*([^/]+?)\s*/\s*PIN\s*(\d{6})"),
     ("geo_state", "geo_pin")),
    # billing tier 2 (PIN verified) 157.45.192.254 / Dakshina Kannada / KA 575001 · source …
    # (bản cũ tiếng Việt: "billing mức 2 (PIN xác thực) …")
    (re.compile(r"billing (?:mức 2|tier 2) \([^)]*\)\s*\S+\s*/\s*([^/]+?)\s*/\s*[A-Z]{2}\s*(\d{6})"),
     ("geo_city", "geo_pin")),
    # IN details filled into PM billing_details: Vivaan Joshi / 67 Janpath / New Delhi 110001
    # Địa chỉ có thể CHỨA " / " (vd "90, Golden Residency, Pipola / Tehri Garhwal") nên
    # nhóm địa chỉ phải THAM LAM (`.*`) — bản cũ dùng `.+?` nên 12,8% bản ghi bị tách sai:
    # thành phố bị dính cả địa chỉ, PIN/city lệch nhau.
    (re.compile(r"details filled into PM billing_details:\s*(?P<name>[^/]+?)\s*/\s*"
                r"(?P<addr>.*)\s*/\s*(?P<city>[^/]+?)\s+(?P<pin>\d{6})\s*$"),
     ("billing_name", "_addr", "billing_city", "billing_pin")),
)


# Nguồn địa chỉ hoá đơn, đọc từ log để biết đơn nào dùng list cứng (địa chỉ không
# khớp IP) — trước đây phải suy từ thành phố/PIN nên dễ đoán sai.
_BILLING_CHECK_RE = re.compile(r"billing check:\s*([a-z-]+)")
_BILLING_TIER_PATTERNS: tuple[tuple["re.Pattern[str]", str], ...] = (
    (re.compile(r"billing tier 2 \(PIN verified\)"), "tier2"),
    (re.compile(r"billing geo:\s*[^/]+?\s*/\s*PIN\s*\d{6}"), "tier1"),
    (re.compile(r"billing using the hardcoded list"), "hardcoded-list"),
)


def _absorb_billing(task: Task, line: str) -> dict[str, str]:
    """Bắt thông tin hoá đơn trong 1 dòng log, ghi vào task (mỗi khoá lấy lần đầu).

    Trả về các khoá VỪA ghi (rỗng nếu dòng này không phải dòng hoá đơn) — caller
    bắn event `task_billing` để panel hiện khối "Billing details" ngay, không phải F5.
    """
    written: dict[str, str] = {}
    hit = _BILLING_CHECK_RE.search(line)
    if hit and not task.billing_check:
        task.billing_check = hit.group(1)
        written["billing_check"] = task.billing_check
    if not task.billing_tier:
        for pattern, tier in _BILLING_TIER_PATTERNS:
            if pattern.search(line):
                task.billing_tier = tier
                written["billing_tier"] = tier
                break
    for pattern, keys in _BILLING_PATTERNS:
        hit = pattern.search(line)
        if not hit:
            continue
        for key, value in zip(keys, hit.groups()):
            if key.startswith("_") or not value:
                continue
            if getattr(task, key, ""):
                continue                     # đã có -> không ghi đè
            clean = value.strip()[:80]
            setattr(task, key, clean)
            written[key] = clean
        return written
    return written


def _mk_hooks(job: Job, task: Task):
    # Thu tu buoc chuan cua luong: dung de DON DIEU HOA tien do ben duoi.
    # `steps_for()` tra ve list DICT, khong phai list tuple -> phai lay theo khoa "key"
    # (bản đầu tôi viết `for k, _ in ...` nên dict bị unpack thành ("key","label") và
    # bảng vị trí sai hết, khiến bước vẫn nằm "pending" giữa 2 ô đã xong).
    position = {s["key"]: i for i, s in enumerate(cli.steps_for(job.mode))}

    def emit_step(s: dict) -> None:
        ev = {"type": "step", "task_id": task.task_id, "key": s["key"],
              "status": s["status"]}
        for k in ("detail", "err"):
            if s.get(k):
                ev[k] = str(s[k])[:200]
        job.emit(ev)

    def step(key: str, status: str, **info: Any) -> None:
        s = task.step(key)
        if s is None:
            # buoc khong co trong danh sach hien tai -> them vao cuoi
            s = {"key": key, "label": key, "status": "pending", "detail": "", "err": ""}
            task.steps.append(s)
        s["status"] = status
        if info.get("detail"):
            s["detail"] = str(info["detail"])[:120]
        if info.get("err"):
            s["err"] = str(info["err"])[:200]
        if info.get("provider"):
            task.provider = str(info["provider"])
        task.updated_at = _now()
        ev = {"type": "step", "task_id": task.task_id, "key": key, "status": status}
        for k in ("detail", "err"):
            if info.get(k):
                ev[k] = str(info[k])[:200]
        job.emit(ev)

        # ĐƠN ĐIỆU HOÁ: bước nào đứng TRƯỚC bước vừa tới mà còn "pending" thì luồng này
        # không dùng tới (ví dụ tax region không bật, PM tạo inline) -> đánh "skip".
        # Không có bước này thì UI hiện ✓ ở giữa, phía trên vẫn còn ô trống -> nhìn như
        # "hiện tùm lùm"/không theo kịp log (lỗi chỉ xảy ra ở chế độ in-process, vì luồng
        # subprocess có `cli.make_step_advancer` riêng).
        pos = position.get(key)
        if pos is None or status not in ("done", "active", "skip", "fail"):
            return
        for other in task.steps:
            if other is s:
                continue
            other_pos = position.get(other["key"])
            # bỏ qua: bước của luồng khác (không có trong `position`), bước phía sau,
            # bước đã có trạng thái, và BƯỚC ĐẦU (runner = cả luồng, không phải bị bỏ).
            if other_pos is None or other_pos == 0 or other_pos >= pos:
                continue
            if other["status"] != "pending":
                continue
            other["status"] = "skip"
            emit_step(other)

    def on_log(line: str) -> None:
        # Chỉ giữ lại đuôi ngắn cho banner lỗi (failReason) — KHÔNG còn đẩy từng dòng
        # lên web nữa: panel log đã bỏ khỏi UI. Toàn bộ log vẫn nằm trong
        # logs/web_<job>.log (job) và logs/upi_<ts>.log (extract_cs tự ghi).
        if len(task.logs) < MAX_LOGS_PER_TASK:
            task.logs.append(line[:500])
        got = _absorb_billing(task, line)
        if got:
            # Panel hiện khối "Billing details" ngay khi bắt được dòng hoá đơn.
            job.emit({"type": "task_billing", "task_id": task.task_id, "billing": got})

    def on_geo(info: dict) -> None:
        loc = ", ".join(p for p in (info.get("city"), info.get("region"), "IN") if p)
        eg = task.egress or {}
        eg["billing_geo"] = {"ip": info.get("ip") or "", "location": loc,
                             "approx": bool(info.get("approx"))}
        eg.setdefault("probed_at", _now())
        task.egress = eg
        job.emit({"type": "egress", "task_id": task.task_id, **eg})

    return step, on_log, on_geo


_EGRESS_CACHE: dict[str, dict] = {}
_EGRESS_LOCK = threading.Lock()


def _egress_for(proxy: str) -> dict:
    """Do IP exit/billing cho 1 proxy, cache theo proxy (nhieu task dung chung proxy)."""
    if not proxy:
        return {}
    with _EGRESS_LOCK:
        hit = _EGRESS_CACHE.get(proxy)
    if hit is not None:
        return hit
    try:
        info = cli.probe_egress_pair(proxy)
    except Exception:  # noqa: BLE001
        info = {}
    with _EGRESS_LOCK:
        _EGRESS_CACHE[proxy] = info
    return info


def _emit_egress(job: Job, task: Task, proxy: str) -> None:
    """Do va phat IP exit/billing ngay dau task de UI hien som, khong can click."""
    info = _egress_for(proxy)
    if not info:
        return
    ex = info.get("exit") or {}
    bl = info.get("billing") or {}
    _emit_egress_payload(job, task, proxy, ex, bl, info.get("billing_country") or "")


def _emit_egress_payload(job: Job, task: Task, proxy: str, ex: dict, bl: dict,
                         billing_country: str = "") -> None:
    eg = {
        "proxy": proxy,
        "exit": ex,
        "billing": bl,
        "billing_country": billing_country,
        "probed_at": _now(),
        "same_ip": bool(ex.get("ip")) and ex.get("ip") == bl.get("ip"),
    }
    task.egress = eg
    task.updated_at = _now()
    job.emit({"type": "egress", "task_id": task.task_id, **eg})


# ------------------------------------------------ proxy: moi worker tu cham lay
# Vi sao khong cham global truoc khi chay: do that 30 proxy mat 13-45s (tuy muc
# song song), 100 proxy mat 44-77s, va toan bo thoi gian do nam trong POST
# /api/run -> UI dung im o "Creating job... 1m08s" truoc khi task dau tien chay.
# Cach lam dung: moi worker tu lay 1 proxy roi tu cham lay no, chay song song voi
# cac worker khac -> job bat dau ngay, va chi nhung proxy THUC SU dung moi bi cham.
_PROXY_TRIED_LOCK = threading.Lock()
_PROXY_BAD: dict[str, set[str]] = {}
_PROXY_SCAN_INDEX: dict[str, int] = {}
# Ket qua da cham theo proxy (khong theo job): batch 500 task nhung pool 100 thi
# vong tron se dung lai cung proxy -> lan sau khong phai do lai (moi lan do la 1
# request qua proxy ~2-4s). TTL vi session co the doi IP.
_PROXY_VERDICT: dict[str, tuple[float, dict]] = {}
# Lease: proxy dang duoc 1 task chay thi task khac khong lay nua. Tranh 2 task cung
# luc di ra cung 1 IP (de bi risk engine ghep nhom) va tranh pool bi dung don ve
# mot vai con. Het proxy roi moi cho phep dung lai, uu tien con lau nhat.
_PROXY_LEASES: dict[str, dict[str, tuple[str, float]]] = {}   # job_id -> proxy -> (task_id, ts)
_LAST_TASK_PROXY: dict[str, str] = {}                          # "job|task" -> proxy da lease


def _lease_proxy(job_id: str, proxy: str, task_id: str) -> None:
    with _PROXY_TRIED_LOCK:
        _PROXY_LEASES.setdefault(job_id, {})[proxy] = (task_id, time.time())
        _LAST_TASK_PROXY[f"{job_id}|{task_id}"] = proxy


def _release_task_proxy(job_id: str, task_id: str) -> None:
    """Task xong -> tra proxy lai cho pool (goi trong finally cua _run_task)."""
    key = f"{job_id}|{task_id}"
    with _PROXY_TRIED_LOCK:
        proxy = _LAST_TASK_PROXY.pop(key, "")
        leases = _PROXY_LEASES.get(job_id)
        if proxy and leases and proxy in leases:
            leases.pop(proxy, None)


def _leased_proxies(job_id: str) -> dict[str, tuple[str, float]]:
    with _PROXY_TRIED_LOCK:
        return dict(_PROXY_LEASES.get(job_id, {}))


def _clear_job_proxy_state(job_id: str) -> None:
    with _PROXY_TRIED_LOCK:
        _PROXY_BAD.pop(job_id, None)
        _PROXY_SCAN_INDEX.pop(job_id, None)
        _PROXY_PICK_INDEX.pop(job_id, None)
        for key in [k for k in _LAST_TASK_PROXY if k.startswith(f"{job_id}|")]:
            _LAST_TASK_PROXY.pop(key, None)
        _PROXY_LEASES.pop(job_id, None)


def _proxy_exit_ip(proxy: str) -> str:
    return str((_cached_verdict(proxy) or {}).get("ip") or "")


# Cờ khiến proxy bị LOẠI ngay khi bật bộ lọc (không chỉ xếp cuối). `cf-challenge` = IP bị
# Cloudflare chấm xấu, mọi task qua nó chết ở bước tạo checkout.
PROXY_DROP_FLAGS = frozenset({"unreachable", "wrong-country", "tor", "malicious",
                              "cf-challenge"})


def _proxy_verdict_ttl() -> float:
    return float(os.environ.get("UPI_PROXY_VERDICT_TTL") or 900)


def _cached_verdict(proxy: str) -> dict:
    with _PROXY_TRIED_LOCK:
        hit = _PROXY_VERDICT.get(proxy)
    if not hit:
        return {}
    ts, verdict = hit
    ttl = _proxy_verdict_ttl()
    if ttl > 0 and time.time() - ts > ttl:
        return {}
    return verdict


def _cache_verdict(proxy: str, verdict: dict) -> None:
    with _PROXY_TRIED_LOCK:
        _PROXY_VERDICT[proxy] = (time.time(), verdict)


def proxy_quality_mode() -> str:
    """`lazy` (mac dinh): worker tu cham lay; `pre`: cham global truoc khi chay; `off`: bo qua."""
    raw = str(os.environ.get("UPI_PROXY_QUALITY") or "lazy").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return "off"
    if raw in ("pre", "global"):
        return "pre"
    return "lazy"


def _mark_proxy_bad(job_id: str, proxy: str) -> None:
    with _PROXY_TRIED_LOCK:
        _PROXY_BAD.setdefault(job_id, set()).add(proxy)


def _bad_proxies(job_id: str) -> set[str]:
    with _PROXY_TRIED_LOCK:
        return set(_PROXY_BAD.get(job_id, ()))


def _claim_candidate(job_id: str, proxies: list[str], task_id: str = "") -> str:
    """Chon proxy VA lease trong CUNG mot lan giu lock (nguyen tu).

    Vi sao phai gop: neu chon truoc roi lease sau thi 2 worker co the cung doc thay
    mot con "chua ai dung" roi cung nhan -> trung proxy, hoac trung ca IP exit (2
    entry khac nhau tro ve cung 1 IP). Do that truoc khi gop: 20 luong tranh nhau
    tren pool 10 (co 2 con trung IP) van lot 1 luot trung IP.

    Thu tu uu tien:
      1. chua bi danh hong + chua ai lease + IP exit chua ai dung
      2. chua bi danh hong + chua ai lease
      3. het proxy roi: dung lai con co lease CU nhat (chia deu, khong don 1 con)
    """
    if not proxies:
        return ""
    with _PROXY_TRIED_LOCK:
        bad = set(_PROXY_BAD.get(job_id, ()))
        leases = dict(_PROXY_LEASES.get(job_id, {}))
        used_ips = set()
        for leased in leases:
            ip = str((_PROXY_VERDICT.get(leased, (0, {}))[1] or {}).get("ip") or "")
            if ip:
                used_ips.add(ip)
        start = _PROXY_SCAN_INDEX.get(job_id, 0)

        def walk(pred):
            for offset in range(len(proxies)):
                idx = (start + offset) % len(proxies)
                cand = proxies[idx]
                if cand and cand not in bad and pred(cand):
                    return idx, cand
            return -1, ""

        def ip_of(cand):
            return str((_PROXY_VERDICT.get(cand, (0, {}))[1] or {}).get("ip") or "")

        idx, picked = walk(lambda c: c not in leases and (ip_of(c) or "x") not in used_ips)
        if not picked:
            idx, picked = walk(lambda c: c not in leases)
        if not picked:
            free = [(i, p) for i, p in enumerate(proxies) if p and p not in bad]
            if free:
                idx, picked = min(free, key=lambda ip: leases.get(ip[1], ("", 0.0))[1])
        if not picked:
            return ""
        _PROXY_SCAN_INDEX[job_id] = idx + 1
        if task_id:
            _PROXY_LEASES.setdefault(job_id, {})[picked] = (task_id, time.time())
            _LAST_TASK_PROXY[f"{job_id}|{task_id}"] = picked
        return picked


def _release_proxy(job_id: str, proxy: str) -> None:
    """Tra 1 proxy cu the (dung khi verify that bai)."""
    with _PROXY_TRIED_LOCK:
        leases = _PROXY_LEASES.get(job_id)
        if leases and proxy in leases:
            leases.pop(proxy, None)


def _address_check_enabled() -> bool:
    """Kiểm tra địa chỉ ngay lúc chọn proxy (mặc định BẬT, tắt bằng UPI_ADDRESS_PRECHECK=0)."""
    raw = str(os.environ.get("UPI_ADDRESS_PRECHECK") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


_ADDRESS_OK: dict[str, tuple[float, bool, str]] = {}
_ADDRESS_LOCK = threading.Lock()


def _address_ok(proxy: str) -> tuple[bool, str]:
    """(ok, lý do) — cache theo proxy để không tra lại geo cho cùng session."""
    ttl = float(os.environ.get("UPI_ADDRESS_PRECHECK_TTL") or 900)
    with _ADDRESS_LOCK:
        hit = _ADDRESS_OK.get(proxy)
    if hit and ttl > 0 and time.time() - hit[0] <= ttl:
        return hit[1], hit[2]
    try:
        import extract_cs  # noqa: PLC0415 — import muộn: chỉ nạp khi thật sự kiểm tra
        ok, why = extract_cs.billing_address_ok(proxy)
    except Exception as exc:        # noqa: BLE001 — không kiểm tra được thì chạy như cũ
        ok, why = True, f"check skipped ({type(exc).__name__})"
    with _ADDRESS_LOCK:
        if len(_ADDRESS_OK) > 4000:
            for old in list(_ADDRESS_OK)[:1024]:
                _ADDRESS_OK.pop(old, None)
        _ADDRESS_OK[proxy] = (time.time(), ok, why)
    return ok, why


def _verdict_usable(verdict: dict, target_country: str) -> bool:
    if not verdict or not verdict.get("ok"):
        return False
    flags = set(verdict.get("flags") or ())
    # `cf-challenge` = proxy bị Cloudflare chấm xấu: mọi task qua nó chết ở bước tạo checkout
    # (đo 18/10: 14/24 proxy như vậy), nên loại ngay khi bật bộ lọc proxy.
    if PROXY_DROP_FLAGS & flags:
        return False
    cc = str(verdict.get("country") or "").upper()
    return (not target_country) or cc == str(target_country).upper()


def _task_seed_file(job: Job, task: Task, proxy: str, proxies: list[str], backups: int = 5) -> Path:
    """Seed rieng cho 1 task: proxy da verify dung dau, sau do vai con du phong.

    extract_cs doc `UPI_PROXY_SEED_FILE` va tu chon seed trong danh sach do, nen
    dua proxy da verify len dau la task dung dung IP sach vua cham, ma van con con
    du phong de xoay neu con dau hong giua flow.
    """
    lines = [proxy]
    bad = _bad_proxies(job.job_id)
    for cand in proxies:
        if len(lines) > backups:
            break
        if cand and cand != proxy and cand not in bad:
            lines.append(cand)
    path = STATE_DIR / f"web_seed_{job.job_id}_{task.task_id}.txt"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _address_usable(job: Job, task: Task, proxy: str) -> bool:
    """Proxy có dựng được ĐỊA CHỈ hoá đơn không (kiểm ngay lúc chọn proxy).

    Vì sao ở đây: nếu để tới bước điền billing mới phát hiện thì task đã chạy hết checkout →
    retry phải chạy lại cả flow (đắt như dính risk decline). Kiểm ở tầng chọn proxy thì chỉ
    mất ~1-3s (có cache) và worker đổi ngay sang proxy khác.
    """
    if not _address_check_enabled():
        return True
    ok, why = _address_ok(proxy)
    if ok:
        return True
    job.emit({"type": "log", "task_id": task.task_id, "line":
              f"proxy bị loại: không dựng được địa chỉ hoá đơn ({why[:80]})"})
    return False


def _acquire_proxy(job: Job, task: Task,
                   proxies: list[str]) -> tuple[str, Path, dict]:
    """Worker tu lay + tu cham proxy cua minh. Tra (proxy, seed_file, verdict).

    Cham ~2-5s trong CHINH thread cua worker, song song voi cac worker khac. Con
    nao hong (khong ket noi duoc / sai nuoc / tor / malicious) thi danh dau de
    worker khac khong thu lai, roi thu con ke tiep.

    Luon tra ve seed file RIENG cua task (khong phai file pool): backend chi nhan
    `seed_file` chu khong nhan chuoi proxy, nen dua ca pool lam seed thi flow tu
    boc mot proxy bat ky — phot lo proxy da chia + thu tu da tron, va IP hien tren
    panel se khong phai IP flow thuc su dung.
    """
    if proxy_quality_mode() != "lazy" or not job.filter_proxies:
        # Tat loc: lay proxy theo thu tu pool, khong cham, khong loai, khong sinh bu.
        proxy = _pick_proxy(job.job_id, proxies, task.task_id)
        return proxy, _task_seed_file(job, task, proxy, proxies), {}
    try:
        import ippure  # noqa: PLC0415
    except Exception:  # noqa: BLE001
        proxy = _pick_proxy(job.job_id, proxies, task.task_id)
        return proxy, _task_seed_file(job, task, proxy, proxies), {}
    if not ippure.enabled():
        proxy = _pick_proxy(job.job_id, proxies, task.task_id)
        return proxy, _task_seed_file(job, task, proxy, proxies), {}

    attempts = max(1, int(os.environ.get("UPI_PROXY_TRY") or 4))
    for _ in range(attempts):
        cand = _claim_candidate(job.job_id, proxies, task.task_id)
        if not cand:
            break
        cached = _cached_verdict(cand)
        if cached:
            # da cham roi (batch lon dung lai proxy) -> khoi do lai 2-4s
            if _verdict_usable(cached, job.country) and _address_usable(job, task, cand):
                return cand, _task_seed_file(job, task, cand, proxies), cached
            _mark_proxy_bad(job.job_id, cand)
            _release_proxy(job.job_id, cand)
            continue
        verdict = ippure.check(cand, job.country, cli.proxy_persona_timezone(),
                               timeout=20, history=cli.proxy_history(cand))
        _cache_verdict(cand, verdict)
        if _verdict_usable(verdict, job.country):
            job.emit({"type": "log", "task_id": task.task_id, "line":
                      f"proxy {verdict.get('label') or '?'}: {verdict.get('grade')} "
                      f"{verdict.get('score')} điểm · {verdict.get('country')} · "
                      f"risk={verdict.get('risk')} · {verdict.get('user_type') or '?'} · "
                      f"{verdict.get('latency_ms')}ms"})
            if _address_usable(job, task, cand):
                return cand, _task_seed_file(job, task, cand, proxies), verdict
            _mark_proxy_bad(job.job_id, cand)
            _release_proxy(job.job_id, cand)
            continue
        _mark_proxy_bad(job.job_id, cand)
        _release_proxy(job.job_id, cand)
        job.emit({"type": "log", "task_id": task.task_id, "line":
                  f"proxy {verdict.get('label') or '?'} bị loại: {','.join(verdict.get('flags') or ['?'])}"})

    # Het con dung duoc -> sinh them 1 session moi ngay tai day (neu bat)
    gen_gate = str(os.environ.get("UPI_PROXY_GENERATE") or "1").strip().lower()
    if gen_gate not in ("0", "false", "no", "off"):
        try:
            import proxy_pool  # noqa: PLC0415
            extra, _ = proxy_pool.harvest(
                proxies[0], 1, "B", job.country, cli.proxy_persona_timezone(),
                workers=1, timeout=20, max_attempts=6,
                sess_time=int(os.environ.get("UPI_PROXY_SESS_TIME") or 30),
                log=lambda *a: None)
            if extra:
                cand = extra[0]["proxy"]
                proxies.append(cand)
                job.emit({"type": "log", "task_id": task.task_id, "line":
                          f"pool hết proxy dùng được -> sinh session mới {extra[0].get('label')} "
                          f"({extra[0].get('country')}, risk={extra[0].get('risk')})"})
                _lease_proxy(job.job_id, cand, task.task_id)
                return cand, _task_seed_file(job, task, cand, proxies), extra[0]
        except Exception:  # noqa: BLE001 — sinh lỗi thì vẫn chạy bằng pool cũ
            pass

    # Hết con "sạch": vẫn phải chạy -> dùng con kế tiếp chưa bị đánh hỏng, và vẫn ghi seed
    # RIÊNG cho task (backend chỉ nhận seed_file, không nhận chuỗi proxy).
    cand = _claim_candidate(job.job_id, proxies, task.task_id) or (proxies[0] if proxies else "")
    return cand, _task_seed_file(job, task, cand, proxies), {}


def _set_flow(job: Job, task: Task, flow: str, session: str = "", provider: str = "") -> None:
    task.flow = flow
    if session:
        task.session = session
    if provider:
        task.provider = provider
    task.set_steps(cli.steps_for(flow))
    job.emit({"type": "task_flow", "task_id": task.task_id, "flow": flow,
              "flow_label": FLOW_LABEL.get(flow, flow), "session": task.session,
              "provider": task.provider, "steps": deepcopy(task.steps)})


def _verify_link(job: Job, task: Task, result: dict, link: str) -> None:
    """Bước 8 của `upi-zero-link`: hỏi chính trang instructions xem link có thật.

    `hosted_instructions_url` **vẫn được Stripe trả về khi setup_intent bị từ chối**,
    nên "flow trả về link" không đồng nghĩa "có uỷ nhiệm ₹0". Trước đây chỗ này suy
    đoán qua số tiền trong log (`amount=INR 1999.00` -> fail) — đó là xấp xỉ. Giờ
    đọc thẳng `intent_state` + `fam` từ payload: dữ liệu gốc, không đoán.

    Chỉ đánh fail khi:
      • trang đọc được và phán đoán là KHÔNG phải uỷ nhiệm ₹0, **và** job có đặt promo
        (đặt promo nghĩa là khách yêu cầu ₹0; không đặt promo thì chuỗi thu tiền là
        kết quả hợp lệ, chỉ ghi nhãn lại để UI phân biệt);
      • hoặc trang trả 4xx (link chết thật).
    Trang không đọc được (5xx / mạng) thì giữ nguyên kết quả và ghi chú — không
    kết luận oan.
    """
    if not link or not artifact_probe.is_upi_instructions_url(link):
        return
    now = int(time.time())
    # ĐĂNG KÝ VÀO SỔ TRƯỚC KHI DÒ. Nếu chỉ ghi sau khi dò xong thì link có lần dò đầu lỗi
    # (SSLError/timeout) sẽ KHÔNG BAO GIỜ vào sổ -> monitor không bao giờ thử lại, và chip
    # đứng ở "Not verified yet" vĩnh viễn (đo thật 22/10, task t4508 job b0089de449d9: proxy
    # ghim lỗi SSLError, dò bằng 3 proxy khác đều ra `succeeded`).
    link_watch_upsert(job.job_id, task.task_id, link, _task_proxy(task),
                      state="", checked_at=now, first_seen=now)
    _ledger_save()
    probe = artifact_probe.probe(link, proxy=_task_proxy(task))
    result["intent_state"] = probe.get("intent_state")
    result["fam"] = probe.get("fam")
    result["am"] = probe.get("am")

    # Mốc gốc cho vòng dò nền: link vừa ra đời BÂY GIỜ, và trạng thái vừa đo được
    # là baseline. Nhờ vậy `age` trong log ("khách quét sau Ns") đếm từ lúc tạo link,
    # không phải từ lần dò đầu tiên của monitor (sẽ trễ tới 30s và luôn ra ~0).
    task.link_first_seen = now
    task.link_checked_at = now
    task.link_fam = str(probe.get("fam") or "")
    task.link_zero = bool(probe.get("zero_mandate"))
    task.link_note = str(probe.get("zero_label") or "")

    if not probe.get("ok"):
        err = str(probe.get("error") or "")
        if artifact_probe.is_inconclusive(err):
            note = "link unverified (%s)" % err
            task.error = ("%s | %s" % (task.error, note)) if task.error else note
            task.link_state = ""            # chưa kết luận -> monitor sẽ dò tiếp
            return
        task.raw_status = "FAIL"
        task.error = "link unusable: %s" % err
        task.link_state = "failed"
        return

    task.link_state = str(probe.get("status") or "")
    # Ghi vào SỔ BỀN VỮNG: job chỉ sống trong RAM, mà khách có thể duyệt uỷ nhiệm sau nhiều giờ.
    link_watch_upsert(job.job_id, task.task_id, link, _task_proxy(task),
                      state=task.link_state, checked_at=now, first_seen=now, flip_at=now,
                      fam=task.link_fam, zero=bool(task.link_zero), note=task.link_note)
    if task.link_state == "succeeded":
        # Mandate đã được duyệt ngay lúc task xong -> dò Plus luôn (khỏi chờ monitor).
        _start_plus_check(job, task)

    good, label = artifact_probe.judge(probe)
    result["zero_mandate"] = bool(good)
    result["link_label"] = label
    if good:
        return
    if str(job.promo or "off").strip().lower() not in ("", "off"):
        task.raw_status = "FAIL"
        task.error = ("promo '%s' but the link is not a ₹0 mandate — %s"
                      % (job.promo, label))


def _finish(job: Job, task: Task, result: dict, t0: float) -> None:
    task.duration_ms = int((time.time() - t0) * 1000)
    task.raw_status = str(result.get("status") or "UNKNOWN")
    task.error = result.get("err") or result.get("error") or None
    # Lỗi cấp ACCOUNT (Stripe risk decline) -> đánh dấu để _run_with_retries không thử lại.
    if result.get("risk_decline") or "risk decline" in (task.error or "").lower():
        task.no_retry = True
        task.no_retry_reason = "Stripe risk decline (account flagged)"
        job.risk_decline_count += 1
    elif "already paid" in (task.error or "").lower():
        # Account đã trả tiền rồi: chạy lại chỉ tốn thêm 1 flow đầy đủ rồi cũng vậy.
        task.no_retry = True
        task.no_retry_reason = "user already paid — retry costs a full flow and cannot help"
    elif _retry_stall_disabled() and "flow stopped at step 'approve'" in (task.error or ""):
        # Nhóm lỗi chung 'stopped at approve': đo 3.965 task có retry trong log còn giữ ->
        # 0 lần ra link, và 82,2% lần chạy lại lặp y hệt lỗi cũ (lỗi ở account/session,
        # không phải lỗi tạm thời). Bật lại bằng UPI_RETRY_APPROVE_STALL=1 nếu muốn thử.
        task.no_retry = True
        task.no_retry_reason = ("flow stalled at approve (measured: 0/3965 retries produced "
                                "a link, 82% repeat the same failure)")
    elif "only a hosted checkout link" in (task.error or ""):
        # Stripe chỉ trả link hosted, không mở được đường UPI ₹0 -> lỗi cấp account/session.
        task.no_retry = True
        task.no_retry_reason = "no ₹0 UPI path for this account (hosted link only)"
    elif "billing country must match" in (task.error or "").lower():
        # 4.607 account dính lỗi này -> chỉ 0,5% từng ra mã (so với ~3% chung) => cấp account.
        task.no_retry = True
        task.no_retry_reason = ("account region does not match the IN billing country "
                                "(OpenAI 400) — retry cannot fix it")
    elif ("blocked checkout creation" in (task.error or "")
          or "chặn tạo checkout" in (task.error or "")
          or "cf-mitigated=challenge" in (task.error or "")):
        # Cloudflare chặn ngay bước tạo checkout: lần chạy lại gần như chắc chắn bị chặn
        # tiếp (đo thật 12 job gần nhất: 2.209 lần thử lại sau 403 -> 0 lần ra link),
        # nên retry chỉ đốt thêm dung lượng proxy. Bỏ retry cho tới khi hết sóng 403.
        task.no_retry = True
        task.no_retry_reason = "checkout blocked HTTP 403 (Cloudflare) — retry wastes proxy"
    elif result.get("promo_not_eligible"):
        # Account không được hưởng promo 0₫ -> chạy lại cũng vậy, khỏi tốn ~2 phút nữa.
        task.no_retry = True
        task.no_retry_reason = "account not eligible for ₹0 promo"
        job.promo_not_eligible_count += 1

    # Dung lượng proxy mà acc này đã tiêu (byte) — cộng dồn nếu task chạy nhiều lần.
    task.net_out += int(result.get("net_bytes_out") or 0)
    task.net_in += int(result.get("net_bytes_in") or 0)
    task.net_requests += int(result.get("net_requests") or 0)

    link = result.get("upi_link") or ""
    _verify_link(job, task, result, link)

    ok = task.raw_status in OK_STATUS
    if link or result.get("qr_png"):
        task.artifact = {
            "upi_link": link or None,
            "qr_png": result.get("qr_png") or None,
            "qr_svg": result.get("qr_svg") or None,
            "amount_minor": result.get("amount_minor"),
            "intent": result.get("intent") or None,
            # nhãn thật của link, để UI phân biệt uỷ nhiệm ₹0 với chuỗi thu tiền
            "fam": result.get("fam") or None,
            "intent_state": result.get("intent_state") or None,
            "zero_mandate": bool(result.get("zero_mandate")),
        }
        job.emit({"type": "task_artifact", "task_id": task.task_id, **task.artifact})

    # Luồng dừng: đánh dấu BƯỚC ĐÃ CHẾT để card hiện đúng chỗ hỏng.
    # Trước đây chỉ đánh dấu bước đang 'active'; nếu lỗi tới lúc không bước nào active
    # (vd chết ở tạo checkout ngay sau khi các bước trước đã 'done'/'skip') thì KHÔNG bước
    # nào đỏ -> UI không cho biết hỏng ở đâu (93% task của job 380a86aad3a0 rơi vào ca này).
    marked = False
    for s in task.steps:
        if s["status"] == "active":
            s["status"] = "fail"
            if not s.get("err"):
                s["err"] = task.error or task.raw_status
            marked = True
    if not marked and not ok:
        for s in task.steps:
            if s["status"] == "pending":
                s["status"] = "fail"
                s["err"] = s.get("err") or (task.error or task.raw_status)
                job.emit({"type": "step", "task_id": task.task_id, "key": s["key"],
                          "status": "fail", "err": str(s["err"])[:200]})
                break

    task.status = "done" if ok else ("stopped" if task.raw_status.upper() == "STOPPED" else "fail")
    task.updated_at = _now()
    task.done_at = int(time.time())
    job.emit({"type": "task_done", "task_id": task.task_id, "ok": ok,
              "status": task.raw_status, "error": task.error,
              "duration_ms": task.duration_ms, "done_at": task.done_at,
              "net_out": task.net_out, "net_in": task.net_in,
              "net_requests": task.net_requests})
    job.emit({"type": "job_progress", "job_id": job.job_id, "done": job.done,
              "ok": job.ok, "fail": job.fail, "total": job.total})


# -------------------------------------------------------------- task running
def _run_task(job: Job, task: Task, proxy_file: Path, proxies: list[str]) -> bool:
    with job._lock:
        if job.stop_requested:
            _finish(job, task, {"status": "STOPPED", "err": "job stopped"}, time.time())
            return False
        task.status = "running"
        task.run += 1
        task.error = None
        task.updated_at = _now()
        job.emit({"type": "task_start", "task_id": task.task_id, "run": task.run,
                  "flow": task.flow})

    step, on_log, on_geo = _mk_hooks(job, task)
    # Moi worker tu lay + tu cham proxy cua minh (xem _acquire_proxy): job bat dau
    # ngay, khong con phai cho cham ca pool o POST /api/run. Proxy da verify duoc
    # ghi thanh seed rieng cua task -> ca luong CS cung dung dung IP sach do.
    proxy, seed, verdict = _acquire_proxy(job, task, proxies)
    # Ghi lai proxy cua task: link monitor (_monitor_one) chay SAU khi task xong va lease da
    # tra ve pool, nhưng vẫn phải dò trang instructions qua proxy — không thì lộ IP thật.
    task.proxy = proxy
    if job.stop_requested:
        _finish(job, task, {"status": "STOPPED", "err": "job stopped"}, time.time())
        return False
    if verdict:
        ex = {k: verdict.get(k) for k in ("ip", "country", "timezone", "risk", "verdict",
                                          "user_type", "isp", "latency_ms", "family")}
        ex["country"] = str(verdict.get("country") or "")
        _emit_egress_payload(job, task, proxy, ex, {})
    elif proxy:
        # Do IP exit/billing o thread rieng -> khong chan luong chinh,
        # UI hien IP ngay khi probe xong (thuong <2s).
        threading.Thread(target=_emit_egress, args=(job, task, proxy), daemon=True).start()
    t0 = time.time()
    try:
        if job.mode == "cs":
            _set_flow(job, task, "cs")
            r = backend.run_cs(_backend_request(job, task, proxy, seed),
                               _backend_hooks(job, task, step, on_log, on_geo))
        elif job.mode == "oaics":
            _set_flow(job, task, "oaics")
            r = backend.run_oaics(_backend_request(job, task, proxy, seed),
                                  _backend_hooks(job, task, step, on_log, on_geo))
        else:
            # auto: do provider truoc, roi chay dung luong
            d = backend.detect(_backend_request(job, task, proxy, seed),
                               _backend_hooks(job, task, step, on_log, on_geo))
            kind = d.get("kind")
            if d.get("status") == "error" and not kind:
                # khong do duoc provider (vd sentinel loi / token 401) -> khong chay
                # luong nao, va KHONG gan nhan flow de UI khong hien sai.
                r = {"email": task.email, "status": "error",
                     "err": d.get("err") or "detect failed"}
            elif kind == "oaics":
                _set_flow(job, task, "oaics", session=str(d.get("session") or ""),
                          provider=str(d.get("provider") or ""))
                r = backend.run_oaics(_backend_request(job, task, proxy, seed),
                                      _backend_hooks(job, task, step, on_log, on_geo))
            else:
                # cs_ hoac kind la (khong nhan dang duoc) -> di luong cs
                _set_flow(job, task, "cs", session=str(d.get("session") or ""),
                          provider=str(d.get("provider") or ""))
                r = backend.run_cs(_backend_request(job, task, proxy, seed),
                                   _backend_hooks(job, task, step, on_log, on_geo))
    except Exception as exc:  # noqa: BLE001
        r = {"email": task.email, "status": "error", "err": str(exc)[:200]}
    finally:
        # tra proxy lai pool de task khac dung duoc (xem _lease_proxy)
        _release_task_proxy(job.job_id, task.task_id)
    _finish(job, task, r, t0)
    return task.status == "done"


def _backend_request(job: Job, task: Task, proxy: str, seed: Path) -> backend.BackendRequest:
    return backend.BackendRequest(
        token=task.token, proxy=proxy, seed_file=seed, state_dir=job.state_dir,
        index=task.index, country=job.country, promo=job.promo,
        retry_limit=job.retries, stripe_telemetry=job.stripe_telemetry,
        dump_traffic=job.dump_traffic,
    )


def _backend_hooks(job: Job, task: Task, step, on_log, on_geo) -> backend.BackendHooks:
    return backend.BackendHooks(
        step=step, on_log=on_log, on_geo=on_geo,
        should_stop=lambda: job.stop_requested,
    )


def _cleanup_job_files(job: Job) -> None:
    """Job xong -> xoá seed tạm của TỪNG task.

    Cổng proxy theo nhu cầu ghi 1 file seed cho mỗi task (`web_seed_<job>_<task>.txt`)
    -> batch 1000 token là 1000 file nhỏ nằm lại trong cs_state. Giữ lại:
      • `web_proxies_<job>.txt` — retry_task() cần để retry task của job đã xong;
      • `web_good_<job>.txt`     — danh sách proxy đã đạt ngưỡng, hữu ích khi tra cứu.
    """
    try:
        n = 0
        for p in STATE_DIR.glob(f"web_seed_{job.job_id}_*.txt"):
            p.unlink(missing_ok=True)
            n += 1
        # GIỮ web_proxies_<job>.txt: retry_task() đọc lại nó khi bạn retry 1 task của
        # job đã xong. Chỉ seed tạm của từng task mới là rác thật (1 file/task).
        if n:
            print("[cleanup] job %s: removed %d temp seed file(s)" % (job.job_id, n),
                  flush=True)
    except OSError:
        pass


def _reset_task_for_retry(job: Job, task: Task) -> None:
    task.flow = None
    task.session = ""
    task.provider = ""
    task.steps = [{**s, "status": "pending", "detail": "", "err": ""}
                  for s in cli.steps_for(job.mode)]
    task.artifact = None
    task.error = None
    task.raw_status = ""
    task.duration_ms = None
    task.done_at = 0
    task.link_state = ""
    task.link_checked_at = task.link_first_seen = task.link_flip_at = 0
    task.link_fam = task.link_note = ""
    task.link_zero = False
    task.no_retry = False
    task.no_retry_reason = ""
    task.net_out = task.net_in = task.net_requests = 0   # đo lại cho lần chạy mới
    task.egress = None
    # Xoá hoá đơn của lần chạy trước: dữ liệu lưu lại phải là của LẦN CHẠY CUỐI,
    # nếu không thì task retry xong vẫn mang vùng/PIN của lần 1 (đọc sai vùng nào ra mã).
    task.geo_state = task.geo_city = task.geo_pin = ""
    task.billing_tier = ""
    task.billing_check = ""
    task.billing_name = task.billing_city = task.billing_pin = ""
    task.status = "pending"
    task.updated_at = _now()
    job.emit({"type": "task_init", "task_id": task.task_id, "index": task.index,
              "email": task.email, "flow": None, "steps": deepcopy(task.steps), "run": task.run})


def _retry_stall_disabled() -> bool:
    """Có bỏ retry cho nhóm lỗi chung "flow stopped at step 'approve'" không (mặc định CÓ)."""
    raw = str(os.environ.get("UPI_RETRY_APPROVE_STALL") or "0").strip().lower()
    return raw not in ("1", "true", "yes", "on")


def _is_address_error(error: str | None) -> bool:
    """Task chết vì KHÔNG dựng được địa chỉ hoá đơn (thường do IP exit không định vị được)."""
    low = str(error or "").lower()
    return "no billing address" in low or "billing address" in low


def _run_with_retries(job: Job, task: Task, proxy_file: Path,
                      proxies: list[str]) -> None:
    # `job.retries` = số lần chạy TỐI ĐA cho mỗi acc, không phải số lần thử lại.
    # Đặt 3 -> chạy lần 1, 2, 3 rồi dừng, KHÔNG bao giờ tới lần 4 (trước đây là
    # `range(retries + 1)` nên đặt 3 lại chạy 4 lần — gây nhầm khi đọc "Run 4").
    address_retry_left = 1        # xem chú thích ở dưới (lỗi địa chỉ = lỗi CHỌN PROXY)
    for attempt in range(max(1, job.retries)):
        if job.stop_requested:
            if task.status == "pending":
                _finish(job, task, {"status": "STOPPED", "err": "job stopped"}, time.time())
            break
        if attempt:
            if task.no_retry:
                # Account đã bị Stripe gắn cờ (risk decline): chạy lại chỉ tốn thêm
                # ~2 phút + 1 checkout nữa rồi cũng decline y hệt.
                job.emit({"type": "log", "task_id": task.task_id, "line":
                          "retry skipped: %s"
                          % (task.no_retry_reason or "account-level error")})
                break
            _reset_task_for_retry(job, task)
        if _run_task(job, task, proxy_file, proxies):
            break
        if address_retry_left and _is_address_error(task.error):
            # Không dựng được địa chỉ hoá đơn = lỗi CHỌN PROXY (IP exit không định vị được),
            # không phải lỗi của account — nên đổi proxy khác rồi chạy lại, chứ không bỏ task.
            # Chạy lại rất rẻ (~15-20s, chết ngay ở đầu round) nên cho thêm 1 lượt kể cả khi
            # người dùng đặt `retries = 1`.
            address_retry_left -= 1
            job.emit({"type": "log", "task_id": task.task_id, "line":
                      "no billing address from this proxy — switching to another proxy "
                      "and running once more"})
            _reset_task_for_retry(job, task)
            if _run_task(job, task, proxy_file, proxies):
                break


def _cleanup_task_files(job: Job, task: Task) -> None:
    """Xoá file tạm của task ngay khi xong.

    Vì sao: mỗi task sinh `cs_state/web_seed_<job>_<task>.txt` (và `cs_state/<job>/cs_<idx>.json`)
    mà KHÔNG ai xoá. Đo 19/10: **10.492 file web_seed_*** + 1.000 cs_*.json — chính đống file này
    làm btrfs hết chỗ metadata và khiến desk ghi results.json thất bại (`ENOSPC`) dù `df` còn 35 GB
    (xem README). File seed chỉ cần trong lúc task chạy.
    """
    for path in (STATE_DIR / f"web_seed_{job.job_id}_{task.task_id}.txt",
                 job.state_dir / f"cs_{task.index}.json"):
        try:
            path.unlink()
        except OSError:
            pass


def _run_task_limited(job: Job, task: Task, proxy_file: Path, proxies: list[str]) -> None:
    """Only the job executor calls this; queued work checks Stop on entry."""
    try:
        _run_with_retries(job, task, proxy_file, proxies)
    except Exception as exc:  # include proxy/setup errors, not just protocol errors
        _finish(job, task, {"status": "ERROR", "err": str(exc)[:200]}, time.time())
    finally:
        _release_task_proxy(job.job_id, task.task_id)
        _cleanup_task_files(job, task)
        with job._lock:
            job._pending.discard(task.task_id)
            if not job._pending:
                _complete_job(job)
            else:
                job.write_results(checkpoint=True)


def _cleanup_job_files(job: Job) -> None:
    """Dọn file tạm của cả job (seed lẻ + thư mục state) sau khi job kết thúc."""
    try:
        for path in STATE_DIR.glob(f"web_seed_{job.job_id}_*.txt"):
            try:
                path.unlink()
            except OSError:
                pass
        for path in STATE_DIR.glob(f"web_proxies_{job.job_id}*.txt"):
            try:
                path.unlink()
            except OSError:
                pass
        shutil.rmtree(job.state_dir, ignore_errors=True)
    except OSError:
        pass


def _complete_job(job: Job) -> None:
    """Caller holds job._lock; append/retry cannot race finalization."""
    _clear_job_proxy_state(job.job_id)
    _cleanup_job_files(job)
    job.duration_ms = int((time.time() - job._t0) * 1000)
    job.finished_at = _now()
    job.status = "stopped" if job.stop_requested else "done"
    job.emit({"type": "job_done", "job_id": job.job_id, "ok": job.ok,
              "fail": job.fail, "done": job.done, "total": job.total,
              "status": job.status, "duration_ms": job.duration_ms,
              "stop_reason": job.stop_reason})
    # luu ra file truoc khi dong log -> khong mat khi server restart
    job.write_results()
    _cleanup_job_files(job)
    job.close_log()
    executor, job._executor = job._executor, None
    if executor is not None:
        executor.shutdown(wait=False)


def _enqueue_tasks(job: Job, tasks: list[Task]) -> None:
    """Reserve the entire batch before any worker can complete it.

    Caller holds job._lock. One standard executor is the queue and the worker
    limit; no thread-per-append and no second semaphore/pool to coordinate.
    """
    if not tasks:
        return
    reopening = job.status != "running"
    if reopening:
        job.status = "running"
        job.stop_requested = False
        job.stop_reason = ""
        job.finished_at = None
        job.duration_ms = None
        with job._log_lock:
            if job._log_fh is None:
                try:
                    job._log_fh = job.log_path.open("a", encoding="utf-8")
                except OSError:
                    pass
    if job._executor is None:
        job._executor = ThreadPoolExecutor(max_workers=job.workers,
                                           thread_name_prefix=f"job-{job.job_id}")
    job._pending.update(t.task_id for t in tasks)
    if reopening:
        job.emit({"type": "job_start", "job_id": job.job_id, "mode": job.mode,
                  "total": job.total, "workers": job.workers, "retries": job.retries,
                  "country": job.country, "promo": job.promo, "proxy_source": job.proxy_source,
                  "started_at": job.started_at})
    for task in tasks:
        job.emit({"type": "task_init", "task_id": task.task_id, "index": task.index,
                  "email": task.email, "flow": task.flow,
                  "steps": deepcopy(task.steps), "run": task.run})
    job.write_results(checkpoint=not reopening)
    for task in tasks:
        job._executor.submit(_run_task_limited, job, task, job._proxy_file, job._proxies)


# ---------------------------------------------------------------- public API
def _require_disk_space(tasks: int) -> None:
    """Chặn chạy job khi đĩa gần đầy.

    Vì sao: 19/10 đĩa nghẽn (cs_state 360k file + logs 3,2 GB) làm desk ghi results thất bại
    suốt 35 phút mà không báo gì; khi process dừng thì mất 1.345 task kết quả. Snapshot của
    một job 5.000 acc ~ 11-24 MB, checkpoint vài giây/lần, nên cần dư địa thật.
    Đặt UPI_MIN_FREE_MB=0 để tắt kiểm tra.
    """
    need_mb = int(os.environ.get("UPI_MIN_FREE_MB") or 1024)
    if need_mb <= 0:
        return
    try:
        usage = shutil.disk_usage(str(ROOT))
    except OSError:
        return
    free_mb = usage.free / (1024 * 1024)
    if free_mb < need_mb:
        raise ValueError(
            f"only {free_mb:.0f} MB free (need >= {need_mb} MB) — run "
            f"./clean_logs.sh --apply first (old flow logs + cs_state of old jobs), then retry")


def start_job(tokens: list[str], mode: str, country: str, promo: str, workers: int,
              proxies: str | None, retries: int = 1, mix: bool = False,
              filter_proxies: bool = True, stripe_telemetry: bool = False,
              dump_traffic: bool = False) -> Job:
    tokens = [t.strip() for t in tokens if t.strip()]
    if not tokens:
        raise ValueError("no tokens")
    if MAX_TOKENS_PER_BATCH and len(tokens) > MAX_TOKENS_PER_BATCH:
        raise ValueError(f"max {MAX_TOKENS_PER_BATCH} tokens per batch")
    if not 1 <= workers <= MAX_WORKERS:
        raise ValueError(f"workers must be within 1..{MAX_WORKERS}")
    if not 0 <= retries <= 5:
        raise ValueError("retries must be within 0..5")
    if mode not in ("auto", "oaics", "cs"):
        raise ValueError("invalid mode")

    _require_disk_space(len(tokens))
    STATE_DIR.mkdir(exist_ok=True)
    job_id = secrets.token_hex(6)
    proxy_file, px, label, quality = _prepare_proxies(job_id, proxies, len(tokens), mix,
                                                      filter_proxies)
    job = Job(job_id, mode, tokens, country, promo, workers, label, retries,
              filter_proxies, stripe_telemetry, dump_traffic)
    job._proxy_file, job._proxies = proxy_file, px

    with _JOBS_LOCK:
        _JOBS[job_id] = job
        _JOB_ORDER.append(job_id)
        # Never evict live work: it must remain visible and stoppable.
        for old in list(_JOB_ORDER):
            if len(_JOB_ORDER) <= MAX_JOBS:
                break
            if _JOBS[old].status != "running":
                _JOB_ORDER.remove(old)
                _JOBS.pop(old, None)

    job.emit({"type": "job_start", "job_id": job_id, "mode": mode, "total": job.total,
              "country": country, "promo": promo, "workers": job.workers,
              "retries": job.retries,
              "proxy_source": label, "started_at": job.started_at})
    if quality:
        # UI hien duoc chat luong pool: diem/risk/user_type cua proxy tot nhat +
        # may con te nhat, de biet pool co dang dung hay khong.
        job.emit({"type": "proxy_quality", **quality})
        best = quality.get("best") or {}
        if best:
            gen = quality.get("generated") or {}
            job.emit({"type": "log", "task_id": "*", "line":
                      f"proxy quality: chấm {quality.get('scanned')} proxy trong "
                      f"{quality.get('seconds')}s | tốt nhất {best.get('grade')} "
                      f"{best.get('score')} điểm, {best.get('country')}, risk={best.get('risk')}, "
                      f"{best.get('user_type')}, {str(best.get('isp'))[:32]} "
                      f"({best.get('latency_ms')}ms)"
                      + (f" | dropped {quality['excluded']} dead/flagged proxies"
                         if quality.get("excluded") else "")
                      + (f" | thiếu {quality['short']} -> sinh bù {gen.get('verified')} proxy "
                         f"mới (session {gen.get('sess_minutes')} phút, {gen.get('attempts')} lần thử, "
                         f"{gen.get('seconds')}s)" if gen else "")
                      + (f" | sinh bù lỗi: {quality['generate_error']}"
                         if quality.get("generate_error") else "")})
        elif quality.get("mode") == "lazy":
            # mặc định: không chấm global -> mỗi worker tự chấm lấy trước khi chạy
            job.emit({"type": "log", "task_id": "*", "line":
                      "proxy quality: chấm theo từng worker (job bắt đầu ngay; mỗi worker "
                      "tự đo + tự chấm proxy của mình trước khi chạy)"})
        elif quality.get("error"):
            job.emit({"type": "log", "task_id": "*",
                      "line": f"proxy quality: bỏ qua ({quality['error']})"})

    with job._lock:
        _enqueue_tasks(job, list(job.tasks.values()))
    return job


def stop_job(job_id: str) -> bool:
    job = get_job(job_id)
    if not job:
        return False
    with job._lock:
        if job.status != "running":
            return False
        job.stop_requested = True
        if not job.stop_reason:
            job.stop_reason = "stopped on request (you pressed Stop) — remaining tasks are skipped"
        job.emit({"type": "job_stopping", "job_id": job.job_id})
        job.emit({"type": "log", "task_id": "*", "line": "== stop requested =="})
    return True


def retry_task(job_id: str, task_id: str) -> bool:
    job = get_job(job_id)
    if not job:
        return False
    with job._lock:
        task = job.tasks.get(task_id)
        if (task is None or task_id in job._pending
                or task.status in ("running", "pending")
                or (job.status == "running" and job.stop_requested)):
            return False
        if job._proxy_file is None or not job._proxy_file.is_file():
            return False
        task.logs = []
        _reset_task_for_retry(job, task)
        _enqueue_tasks(job, [task])
    return True


def append_tasks(job_id: str, tokens: list[str]) -> dict:
    """Thêm AT (token) vào queue của job đang chạy (hoặc job đã xong).

    Vì sao cần: trước đây muốn chạy thêm acc thì phải bấm Stop rồi gửi job mới —
    mất phần đang chạy, và phải chấm lại pool proxy từ đầu. Hàm này nối thẳng vào
    job hiện có: **giữ nguyên pool proxy của job**, thêm task vào cuối `order`,
    rồi đưa task vào cùng executor đang chạy.

    Bỏ qua token trùng (đã có trong job, hoặc trùng nhau trong lần thêm này) —
    chạy lại cùng một acc vừa tốn thời gian vừa tốn hạn mức proxy.
    """
    job = get_job(job_id)
    if job is None:
        return {"ok": False, "error": "job does not exist", "added": 0, "skipped": 0}

    clean = [str(t).strip() for t in (tokens or []) if str(t).strip()]
    with job._lock:
        if job.stop_requested:
            return {"ok": False, "added": 0, "skipped": 0,
                    "error": "job is stopping — cannot push more. Press Run to start a new job."}
        if job._proxy_file is None or not job._proxy_file.is_file():
            return {"ok": False, "added": 0, "error": "job proxy file is missing"}
        known = {t.token for t in job.tasks.values()}
        fresh = []
        skipped = 0
        for tok in clean:
            if tok in known:
                skipped += 1
                continue
            known.add(tok)
            fresh.append(tok)
        if not fresh:
            return {"ok": True, "added": 0, "skipped": skipped, "total": job.total}
        if MAX_TOKENS_PER_BATCH and len(job.tasks) + len(fresh) > MAX_TOKENS_PER_BATCH:
            return {"ok": False, "added": 0, "skipped": skipped,
                    "error": f"max {MAX_TOKENS_PER_BATCH} tokens per batch"}

        new_tasks: list[Task] = []
        for tok in fresh:
            idx = job._next_index
            job._next_index += 1
            tid = f"t{idx + 1}"
            t = Task(task_id=tid, index=idx, token=tok, email=cli.decode_email(tok))
            t.set_steps(cli.steps_for(job.mode))
            job.tasks[tid] = t
            job.order.append(tid)
            new_tasks.append(t)
        job.total = len(job.order)
        _enqueue_tasks(job, new_tasks)
        job.emit({"type": "log", "task_id": "*", "line":
                  f"đã thêm {len(new_tasks)} acc vào queue (bỏ qua {skipped} acc trùng) — tổng {job.total}"})
        return {"ok": True, "added": len(new_tasks), "skipped": skipped, "total": job.total}


def job_stats(job: Job) -> dict:
    return {"job_id": job.job_id, "status": job.status, "total": job.total,
            "done": job.done, "ok": job.ok, "fail": job.fail}


def sse_format(event: dict) -> str:
    return f"data: {json.dumps(event, ensure_ascii=False, separators=(',', ':'))}\n\n"
