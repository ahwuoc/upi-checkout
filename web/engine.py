#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""engine.py — chạy job trích xuất QR UPI và phát sự kiện progress cho web UI.

Không chứa logic protocol: mọi bước gọi thẳng các hàm trong `cli.py`
(detect_one / oaics_one / cs_subprocess_one) qua progress hook.

Mô hình:
    Job  -> nhiều Task (1 task = 1 access token)
    Task -> nhiều Step (key, label, status) do cli.steps_for(flow) quyết định

Sự kiện phát ra được app.py đẩy qua SSE.
"""

from __future__ import annotations

import json
import os
import queue
import secrets
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent  # upi-checkout/
sys.path.insert(0, str(ROOT))

import cli  # noqa: E402
import artifact_probe  # noqa: E402

DEFAULT_PROXY_FILE = ROOT / "proxy.txt"
STATE_DIR = ROOT / "cs_state"
# Cho phép đổi thư mục log bằng env: smoke_ui.py trỏ vào thư mục tạm để job test
# KHÔNG rơi vào logs/ thật (trước đây mỗi lần chạy test lại để lại một job 0 task,
# và trang tự attach vào cái mới nhất -> mở lên thấy bảng trống).
_LOG_DIR_ENV = os.environ.get("UPI_WEB_LOG_DIR", "").strip()
LOG_DIR = Path(_LOG_DIR_ENV) if _LOG_DIR_ENV else ROOT / "logs"
MAX_JOBS = 20
MAX_LOGS_PER_TASK = 200
# Trần số access token mỗi batch. 1000 task × ~200s / 20 worker ≈ 3 tiếng;
# snapshot bị cắt log theo `Job._log_tail()` để payload không phình theo.
MAX_TOKENS_PER_BATCH = 1000
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
            "logs": self.logs[-max(0, log_tail):] if log_tail else [],
        }


class Job:
    def __init__(self, job_id: str, mode: str, tokens: list[str], country: str,
                 promo: str, workers: int, proxy_source: str, retries: int = 1) -> None:
        self.job_id = job_id
        self.mode = mode
        self.country = country
        self.promo = promo
        self.workers = workers
        self.retries = retries
        self.proxy_source = proxy_source
        self.total = len(tokens)
        self.tasks: dict[str, Task] = {}
        self.order: list[str] = []
        # cổng proxy theo nhu cầu (bật bằng configure_proxy_gate khi min_score > 0)
        self.min_score = 0
        self.lazy_gate = False
        self._proxy_raw: list[str] = []
        self._proxy_cursor = 0
        self._good_cursor = 0
        self._proxy_score: dict[str, Any] = {}     # proxy -> kết quả score (None = đang đo)
        self._proxy_good: list[str] = []
        self._tested = 0
        self._good_path: Path | None = None
        self._proxy_lock = threading.Lock()
        self.status = "running"           # running | done | stopped
        self.stop_requested = False
        self.started_at = _now()
        self.finished_at: str | None = None
        self.duration_ms: int | None = None
        self._t0 = time.time()
        self._lock = threading.Lock()
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
        if self._log_fh is None:
            return
        with self._log_lock:
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

    def write_results(self) -> None:
        """Ghi snapshot cuoi cung ra file JSON (doc lap voi RAM)."""
        try:
            self.results_path.write_text(
                json.dumps(self.snapshot(), ensure_ascii=False, indent=2),
                encoding="utf-8")
        except OSError:
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
            subs = list(self._subs)
        for q in subs:
            try:
                q.put_nowait(event)
            except queue.Full:
                # client qua cham -> bo event cu nhat de giu tien do moi nhat
                try:
                    q.get_nowait()
                    q.put_nowait(event)
                except (queue.Empty, queue.Full):
                    pass

    # -- thong ke ----------------------------------------------------------
    @property
    def done(self) -> int:
        return sum(1 for t in self.tasks.values() if t.status in ("done", "fail", "stopped"))

    @property
    def ok(self) -> int:
        return sum(1 for t in self.tasks.values() if t.status == "done" and t.raw_status in OK_STATUS)

    @property
    def fail(self) -> int:
        return sum(1 for t in self.tasks.values() if t.status == "fail")

    # ---------------------- cổng proxy theo nhu cầu ----------------------
    def configure_proxy_gate(self, proxies: list[str], min_score: int) -> None:
        """Bật chế độ 'worker tự đo proxy' cho job này."""
        self._proxy_raw = list(proxies)
        self.min_score = int(min_score)
        self.lazy_gate = bool(self._proxy_raw and self.min_score > 0)
        self._good_path = STATE_DIR / f"web_good_{self.job_id}.txt"
        if self.lazy_gate:
            self._good_path.write_text("", encoding="utf-8")

    def _claim_next_proxy(self) -> str | None:
        """Giữ chỗ proxy kế tiếp chưa đo (gọi trong lock). None = hết pool."""
        while self._proxy_cursor < len(self._proxy_raw):
            cand = self._proxy_raw[self._proxy_cursor]
            self._proxy_cursor += 1
            if cand in self._proxy_score:
                continue
            self._proxy_score[cand] = None      # đang đo
            return cand
        return None

    def acquire_proxy(self, task: Task) -> tuple[str | None, Path | None]:
        """Proxy cho MỘT task, đo theo nhu cầu.

        Ưu tiên proxy tốt chưa ai dùng; hết thì tự đo proxy kế tiếp trong pool và
        dùng luôn nếu đạt ngưỡng. Pool cạn mà không có proxy nào đạt -> (None, None).
        """
        if not self.lazy_gate:
            return None, None
        while True:
            pick = None
            with self._proxy_lock:
                if self._good_cursor < len(self._proxy_good):
                    pick = self._proxy_good[self._good_cursor]
                    self._good_cursor += 1
                elif self._proxy_cursor >= len(self._proxy_raw):
                    if self._proxy_good:        # đo hết pool -> xoay vòng proxy tốt
                        pick = self._proxy_good[self._good_cursor % len(self._proxy_good)]
                        self._good_cursor += 1
                    else:
                        return None, None
                else:
                    cand = self._claim_next_proxy()
                    if cand is None:
                        continue
            if pick is not None:
                return pick, self._seed_file(task, pick)

            # Đo NGOÀI lock: giữ lock trong lúc gọi mạng là bóp nghẹt mọi worker.
            res = cli.score_proxy(cand, self.country or "IN", timeout=LAZY_SCORE_TIMEOUT)
            score = int(res.get("score") or 0)
            keep = bool(res.get("ok")) and score >= self.min_score
            with self._proxy_lock:
                self._proxy_score[cand] = res
                self._tested += 1
                if keep:
                    self._proxy_good.append(cand)
                    try:
                        with self._good_path.open("a", encoding="utf-8") as fh:
                            fh.write(cand + "\n")
                    except OSError:
                        pass
                tested, nkeep = self._tested, len(self._proxy_good)
                total = len(self._proxy_raw)
            if keep:
                self.emit({"type": "log", "task_id": "*", "line":
                           "proxy gate: keeper #%d score=%d (tested %d/%d)"
                           % (nkeep, score, tested, total)})
            elif tested % LAZY_LOG_EVERY == 0:
                self.emit({"type": "log", "task_id": "*", "line":
                           "proxy gate: tested %d/%d, keepers %d (last score=%d)"
                           % (tested, total, nkeep, score)})

    def gate_error(self) -> str:
        """Lý do không lấy được proxy nào cho task (để UI hiện đúng, không đoán)."""
        return ("no proxy scored ≥ %d (tested %d/%d, keepers %d). "
                "Lower the threshold or change the pool."
                % (self.min_score, self._tested, len(self._proxy_raw),
                   len(self._proxy_good)))

    def _seed_file(self, task: Task, chosen: str) -> Path:
        """File seed cho 1 task: proxy của task lên đầu, kèm vài proxy tốt dự phòng
        (để retry bên trong extract_cs vẫn có cái khác mà đổi)."""
        with self._proxy_lock:
            others = [p for p in self._proxy_good if p != chosen][:SEED_FALLBACKS]
        path = STATE_DIR / f"web_seed_{self.job_id}_{task.task_id}.txt"
        path.write_text("\n".join([chosen] + others) + "\n", encoding="utf-8")
        return path

    def snapshot(self) -> dict:
        return {
            "job_id": self.job_id,
            "mode": self.mode,
            "country": self.country,
            "promo": self.promo,
            "workers": self.workers,
            "retries": self.retries,
            "total": self.total,
            "done": self.done,
            "ok": self.ok,
            "fail": self.fail,
            "status": self.status,
            "stop_requested": self.stop_requested,
            "proxy_source": self.proxy_source,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_ms": self.duration_ms,
            "log_path": str(self.log_path),
            "results_path": str(self.results_path),
            "tasks": [self.tasks[tid].to_dict(log_tail=self._log_tail())
                      for tid in self.order],
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
_POOLS: dict[str, ThreadPoolExecutor] = {}


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
MONITOR_MAX_IDLE_MIN = 30.0     # job xong lâu hơn mức này thì thôi không dò nữa

# ---- cổng proxy theo nhu cầu (lazy gate) -------------------------------------
# Trước đây `min_score > 0` quét + chấm điểm CẢ pool ngay trong POST /api/run rồi
# mới tạo job: 500 proxy / 12 luồng / timeout 15s = 2-3 phút đứng chờ, chưa có job,
# mà đó lại là 12 luồng trong khi job được set tới 40-64 worker.
# Giờ mỗi worker tự đo proxy khi nó cần: lấy proxy tốt chưa ai dùng, hết thì đo
# tiếp proxy kế tiếp trong pool. Job chạy ngay, và việc đo nằm trong chính worker.
LAZY_SCORE_TIMEOUT = int(os.environ.get("UPI_LAZY_SCORE_TIMEOUT") or 8)
SEED_FALLBACKS = int(os.environ.get("UPI_SEED_FALLBACKS") or 7)   # proxy dự phòng ghi kèm seed
LAZY_LOG_EVERY = 25            # log tiến độ đo mỗi N proxy (đo đủ nhanh thì khỏi spam)


def _link_monitor_targets() -> list[tuple[Job, Task]]:
    """Task cần dò: có link QR, chưa ở trạng thái cuối, job còn 'nóng'."""
    cutoff = time.time() - MONITOR_MAX_IDLE_MIN * 60
    out: list[tuple[Job, Task]] = []
    with _JOBS_LOCK:
        jobs = list(_JOBS.values())
    for job in jobs:
        if job.finished_at:
            try:
                end = time.mktime(time.strptime(job.finished_at, "%Y-%m-%d %H:%M:%S"))
            except ValueError:
                end = time.time()
            if end < cutoff:
                continue
        with job._lock:
            tasks = list(job.tasks.values())
        for task in tasks:
            link = (task.artifact or {}).get("upi_link") or ""
            if not link or not artifact_probe.is_upi_instructions_url(link):
                continue
            if task.link_state in LINK_TERMINAL:
                continue
            out.append((job, task))
    return out


def _monitor_one(job: Job, task: Task) -> None:
    link = (task.artifact or {}).get("upi_link") or ""
    probe = artifact_probe.probe(link, fresh=True)
    now = int(time.time())
    if not task.link_first_seen:
        task.link_first_seen = now
    task.link_checked_at = now

    if not probe.get("ok"):
        err = str(probe.get("error") or "")
        if artifact_probe.is_inconclusive(err):
            return                      # chưa đọc được -> giữ nguyên trạng thái cũ
        new_state, fam, zero, note = "failed", task.link_fam, False, "link unreadable: %s" % err
    else:
        new_state = str(probe.get("status") or "")
        fam = str(probe.get("fam") or "")
        zero = bool(probe.get("zero_mandate"))
        note = str(probe.get("zero_label") or "")

    old = task.link_state
    task.link_fam = fam or task.link_fam
    task.link_zero = zero
    task.link_note = note
    if new_state == old:
        return

    task.link_state = new_state
    task.link_flip_at = now
    age = now - (task.link_first_seen or now)
    label = {
        "succeeded": "CUSTOMER SCANNED — mandate succeeded",
        "failed": "link dead (Stripe declined / QR expired)",
        "canceled": "link cancelled",
        "expired": "link expired, never scanned",
    }.get(new_state, "state=%s" % (new_state or "?"))
    job.log_line("[%s] link: %s -> %s sau %ds (%s; zero=%s fam=%s)"
                 % (task.task_id, old or "unknown", new_state or "?", age,
                    label, zero, fam or "?"))
    job.emit({"type": "task_link", "task_id": task.task_id, "link_state": new_state,
              "link_checked_at": now, "link_flip_at": now, "link_fam": task.link_fam,
              "link_zero": zero, "link_note": note, "age": age, "probe": probe})


def _link_monitor_loop() -> None:
    while True:
        try:
            time.sleep(MONITOR_SECS)
            targets = _link_monitor_targets()
            if not targets:
                continue
            with ThreadPoolExecutor(max_workers=max(1, MONITOR_CONC)) as ex:
                list(ex.map(lambda jt: _monitor_one(*jt), targets))
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
                      if scope == "all" or _bucket({
                          "status": task.status, "raw_status": getattr(task, "raw_status", ""),
                      }) == scope]
            for tid in doomed:
                job.tasks.pop(tid, None)
                if tid in job.order:
                    job.order.remove(tid)
            removed = len(doomed)
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
    snapshot["done"] = sum(1 for t in kept if _bucket(t) in ("success", "fail"))
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
            if job.status == "running" and task.status in ("running", "pending"):
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


def get_saved_snapshot(job_id: str) -> dict | None:
    if len(job_id) != 12 or any(c not in "0123456789abcdef" for c in job_id.lower()):
        return None
    path = LOG_DIR / f"web_{job_id}_results.json"
    try:
        return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else None
    except (OSError, json.JSONDecodeError):
        return None


def list_jobs() -> list[dict]:
    out = []
    seen = set()
    for jid in reversed(_JOB_ORDER):
        j = _JOBS.get(jid)
        if j:
            out.append({"job_id": j.job_id, "status": j.status, "total": j.total,
                        "done": j.done, "ok": j.ok, "fail": j.fail,
                        "started_at": j.started_at})
            seen.add(jid)
    try:
        result_files = sorted(LOG_DIR.glob("web_*_results.json"),
                              key=lambda p: p.stat().st_mtime, reverse=True)
    except OSError:
        result_files = []
    for path in result_files:
        snapshot = get_saved_snapshot(path.name[4:-13])
        if not snapshot or snapshot.get("job_id") in seen:
            continue
        out.append({key: snapshot.get(key) for key in
                    ("job_id", "status", "total", "done", "ok", "fail", "started_at")})
        seen.add(snapshot.get("job_id"))
        if len(out) >= MAX_JOBS:
            break
    return out[:MAX_JOBS]


# ------------------------------------------------------------- proxy helpers
def parse_proxy_lines(text: str) -> list[str]:
    """Chuẩn hoá pool proxy về URL form `http://user:pass@host:port`.

    QUAN TRỌNG: extract_cs.py (chạy trong subprocess luồng cs_) đọc file seed và
    cần username nằm sau `@` để rewrite `region-XX`. Dạng compact
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


def _prepare_proxies(job_id: str, proxies: str | None,
                     min_score: int = 0) -> tuple[Path, list[str], str, bool]:
    """Tra ve (proxy_file de truyen cho cs_runner, danh sach proxy, nhan hien thi).

    `min_score` > 0: do + cham diem ca pool truoc, chi giu proxy dat nguong
    (proxy sach / risk thap) roi moi chay.
    """
    if proxies and proxies.strip():
        px = parse_proxy_lines(proxies)
        label = f"custom proxy pool ({len(px)} proxies)"
    else:
        px = cli.load_proxies(DEFAULT_PROXY_FILE)
        label = f"{DEFAULT_PROXY_FILE.name} ({len(px)} proxy)"

    # min_score > 0: KHÔNG quét trước nữa — để từng worker tự đo proxy khi cần
    # (xem Job.acquire_proxy). Quét cả pool ở đây làm POST /api/run treo 2-3 phút
    # trong khi chưa có job nào để xem.
    needs_gate = bool(min_score > 0 and px)
    if needs_gate:
        label = f"{label} · lazy gate ≥{min_score}"

    path = STATE_DIR / f"web_proxies_{job_id}.txt"
    # Ghi URL form (khong phai nguyen van textarea) — xem parse_proxy_lines()
    path.write_text("\n".join(px) + "\n", encoding="utf-8")
    return path, px or [""], label, needs_gate


# -------------------------------------------------------------- step helpers
def _mk_hooks(job: Job, task: Task):
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

    def on_log(line: str) -> None:
        if len(task.logs) < MAX_LOGS_PER_TASK:
            task.logs.append(line[:500])
        job.emit({"type": "log", "task_id": task.task_id, "line": line[:500]})

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
    eg = {
        "proxy": proxy,
        "exit": ex,
        "billing": bl,
        "billing_country": info.get("billing_country") or "",
        "probed_at": _now(),
        "same_ip": bool(ex.get("ip")) and ex.get("ip") == bl.get("ip"),
    }
    task.egress = eg
    task.updated_at = _now()
    job.emit({"type": "egress", "task_id": task.task_id, **eg})


def _set_flow(job: Job, task: Task, flow: str, session: str = "", provider: str = "") -> None:
    task.flow = flow
    if session:
        task.session = session
    if provider:
        task.provider = provider
    task.set_steps(cli.steps_for(flow))
    job.emit({"type": "task_flow", "task_id": task.task_id, "flow": flow,
              "flow_label": FLOW_LABEL.get(flow, flow), "session": task.session,
              "provider": task.provider, "steps": task.steps})


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
    probe = artifact_probe.probe(link)
    result["intent_state"] = probe.get("intent_state")
    result["fam"] = probe.get("fam")
    result["am"] = probe.get("am")

    # Mốc gốc cho vòng dò nền: link vừa ra đời BÂY GIỜ, và trạng thái vừa đo được
    # là baseline. Nhờ vậy `age` trong log ("khách quét sau Ns") đếm từ lúc tạo link,
    # không phải từ lần dò đầu tiên của monitor (sẽ trễ tới 30s và luôn ra ~0).
    now = int(time.time())
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
    if result.get("risk_decline"):
        task.no_retry = True
        task.no_retry_reason = "Stripe risk decline (account flagged)"

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

    # neu luong dung som: buoc dang active nhung khong co loi -> danh dau fail
    for s in task.steps:
        if s["status"] == "active":
            s["status"] = "fail"
            if not s.get("err"):
                s["err"] = task.error or task.raw_status

    task.status = "done" if ok else "fail"
    task.updated_at = _now()
    task.done_at = int(time.time())
    job.emit({"type": "task_done", "task_id": task.task_id, "ok": ok,
              "status": task.raw_status, "error": task.error,
              "duration_ms": task.duration_ms, "done_at": task.done_at})
    job.emit({"type": "job_progress", "job_id": job.job_id, "done": job.done,
              "ok": job.ok, "fail": job.fail, "total": job.total})


# -------------------------------------------------------------- task running
def _run_task(job: Job, task: Task, proxy_file: Path, proxies: list[str]) -> bool:
    task.status = "running"
    task.run += 1
    task.error = None
    task.updated_at = _now()
    job.emit({"type": "task_start", "task_id": task.task_id, "run": task.run,
              "flow": task.flow})

    step, on_log, on_geo = _mk_hooks(job, task)
    proxy = proxies[task.index % len(proxies)] if proxies else ""
    seed = proxy_file
    if job.lazy_gate:
        # Worker tự lấy proxy đạt ngưỡng cho task này (đo ngay lúc cần, không chờ
        # quét cả pool). Không có cái nào đạt -> fail thẳng, nói rõ đã đo bao nhiêu.
        picked, seed = job.acquire_proxy(task)
        if not picked:
            _finish(job, task, {"email": task.email, "status": "error",
                                "err": job.gate_error()}, time.time())
            return False
        proxy = picked
    # Do IP exit/billing o thread rieng -> khong chan luong chinh,
    # UI hien IP ngay khi probe xong (thuong <2s).
    if proxy:
        threading.Thread(target=_emit_egress, args=(job, task, proxy), daemon=True).start()
    t0 = time.time()
    try:
        if job.mode == "cs":
            _set_flow(job, task, "cs")
            r = cli.cs_subprocess_one(task.token, seed, job.promo, STATE_DIR,
                                      task.index, step=step, on_log=on_log,
                                      retry_limit=job.retries,
                                      should_stop=lambda: job.stop_requested)
        elif job.mode == "oaics":
            _set_flow(job, task, "oaics")
            r = cli.oaics_one(task.token, proxy, job.country, job.promo,
                              step=step, on_geo=on_geo)
        else:
            # auto: do provider truoc, roi chay dung luong
            d = cli.detect_one(task.token, proxy, job.country, step=step)
            kind = d.get("kind")
            if d.get("status") == "error" and not kind:
                # khong do duoc provider (vd sentinel loi / token 401) -> khong chay
                # luong nao, va KHONG gan nhan flow de UI khong hien sai.
                r = {"email": task.email, "status": "error",
                     "err": d.get("err") or "detect failed"}
            elif kind == "oaics":
                _set_flow(job, task, "oaics", session=str(d.get("session") or ""),
                          provider=str(d.get("provider") or ""))
                r = cli.oaics_one(task.token, proxy, job.country, job.promo,
                                  step=step, on_geo=on_geo)
            else:
                # cs_ hoac kind la (khong nhan dang duoc) -> di luong cs
                _set_flow(job, task, "cs", session=str(d.get("session") or ""),
                          provider=str(d.get("provider") or ""))
                r = cli.cs_subprocess_one(task.token, seed, job.promo, STATE_DIR,
                                          task.index, step=step, on_log=on_log,
                                          retry_limit=job.retries,
                                          should_stop=lambda: job.stop_requested)
    except Exception as exc:  # noqa: BLE001
        r = {"email": task.email, "status": "error", "err": str(exc)[:200]}
    _finish(job, task, r, t0)
    return task.status == "done"


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
    task.status = "pending"
    task.updated_at = _now()
    job.emit({"type": "task_init", "task_id": task.task_id, "index": task.index,
              "email": task.email, "flow": None, "steps": task.steps, "run": task.run})


def _run_with_retries(job: Job, task: Task, proxy_file: Path,
                      proxies: list[str]) -> None:
    for retry in range(job.retries + 1):
        if retry:
            if job.stop_requested:
                break
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


def _job_worker(job: Job, proxy_file: Path, proxies: list[str]) -> None:
    for tid in job.order:
        job.emit({"type": "task_init", "task_id": tid, "index": job.tasks[tid].index,
                  "email": job.tasks[tid].email, "flow": job.tasks[tid].flow,
                  "steps": job.tasks[tid].steps, "run": 0})

    def work(tid: str) -> None:
        if job.stop_requested:
            t = job.tasks[tid]
            t.status = "stopped"
            job.emit({"type": "task_done", "task_id": tid, "ok": False,
                      "status": "STOPPED", "error": "job stopped", "duration_ms": 0})
            return
        _run_with_retries(job, job.tasks[tid], proxy_file, proxies)

    with ThreadPoolExecutor(max_workers=max(1, min(job.workers, job.total))) as ex:
        list(ex.map(work, job.order))

    job.duration_ms = int((time.time() - job._t0) * 1000)
    job.finished_at = _now()
    job.status = "stopped" if job.stop_requested else "done"
    job.emit({"type": "job_done", "job_id": job.job_id, "ok": job.ok,
              "fail": job.fail, "done": job.done, "total": job.total,
              "status": job.status, "duration_ms": job.duration_ms})
    # luu ra file truoc khi dong log -> khong mat khi server restart
    job.write_results()
    _cleanup_job_files(job)
    job.close_log()
    with _JOBS_LOCK:
        _POOLS.pop(job.job_id, None)


# ---------------------------------------------------------------- public API
def start_job(tokens: list[str], mode: str, country: str, promo: str, workers: int,
              proxies: str | None, min_score: int = 0, retries: int = 1) -> Job:
    tokens = [t.strip() for t in tokens if t.strip()]
    if not tokens:
        raise ValueError("no tokens")
    if len(tokens) > MAX_TOKENS_PER_BATCH:
        raise ValueError(f"max {MAX_TOKENS_PER_BATCH} tokens per batch")

    STATE_DIR.mkdir(exist_ok=True)
    job_id = secrets.token_hex(6)
    proxy_file, px, label, needs_gate = _prepare_proxies(job_id, proxies, min_score)
    job = Job(job_id, mode, tokens, country, promo, workers, label, retries)
    if needs_gate:
        job.configure_proxy_gate(px, min_score)

    with _JOBS_LOCK:
        _JOBS[job_id] = job
        _JOB_ORDER.append(job_id)
        while len(_JOB_ORDER) > MAX_JOBS:
            old = _JOB_ORDER.pop(0)
            _JOBS.pop(old, None)

    if job.lazy_gate:
        job.emit({"type": "log", "task_id": "*", "line":
                  "proxy gate ≥%d: each worker scores its own proxy on demand "
                  "(pool %d, timeout %ds)"
                  % (job.min_score, len(job._proxy_raw), LAZY_SCORE_TIMEOUT)})
    job.emit({"type": "job_start", "job_id": job_id, "mode": mode, "total": job.total,
              "country": country, "promo": promo, "workers": job.workers,
              "retries": job.retries,
              "proxy_source": label, "started_at": job.started_at})

    th = threading.Thread(target=_job_worker, args=(job, proxy_file, px), daemon=True)
    th.start()
    return job


def stop_job(job_id: str) -> bool:
    job = get_job(job_id)
    if not job or job.status != "running":
        return False
    job.stop_requested = True
    job.emit({"type": "job_stopping", "job_id": job.job_id})
    job.emit({"type": "log", "task_id": "*", "line": "== stop requested =="})
    return True


def retry_task(job_id: str, task_id: str) -> bool:
    job = get_job(job_id)
    if not job or task_id not in job.tasks:
        return False
    task = job.tasks[task_id]
    if task.status == "running":
        return False

    proxy_file = DEFAULT_PROXY_FILE
    if job.proxy_source.startswith("custom proxy pool"):
        p = STATE_DIR / f"web_proxies_{job_id}.txt"
        if not p.exists():
            # Thiếu file proxy của job -> KHÔNG retry bằng pool mặc định: task sẽ chạy
            # bằng proxy khác hẳn pool bạn chọn mà không ai biết. Thà báo không retry được.
            return False
        proxy_file = p
    proxies = cli.load_proxies(proxy_file)

    task.logs = []
    _reset_task_for_retry(job, task)
    threading.Thread(target=_run_with_retries, args=(job, task, proxy_file, proxies),
                     daemon=True).start()
    return True


def job_stats(job: Job) -> dict:
    return {"job_id": job.job_id, "status": job.status, "total": job.total,
            "done": job.done, "ok": job.ok, "fail": job.fail}


def sse_format(event: dict) -> str:
    return f"data: {json.dumps(event, ensure_ascii=False, separators=(',', ':'))}\n\n"
