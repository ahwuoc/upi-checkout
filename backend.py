#!/usr/bin/env python3
"""Single backend seam used by the web job runner.

The queue layer should know about task lifecycle and SSE only. Protocol details
belong here, behind one small request + hooks interface. OAICS and provider
detection keep using their existing Python implementations until their protocol
code is moved behind the same adapter.

CHẾ ĐỘ CHẠY LUỒNG CS (`UPI_WEB_CS_MODE`):
  • `subprocess` (mặc định) — mỗi task một tiến trình `extract_cs.py` (qua
    `cli.cs_subprocess_one`), cô lập bộ nhớ giữa các task.
  • `inprocess` — gọi `cs_backend.run_in_process`, chạy trong chính process web.
    Bản hiện tại dùng thread-local execution_context, không còn `_RUN_LOCK`.
  Giới hạn số task đồng thời do executor trong web/engine.py quản lý; mỗi job
  truyền một state_dir riêng cho cả hai chế độ. Mặc định backend không thay đổi.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import cli
import cs_backend


StepHook = Callable[..., None]
LogHook = Callable[[str], None]
GeoHook = Callable[[dict], None]
StopHook = Callable[[], bool]


def cs_mode() -> str:
    """`subprocess` (mặc định) hay `inprocess` — đọc mỗi lần gọi để đổi env là ăn ngay."""
    raw = str(os.environ.get("UPI_WEB_CS_MODE") or "subprocess").strip().lower()
    return raw if raw in ("subprocess", "inprocess") else "subprocess"


@dataclass(frozen=True)
class BackendRequest:
    token: str
    proxy: str
    seed_file: Path
    state_dir: Path
    index: int
    country: str
    promo: str
    retry_limit: int


@dataclass(frozen=True)
class BackendHooks:
    step: StepHook
    on_log: LogHook
    on_geo: GeoHook
    should_stop: StopHook


def detect(request: BackendRequest, hooks: BackendHooks) -> dict:
    return cli.detect_one(request.token, request.proxy, request.country,
                          step=hooks.step)


def run_oaics(request: BackendRequest, hooks: BackendHooks) -> dict:
    return cli.oaics_one(request.token, request.proxy, request.country,
                         request.promo, step=hooks.step, on_geo=hooks.on_geo)


def run_cs(request: BackendRequest, hooks: BackendHooks) -> dict:
    """Run CS: mỗi task một tiến trình riêng (mặc định) để song song thật."""
    if cs_mode() == "subprocess":
        return _run_cs_subprocess(request, hooks)
    return _run_cs_inprocess(request, hooks)


def _run_cs_subprocess(request: BackendRequest, hooks: BackendHooks) -> dict:
    """Một tiến trình `extract_cs.py` cho mỗi task.

    Song song thật vì mỗi process có state riêng (proxy_state, redaction set), nên
    không cần `_RUN_LOCK` và không rò dữ liệu giữa các account. Đổi lại tốn
    ~70–100MB RAM mỗi task đang chạy. `cs_subprocess_one` đã lo: map marker ->
    step cho UI, timeout 240s, và kill process khi bấm Stop.
    """
    hooks.step("runner", "active")
    try:
        payload = cli.cs_subprocess_one(
            request.token,
            request.seed_file,
            request.promo,
            request.state_dir,
            request.index,
            step=hooks.step,
            on_log=hooks.on_log,
            retry_limit=request.retry_limit,
            should_stop=hooks.should_stop,
        )
    except Exception as exc:  # noqa: BLE001 — giữ nguyên hợp đồng: luôn trả dict
        payload = {"email": "", "status": "ERROR", "err": str(exc)[:200]}
    hooks.step("runner", "done")
    return payload


def _run_cs_inprocess(request: BackendRequest, hooks: BackendHooks) -> dict:
    """Chạy trong process web với execution context riêng cho từng request."""
    hooks.step("runner", "active")

    def on_line(line: str) -> None:
        hooks.on_log(line)
        low = line.lower()
        if "skipping apply promotion" in low:
            hooks.step("promo", "skip", detail="checkout already ₹0")
        elif "upi confirm inline details:" in low:
            hooks.step("pm", "skip", detail="inline PM at confirm")
        hit = cli.cs_stage_for_line(line)
        if hit:
            key, completed = hit
            hooks.step(key, "done" if completed else "active")

    raw = cs_backend.run_in_process(
        request.token,
        request.seed_file,
        request.promo,
        request.state_dir,
        request.index,
        on_line=on_line,
        should_stop=hooks.should_stop,
        retry_limit=request.retry_limit,
        country=request.country,
    )
    hooks.step("runner", "done")
    payload: dict[str, Any] = dict(raw.data or {})
    if raw.stopped:
        payload = {"status": "STOPPED", "err": "job stopped", **payload}
    elif raw.timed_out:
        payload = {"status": "TIMEOUT", "err": "timeout 240s", **payload}
    elif raw.error and not payload:
        payload = {"status": "ERROR", "err": raw.error}
    return payload


__all__ = ["BackendHooks", "BackendRequest", "detect", "run_cs", "run_oaics"]
