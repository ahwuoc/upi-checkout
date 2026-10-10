#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""In-process CS backend.

The web layer used to start ``extract_cs.py`` as a child process for every
account.  That made the protocol implementation hard to compose and made the
stop signal depend on killing a process.  This module provides the small
backend boundary the callers need instead:

* configure one request with an explicit token, proxy seed file and retry
  limit;
* stream the exact protocol log lines produced by :mod:`extract_cs` to a caller;
* propagate a cooperative stop event into the CS flow; and
* keep token, retry and country settings inside an execution context owned by
  the request.

The protocol implementation remains in ``extract_cs.py``. This adapter does
not alter requests, approval handling, response parsing, or retry policy.
The execution context stores request configuration and output hooks per thread.
Callers must provide distinct state directories for independent jobs because
task indices alone are only unique within a job.
"""

from __future__ import annotations

import io
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

LineCallback = Callable[[str], None]
StopCallback = Callable[[], bool]

@dataclass
class BackendResult:
    """Raw result from one CS flow.

    ``lines`` contains the same redacted protocol stream that the old runner
    exposed. Keeping this boundary raw means the existing marker parser, retry
    classification and UI step hooks can be reused unchanged by the caller.
    """

    exit_code: int | None = None
    lines: list[str] = field(default_factory=list)
    stopped: bool = False
    timed_out: bool = False
    error: str = ""
    data: dict = field(default_factory=dict)

class _LineWriter(io.TextIOBase):
    """Text stream used for legacy marker prints while the flow is embedded."""

    def __init__(self, lines: list[str], callback: LineCallback | None) -> None:
        super().__init__()
        self._lines = lines
        self._callback = callback
        self._pending = ""

    def writable(self) -> bool:  # pragma: no cover - trivial protocol method
        return True

    def write(self, text: str) -> int:
        if not text:
            return 0
        self._pending += str(text)
        while "\n" in self._pending:
            line, self._pending = self._pending.split("\n", 1)
            self._publish(line)
        return len(text)

    def flush(self) -> None:
        if self._pending:
            self._publish(self._pending)
            self._pending = ""

    def _publish(self, line: str) -> None:
        clean = line.rstrip("\r")
        self._lines.append(clean)
        if self._callback is not None:
            self._callback(clean)

# Configuration and output hooks are installed in execution_context below.

def _env_values(token: str, proxy_path: Path, state_file: Path, promo: str,
                retry_limit: int | None, country: str) -> dict[str, str]:
    """Return settings previously supplied to the child runner."""

    values = {
        "PYTHONUNBUFFERED": "1",
        "UPI_TOKEN": token,
        "UPI_PROXY_SEED_FILE": str(proxy_path.resolve()),
        "UPI_PROXY_STATE_FILE": str(state_file.resolve()),
        "UPI_PROXY_REMOVE_FAILED": "0",
        "UPI_REQUIRE_ZERO": "0",
        "PP_PROMO_MODE": promo,
        "UPI_CHECKOUT_COUNTRY": country,
        "UPI_BILLING_COUNTRY": country,
        "UPI_PROVIDER_COUNTRY": country,
        "UPI_PROMOTION_COUNTRY": country,
        "UPI_MAX_RETRY": "1",
        "UPI_CHECKOUT_RETRY_MAX": "1",
        "UPI_PROVIDER_RETRY_MAX": "1",
        "UPI_APPROVE_RETRY_MAX": "1",
        "UPI_MAX_APPROVE_BLOCKED": "1",
    }
    if retry_limit is not None:
        limit = str(max(1, min(5, int(retry_limit))))
        values.update({
            "UPI_MAX_RETRY": limit,
            "UPI_CHECKOUT_RETRY_MAX": limit,
            "UPI_PROVIDER_RETRY_MAX": limit,
            "UPI_APPROVE_RETRY_MAX": limit,
            "UPI_MAX_APPROVE_BLOCKED": limit,
        })
    return values

def _reset_extract_state(module: object, state_file: Path) -> None:
    """Reset request-scoped globals before invoking an imported extract module."""

    # The attributes are intentionally accessed defensively so this adapter can
    # work with a future extract module that no longer needs these globals.
    lock = getattr(module, "_proxy_state_lock", None)
    if lock is None:
        setattr(module, "_proxy_state", None)
        return
    with lock:
        setattr(module, "_proxy_state", None)

def run_in_process(
    token: str,
    proxy_path: Path,
    promo: str,
    state_dir: Path,
    idx: int,
    *,
    on_line: LineCallback | None = None,
    should_stop: StopCallback | None = None,
    retry_limit: int | None = None,
    country: str = "IN",
    timeout: float = 240.0,
) -> BackendResult:
    """Run one CS flow in this interpreter.

    ``extract_cs`` is executed on a worker thread so the caller can observe the
    stop callback while network I/O is in progress.  Cancellation is
    cooperative: once ``should_stop`` returns true, the event is passed to the
    already-supported checks in ``run_single_link_mode`` and
    ``run_provider_flow``.  The adapter waits for the worker to unwind instead
    of abandoning a thread that could still hold credentials or proxy state.
    """

    result = BackendResult()
    lines: list[str] = []
    stop_event = threading.Event()
    started = time.monotonic()
    worker_error: list[BaseException] = []

    # Lazy import prevents cli -> cs_backend -> extract_cs import cycles at
    # startup and keeps the CLI's OAICS-only path lightweight.
    import extract_cs

    state_dir.mkdir(parents=True, exist_ok=True)
    state_file = state_dir / f"cs_{idx}.json"
    # Keep the same state shape and file location used by the legacy CLI path.
    state_file.write_text(extract_cs.STATE_TMPL if hasattr(extract_cs, "STATE_TMPL")
                          else '{"seed": {}, "checkout": {}, "promotion": {}, "provider": {}, "pair": {}}',
                          encoding="utf-8")
    values = _env_values(token, proxy_path, state_file, promo, retry_limit, country)
    writer = _LineWriter(lines, on_line)

    def on_result(values: dict) -> None:
        result.data.update(values)

    def worker() -> None:
        try:
            _reset_extract_state(extract_cs, state_file)
            access_token, session_token = extract_cs.normalize_token(token)
            if not access_token:
                result.exit_code = 1
                extract_cs.log("access_token is empty", "[ERROR] ")
                return
            proxy_seeds = extract_cs.load_proxy_file(proxy_path)
            # `execution_context` keeps token/config/log state request-local.
            # Các marker trước đây in bằng print() đã chuyển sang emit_output(),
            # nên KHÔNG cần redirect_stdout (vốn là global của cả process) nữa —
            # nhờ vậy nhiều task CS chạy song song trong cùng 1 interpreter được.
            with extract_cs.execution_context(
                    values, on_output=lambda line: writer.write(line + "\n"),
                    on_result=on_result, should_stop=should_stop, timeout=timeout):
                result.exit_code = extract_cs.run_single_link_mode(
                    access_token,
                    session_token,
                    proxy_seeds,
                    stop_event=stop_event,
                )
        except BaseException as exc:  # report to the caller; never leak a thread
            worker_error.append(exc)
            result.error = str(exc)[:300]
            result.exit_code = 1
        finally:
            writer.flush()

    # Mỗi task CS chạy trên một worker thread riêng; state của từng task nằm trong
    # execution_context (thread-local) nên không cần khoá toàn cục nữa.
    thread = threading.Thread(target=worker, name=f"cs-backend-{idx}", daemon=False)
    thread.start()
    while thread.is_alive():
        if should_stop is not None and should_stop():
            result.stopped = True
            stop_event.set()
        if timeout > 0 and time.monotonic() - started >= timeout:
            result.timed_out = True
            stop_event.set()
        time.sleep(0.1)
    thread.join()

    result.lines = lines
    if worker_error and not result.error:
        result.error = str(worker_error[0])[:300]
    try:
        import cli
        if result.stopped:
            result.data = {"status": "STOPPED", "err": "job stopped"}
        elif result.timed_out:
            result.data = {"status": "TIMEOUT", "err": "timeout 240s"}
        else:
            stage = "artifact"
            for key, _ in cli.STEPS_CS:
                if any(key in line.lower() for line in lines):
                    stage = key
            approve_ok = any("approve ok" in line.lower() for line in lines)
            status, err, link, has_qr = cli.classify_cs_outcome(
                lines, stage, result.error or None, approve_ok, promo)
            fallback = {
                "status": status,
                "err": err,
                "upi_link": link if status == "LINK" else None,
                "qr_png": cli.extract_marker_url(lines, cli.QR_PNG_MARKER) if status == "LINK" else None,
                "qr_svg": cli.extract_marker_url(lines, cli.QR_SVG_MARKER) if status == "LINK" else None,
                "approve_ok": approve_ok,
                "risk_decline": cli.is_risk_decline(lines),
                "promo_not_eligible": "promo_not_eligible" in (err or "")
                    or any("promo_not_eligible" in line for line in lines),
                "amount_minor": cli.extract_amount_minor(lines),
            }
            # A successful in-process run reports its artifact directly from
            # the protocol instead of recovering it from printed markers.
            if not result.data:
                result.data = fallback
            else:
                result.data.setdefault("status", fallback["status"])
                result.data.setdefault("err", fallback["err"])
                result.data.setdefault("amount_minor", fallback["amount_minor"])
                result.data.setdefault("risk_decline", fallback["risk_decline"])
                result.data.setdefault("promo_not_eligible", fallback["promo_not_eligible"])
                result.data.setdefault("upi_link", fallback["upi_link"])
                if result.data.get("upi_link") and not result.data.get("qr_png"):
                    result.data["qr_png"] = fallback["qr_png"]
                if result.data.get("upi_link") and not result.data.get("qr_svg"):
                    result.data["qr_svg"] = fallback["qr_svg"]
    except Exception as exc:  # keep a backend result even if classification fails
        result.data = {"status": "ERROR", "err": result.error or str(exc)[:240]}
    return result

__all__ = ["BackendResult", "run_in_process"]
