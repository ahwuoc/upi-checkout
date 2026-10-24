#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""app.py — web server cho upi-checkout: dán nhiều AT -> xem progress từng bước.

Chạy:
    cd upi-checkout/web
    python3 app.py                 # http://127.0.0.1:8099
    python3 app.py --port 9000

Backend protocol nằm nguyên trong `../cli.py`; file này chỉ lo HTTP + SSE.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import queue
import re
import threading
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import engine
import prefilter
import artifact_probe
import global_stats

HERE = Path(__file__).resolve().parent
STATIC = HERE / "static"

app = FastAPI(title="UPI QR Extractor", docs_url=None, redoc_url=None)

# Vòng dò link chạy NỀN trong process server: đóng browser vẫn biết khách quét lúc
# nào / link chết sau bao lâu. Bật ở đây (không bật khi import engine) để test và
# CLI không dính tác dụng phụ.
# Nạp link của các job CŨ (đọc từ snapshot trên đĩa) vào sổ theo dõi: job chỉ sống trong
# RAM, mà khách có thể duyệt uỷ nhiệm ₹0 sau nhiều giờ — không nạp thì link cũ đứng im ở
# "Awaiting payment" mãi. Chạy ở thread riêng để không làm chậm lúc khởi động (đọc vài chục
# file JSON vài MB).
threading.Thread(target=engine.backfill_link_watch_from_disk, name="link-backfill",
                 daemon=True).start()
engine.start_link_monitor()

HEARTBEAT_SECS = 15.0
MODES = {"auto", "oaics", "cs"}


@app.middleware("http")
async def _no_store_ui(request, call_next):
    """UI 资源一律不缓存。

    `/static` 是 StaticFiles 直接吐出来的，**默认不带 Cache-Control**，只有
    etag / last-modified。浏览器遇到这种响应会按「启发式新鲜度」（大约
    (now - last-modified) 的 10%）自己决定要不要复用 —— 文件越老，这个窗口越长，
    于是改完 CSS 刷新页面看到的还是旧样式，还以为是没改对。

    这是一个 localhost 面板，重下 28 KB CSS / 40 KB JS 没有任何代价，
    所以直接 no-store。SSE 流 ( /api/stream ) 排除在外。
    """
    response = await call_next(request)
    if not request.url.path.startswith("/api/stream"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    return response


class RunRequest(BaseModel):
    tokens: str = Field(default="", description="one access token per line")
    mode: str = "auto"
    country: str = "IN"
    promo: str = "off"
    workers: int = Field(default=4, ge=1, le=engine.MAX_WORKERS)
    retries: int = Field(default=3, ge=1, le=5)
    proxies: str | None = None
    mix: bool = Field(default=False, description="shuffle the pool before assigning proxies")
    filter_proxies: bool = Field(default=True, description="probe + drop dead/wrong-country proxies")
    stripe_telemetry: bool = Field(default=False, description="register the Stripe session at m.stripe.com")
    dump_traffic: bool = Field(default=False, description="save every API call (headers + body) as HAR entries under dumps/")
    prefilter: bool = Field(default=False, description="mỗi worker tự check token (1 GET) trước khi chạy")


class PrefilterRequest(BaseModel):
    """Kiểm tra token trước khi chạy (GET accounts/check, không tạo checkout)."""
    tokens: str = Field(default="", description="one access token per line")
    workers: int = Field(default=8, ge=1, le=32)
    proxies: str | None = Field(default=None, description="proxy list; trống = proxy.txt")
    promo: str = "plus-1-month-free"


class AppendRequest(BaseModel):
    tokens: str = Field(default="", description="one access token per line")


class ClearRequest(BaseModel):
    scope: str = Field(default="all", description="all|running|queued|success|fail")


def _split_tokens(raw: str) -> list[str]:
    """Nhận mỗi dòng 1 JWT, hoặc 1 mảng JSON, hoặc object có accessToken."""
    raw = (raw or "").strip()
    if not raw:
        return []
    if raw[0] in "[{":
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            data = None
        if isinstance(data, dict):
            data = [data]
        if isinstance(data, list):
            out = []
            for it in data:
                if isinstance(it, str):
                    out.append(it.strip())
                elif isinstance(it, dict):
                    for k in ("accessToken", "access_token", "token"):
                        if it.get(k):
                            out.append(str(it[k]).strip())
                            break
            return [t for t in out if t]
    return [ln.strip() for ln in raw.splitlines() if ln.strip()]


# ------------------------------------------------------------------- static
FE_DIST = STATIC / "dist"


def _vue_index() -> FileResponse | None:
    """index.html của bản Vue (nếu đã build)."""
    f = FE_DIST / "index.html"
    return FileResponse(f) if f.is_file() else None


@app.get("/")
def index() -> FileResponse:
    """Giao diện duy nhất: bản Vue + shadcn-vue (`web/fe` -> `web/static/dist`)."""
    vue = _vue_index()
    if vue is None:
        raise HTTPException(
            500,
            "UI not built yet: pnpm -C web/fe install && pnpm -C web/fe build",
        )
    return vue


# Bản Vue 3 + shadcn-vue (web/fe -> build ra static/dist, base '/next/').
# Chạy SONG SONG với bản vanilla ở '/' để so sánh trước khi thay hẳn.
# Asset của bản build (`/assets/index-*.js|css`) — tách riêng để không lẫn với `/static`.
if (FE_DIST / "assets").is_dir():
    app.mount("/assets", StaticFiles(directory=str(FE_DIST / "assets")), name="assets")

# File trong `web/fe/public/` (vd `sounds/success.mp3` — tiếng thông báo khi có link) được Vite
# copy nguyên sang `dist/sounds/`, nên phải mount riêng; nếu không thì trình duyệt nhận 404.
if (FE_DIST / "sounds").is_dir():
    app.mount("/sounds", StaticFiles(directory=str(FE_DIST / "sounds")), name="sounds")


@app.get("/next")
def next_index() -> RedirectResponse:
    """Đường cũ của bản thử -> giờ bản Vue đã ở `/`; giữ redirect cho bookmark."""
    return RedirectResponse("/", status_code=308)


# ---------------------------------------------------------------------- api
@app.post("/api/run")
def api_run(req: RunRequest) -> dict:
    tokens = _split_tokens(req.tokens)
    if not tokens:
        raise HTTPException(400, "No access tokens provided.")
    if req.mode not in MODES:
        raise HTTPException(400, f"invalid mode: {req.mode}")
    try:
        job = engine.start_job(tokens, req.mode, req.country, req.promo,
                               req.workers, req.proxies, req.retries, req.mix,
                               req.filter_proxies, req.stripe_telemetry, req.dump_traffic,
                               req.prefilter)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"job_id": job.job_id, "total": job.total}


@app.get("/api/jobs")
def api_jobs() -> dict:
    return {"jobs": engine.list_jobs()}


@app.get("/api/stats/global")
def api_stats_global() -> dict:
    """Thống kê global: mỗi ngày trích xuất được bao nhiêu mã ₹0.00.

    Chỉ đếm mã ₹0 (mã giá gốc không dùng được). Quét file kết quả trên đĩa
    (web + CLI) nên sống qua restart, có cache theo (mtime, size) nên mở trang
    không phải chờ quét lại.
    """
    return global_stats.daily()


@app.get("/api/log/{job_id}")
def api_log(job_id: str, download: int = 0) -> FileResponse:
    """Tải file log của job (bền vững, không mất khi restart)."""
    if not re.fullmatch(r"[0-9a-f]{12}", job_id):
        raise HTTPException(404, "no log file for this job")
    job = engine.get_job(job_id)
    if job:
        path = job.log_path
    else:
        # job đã bị đẩy khỏi RAM -> vẫn còn file trên đĩa
        path = engine.LOG_DIR / f"web_{job_id}.log"
    if not path.is_file():
        raise HTTPException(404, "no log file for this job")
    headers = {"Content-Disposition": f'attachment; filename="{path.name}"'} if download else None
    return FileResponse(path, media_type="text/plain; charset=utf-8", headers=headers)


@app.get("/api/results/{job_id}")
def api_results(job_id: str) -> dict:
    """Kết quả cuối của job: ưu tiên file trên đĩa, không có thì lấy từ RAM."""
    job = engine.get_job(job_id)
    if job:
        return job.snapshot()
    snapshot = engine.get_saved_snapshot(job_id)
    if snapshot is None:
        raise HTTPException(404, "job does not exist")
    return snapshot


@app.get("/api/state/{job_id}")
def api_state(job_id: str) -> dict:
    # `get_job_overlay` = snapshot của job trong RAM đã trộn trạng thái link mới nhất từ sổ
    # theo dõi bền vững (link của job cũ vẫn được dò tiếp sau restart).
    live = engine.get_job_overlay(job_id)
    if live is not None:
        return live
    snapshot = engine.get_saved_snapshot(job_id)
    if snapshot is None:
        raise HTTPException(404, "job does not exist")
    return snapshot


@app.post("/api/retry/{job_id}/{task_id}")
def api_retry(job_id: str, task_id: str) -> dict:
    if not engine.retry_task(job_id, task_id):
        raise HTTPException(400, "this task cannot be retried")
    return {"ok": True}


@app.post("/api/append/{job_id}")
def api_append(job_id: str, req: AppendRequest) -> dict:
    """Thêm AT (token) vào queue của job đang chạy — không cần bấm Stop.

    Dùng pool proxy sẵn có của job, nên không phải chấm lại chất lượng proxy.
    """
    tokens = _split_tokens(req.tokens)
    if not tokens:
        raise HTTPException(400, "no tokens")
    result = engine.append_tasks(job_id, tokens)
    if not result.get("ok"):
        raise HTTPException(400, result.get("error") or "append failed")
    return result


@app.post("/api/prefilter")
def api_prefilter(req: PrefilterRequest) -> dict:
    """Chạy bộ lọc token nền; trả id để client poll `/api/prefilter/{id}`.

    1 request GET mỗi token (`accounts/check`), KHÔNG tạo checkout — nên kiểm 200 acc
    nhanh hơn nhiều so với để pipeline tự phát hiện (mỗi acc ineligible tốn ~5 request).
    """
    tokens = [t.strip() for t in req.tokens.splitlines() if t.strip()]
    if not tokens:
        raise HTTPException(400, "No access tokens provided.")
    proxies = req.proxies if (req.proxies and req.proxies.strip()) else None
    try:
        pool = engine.parse_proxy_lines(proxies) if proxies else engine.cli.load_proxies(
            engine.DEFAULT_PROXY_FILE)
    except Exception:  # noqa: BLE001 — thiếu proxy.txt vẫn cho kiểm tra (đi trực tiếp)
        pool = []
    run_id = prefilter.start(tokens, [p for p in pool if p], req.workers, req.promo)
    return {"id": run_id, "total": len(tokens), "proxies": len([p for p in pool if p])}


@app.get("/api/prefilter/{run_id}")
def api_prefilter_status(run_id: str) -> dict:
    snap = prefilter.status(run_id)
    if snap is None:
        raise HTTPException(404, "prefilter run does not exist")
    return snap


@app.post("/api/prefilter/{run_id}/stop")
def api_prefilter_stop(run_id: str) -> dict:
    if not prefilter.stop(run_id):
        raise HTTPException(400, "run is not active")
    return {"stopped": True}


@app.post("/api/stop/{job_id}")
def api_stop(job_id: str) -> dict:
    if not engine.stop_job(job_id):
        raise HTTPException(400, "job is not running or does not exist")
    return {"stopped": True}


@app.post("/api/clear/{job_id}")
def api_clear(job_id: str, req: ClearRequest) -> dict:
    """Xoá task của một tab. `scope` khớp tên tab: all|running|queued|success|fail|stopped."""
    result = engine.clear_tasks(job_id, req.scope)
    if not result.get("ok"):
        raise HTTPException(400, result.get("error") or "delete failed")
    return result


@app.get("/api/task-token/{job_id}/{task_id}")
def api_task_token(job_id: str, task_id: str) -> dict:
    """Trả access token của MỘT task (nút "copy AT" trên card).

    Vì sao không nhét token vào snapshot: token KHÔNG được ghi ra `results.json`/headline (1 JWT
    ~2 KB × 6.000 task = ~12 MB mỗi snapshot, và token nằm lại trên đĩa). Endpoint này chỉ đọc
    từ RAM của job đang sống, trả đúng 1 token khi người dùng bấm copy.
    """
    job = engine.get_job(job_id)
    if not job:
        raise HTTPException(
            404,
            "job không còn trong RAM (desk đã restart) — token không lưu ra đĩa nên không lấy lại được",
        )
    task = job.tasks.get(task_id)
    if task is None:
        raise HTTPException(404, "task does not exist")
    if not task.token:
        raise HTTPException(404, "token is empty for this task")
    return {"task_id": task_id, "token": task.token}


@app.post("/api/remove/{job_id}/{task_id}")
def api_remove(job_id: str, task_id: str) -> dict:
    """Xoá 1 task lẻ (nút ✕ trên từng card). Xoá cả RAM lẫn file đã lưu."""
    result = engine.remove_task(job_id, task_id)
    if not result.get("ok"):
        raise HTTPException(400, result.get("error") or "delete failed")
    return result


@app.get("/api/artifact")
def api_artifact(url: str, fresh: int = 0) -> dict:
    """抓指引页 -> 金额 / 到期时间 / 状态 / 链的类型。

    金额和 expires_at 在落盘的 artifact 里都是 null，只能现抓。
    前端每 20 秒轮询时带 fresh=1 绕过 120 秒缓存。
    """
    return artifact_probe.probe(url, fresh=bool(fresh))


@app.get("/api/stream/{job_id}")
async def api_stream(job_id: str) -> StreamingResponse:
    job = engine.get_job(job_id)
    if not job:
        snapshot = engine.get_saved_snapshot(job_id)
        if snapshot is None:
            raise HTTPException(404, "job does not exist")

        async def saved_gen():
            yield engine.sse_format({"type": "state", "job": snapshot})
            yield engine.sse_format({
                "type": "job_done", "job_id": snapshot.get("job_id"),
                "ok": snapshot.get("ok", 0), "fail": snapshot.get("fail", 0),
                "done": snapshot.get("done", 0), "total": snapshot.get("total", 0),
                "status": snapshot.get("status", "done"),
                "duration_ms": snapshot.get("duration_ms"),
            })

        return StreamingResponse(
            saved_gen(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no",
                     "Connection": "keep-alive"},
        )

    async def gen():
        with job._lock:
            q = job.subscribe()
            snapshot = job.snapshot()
        loop = asyncio.get_running_loop()
        heartbeat = loop.time() + HEARTBEAT_SECS
        try:
            # gui snapshot dau tien de client hien ngay trang thai hien tai
            yield engine.sse_format({"type": "state", "job": snapshot})
            if job.status != "running":
                yield engine.sse_format({"type": "job_done", "job_id": job.job_id,
                                         "ok": job.ok, "fail": job.fail, "done": job.done,
                                         "total": job.total, "status": job.status,
                                         "duration_ms": job.duration_ms})
            while True:
                frames = []
                # Bounded nonblocking batches: idle browsers no longer occupy
                # the default executor, and bursts amortize ASGI send overhead.
                for _ in range(128):
                    try:
                        ev = q.get_nowait()
                    except queue.Empty:
                        break
                    if ev.get("type") == "resync":
                        with job._lock:
                            while not q.empty():
                                q.get_nowait()
                            ev = {"type": "state", "job": job.snapshot()}
                        frames = []
                    frames.append(engine.sse_format(ev))
                if frames:
                    yield "".join(frames)
                elif loop.time() >= heartbeat:
                    yield ": ping\n\n"
                    heartbeat = loop.time() + HEARTBEAT_SECS
                    if job.status != "running" and q.empty():
                        break
                await asyncio.sleep(0 if frames else 0.05)
        finally:
            job.unsubscribe(q)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no",
                 "Connection": "keep-alive"},
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="UPI QR web UI")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8099)
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args()

    import uvicorn

    print(f"UPI QR web UI -> http://{args.host}:{args.port}")
    uvicorn.run("app:app", host=args.host, port=args.port, reload=args.reload,
                log_level="info")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
