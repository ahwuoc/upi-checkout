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
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import engine
import artifact_probe

HERE = Path(__file__).resolve().parent
STATIC = HERE / "static"

app = FastAPI(title="UPI QR Extractor", docs_url=None, redoc_url=None)

# Vòng dò link chạy NỀN trong process server: đóng browser vẫn biết khách quét lúc
# nào / link chết sau bao lâu. Bật ở đây (không bật khi import engine) để test và
# CLI không dính tác dụng phụ.
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
    workers: int = 4
    retries: int = Field(default=1, ge=0, le=5)
    proxies: str | None = None
    min_score: int = Field(default=0, ge=0, le=100,
                           description=">0: workers score proxies on demand and only "
                                       "use ones at/above this score")


class ScanRequest(BaseModel):
    proxies: str | None = None
    country: str = "IN"
    workers: int = Field(default=12, ge=1, le=64)


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
@app.get("/")
def index() -> FileResponse:
    f = STATIC / "index.html"
    if not f.is_file():
        raise HTTPException(500, "static/index.html is missing")
    return FileResponse(f)


app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


# ---------------------------------------------------------------------- api
@app.post("/api/run")
def api_run(req: RunRequest) -> dict:
    tokens = _split_tokens(req.tokens)
    if not tokens:
        raise HTTPException(400, "No access tokens provided.")
    if req.mode not in MODES:
        raise HTTPException(400, f"invalid mode: {req.mode}")
    if req.workers < 1 or req.workers > 64:
        raise HTTPException(400, "workers must be within 1..64")
    try:
        job = engine.start_job(tokens, req.mode, req.country, req.promo,
                               req.workers, req.proxies, req.min_score, req.retries)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"job_id": job.job_id, "total": job.total}


@app.post("/api/scan-proxies")
async def api_scan_proxies(req: ScanRequest) -> dict:
    """Quét + chấm điểm cả pool trước khi chạy: proxy sạch / risk thấp xếp trước."""
    if req.proxies and req.proxies.strip():
        px = engine.parse_proxy_lines(req.proxies)
    else:
        px = engine.cli.load_proxies(engine.DEFAULT_PROXY_FILE)
    if not px:
        raise HTTPException(400, "Pool is empty.")

    results = await asyncio.to_thread(
        engine.cli.scan_proxies, px, req.country, req.workers, 15)
    grades: dict[str, int] = {}
    for r in results:
        grades[r["grade"]] = grades.get(r["grade"], 0) + 1
    return {
        "total": len(results),
        "grades": grades,
        "usable": sum(1 for r in results if r["ok"] and "wrong-country" not in r["flags"]),
        "results": results,
    }


@app.get("/api/jobs")
def api_jobs() -> dict:
    return {"jobs": engine.list_jobs()}


@app.get("/api/log/{job_id}")
def api_log(job_id: str, download: int = 0) -> FileResponse:
    """Tải file log của job (bền vững, không mất khi restart)."""
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
    job = engine.get_job(job_id)
    if job:
        return job.snapshot()
    snapshot = engine.get_saved_snapshot(job_id)
    if snapshot is None:
        raise HTTPException(404, "job does not exist")
    return snapshot


@app.post("/api/retry/{job_id}/{task_id}")
def api_retry(job_id: str, task_id: str) -> dict:
    if not engine.retry_task(job_id, task_id):
        raise HTTPException(400, "this task cannot be retried")
    return {"ok": True}


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

    q = job.subscribe()

    async def gen():
        try:
            # gui snapshot dau tien de client hien ngay trang thai hien tai
            yield engine.sse_format({"type": "state", "job": job.snapshot()})
            if job.status != "running":
                yield engine.sse_format({"type": "job_done", "job_id": job.job_id,
                                         "ok": job.ok, "fail": job.fail, "done": job.done,
                                         "total": job.total, "status": job.status,
                                         "duration_ms": job.duration_ms})
            while True:
                try:
                    ev = await asyncio.to_thread(q.get, True, HEARTBEAT_SECS)
                except queue.Empty:
                    yield ": ping\n\n"
                    if job.status != "running" and q.empty():
                        break
                    continue
                yield engine.sse_format(ev)
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
