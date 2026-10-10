"""Offline UI preview: python3 tests/preview_server.py --port 8101.

Uses the real HTTP/SSE/queue and a simulated backend. Never contacts payment or
proxy services; all state lives in a temporary directory, not production logs.
"""
import argparse
import base64
import json
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "web"))
import engine


def fake_token(index):
    body = {"https://api.openai.com/profile": {"email": f"demo{index:04d}@example.test"}}
    payload = base64.urlsafe_b64encode(json.dumps(body).encode()).decode().rstrip("=")
    return f"demo.{payload}.signature"


def simulated_backend(request, hooks):
    for key, _ in engine.cli.STEPS_CS:
        if hooks.should_stop():
            return {"status": "STOPPED", "err": "Offline preview stopped"}
        hooks.step(key, "active")
        time.sleep(0.12)
        hooks.step(key, "done")
    if request.index % 7 == 0:
        return {"status": "FAIL", "err": "Simulated error — retry is available"}
    return {"status": "LINK", "upi_link": "", "amount_minor": 0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8101)
    parser.add_argument("--demo-tasks", type=int, default=48)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="upi-ui-preview-") as directory:
        root = Path(directory)
        engine.LOG_DIR = root / "logs"
        engine.STATE_DIR = root / "state"
        seed = root / "proxies.txt"
        seed.touch()
        engine.DEFAULT_PROXY_FILE = seed
        with (patch.object(engine, "start_link_monitor"),
              patch.object(engine, "_prepare_proxies", return_value=(seed, [""], "offline simulation", {})),
              patch.object(engine, "_acquire_proxy", return_value=("", seed, {})),
              patch.object(engine.backend, "detect", return_value={"kind": "cs", "provider": "demo"}),
              patch.object(engine.backend, "run_cs", side_effect=simulated_backend),
              patch.object(engine.backend, "run_oaics", side_effect=simulated_backend),
              patch("requests.sessions.Session.request", side_effect=RuntimeError("Network disabled in offline preview"))):
            import app
            import uvicorn
            if args.demo_tasks:
                engine.start_job([fake_token(i) for i in range(args.demo_tasks)],
                                 "cs", "IN", "off", 8, None, retries=1)
            print("OFFLINE PREVIEW — no real checkout or payment requests", flush=True)
            try:
                uvicorn.run(app.app, host="127.0.0.1", port=args.port, log_level="warning")
            finally:
                for job in list(engine._JOBS.values()):
                    engine.stop_job(job.job_id)
                    with job._lock:
                        executor = job._executor
                    if executor:
                        executor.shutdown(wait=True)
                    job.close_log()


if __name__ == "__main__":
    main()
