"""Offline API/SSE checks: python3 -m unittest discover -s tests -v.

Call endpoints and their async generators directly; no HTTP client or server is
needed. The link monitor and outbound requests stay disabled throughout.
"""
import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException
from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "web"))
import engine

with patch.object(engine, "start_link_monitor"), patch(
    "requests.sessions.Session.request", side_effect=AssertionError("network forbidden")
):
    import app as web_app


def events(chunk):
    return [json.loads(line[6:]) for line in chunk.splitlines() if line.startswith("data: ")]


class ApiRequestTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch("requests.sessions.Session.request",
                                side_effect=AssertionError("network forbidden")))

    def test_token_formats_share_one_parser(self):
        for raw, expected in (
            (" first \n\n second ", ["first", "second"]),
            ('["first", {"accessToken":" second "}, {"access_token":"third"}, {"token":"fourth"}, "", {}]',
             ["first", "second", "third", "fourth"]),
            ('{"accessToken":" first "}', ["first"]),
            ("[]", []),
            (" \n ", []),
        ):
            with self.subTest(raw=raw):
                self.assertEqual(web_app._split_tokens(raw), expected)

    def test_run_forwards_parsed_tokens_and_worker_settings(self):
        job = SimpleNamespace(job_id="abcdef123456", total=2)
        with patch.object(engine, "start_job", return_value=job) as start:
            req = web_app.RunRequest(tokens='["one", {"token":"two"}]', mode="cs",
                                     country="IN", promo="off", workers=16, retries=0,
                                     proxies="offline proxy fixture")
            self.assertEqual(web_app.api_run(req), {"job_id": job.job_id, "total": 2})
        start.assert_called_once_with(["one", "two"], "cs", "IN", "off", 16,
                                      "offline proxy fixture", 0)

    def test_invalid_run_never_reaches_engine(self):
        with patch.object(engine, "start_job") as start:
            for values in ({"tokens": " \n "}, {"tokens": "one", "mode": "invalid"}):
                with self.subTest(values=values), self.assertRaises(HTTPException) as error:
                    web_app.api_run(web_app.RunRequest(**values))
                self.assertEqual(error.exception.status_code, 400)
        start.assert_not_called()

    def test_worker_and_retry_bounds_are_validated(self):
        for values in ({"workers": 0}, {"workers": engine.MAX_WORKERS + 1},
                       {"retries": -1}, {"retries": 6}):
            with self.subTest(values=values), self.assertRaises(ValidationError):
                web_app.RunRequest(tokens="one", **values)
        self.assertEqual(web_app.RunRequest(workers=engine.MAX_WORKERS, retries=0).workers,
                         engine.MAX_WORKERS)

    def test_engine_validation_becomes_client_error(self):
        with patch.object(engine, "start_job", side_effect=ValueError("batch too large")):
            with self.assertRaises(HTTPException) as error:
                web_app.api_run(web_app.RunRequest(tokens="one"))
        self.assertEqual((error.exception.status_code, error.exception.detail),
                         (400, "batch too large"))

    def test_append_accepts_the_same_json_format_as_run(self):
        result = {"ok": True, "added": 2, "skipped": 0, "total": 3}
        with patch.object(engine, "append_tasks", return_value=result) as append:
            actual = web_app.api_append("abcdef123456", web_app.AppendRequest(
                tokens='["one", {"access_token":"two"}]'))
        self.assertEqual(actual, result)
        append.assert_called_once_with("abcdef123456", ["one", "two"])

    def test_append_rejects_empty_input_and_engine_errors(self):
        with patch.object(engine, "append_tasks") as append:
            with self.assertRaises(HTTPException) as error:
                web_app.api_append("abcdef123456", web_app.AppendRequest(tokens="[]"))
            self.assertEqual(error.exception.status_code, 400)
            append.assert_not_called()
            append.return_value = {"ok": False, "error": "job is stopping"}
            with self.assertRaises(HTTPException) as error:
                web_app.api_append("abcdef123456", web_app.AppendRequest(tokens="one"))
            self.assertEqual((error.exception.status_code, error.exception.detail),
                             (400, "job is stopping"))


class StreamTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="upi-api-test-")
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        self.enterContext(patch.object(engine, "LOG_DIR", root / "logs"))
        self.enterContext(patch.object(engine, "STATE_DIR", root / "state"))
        self.enterContext(patch("requests.sessions.Session.request",
                                side_effect=AssertionError("network forbidden")))
        self.job = engine.Job("abcdef123456", "cs", ["offline-token"], "IN", "off",
                              2, "offline pool", 0)
        self.addCleanup(self.job.close_log)
        self.enterContext(patch.object(engine, "get_job", return_value=self.job))
        self.enterContext(patch.object(engine, "get_saved_snapshot", return_value=None))

    async def open_stream(self):
        response = await web_app.api_stream(self.job.job_id)
        stream = response.body_iterator
        async def close():
            await stream.aclose()
        self.addAsyncCleanup(close)
        return response, stream

    async def test_initial_snapshot_and_close_release_subscription(self):
        response, stream = await self.open_stream()
        self.assertEqual(response.media_type, "text/event-stream")
        self.assertEqual(response.headers["X-Accel-Buffering"], "no")
        self.assertEqual(self.job._subs, [])
        first = events(await anext(stream))
        self.assertEqual(len(first), 1)
        self.assertEqual(first[0]["type"], "state")
        self.assertEqual(first[0]["job"]["total"], 1)
        self.assertEqual(first[0]["job"]["tasks"][0]["status"], "pending")
        self.assertEqual(len(self.job._subs), 1)
        await stream.aclose()
        self.assertEqual(self.job._subs, [])

    async def test_bursts_are_batched_and_keep_event_order(self):
        _, stream = await self.open_stream()
        await anext(stream)
        for index in range(260):
            self.job.emit({"type": "job_progress", "sequence": index})
        batches = [events(await asyncio.wait_for(anext(stream), 1)) for _ in range(3)]
        self.assertEqual([len(batch) for batch in batches], [128, 128, 4])
        self.assertEqual([event["sequence"] for batch in batches for event in batch],
                         list(range(260)))

    async def test_overflow_replaces_stale_events_with_fresh_state(self):
        with patch.object(engine, "SUBSCRIBER_QUEUE", 2):
            _, stream = await self.open_stream()
            await anext(stream)
        task = self.job.tasks["t1"]
        task.status, task.raw_status = "fail", "ERROR"
        for index in range(5):
            self.job.emit({"type": "job_progress", "sequence": index})
        batch = events(await asyncio.wait_for(anext(stream), 1))
        self.assertEqual([event["type"] for event in batch], ["state"])
        snapshot = batch[0]["job"]
        self.assertEqual((snapshot["done"], snapshot["fail"], snapshot["queued"]), (1, 1, 0))
        self.assertEqual(snapshot["tasks"][0]["raw_status"], "ERROR")

    async def test_idle_wait_uses_no_thread_and_cancellation_unsubscribes(self):
        _, stream = await self.open_stream()
        await anext(stream)
        with patch.object(asyncio, "to_thread", side_effect=AssertionError("idle thread")) as offload:
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(anext(stream), 0.06)
            offload.assert_not_called()
        self.assertEqual(self.job._subs, [])

    async def test_idle_stream_sends_heartbeat(self):
        with patch.object(web_app, "HEARTBEAT_SECS", 0):
            _, stream = await self.open_stream()
            await anext(stream)
            self.assertEqual(await asyncio.wait_for(anext(stream), 1), ": ping\n\n")

    async def test_saved_stream_is_finite_and_missing_job_is_not_found(self):
        saved = {"job_id": self.job.job_id, "status": "stopped", "total": 1,
                 "done": 1, "ok": 0, "fail": 0, "tasks": []}
        with patch.object(engine, "get_job", return_value=None):
            with patch.object(engine, "get_saved_snapshot", return_value=saved):
                _, stream = await self.open_stream()
                received = [event async for chunk in stream for event in events(chunk)]
            self.assertEqual([event["type"] for event in received], ["state", "job_done"])
            self.assertEqual(received[1]["status"], "stopped")
            self.assertEqual(self.job._subs, [])
            with self.assertRaises(HTTPException) as error:
                await web_app.api_stream(self.job.job_id)
            self.assertEqual(error.exception.status_code, 404)


if __name__ == "__main__":
    unittest.main()
