"""Offline scheduler regressions: python3 -m unittest discover -s tests -v.

Every checkout/proxy call is replaced; no tokens or payment requests leave here.
"""
import json
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "web"))
import engine


def wait_for(predicate, timeout=3):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError("scheduler did not reach expected state")


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="upi-queue-test-")
        self.root = Path(self.tmp.name)
        self.gates = []
        self.jobs = []
        self.calls = []
        self.active = self.peak = 0
        self.delay = 0
        self.guard = threading.Lock()
        self.seed = self.root / "proxies.txt"
        self.seed.touch()
        for target, value in (("LOG_DIR", self.root / "logs"),
                              ("STATE_DIR", self.root / "state"),
                              ("DEFAULT_PROXY_FILE", self.seed)):
            self.enterContext(patch.object(engine, target, value))
        self.enterContext(patch.object(engine, "_prepare_proxies", return_value=(self.seed, [""], "test pool", {})))
        self.enterContext(patch.object(engine, "_acquire_proxy", side_effect=lambda *a: ("", self.seed, {})))
        self.run = self.enterContext(patch.object(engine.backend, "run_cs", side_effect=self.backend))
        self.enterContext(patch("requests.sessions.Session.request", side_effect=AssertionError("network forbidden")))

    def backend(self, request, hooks):
        with self.guard:
            self.calls.append(request.token)
            self.active += 1
            self.peak = max(self.peak, self.active)
        try:
            if self.delay:
                time.sleep(self.delay)
            for gate in self.gates:
                if not gate.wait(2):
                    raise AssertionError("test gate timed out")
            return {"status": "FAIL", "err": "offline fixture"}
        finally:
            with self.guard:
                self.active -= 1

    def start(self, count=1, workers=2, retries=0):
        job = engine.start_job([f"token-{i}" for i in range(count)], "cs", "IN", "off", workers, None, retries)
        self.jobs.append(job)
        return job

    def tearDown(self):
        for gate in self.gates:
            gate.set()
        for job in self.jobs:
            job.stop_requested = True
        wait_for(lambda: self.active == 0)
        for job in self.jobs:
            pool = getattr(job, "_executor", None)
            if pool:
                pool.shutdown(wait=True)
            # Legacy implementation has separate append threads.
            wait_for(lambda: all(t.status not in ("pending", "running") for t in job.tasks.values()))
            job.close_log()
        engine._JOBS.clear()
        engine._JOB_ORDER.clear()
        self.doCleanups()
        self.tmp.cleanup()

    def test_pending_task_cannot_be_retried_twice(self):
        gate = threading.Event()
        self.gates.append(gate)
        job = self.start(2, workers=1)
        wait_for(lambda: self.active == 1)
        self.assertFalse(engine.retry_task(job.job_id, "t2"))
        gate.set()
        wait_for(lambda: job.status == "done")
        self.assertEqual(sorted(self.calls), ["token-0", "token-1"])

    def test_original_batch_does_not_finish_appended_work(self):
        first, added = threading.Event(), threading.Event()
        self.gates.extend([first, added])
        def backend(request, hooks):
            self.calls.append(request.token)
            (first if request.token == "token-0" else added).wait(2)
            return {"status": "FAIL"}
        self.run.side_effect = backend
        job = self.start(workers=2)
        wait_for(lambda: "token-0" in self.calls)
        self.assertTrue(engine.append_tasks(job.job_id, ["added"])["ok"])
        wait_for(lambda: "added" in self.calls)
        first.set()
        wait_for(lambda: job.tasks["t1"].status == "fail")
        time.sleep(0.025)
        self.assertEqual(job.status, "running")
        self.assertIsNotNone(job._log_fh)
        added.set()
        wait_for(lambda: job.status == "done")
        self.assertEqual(job.done, 2)

    def test_retry_reopens_job_and_persists_final_result(self):
        job = self.start()
        wait_for(lambda: job.status == "done" and job._log_fh is None)
        gate = threading.Event()
        self.gates.append(gate)
        self.assertTrue(engine.retry_task(job.job_id, "t1"))
        wait_for(lambda: self.active == 1)
        self.assertEqual(job.status, "running")
        self.assertIsNone(job.finished_at)
        self.assertIsNotNone(job._log_fh)
        self.assertFalse(engine.retry_task(job.job_id, "t1"))
        gate.set()
        wait_for(lambda: job.status == "done" and job._log_fh is None)
        saved = json.loads(job.results_path.read_text())
        self.assertEqual(saved["tasks"][0]["run"], 2)

    def test_stop_never_starts_queued_appended_tasks(self):
        gate = threading.Event()
        self.gates.append(gate)
        job = self.start(workers=1)
        wait_for(lambda: self.active == 1)
        self.assertTrue(engine.append_tasks(job.job_id, ["added-1", "added-2"])["ok"])
        time.sleep(0.02)
        self.assertTrue(engine.stop_job(job.job_id))
        gate.set()
        wait_for(lambda: all(t.status not in ("pending", "running") for t in job.tasks.values()))
        self.assertEqual(self.calls, ["token-0"])
        self.assertEqual(job.tasks["t2"].status, "stopped")
        self.assertEqual(job.fail, 1)

    def test_all_appends_share_one_bounded_pool(self):
        gate = threading.Event()
        self.gates.append(gate)
        job = self.start(4, workers=4)
        wait_for(lambda: self.active == 4)
        before = threading.active_count()
        for i in range(12):
            self.assertTrue(engine.append_tasks(job.job_id, [f"append-{i}"])["ok"])
        self.assertLessEqual(threading.active_count() - before, 1)
        gate.set()
        wait_for(lambda: job.status == "done")
        self.assertEqual(len(self.calls), 16)
        self.assertEqual(self.peak, 4)

    def test_append_enforces_total_batch_limit(self):
        gate = threading.Event()
        self.gates.append(gate)
        job = self.start(2)
        with patch.object(engine, "MAX_TOKENS_PER_BATCH", 3):
            result = engine.append_tasks(job.job_id, ["new-1", "new-2"])
        self.assertFalse(result["ok"])
        self.assertEqual(job.total, 2)

    def test_parallel_jobs_use_separate_state_paths(self):
        first, second = self.start(), self.start()
        a = engine._backend_request(first, first.tasks["t1"], "", self.seed)
        b = engine._backend_request(second, second.tasks["t1"], "", self.seed)
        self.assertNotEqual(a.state_dir, b.state_dir)

    def test_setup_exception_finishes_task_and_job(self):
        with patch.object(engine, "_acquire_proxy", side_effect=RuntimeError("fixture setup failed")):
            job = self.start()
            wait_for(lambda: job.status == "done")
        self.assertEqual(job.tasks["t1"].status, "fail")
        self.assertIn("fixture setup failed", job.tasks["t1"].error)

    def test_slow_subscriber_gets_resync_marker(self):
        job = self.start()
        wait_for(lambda: job.status == "done")
        with patch.object(engine, "SUBSCRIBER_QUEUE", 2):
            subscriber = job.subscribe()
        for i in range(5):
            job.emit({"type": "step", "task_id": "t1", "key": "checkout", "status": str(i)})
        events = []
        while not subscriber.empty():
            events.append(subscriber.get_nowait())
        self.assertTrue(any(e["type"] == "resync" for e in events))
        self.assertLessEqual(len(events), 2)

    def test_stop_during_preparation_does_not_start_backend(self):
        gate = threading.Event()
        self.gates.append(gate)
        def prepare(*args):
            gate.wait(2)
            return "", self.seed, {}
        with patch.object(engine, "_acquire_proxy", side_effect=prepare):
            job = self.start()
            wait_for(lambda: job.tasks["t1"].status == "running")
            engine.stop_job(job.job_id)
            gate.set()
            wait_for(lambda: job.status == "stopped")
        self.assertEqual(self.calls, [])
        self.assertEqual(job.tasks["t1"].status, "stopped")

    def test_reserved_failed_task_cannot_be_removed_during_retry(self):
        gate = threading.Event()
        self.gates.append(gate)
        job = self.start()
        wait_for(lambda: self.active == 1)
        job.tasks["t1"].status = "fail"  # transient state before auto retry
        self.assertFalse(engine.remove_task(job.job_id, "t1")["ok"])
        self.assertEqual(engine.clear_tasks(job.job_id, "fail")["removed"], 0)

    def test_registry_never_evicts_live_jobs(self):
        gate = threading.Event()
        self.gates.append(gate)
        with patch.object(engine, "MAX_JOBS", 1):
            first, second = self.start(), self.start()
        self.assertIs(engine.get_job(first.job_id), first)
        self.assertIs(engine.get_job(second.job_id), second)

    def test_retry_events_and_snapshots_are_detached(self):
        job = self.start()
        wait_for(lambda: job.status == "done")
        sub = job.subscribe()
        task = job.tasks["t1"]
        engine._reset_task_for_retry(job, task)
        event = sub.get_nowait()
        snapshot = job.snapshot()
        task.steps[0]["status"] = "done"
        self.assertEqual(event["steps"][0]["status"], "pending")
        self.assertEqual(snapshot["tasks"][0]["steps"][0]["status"], "pending")
        task.status = "fail"

    def test_interrupted_checkpoint_is_stopped_not_resumed(self):
        job = self.start()
        wait_for(lambda: job.status == "done")
        job.status = "running"
        job.tasks["t1"].status = "pending"
        job.write_results()
        engine._JOBS.pop(job.job_id)
        saved = engine.get_saved_snapshot(job.job_id)
        self.assertEqual(saved["status"], "stopped")
        self.assertEqual(saved["tasks"][0]["status"], "stopped")
        job.tasks["t1"].status = "fail"

    def test_large_batch_runs_in_parallel_with_consistent_snapshots(self):
        self.delay = 0.003
        started = time.perf_counter()
        job = self.start(1000, workers=16)
        while job.status == "running":
            snap = job.snapshot()
            self.assertEqual(snap["total"], 1000)
            self.assertEqual(snap["done"] + snap["running"] + snap["queued"], 1000)
            self.assertLess(time.perf_counter() - started, 10)
            time.sleep(0.01)
        self.assertEqual(len(self.calls), 1000)
        self.assertEqual(len(set(self.calls)), 1000)
        self.assertGreater(self.peak, 1)
        self.assertLessEqual(self.peak, 16)
        wait_for(lambda: job._log_fh is None)
        saved = json.loads(job.results_path.read_text())
        self.assertEqual(saved["done"], 1000)


if __name__ == "__main__":
    unittest.main()
