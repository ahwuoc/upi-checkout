"""Offline scheduler regressions: python3 -m unittest discover -s tests -v.

Every checkout/proxy call is replaced; no tokens or payment requests leave here.
"""
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
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

    def test_no_batch_limit_by_default_and_append_is_unbounded(self):
        # Mặc định MAX_TOKENS_PER_BATCH = 0 -> không chặn; chỉ chặn khi đặt UPI_MAX_BATCH.
        self.assertEqual(engine.MAX_TOKENS_PER_BATCH, 0)
        gate = threading.Event()
        self.gates.append(gate)
        job = self.start(20, workers=1)
        result = engine.append_tasks(job.job_id, [f"new-{i}" for i in range(30)])
        self.assertTrue(result["ok"], result)
        self.assertEqual(job.total, 50)
        gate.set()
        wait_for(lambda: job.status == "done")
        self.assertEqual(len(self.calls), 50)

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


class PoolMixTests(unittest.TestCase):
    """Pool dán tay xếp theo khối vùng -> phải trộn trước khi chia cho task."""

    @staticmethod
    def blocks():
        out = [f"us.arxlabs.io:3010:acct-region-IN-sid-{i:02d}-t-5:pw" for i in range(4)]
        for state, size in (("Maharashtra", 8), ("Bihar", 8), ("Delhi", 8)):
            out += [f"us.arxlabs.io:3010:acct-region-IN-st-{state}-sid-{state[:2]}{i:02d}-t-5:pw"
                    for i in range(size)]
        return out

    @staticmethod
    def state_of(proxy: str) -> str:
        return proxy.split("-st-")[1].split("-sid-")[0] if "-st-" in proxy else "none"

    def test_mix_keeps_every_proxy_but_breaks_the_block_order(self):
        pool = self.blocks()
        mixed = engine.mix_pool(pool, "job1")
        self.assertEqual(sorted(mixed), sorted(pool))
        self.assertNotEqual(mixed, pool)
        # 28 proxy / 4 nhóm: 12 đầu phải chạm đủ cả 4 nhóm, không còn 4 dòng "none" liền nhau.
        self.assertEqual(len({self.state_of(p) for p in mixed[:12]}), 4)

    def test_mix_is_seeded_per_job_so_a_rerun_gets_a_new_order(self):
        pool = self.blocks()
        self.assertEqual(engine.mix_pool(pool, "job1"), engine.mix_pool(pool, "job1"))
        self.assertNotEqual(engine.mix_pool(pool, "job1"), engine.mix_pool(pool, "job2"))

    def test_prepare_proxies_only_mixes_when_asked(self):
        with tempfile.TemporaryDirectory(prefix="upi-mix-test-") as tmp:
            with patch.object(engine, "STATE_DIR", Path(tmp)):
                pool = "\n".join(self.blocks())
                _, plain, label, _ = engine._prepare_proxies("job1", pool, 4, False)
                _, mixed, mixed_label, _ = engine._prepare_proxies("job1", pool, 4, True)
        self.assertEqual([self.state_of(p) for p in plain[:4]], ["none"] * 4)
        self.assertNotEqual([self.state_of(p) for p in mixed[:4]], ["none"] * 4)
        self.assertNotIn("mixed", label)
        self.assertIn("mixed", mixed_label)


class BillingCaptureTests(unittest.TestCase):
    """Log hoá đơn -> field của task + event `task_billing` cho panel."""

    def task(self):
        return engine.Task(task_id="t1", index=0, token="tok", email="a@b.c")

    def test_log_lines_fill_the_task_and_emit_one_event(self):
        events = []
        job = SimpleNamespace(mode="cs", emit=events.append)
        t = self.task()
        _, on_log, _ = engine._mk_hooks(job, t)
        on_log("  [PM=IN] IN details filled into PM billing_details: "
               "Debashis Behera / Flat 122, Puravankara / Koderma 825409")
        self.assertEqual((t.billing_name, t.billing_city, t.billing_pin),
                         ("Debashis Behera", "Koderma", "825409"))
        self.assertEqual([e["type"] for e in events], ["task_billing"])
        self.assertEqual(events[0]["billing"]["billing_name"], "Debashis Behera")

        # Dòng hoá đơn thứ hai KHÔNG ghi đè và cũng không bắn event thừa.
        on_log("  [PM=IN] IN details filled into PM billing_details: "
               "Someone Else / Other Street / Patna 800027")
        self.assertEqual(t.billing_name, "Debashis Behera")
        self.assertEqual(len(events), 1)

    def test_geo_line_fills_state_and_pin(self):
        t = self.task()
        written = engine._absorb_billing(t, "billing geo: Karnataka / PIN 575001 / exit 1.2.3.4")
        self.assertEqual(written, {"billing_tier": "tier1", "geo_state": "Karnataka",
                                   "geo_pin": "575001"})
        self.assertEqual(engine._absorb_billing(t, "dòng log bình thường"), {})

    def test_retry_clears_billing_so_results_describe_the_last_run(self):
        t = self.task()
        engine._absorb_billing(t, "billing geo: Karnataka / PIN 575001 / exit 1.2.3.4")
        engine._absorb_billing(t, "  [PM=IN] IN details filled into PM billing_details: "
                                  "Debashis Behera / Flat 122 / Koderma 825409")
        job = SimpleNamespace(mode="cs", emit=lambda ev: None)
        engine._reset_task_for_retry(job, t)
        self.assertEqual((t.geo_state, t.geo_city, t.geo_pin), ("", "", ""))
        self.assertEqual((t.billing_name, t.billing_city, t.billing_pin), ("", "", ""))


class ProxyFilterFlagTests(unittest.TestCase):
    """`filter_proxies=False` -> dùng pool nguyên trạng, không chấm/không loại."""

    def setUp(self):
        # Không để test ghi rác vào cs_state/logs thật.
        self.tmp = tempfile.TemporaryDirectory(prefix="upi-filter-test-")
        root = Path(self.tmp.name)
        (root / "state").mkdir()
        (root / "logs").mkdir()
        self.enterContext(patch.object(engine, "STATE_DIR", root / "state"))
        self.enterContext(patch.object(engine, "LOG_DIR", root / "logs"))
        self.enterContext(patch.object(engine, "DEFAULT_PROXY_FILE", root / "proxy.txt"))

    def tearDown(self):
        engine._JOBS.clear()
        engine._JOB_ORDER.clear()
        self.doCleanups()
        self.tmp.cleanup()

    def job(self, filter_proxies=True, country="IN"):
        return engine.Job("job1", "cs", ["tok"], country, "off", 2, "pool", 1,
                          filter_proxies)

    def task(self):
        return engine.Task(task_id="t1", index=0, token="tok", email="a@b.c")

    def test_filter_off_returns_pool_proxy_without_probing(self):
        job = self.job(filter_proxies=False)
        proxies = ["http://one@h:1", "http://two@h:2"]
        with patch("ippure.check", side_effect=AssertionError("must not probe")) as check:
            proxy, seed, verdict = engine._acquire_proxy(job, self.task(), proxies)
        check.assert_not_called()
        self.assertEqual((proxy, verdict), (proxies[0], {}))
        self.assertFalse(job.filter_proxies)

    def test_filter_off_still_writes_a_per_task_seed_file(self):
        # Backend chi nhan seed_file chu khong nhan chuoi proxy: tra ve file pool thi
        # flow tu boc proxy bat ky -> phot lo proxy da chia va IP hien thi sai.
        job = self.job(filter_proxies=False)
        proxies = [f"http://p{i}@h:{i}" for i in range(1, 9)]
        proxy, seed, _ = engine._acquire_proxy(job, self.task(), proxies)
        lines = seed.read_text().splitlines()
        self.assertEqual(seed.name, "web_seed_job1_t1.txt")
        self.assertEqual(lines[0], proxy)
        self.assertLessEqual(len(lines), 6)
        self.assertNotEqual(seed.name, f"web_proxies_{job.job_id}.txt")

    def test_filter_on_keeps_the_probe_path(self):
        good = {"ok": True, "country": "IN", "flags": [], "grade": "A", "score": 90,
                "label": "proxy#uniq1", "risk": 5, "user_type": "residential",
                "latency_ms": 900}
        proxy_url = "http://unique-filter-test@h:9"
        with patch("ippure.enabled", return_value=True), \
             patch("ippure.check", return_value=good) as check, \
             patch("extract_cs.billing_address_ok", return_value=(True, "")):
            proxy, seed, verdict = engine._acquire_proxy(self.job(), self.task(),
                                                        [proxy_url])
        check.assert_called_once()
        self.assertEqual(proxy, proxy_url)
        self.assertEqual(verdict, good)

    def test_telemetry_flag_reaches_the_backend_request(self):
        job = self.job(filter_proxies=True)
        job.stripe_telemetry = True
        task = self.task()
        req = engine._backend_request(job, task, "http://p@h:1", Path("seed.txt"))
        self.assertTrue(req.stripe_telemetry)
        job.stripe_telemetry = False
        req = engine._backend_request(job, task, "http://p@h:1", Path("seed.txt"))
        self.assertFalse(req.stripe_telemetry)

    def test_prepare_labels_the_job_when_the_filter_is_off(self):
        _, _, label, report = engine._prepare_proxies(
            "job1", "us.arxlabs.io:3010:acct-region-IN-sid-aaaa-t-5:pw", 1, True, False)
        self.assertIn("no filter", label)
        self.assertEqual(report["mode"], "off")
        _, _, on_label, on_report = engine._prepare_proxies(
            "job1", "us.arxlabs.io:3010:acct-region-IN-sid-aaaa-t-5:pw", 1, True, True)
        self.assertNotIn("no filter", on_label)
        self.assertNotEqual(on_report["mode"], "off")


class RetryPolicyTests(unittest.TestCase):
    """403 ở bước tạo checkout -> không retry (retry chỉ tốn proxy)."""

    def finish(self, err):
        job = engine.Job("job1", "cs", ["tok"], "IN", "off", 1, "pool", 1, True)
        task = engine.Task(task_id="t1", index=0, token="tok", email="a@b.c")
        engine._finish(job, task, {"status": "FAIL", "err": err}, time.time())
        return task

    def test_cf_challenge_marks_no_retry(self):
        """Nhãn mới (Cloudflare challenge) vẫn phải bị đánh no_retry như nhãn 403 cũ."""
        t = self.finish("Cloudflare chặn tạo checkout (HTTP 403, cf-mitigated=challenge) — "
                        "proxy IP bị chấm xấu, chưa hề chạm Stripe")
        self.assertTrue(t.no_retry)

    def test_checkout_403_marks_no_retry(self):
        t = self.finish("OpenAI blocked checkout creation (HTTP 403) — never reached Stripe")
        self.assertTrue(t.no_retry)
        self.assertIn("403", t.no_retry_reason)

    def test_stall_at_approve_is_not_retried_by_default(self):
        # Đo 3.965 task có retry trong log còn giữ: 0 lần ra link, 82,2% lặp y hệt lỗi cũ.
        t = self.finish("no link — flow stopped at step 'approve' | last log: x")
        self.assertTrue(t.no_retry)
        self.assertIn("approve", t.no_retry_reason)

    def test_stall_retry_can_be_switched_back_on(self):
        with patch.dict(os.environ, {"UPI_RETRY_APPROVE_STALL": "1"}):
            t = self.finish("no link — flow stopped at step 'approve' | last log: x")
        self.assertFalse(t.no_retry)

    def test_already_paid_and_hosted_only_are_not_retried(self):
        self.assertTrue(self.finish("User is already paid").no_retry)
        t = self.finish("no QR link extracted — only a hosted checkout link (https://x)")
        self.assertTrue(t.no_retry)

    def test_billing_country_mismatch_is_account_level(self):
        # OpenAI 400 "Billing country must match request country": 4.607 account dính, chỉ
        # 0,5% từng ra mã (so với ~3% chung) -> không đáng retry.
        t = self.finish('checkout creation failed HTTP 400: {"detail":'
                        '"Billing country must match request country."}')
        self.assertTrue(t.no_retry)
        self.assertIn("region", t.no_retry_reason)

    def test_transport_and_timeout_are_still_retried(self):
        self.assertFalse(self.finish("proxy transport error at 'provider' stage").no_retry)
        self.assertFalse(self.finish("timeout 240s").no_retry)
        self.assertFalse(self.finish("no billing address from the exit IP").no_retry)

    def test_risk_decline_keeps_its_own_reason(self):
        job = engine.Job("job1", "cs", ["tok"], "IN", "off", 1, "pool", 1, True)
        task = engine.Task(task_id="t1", index=0, token="tok", email="a@b.c")
        engine._finish(job, task, {"status": "FAIL", "err": "risk decline",
                                   "risk_decline": True}, time.time())
        self.assertEqual(task.no_retry_reason, "Stripe risk decline (account flagged)")


class FailedStepMarkingTests(unittest.TestCase):
    """Card phải chỉ ra BƯỚC ĐÃ CHẾT, kể cả khi không bước nào đang 'active'."""

    def test_first_pending_step_is_marked_failed(self):
        job = engine.Job("j1", "cs", ["tok"], "IN", "off", 1, "pool", 1, True)
        task = engine.Task(task_id="t1", index=0, token="tok", email="a@b.c")
        task.steps = [{"key": k, "label": lb, "status": "pending", "detail": "", "err": ""}
                      for k, lb in ((st["key"], st["label"]) for st in engine.cli.steps_for("cs"))][:4]
        task.steps[0]["status"] = "done"          # runner xong
        task.steps[1]["status"] = "skip"          # không dùng
        events = []
        job.emit = events.append
        engine._finish(job, task, {"status": "FAIL", "err": "checkout creation failed HTTP 400"}, time.time())
        failed = [s["key"] for s in task.steps if s["status"] == "fail"]
        self.assertEqual(failed, ["promo"])       # bước pending đầu tiên = chỗ chết
        self.assertTrue(any(e.get("status") == "fail" for e in events if e["type"] == "step"))

    def test_active_step_still_wins(self):
        job = engine.Job("j1", "cs", ["tok"], "IN", "off", 1, "pool", 1, True)
        task = engine.Task(task_id="t1", index=0, token="tok", email="a@b.c")
        task.steps = [{"key": "runner", "label": "r", "status": "active", "detail": "", "err": ""},
                      {"key": "checkout", "label": "c", "status": "pending", "detail": "", "err": ""}]
        job.emit = lambda ev: None
        engine._finish(job, task, {"status": "FAIL", "err": "x"}, time.time())
        self.assertEqual([s["key"] for s in task.steps if s["status"] == "fail"], ["runner"])


class AddressPrecheckTests(unittest.TestCase):
    """Proxy không dựng được địa chỉ hoá đơn thì loại NGAY lúc chọn proxy.

    Nếu để tới bước điền billing mới phát hiện thì task đã chạy hết checkout, và retry là
    phải chạy lại cả flow — đắt như dính risk decline. Kiểm ở đây chỉ tốn ~1-3s (có cache).
    """

    def setUp(self):
        engine._ADDRESS_OK.clear()
        self.enterContext(patch.object(engine, "_address_check_enabled", return_value=True))

    def tearDown(self):
        self.doCleanups()

    def test_bad_proxy_is_rejected_and_logged(self):
        job = engine.Job("j1", "cs", ["tok"], "IN", "off", 1, "pool", 1, True)
        task = engine.Task(task_id="t1", index=0, token="tok", email="a@b.c")
        events = []
        job.emit = events.append
        with patch("extract_cs.billing_address_ok", return_value=(False, "geo lookup failed")):
            self.assertFalse(engine._address_usable(job, task, "http://bad@h:1"))
        lines = [e.get("line", "") for e in events if e.get("type") == "log"]
        self.assertTrue(any("không dựng được địa chỉ" in l for l in lines), lines)

    def test_good_proxy_passes_and_is_cached(self):
        job = engine.Job("j1", "cs", ["tok"], "IN", "off", 1, "pool", 1, True)
        task = engine.Task(task_id="t1", index=0, token="tok", email="a@b.c")
        job.emit = lambda ev: None
        with patch("extract_cs.billing_address_ok", return_value=(True, "")) as check:
            self.assertTrue(engine._address_usable(job, task, "http://good@h:2"))
            self.assertTrue(engine._address_usable(job, task, "http://good@h:2"))
        self.assertEqual(check.call_count, 1)          # lần 2 lấy từ cache

    def test_check_can_be_disabled(self):
        job = engine.Job("j1", "cs", ["tok"], "IN", "off", 1, "pool", 1, True)
        task = engine.Task(task_id="t1", index=0, token="tok", email="a@b.c")
        job.emit = lambda ev: None
        with patch.object(engine, "_address_check_enabled", return_value=False), \
             patch("extract_cs.billing_address_ok", return_value=(False, "x")) as check:
            self.assertTrue(engine._address_usable(job, task, "http://bad@h:1"))
        check.assert_not_called()


class AddressRetryWithAnotherProxyTests(unittest.TestCase):
    """Không dựng được địa chỉ = lỗi CHỌN PROXY -> đổi proxy chạy lại, không bỏ task."""

    def test_error_detection(self):
        self.assertTrue(engine._is_address_error(
            "no billing address from the exit IP (geo lookup failed)"))
        self.assertFalse(engine._is_address_error("Stripe risk decline"))
        self.assertFalse(engine._is_address_error(None))


class StepProgressTests(unittest.TestCase):
    """Tiến độ bước phải ĐƠN ĐIỆU: không còn ô "pending" nằm trước ô đã xong.

    Lỗi cũ chỉ có ở chế độ in-process (desk): luồng subprocess có
    `cli.make_step_advancer` riêng, còn in-process thì không -> UI hiện ✓ ở giữa mà phía
    trên vẫn trống, nhìn như "step hiện tùm lùm, không theo kịp log".
    """

    def hooks(self):
        job = engine.Job("job1", "cs", ["tok"], "IN", "off", 1, "pool", 1, True)
        task = engine.Task(task_id="t1", index=0, token="tok", email="a@b.c")
        task.steps = [{"key": st["key"], "label": st["label"], "status": "pending",
                       "detail": "", "err": ""} for st in engine.cli.steps_for("cs")]
        events = []
        job.emit = events.append
        step, _, _ = engine._mk_hooks(job, task)
        return task, step, events

    def test_no_pending_step_before_a_finished_one(self):
        task, step, events = self.hooks()
        step("runner", "active")
        for line in ("Checkout promo: mode=campaign",
                     "IN Bootstrap Stripe init ok, amount=INR 0.00",
                     "Step 2: first attempt PM=IN...",
                     "approve attempt 1/1 / proxy=proxy#x"):
            hit = engine.cli.cs_stage_for_line(line)
            self.assertIsNotNone(hit, line)
            step(hit[0], "done" if hit[1] else "active")
        keys = [s["key"] for s in task.steps]
        done_at = max(i for i, s in enumerate(task.steps) if s["status"] == "done")
        pending_before = [s["key"] for s in task.steps[:done_at] if s["status"] == "pending"]
        self.assertEqual(pending_before, [], keys)
        # và bước bỏ qua phải được phát event để UI cập nhật ngay
        skipped = [e["key"] for e in events if e.get("status") == "skip"]
        self.assertIn("tax", skipped)
        self.assertIn("pm", skipped)
        # runner (bước đầu) không bao giờ bị đánh skip
        self.assertNotIn("runner", skipped)

    def test_steps_stay_monotonic_across_a_retry(self):
        task, step, _ = self.hooks()
        step("runner", "active")
        step("checkout", "done")
        job = SimpleNamespace(mode="cs", emit=lambda ev: None)
        engine._reset_task_for_retry(job, task)
        self.assertTrue(all(s["status"] == "pending" for s in task.steps))
        step("stripe_init", "done")
        done_at = max(i for i, s in enumerate(task.steps) if s["status"] == "done")
        # checkout (bước 2) bị bỏ qua ở vòng này -> 'skip', chỉ còn runner (bước đầu, không
        # bao giờ tự đánh skip) là pending; nhưng nó KHÔNG nằm trước ô done nào khác.
        self.assertEqual([s["key"] for s in task.steps[:done_at]
                          if s["status"] == "pending"], ["runner"])


class BillingTierTests(unittest.TestCase):
    """Đơn nào dùng địa chỉ list cứng thì phải thấy được trong kết quả."""

    def task(self):
        return engine.Task(task_id="t1", index=0, token="tok", email="a@b.c")

    def test_tier_is_recorded_once_per_attempt(self):
        t = self.task()
        engine._absorb_billing(t, "billing geo: Karnataka / PIN 575001 / exit 1.2.3.4 (IN)")
        self.assertEqual(t.billing_tier, "tier1")
        # dòng sau (vd tier 2 của lần chạy khác) không ghi đè trong cùng lần chạy
        engine._absorb_billing(t, "billing tier 2 (PIN verified) 1.2.3.4 / Patna / BR 800027")
        self.assertEqual(t.billing_tier, "tier1")

    def test_hardcoded_list_tier(self):
        t = self.task()
        engine._absorb_billing(t, "billing using the hardcoded list (Mumbai) — neither tier 2 nor tier 1")
        self.assertEqual(t.billing_tier, "hardcoded-list")
        self.assertEqual(t.to_dict()["billing_tier"], "hardcoded-list")

    def test_retry_clears_tier(self):
        t = self.task()
        engine._absorb_billing(t, "billing tier 2 (PIN verified) 1.2.3.4 / Patna / BR 800027")
        job = SimpleNamespace(mode="cs", emit=lambda ev: None)
        engine._reset_task_for_retry(job, t)
        self.assertEqual(t.billing_tier, "")


class NetUsageTests(unittest.TestCase):
    """Đo dung lượng proxy mỗi acc: byte vào/ra + số request (bọc ở tầng class)."""

    @staticmethod
    def _stub_transport(body: bytes):
        import requests
        from requests.models import Response

        def fake_send(self, request, **kwargs):
            resp = Response()
            resp.status_code = 200
            resp._content = body
            resp.headers["Content-Length"] = str(len(body))
            resp.request = request
            return resp

        return patch.object(requests.adapters.HTTPAdapter, "send", fake_send)

    def test_counter_covers_module_level_calls_and_sessions(self):
        import requests
        import extract_cs as ex

        with self._stub_transport(b"y" * 2048):
            with ex.execution_context({"UPI_TOKEN": "t"}):
                # gọi thẳng requests.post (cli.py làm vậy cho Stripe) ...
                requests.post("https://api.stripe.com/v1/payment_pages/x/init", data={"k": "v"})
                # ... và qua session tự tạo (không đi qua new_session)
                requests.Session().get("https://chatgpt.com/backend-api/me")
                usage = ex.net_usage()
        self.assertEqual(usage["in"], 4096)          # 2 request × 2048 byte
        self.assertEqual(usage["requests"], 2)
        self.assertGreater(usage["out"], 0)          # body gửi đi cũng phải được đếm
        self.assertEqual(ex.net_usage(), {"out": 0, "in": 0, "requests": 0})

    def test_counter_covers_both_http_clients(self):
        # curl_cffi là client chính của flow, requests là client phụ (cli.py/geobilling).
        import requests
        import extract_cs as ex
        with ex.execution_context({}):
            pass
        self.assertTrue(getattr(requests.sessions.Session.request, "_net_counting", False))
        try:
            from curl_cffi.requests import Session as CurlSession
        except ImportError:
            self.skipTest("curl_cffi không có")
        self.assertTrue(getattr(CurlSession.request, "_net_counting", False))

    def test_size_helper_understands_json_bodies(self):
        import extract_cs as ex
        self.assertEqual(ex._count_request_bytes(b"abc"), 3)
        self.assertEqual(ex._count_request_bytes("abcd"), 4)
        self.assertGreater(ex._count_request_bytes({"a": "b"}), 5)
        self.assertEqual(ex._count_request_bytes(None), 0)

    def test_finish_stores_usage_on_the_task(self):
        job = engine.Job("job1", "cs", ["tok"], "IN", "off", 1, "pool", 1, True)
        task = engine.Task(task_id="t1", index=0, token="tok", email="a@b.c")
        engine._finish(job, task, {"status": "FAIL", "err": "x",
                                   "net_bytes_out": 1234, "net_bytes_in": 567890,
                                   "net_requests": 42}, time.time())
        self.assertEqual((task.net_out, task.net_in, task.net_requests), (1234, 567890, 42))
        self.assertEqual(task.to_dict()["net_in"], 567890)


class CheckpointThreadTests(unittest.TestCase):
    """Checkpoint phải do thread nền ghi, không giữ slot worker.

    Vì sao có test này: trước đây worker tự gọi `write_results(checkpoint=True)` trong
    `finally`; với job 6.000 task mỗi lần ghi tốn 0,1-0,3 s nên "Running" tụt dưới số worker
    (đo thật: đặt 300 worker, UI hiện 288). Test khoá lại hành vi: worker chỉ bật cờ, thread
    nền ghi file.
    """

    def test_request_checkpoint_writes_results_from_background_thread(self):
        job = engine.Job("cp0001", "cs", ["tok-1"], "IN", "off", 2, "test", 1)
        with tempfile.TemporaryDirectory() as tmp:
            job.results_path = Path(tmp) / "web_cp0001_results.json"
            job._last_checkpoint = 0.0
            job.start_checkpoint_thread()
            job.request_checkpoint()
            for _ in range(40):
                if job.results_path.exists():
                    break
                time.sleep(0.05)
            self.assertTrue(job.results_path.exists(), "thread nền phải ghi results.json")
            data = json.loads(job.results_path.read_text())
            self.assertEqual(data["job_id"], "cp0001")
            self.assertEqual(data["total"], 1)
            job.status = "done"   # để thread nền tự thoát
            job.close_log()

    def test_checkpoint_is_throttled_by_interval(self):
        job = engine.Job("cp0002", "cs", ["tok-1"], "IN", "off", 2, "test", 1)
        with tempfile.TemporaryDirectory() as tmp:
            job.results_path = Path(tmp) / "web_cp0002_results.json"
            job._last_checkpoint = time.monotonic()   # vừa ghi xong
            job.write_results(checkpoint=True)
            self.assertFalse(job.results_path.exists(), "chưa qua CHECKPOINT_SECS thì chưa ghi")
            job.close_log()


if __name__ == "__main__":
    unittest.main()
