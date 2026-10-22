"""Vòng dò link nền: link phải được dò TIẾP cả khi job đã xong / desk đã restart.

Đo thật 16/10 (job `3dcd91d4f58a`, 5.859 acc): 7 link, lần dò cuối cách 32–42 phút; 2 link
đã được khách duyệt (`intent_state=succeeded`) mà UI vẫn ghi "Awaiting payment". Hai nguyên
nhân: (1) vòng dò bỏ job sau 30 phút, (2) job chỉ sống trong RAM nên restart desk là mất →
sổ theo dõi bền vững (`cs_state/link_watch.json`) là nguồn sự thật.
"""
import json
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "web"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import engine  # noqa: E402

LINK = "https://payments.stripe.com/upi/instructions/abc123"
JOB = "job-abcdef123456"


def proxy_kwargs(seen: dict):
    """side_effect cho artifact_probe.probe: ghi lại kwargs rồi trả về trạng thái waiting."""
    def _probe(*_a, **kwargs):
        seen.update(kwargs)
        return {"ok": True, "status": "waiting"}
    return _probe


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="upi-ledger-"))
        self._path = engine.LINK_LEDGER_PATH
        engine.LINK_LEDGER_PATH = self.tmp / "link_watch.json"
        with engine._LINK_LEDGER_LOCK:
            engine._LINK_LEDGER.clear()
        engine._LINK_LEDGER_LOADED = False
        self.jobs_backup = dict(engine._JOBS)

    def tearDown(self):
        engine.LINK_LEDGER_PATH = self._path
        with engine._LINK_LEDGER_LOCK:
            engine._LINK_LEDGER.clear()
        engine._LINK_LEDGER_LOADED = False
        engine._JOBS.clear()
        engine._JOBS.update(self.jobs_backup)

    def upsert(self, task_id="t1", state="waiting", checked_at=None, job_id=JOB,
               proxy="http://user:pass@px:8080", link=LINK):
        stamp = int(time.time()) if checked_at is None else checked_at
        entry = engine.link_watch_upsert(job_id, task_id, link, proxy, state=state,
                                         checked_at=stamp, first_seen=stamp)
        engine._ledger_save()
        return entry

    def targets(self):
        return engine._link_monitor_targets()

    # --- danh sách cần dò -------------------------------------------------
    def test_fresh_link_is_hot(self):
        self.upsert(checked_at=int(time.time()) - 10)
        hot, cold = self.targets()
        self.assertEqual((len(hot), len(cold)), (1, 0))

    def test_link_of_a_finished_job_is_cold_but_still_tracked(self):
        """Ca bị bỏ đói trước đây: link đã 2 tiếng -> vẫn phải nằm trong danh sách."""
        self.upsert(checked_at=int(time.time()) - 2 * 3600)
        hot, cold = self.targets()
        self.assertEqual(hot, [])
        self.assertEqual(len(cold), 1)
        self.assertEqual(cold[0]["link"], LINK)

    def test_gives_up_after_the_window(self):
        self.upsert(checked_at=int(time.time()) - int(engine.MONITOR_MAX_IDLE_MIN * 60) - 3600)
        self.assertEqual(self.targets(), ([], []))
        self.assertGreaterEqual(engine.MONITOR_MAX_IDLE_MIN, 12 * 60)      # >= nửa ngày

    def test_terminal_links_are_skipped(self):
        for state in ("succeeded", "failed", "canceled", "expired"):
            self.upsert(task_id="t-" + state, state=state)
        self.assertEqual(self.targets(), ([], []))

    def test_non_link_entries_are_skipped(self):
        engine.link_watch_upsert(JOB, "t9", "https://example.com/x", "")
        self.assertEqual(self.targets(), ([], []))

    # --- dò và cập nhật ---------------------------------------------------
    def test_monitor_updates_ledger_even_without_the_job_in_ram(self):
        """Job cũ (không còn trong RAM) vẫn phải được dò và ghi lại trạng thái."""
        entry = self.upsert(checked_at=int(time.time()) - 7200)
        with patch.object(engine.artifact_probe, "probe",
                          return_value={"ok": True, "status": "succeeded", "fam": "1.00"}):
            engine._monitor_one(entry)
        saved = engine.link_watch_get(JOB, "t1")
        self.assertEqual(saved["state"], "succeeded")
        self.assertGreater(saved["checked_at"], entry["checked_at"])
        on_disk = json.loads(engine.LINK_LEDGER_PATH.read_text())[f"{JOB}|t1"]
        self.assertEqual(on_disk["state"], "succeeded")

    def test_inconclusive_probe_keeps_previous_state(self):
        entry = self.upsert(state="waiting", checked_at=int(time.time()) - 7200)
        with patch.object(engine.artifact_probe, "probe",
                          return_value={"ok": False, "error": "http_503"}):
            engine._monitor_one(entry)
        saved = engine.link_watch_get(JOB, "t1")
        self.assertEqual(saved["state"], "waiting")
        self.assertGreater(saved["checked_at"], entry["checked_at"])

    def test_probe_uses_the_stored_proxy(self):
        entry = self.upsert(checked_at=int(time.time()) - 7200)
        seen: dict = {}
        with patch.object(engine.artifact_probe, "probe", side_effect=proxy_kwargs(seen)):
            engine._monitor_one(entry)
        self.assertEqual(seen.get("proxy"), "http://user:pass@px:8080")

    def test_unchanged_state_still_reports_the_check_time(self):
        emitted: list[dict] = []
        job = SimpleNamespace(job_id=JOB, _lock=threading.RLock(), tasks={})
        task = SimpleNamespace(task_id="t1", link_state="waiting", link_fam="", link_zero=False,
                               link_note="", link_checked_at=0, link_first_seen=0)
        job.tasks["t1"] = task
        job.emit = emitted.append
        job.log_line = lambda *_a, **_k: None
        engine._JOBS[JOB] = job
        entry = self.upsert(checked_at=int(time.time()) - 7200)
        seen: dict = {}
        with patch.object(engine.artifact_probe, "probe", side_effect=proxy_kwargs(seen)):
            engine._monitor_one(entry)
        self.assertEqual([e["type"] for e in emitted], ["task_link_checked"])
        self.assertGreater(task.link_checked_at, 0)

    def test_state_flip_notifies_the_ui_and_flips_the_task(self):
        emitted: list[dict] = []
        job = SimpleNamespace(job_id=JOB, _lock=threading.RLock(), tasks={})
        task = SimpleNamespace(task_id="t1", link_state="waiting", link_fam="", link_zero=False,
                               link_note="", link_checked_at=0, link_first_seen=0,
                               status="done", no_retry=False, plus_state="", plus_note="",
                               plus_checked_at=0, token="tok", egress=None,
                               index=0)
        job.tasks["t1"] = task
        job.emit = emitted.append
        job.log_line = lambda *_a, **_k: None
        engine._JOBS[JOB] = job
        entry = self.upsert(checked_at=int(time.time()) - 7200)
        with patch.object(engine.artifact_probe, "probe",
                          return_value={"ok": True, "status": "succeeded", "fam": "1.00"}):
            engine._monitor_one(entry)
        self.assertEqual(task.link_state, "succeeded")
        # `task_link` (đổi trạng thái) + `task_plus` (bắt đầu dò Plus vì khách đã duyệt)
        self.assertEqual([e["type"] for e in emitted][0], "task_link")

    def test_network_error_retries_with_another_proxy(self):
        """Proxy ghim lỗi TLS thì phải dò hộ bằng proxy khác đã từng đọc được."""
        # một entry "tốt" khác để lấy proxy dự phòng
        self.upsert(task_id="tB", checked_at=int(time.time()) - 600,
                    proxy="http://user:pass@good:9090")
        engine.link_watch_upsert(JOB, "tB", LINK, "http://user:pass@good:9090",
                                 state="succeeded", checked_at=int(time.time()),
                                 proxy_ok_at=int(time.time()))
        engine._ledger_save()
        bad = self.upsert(checked_at=int(time.time()) - 7200,
                          proxy="http://user:pass@bad:8080")

        calls: list[str] = []

        def fake_probe(_url, **_kw):
            calls.append(str(_kw.get("proxy")))
            if "@bad" in str(_kw.get("proxy")):
                return {"ok": False, "error": "SSLError"}
            return {"ok": True, "status": "succeeded", "fam": "1.00"}

        with patch.object(engine.artifact_probe, "probe", side_effect=fake_probe):
            engine._monitor_one(bad)
        saved = engine.link_watch_get(JOB, "t1")
        self.assertEqual(saved["state"], "succeeded")
        # đã đổi sang proxy đọc được để lần sau khỏi lỗi lại
        self.assertEqual(saved["proxy"], "http://user:pass@good:9090")
        self.assertGreater(saved.get("proxy_ok_at") or 0, 0)

    def test_link_registered_even_when_first_probe_fails(self):
        """Link phải vào SỔ ngay cả khi lần dò đầu lỗi — nếu không nó bị bỏ đói vĩnh viễn."""
        emitted: list[dict] = []
        job = SimpleNamespace(job_id=JOB, _lock=threading.RLock(), tasks={})
        task = SimpleNamespace(task_id="t1", link_state="", link_fam="", link_zero=False,
                               link_note="", link_checked_at=0, link_first_seen=0,
                               error="", raw_status="PASS", proxy="http://user:pass@px:8080",
                               egress=None, plus_state="", plus_note="", plus_checked_at=0,
                               token="tok", index=0)
        job.tasks["t1"] = task
        job.emit = emitted.append
        job.log_line = lambda *_a, **_k: None
        engine._JOBS[JOB] = job
        with patch.object(engine.artifact_probe, "probe",
                          return_value={"ok": False, "error": "SSLError"}):
            engine._verify_link(job, task, {}, LINK)
        saved = engine.link_watch_get(JOB, "t1")
        self.assertTrue(saved)
        self.assertEqual(saved["proxy"], "http://user:pass@px:8080")
        self.assertEqual(saved["link"], LINK)
        self.assertEqual(task.link_state, "")          # chưa kết luận, nhưng ĐÃ được theo dõi
        self.assertIn("unverified", task.error)

    # --- UI đọc trạng thái từ sổ -----------------------------------------
    def test_snapshot_overlay_shows_the_new_link_state(self):
        self.upsert(state="succeeded")
        snapshot = {"job_id": JOB,
                    "tasks": [{"task_id": "t1", "link_state": "waiting", "link_checked_at": 1},
                              {"task_id": "t2", "link_state": "waiting"}]}
        out = engine.link_watch_overlay(snapshot)
        self.assertEqual(out["tasks"][0]["link_state"], "succeeded")
        self.assertEqual(out["tasks"][0]["link_checked_at"],
                         engine.link_watch_get(JOB, "t1")["checked_at"])
        self.assertEqual(out["tasks"][1]["link_state"], "waiting")     # task lạ -> giữ nguyên

    def test_ledger_survives_reload(self):
        self.upsert(state="waiting")
        with engine._LINK_LEDGER_LOCK:
            engine._LINK_LEDGER.clear()
        engine._LINK_LEDGER_LOADED = False
        self.assertEqual(engine.link_watch_get(JOB, "t1")["state"], "waiting")


if __name__ == "__main__":
    unittest.main()
