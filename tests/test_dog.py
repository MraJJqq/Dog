import unittest
from datetime import datetime,timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo
import dog

UTC=ZoneInfo("UTC")
def stamp(value): return value.astimezone(UTC).isoformat()

class DogMonitorTests(unittest.TestCase):
    def setUp(self):
        self.now=datetime(2026,11,23,0,8,tzinfo=UTC)
        self.state={"schema_version":2,"runs":{},"slots":{}}

    def make_run(self,status="queued",age=0,conclusion=None):
        created=self.now-timedelta(seconds=age)
        started=created if status=="in_progress" else None
        return {"id":123,"event":"workflow_dispatch","status":status,
                "conclusion":conclusion,"run_attempt":1,
                "created_at":stamp(created),
                "run_started_at":stamp(started) if started else None,
                "updated_at":stamp(self.now),"display_title":"Vegasbark manual-test"}

    def test_queue_waits_until_six_minutes(self):
        self.assertIsNone(dog.monitor_run(self.make_run(age=359),self.state,self.now))
        self.assertEqual(dog.monitor_run(self.make_run(age=360),self.state,self.now+timedelta(seconds=1)),("cancel",123))

    def test_cancel_retries_after_six_minutes(self):
        self.state["runs"]["123"]={"attempt":1,"cancel_requested_attempt":1,
            "cancel_requested_at":stamp(self.now-timedelta(seconds=360)),
            "queued_since":stamp(self.now-timedelta(seconds=720))}
        self.assertEqual(dog.monitor_run(self.make_run(age=720),self.state,self.now),("cancel",123))

    def test_running_over_six_minutes_is_cancelled(self):
        self.assertEqual(dog.monitor_run(self.make_run("in_progress",361),self.state,self.now),("cancel",123))

    def test_failed_run_is_retried(self):
        self.assertEqual(dog.monitor_run(self.make_run("completed",400,"failure"),self.state,self.now),("rerun",123))

    def test_successful_run_is_not_retried(self):
        self.assertIsNone(dog.monitor_run(self.make_run("completed",400,"success"),self.state,self.now))

    def test_manual_and_scheduled_scans_are_monitored(self):
        self.assertTrue(dog.is_scan_run({"event":"workflow_dispatch"}))
        self.assertTrue(dog.is_scan_run({"event":"schedule"}))
        self.assertFalse(dog.is_scan_run({"event":"push"}))

    def test_slot_recovery_waits_six_minutes(self):
        slot=datetime(2026,11,23,8,2,tzinfo=ZoneInfo("Asia/Shanghai"))
        key,task=dog._slot_key(slot),dog._task_id(slot)
        state={"schema_version":2,"runs":{},"slots":{
            key:{"task_id":task,"dispatched_at":stamp(self.now-timedelta(seconds=359))}}}
        with patch.object(dog,"SLOTS",("08:02",)):
            self.assertEqual(dog.slot_recovery([],state,self.now),[])
            state["slots"][key]["dispatched_at"]=stamp(self.now-timedelta(seconds=360))
            self.assertEqual(dog.slot_recovery([],state,self.now),[task])

    def test_recovery_run_matches_task_id(self):
        slot=datetime(2026,11,23,8,2,tzinfo=ZoneInfo("Asia/Shanghai"))
        task=dog._task_id(slot)
        self.assertTrue(dog._run_matches_slot({"event":"workflow_dispatch","display_title":f"Vegasbark {task}"},slot,task))

    def test_native_schedule_runs_map_to_non_overlapping_slots(self):
        first=datetime(2026,11,23,8,2,tzinfo=ZoneInfo("Asia/Shanghai"))
        second=datetime(2026,11,23,10,2,tzinfo=ZoneInfo("Asia/Shanghai"))
        delayed={"event":"schedule","created_at":stamp(first+timedelta(minutes=58))}
        next_slot={"event":"schedule","created_at":stamp(second+timedelta(seconds=10))}
        self.assertTrue(dog._run_matches_slot(delayed,first,dog._task_id(first)))
        self.assertFalse(dog._run_matches_slot(delayed,second,dog._task_id(second)))
        self.assertTrue(dog._run_matches_slot(next_slot,second,dog._task_id(second)))
        self.assertFalse(dog._run_matches_slot(next_slot,first,dog._task_id(first)))

    def test_non_success_run_search_splits_past_api_limit(self):
        from datetime import date
        first=[{"id":i} for i in range(1000)]
        with patch("dog._list_runs",side_effect=[first,[{"id":"left"}],[{"id":"right"}]]) as search:
            runs=dog._list_status_runs("failure",date(2026,1,1),date(2026,1,2))
        self.assertEqual([r["id"] for r in runs],["left","right"])
        self.assertEqual(search.call_count,3)

    def test_unchanged_state_does_not_create_a_commit(self):
        import json
        state={"schema_version":2,"runs":{},"slots":{}}
        dog.STATE_CONTENT=json.dumps(state,ensure_ascii=False,indent=2)+"\n"
        with patch("dog.api") as api:
            dog.save_state(state,"existing-sha")
        api.assert_not_called()

    def test_legacy_state_migrates_pending_slot(self):
        state=dog.normalize_state({"task_id":"auto:2026-11-23:16:02",
            "slot":"2026-11-23T16:02:00+08:00",
            "dispatch_requested_at":"2026-11-23T18:48:35+08:00"})
        self.assertIn("2026-11-23 16:02",state["slots"])
        self.assertNotIn("task_id",state)

if __name__=="__main__":
    unittest.main()
