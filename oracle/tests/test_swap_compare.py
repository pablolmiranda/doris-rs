"""swap/swap_compare.py: what the FE shows before and after a swap must be the same, except clocks,
the binary's version and the declared deployment facts; and the FE must not have sent DROP or
CLONE tasks to repair anything."""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "swap"))
import swap_compare as compare  # noqa: E402

TABLET = {
    "TabletId": "11", "ReplicaId": "12", "BackendId": "13", "SchemaHash": "14", "Version": "1",
    "LstSuccessVersion": "1", "LstFailedVersion": "-1", "LstFailedTime": "NULL", "LocalDataSize": "0",
    "RowCount": "0", "State": "NORMAL", "LstConsistencyCheckTime": "NULL", "QueryHits": "0",
    "PathHash": "9040051238759044505", "MetaUrl": "http://0.250.250.254:8040/api/meta/header/11",
}
BACKEND = {
    "BackendId": "13", "Host": "0.250.250.254", "HeartbeatPort": "9050", "Alive": "true", "TabletNum": "7",
    "LastStartTime": "2026-11-05 10:00:00", "LastHeartbeat": "2026-11-05 10:00:05", "Version": "doris-4.1.5",
    "CpuCores": "8", "Memory": "16.00 GB", "TotalCapacity": "926 GB", "AvailCapacity": "79 GB",
    "DataUsedCapacity": "0.000", "TrashUsedCapacity": "0.000", "UsedPct": "91 %", "MaxDiskUsedPct": "91 %",
    "Status": "{}", "ErrMsg": "",
}


def snap(tablet=None, backend=None, tasks=()):
    return {"tablets": {"m3.dup": [dict(TABLET, **(tablet or {}))]}, "backend": dict(BACKEND, **(backend or {})), "tasks": list(tasks)}


class SwapCompareTests(unittest.TestCase):
    def test_identical_snapshots_pass(self):
        self.assertEqual(compare.differences(snap(), snap()), [])

    def test_a_changed_tablet_field_fails(self):
        for field in ("TabletId", "ReplicaId", "BackendId", "SchemaHash", "Version", "PathHash", "State"):
            diffs = compare.differences(snap(), snap(tablet={field: "999"}))
            self.assertTrue(diffs and field in diffs[0], f"{field}: {diffs}")

    def test_a_missing_tablet_fails(self):
        after = snap()
        after["tablets"]["m3.dup"] = []
        self.assertTrue(compare.differences(snap(), after))

    def test_clocks_versions_and_facts_are_ignored(self):
        changed = {f: "x" for f in ("LastStartTime", "LastHeartbeat", "Version", "CpuCores", "Memory", "TotalCapacity",
                                    "AvailCapacity", "DataUsedCapacity", "TrashUsedCapacity", "UsedPct", "MaxDiskUsedPct", "Status")}
        tablet = {"LstFailedTime": "x", "LstConsistencyCheckTime": "x", "QueryHits": "5"}
        self.assertEqual(compare.differences(snap(), snap(tablet=tablet, backend=changed)), [])

    def test_a_backend_field_change_fails(self):
        for field in ("BackendId", "Host", "HeartbeatPort", "Alive", "TabletNum"):
            diffs = compare.differences(snap(), snap(backend={field: "x"}))
            self.assertTrue(diffs and field in diffs[0], f"{field}: {diffs}")

    def test_a_drop_or_clone_task_fails(self):
        for task in ("DROP", "CLONE"):
            diffs = compare.differences(snap(), snap(tasks=["REPORT", task]))
            self.assertTrue(any(task in d for d in diffs), f"{task}: {diffs}")
        self.assertEqual(compare.differences(snap(), snap(tasks=["PUBLISH_VERSION"])), [])

    def test_tasks_are_read_from_both_logs(self):
        rs = "doris-rs be: listening\ndoris-rs be: task DROP signature=5\ndoris-rs be: task CREATE signature=6\n"
        cpp = "I1105 successfully submit task|type=CLONE|signature=7\nI1105 other line type=X\n"
        self.assertEqual(compare.tasks_in_log(rs), ["DROP", "CREATE"])
        self.assertEqual(compare.tasks_in_log(cpp), ["CLONE"])


if __name__ == "__main__":
    unittest.main()
