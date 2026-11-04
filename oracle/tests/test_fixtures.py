"""The M2 BE fixtures (crates/doris-be/tests/fixtures/m2), captured from the reference C++ BE."""
import json
import os
import struct
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "wire", "bin"))
import be_fixtures  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(__file__), "..", "..", "crates", "doris-be", "tests", "fixtures", "m2")

REQUIRED = ["heartbeat", "report", "submit_tasks", "finishTask", "get_tablet_stat",
            "publish_topic_info", "get_dictionary_status"]


def _field(ftype, fid, payload):
    return struct.pack(">bh", ftype, fid) + payload


def submit_tasks_call(tasks):
    """A strict-binary submit_tasks call: list<TAgentTaskRequest> of (task_type, tablet_id or None)."""
    items = b""
    for task_type, tablet_id in tasks:
        body = _field(8, 1, struct.pack(">i", 2)) + _field(8, 2, struct.pack(">i", task_type))
        body += _field(10, 3, struct.pack(">q", 99))
        if tablet_id is not None:
            req = _field(10, 1, struct.pack(">q", tablet_id)) + b"\x00"
            body += _field(12, 5 if task_type == 0 else 6, req)
        items += body + b"\x00"
    args = _field(15, 1, struct.pack(">bi", 12, len(tasks)) + items) + b"\x00"
    name = b"submit_tasks"
    return struct.pack(">Ii", 0x80010001, len(name)) + name + struct.pack(">i", 1) + args


class TaskIndexTests(unittest.TestCase):
    def test_decodes_task_types_and_created_tablets(self):
        idx = be_fixtures.task_index([submit_tasks_call([(0, 11), (1, 12), (33, None)])])
        self.assertEqual(idx["task_types"], ["CREATE", "DROP", "UPDATE_VISIBLE_VERSION"])
        self.assertEqual(idx["created_tablets"], [11])
        self.assertEqual(idx["dropped_tablets"], [12])


class FixtureSetTests(unittest.TestCase):
    def test_fixture_set_is_complete(self):
        with open(os.path.join(FIXTURES, "manifest.json"), encoding="utf-8") as fh:
            m = json.load(fh)
        for method in REQUIRED:
            kinds = {e["kind"] for e in m["messages"] if e["method"] == method}
            self.assertTrue({"call", "reply"} <= kinds, f"{method}: {kinds}")
        for t in ("CREATE", "DROP", "UPDATE_VISIBLE_VERSION"):
            self.assertIn(t, m["task_types"])
        # Headers cover the tablets the scenario created (its own tables, saved before any data
        # landed); the FE's internal tables are created at start and hold data, so their headers
        # are not M2 fixtures.
        headers = {int(f[:-5]) for f in os.listdir(os.path.join(FIXTURES, "headers"))}
        self.assertGreaterEqual(len(headers), 7)
        self.assertLessEqual(headers, set(m["created_tablets"]))
        self.assertLessEqual(set(m["dropped_tablets"]), headers)
        with open(os.path.join(FIXTURES, "layout.txt"), encoding="utf-8") as fh:
            layout = fh.read()
        self.assertIn("$ROOT/data", layout)
        self.assertIn("# after drop", layout)


if __name__ == "__main__":
    unittest.main()
