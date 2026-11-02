import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bin"))
import triage  # noqa: E402


class TriageTests(unittest.TestCase):
    def test_groups_by_dir_and_phrase(self):
        suites = {
            "a_p0.x.s1": {"status": "passed", "message": None, "details": None},
            "a_p0.x.s2": {"status": "failed", "message": "Exception in s2",
                          "details": "errCode = 2, detailMessage = Table tbl_17 does not exist\n\tat foo"},
            "a_p0.y.s3": {"status": "failed", "message": "boom",
                          "details": "errCode = 2, detailMessage = Table tbl_99 does not exist"},
            "b_p0.s4": {"status": "incomplete", "message": None, "details": None},
        }
        out = triage.triage(suites, 5)
        self.assertIn("a_p0                               2 /     3", out)
        self.assertIn("b_p0                               1 /     1", out)
        self.assertIn("2  errCode = N, detailMessage = Table tbl_N does not exist", out)
        self.assertIn("1  <no recognizable server error>", out)


if __name__ == "__main__":
    unittest.main()
