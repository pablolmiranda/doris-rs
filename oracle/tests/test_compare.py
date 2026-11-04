import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bin"))
import compare  # noqa: E402


def s(status):
    return {"status": status, "duration_ms": 1, "message": None}


class CompareTests(unittest.TestCase):
    def setUp(self):
        self.baseline = {
            "a_same": s("passed"),
            "b_regress": s("passed"),
            "c_missing": s("passed"),
            "d_fixed": s("failed"),
            "e_known": s("failed"),
            "f_known_missing": s("failed"),
        }
        self.candidate = {
            "a_same": s("passed"),
            "b_regress": s("incomplete"),
            "d_fixed": s("passed"),
            "e_known": s("failed"),
            "g_new": s("passed"),
        }
        self.report = compare.classify(self.baseline, self.candidate)

    def test_buckets(self):
        self.assertEqual(self.report["same"], ["a_same"])
        self.assertEqual(self.report["regression"], ["b_regress"])
        self.assertEqual(self.report["missing"], ["c_missing"])
        self.assertEqual(self.report["fixed"], ["d_fixed"])
        self.assertEqual(self.report["known"], ["e_known", "f_known_missing"])
        self.assertEqual(self.report["new"], ["g_new"])

    def test_gate_fails_on_regression_and_missing(self):
        self.assertEqual(compare.gate(self.report, set(), False), ["b_regress", "c_missing"])

    def test_allow_missing(self):
        self.assertEqual(compare.gate(self.report, set(), True), ["b_regress"])

    def test_ignore_list(self):
        self.assertEqual(compare.gate(self.report, {"b_regress", "c_missing"}, False), [])


class IgnoreFileTests(unittest.TestCase):
    def test_inline_comments_after_a_name_are_allowed(self):
        import json, tempfile
        with tempfile.TemporaryDirectory() as d:
            base, cand, ign = (os.path.join(d, n) for n in ("b.json", "c.json", "i.txt"))
            json.dump({"suites": {"a": s("passed")}}, open(base, "w"))
            json.dump({"suites": {"a": s("failed")}}, open(cand, "w"))
            open(ign, "w").write("a  # flaky on reference: failed in run2\n")
            self.assertEqual(compare.main(["compare", base, cand, "--ignore", ign]), 0)


if __name__ == "__main__":
    unittest.main()
