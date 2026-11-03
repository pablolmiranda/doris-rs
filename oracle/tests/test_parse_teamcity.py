import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bin"))
import parse_teamcity as pt  # noqa: E402

LOG = """
Using JDK 17: /opt/java
##teamcity[flowStarted flowId='1' timestamp='2026-11-02T10:00:00.000+0000']
##teamcity[testStarted name='test_select' flowId='1' timestamp='2026-11-02T10:00:00.000+0000']
##teamcity[testStdOut name='test_select' out='running|n' flowId='1' timestamp='x']
##teamcity[testFinished name='test_select' flowId='1' duration='1234' timestamp='x']
##teamcity[flowFinished flowId='1' timestamp='x']
##teamcity[testStarted name='test_variant_basic' flowId='2' timestamp='x']
##teamcity[testFailed name='test_variant_basic' message='expected |[1|] but got |[2|]|nline two' flowId='2' details='stack|'trace' timestamp='x']
##teamcity[testFinished name='test_variant_basic' flowId='2' duration='77' timestamp='x']
##teamcity[testStarted name='test_crashes_server' flowId='3' timestamp='x']
Test 3 suites, failed 1 suites, fatal 0 scripts, skipped 0 scripts
"""


class ParseTests(unittest.TestCase):
    def setUp(self):
        self.result = pt.parse_lines(LOG.splitlines())
        self.suites = self.result["suites"]

    def test_passed_suite_has_duration(self):
        self.assertEqual(self.suites["test_select"]["status"], "passed")
        self.assertEqual(self.suites["test_select"]["duration_ms"], 1234)

    def test_failed_suite_keeps_unescaped_message(self):
        entry = self.suites["test_variant_basic"]
        self.assertEqual(entry["status"], "failed")
        self.assertEqual(entry["message"], "expected [1] but got [2]\nline two")
        self.assertEqual(entry["duration_ms"], 77)
        self.assertEqual(entry["details"], "stack'trace")

    def test_details_are_truncated(self):
        long = "##teamcity[testFailed name='t' message='m' details='" + "x" * 5000 + "' flowId='9']"
        res = pt.parse_lines([long])
        self.assertEqual(len(res["suites"]["t"]["details"]), pt.DETAILS_LIMIT)

    def test_started_without_finish_is_incomplete(self):
        self.assertEqual(self.suites["test_crashes_server"]["status"], "incomplete")

    def test_counts_and_summary(self):
        self.assertEqual(self.result["meta"]["counts"], {"passed": 1, "failed": 1, "incomplete": 1})
        self.assertEqual(self.result["meta"]["summary"]["failed"], 1)
        self.assertEqual(self.result["meta"]["summary"]["total"], 3)

    def test_unescape_pipe_and_quote(self):
        self.assertEqual(pt.unescape("a||b|'c|[d|]"), "a|b'c[d]")

    def test_non_teamcity_lines_ignored(self):
        self.assertNotIn("Using", self.suites)

    def test_suite_window_comes_from_teamcity_timestamps(self):
        lines = [
            "##teamcity[testStarted name='a.b.c' flowId='f' timestamp='2026-11-02T06:11:48.136+0000']",
            "##teamcity[testFinished name='a.b.c' flowId='f' duration='170' timestamp='2026-11-02T06:11:48.305+0000']",
        ]
        entry = pt.parse_lines(lines)["suites"]["a.b.c"]
        self.assertEqual(entry["started_ms"], 1793599908136)
        self.assertEqual(entry["finished_ms"], 1793599908305)

    def test_main_exits_3_when_no_suites(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            log = os.path.join(d, "empty.log")
            out = os.path.join(d, "r.json")
            with open(log, "w") as fh:
                fh.write("no tests here\n")
            self.assertEqual(pt.main(["parse", out, log]), 3)


if __name__ == "__main__":
    unittest.main()
