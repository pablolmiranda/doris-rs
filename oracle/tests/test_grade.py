"""Grade seam: one recorded candidate run in, the two gates' verdict out.

The reference each test grades against is derived from recorded reference runs with
derive.derive, as the real checker does."""
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "checker"))
sys.path.insert(0, os.path.dirname(__file__))
import derive  # noqa: E402
import grade  # noqa: E402
import mysqlfake as f  # noqa: E402
from runfake import e, q, write_run  # noqa: E402

S = "d.s.s"


class GradeTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.scores = self.dir("scores")

    def dir(self, name):
        path = os.path.join(self.tmp.name, name)
        os.makedirs(path, exist_ok=True)
        return path

    def reference(self, *runs):
        """runs: (suites, connections[, facts]) tuples, derived into a reference directory."""
        dirs = [write_run(self.dir(f"ref{i}"), *run) for i, run in enumerate(runs)]
        out = self.dir("reference")
        derive.derive(dirs, out)
        return out

    def reference_status(self, ref):
        with open(os.path.join(ref, "marks.json")) as fh:
            return json.load(fh)["suites"][S]["status"]

    def grade(self, ref, suites, connections, facts=None):
        return grade.grade(write_run(self.dir("cand"), suites, connections, facts), ref, self.scores)


class StrictGateTests(GradeTestCase):
    def test_identical_candidate_passes_both_gates(self):
        conns = [[(150, q("select 1", [["1"]]))]]
        ref = self.reference(({S: ("passed", 100, 200)}, conns))
        verdict = self.grade(ref, {S: ("passed", 1100, 1200)}, [[(1150, q("select 1", [["1"]]))]])
        self.assertEqual(verdict["out"]["passing"], [S])
        self.assertEqual(verdict["strict"]["passing"], [S])
        self.assertEqual(verdict["strict"]["differences"], {})
        self.assertTrue(verdict["ok"])

    def test_prepared_statements_including_reply_less_close_grade(self):
        import struct
        col = f.coldef("r", f.MYSQL_TYPE_LONGLONG, 20)
        def conn(value):
            return [[(150, ([f.prepare("select ?")], f.prepare_ok(1, [col], [col]))),
                     (151, ([f.execute(1, [(f.MYSQL_TYPE_LONGLONG, struct.pack("<q", 5))])],
                            f.binary_resultset([col], [[struct.pack("<q", value)]]))),
                     (152, ([b"\x19" + struct.pack("<I", 1)], []))]]
        ref = self.reference(({S: ("passed", 100, 200)}, conn(5)))
        self.assertEqual(self.grade(ref, {S: ("passed", 100, 200)}, conn(5))["strict"]["passing"], [S])
        self.assertEqual(self.grade(ref, {S: ("passed", 100, 200)}, conn(6))["strict"]["differences"][S]["field"], "rows")

    def test_first_difference_names_statement_and_field(self):
        ref = self.reference(({S: ("passed", 100, 200)}, [[(150, q("select 1", [["1"]])), (160, q("select 2", [["2"]]))]]))
        verdict = self.grade(ref, {S: ("passed", 100, 200)}, [[(150, q("select 1", [["1"]])), (160, q("select 2", [["3"]]))]])
        self.assertEqual(verdict["strict"]["passing"], [])
        diff = verdict["strict"]["differences"][S]
        self.assertEqual((diff["index"], diff["statement"], diff["field"]), (2, "select 2", "rows"))
        self.assertEqual((diff["expected"], diff["actual"]), ([["32"]], [["33"]]))
        self.assertEqual(verdict["out"]["passing"], [S])

    def test_polls_of_different_length_compare_equal(self):
        poll = "SHOW ALTER TABLE COLUMN"
        ref = self.reference(({S: ("passed", 100, 200)},
                              [[(150, q(poll, [["RUNNING"]])), (151, q(poll, [["FINISHED"]])), (160, q("select 1", [["1"]]))]]))
        verdict = self.grade(ref, {S: ("passed", 100, 200)},
                             [[(150, q(poll, [["RUNNING"]])), (151, q(poll, [["RUNNING"]])), (152, q(poll, [["RUNNING"]])),
                               (153, q(poll, [["FINISHED"]])), (160, q("select 1", [["1"]]))]])
        self.assertEqual(verdict["strict"]["passing"], [S])

    def test_poll_ending_differently_is_a_difference(self):
        poll = "SHOW ALTER TABLE COLUMN"
        ref = self.reference(({S: ("passed", 100, 200)}, [[(150, q(poll, [["RUNNING"]])), (151, q(poll, [["FINISHED"]]))]]))
        verdict = self.grade(ref, {S: ("passed", 100, 200)}, [[(150, q(poll, [["RUNNING"]])), (151, q(poll, [["CANCELLED"]]))]])
        self.assertIn(S, verdict["strict"]["differences"])

    def test_interleaving_between_connections_does_not_matter(self):
        def run(t_b):
            return [[(150, q("select 1", [["1"]])), (170, q("select 3", [["3"]]))], [(t_b, q("select 2", [["2"]]))]]
        ref = self.reference(({S: ("passed", 100, 200)}, run(160)), ({S: ("passed", 100, 200)}, run(180)))
        self.assertEqual(self.reference_status(ref), "stable")
        self.assertEqual(self.grade(ref, {S: ("passed", 100, 200)}, run(155))["strict"]["passing"], [S])

    def test_connection_close_timing_does_not_matter(self):
        quit_ = ([b"\x01"], [])
        ref = self.reference(({S: ("passed", 100, 200)}, [[(150, q("select 1", [["1"]])), (160, quit_)]]),
                             ({S: ("passed", 100, 200)}, [[(150, q("select 1", [["1"]])), (260, quit_)]]))
        self.assertEqual(self.reference_status(ref), "stable")

    def test_strict_gate_needs_the_suite_to_pass_first(self):
        conns = [[(150, q("select 1", [["1"]]))]]
        ref = self.reference(({S: ("passed", 100, 200)}, conns))
        verdict = self.grade(ref, {S: ("failed", 100, 200)}, conns)
        self.assertEqual((verdict["out"]["passing"], verdict["strict"]["passing"]), ([], []))

    def test_exchanges_outside_every_window_are_ignored(self):
        ref = self.reference(({S: ("passed", 100, 200)}, [[(150, q("select 1", [["1"]]))]]))
        verdict = self.grade(ref, {S: ("passed", 100, 200)},
                             [[(150, q("select 1", [["1"]]))], [(500, q("select now()", [["x"]]))]])
        self.assertEqual(verdict["strict"]["passing"], [S])


def facts(host, port):
    return {"cluster": host, "facts": [{"name": "be-host", "values": [host], "match": "substring"},
                                       {"name": "be-port", "values": [port], "match": "cell"}]}


class NothingToCompareTests(GradeTestCase):
    W = {S: ("passed", 100, 200)}

    def test_suite_without_recorded_exchanges_does_not_pass_strict_for_free(self):
        ref = self.reference((self.W, [[(500, q("select 1", [["1"]]))]]))  # nothing inside the window
        self.assertEqual(self.reference_status(ref), "no_replies")
        verdict = self.grade(ref, self.W, [[(500, q("select 1", [["1"]]))]])
        self.assertEqual(verdict["strict"]["passing"], [])
        self.assertEqual(verdict["strict"]["not_compared"], {"no_replies": [S]})

    def test_skipped_statements_are_reported_apart(self):
        def run(first):
            return [[(150, first), (160, q("select 2", [["2"]]))]]
        ref = self.reference((self.W, run(q("select 1", [["1"]]))), (self.W, run(e("select 1", 1105, "timeout"))))
        verdict = self.grade(ref, self.W, run(q("select 1", [["7"]])))
        self.assertEqual(verdict["strict"]["skipped_statements"], {S: 1})


class DeploymentFactTests(GradeTestCase):
    def backends(self, host, port, error_host=None):
        return [[(150, q("show backends", [[host, port, "19050"]])),
                 (160, e("select boom()", 1105, f"({error_host or host})[E-1] boom"))]]

    def test_candidate_reporting_its_own_facts_matches(self):
        ref = self.reference(({S: ("passed", 100, 200)}, self.backends("127.0.0.1", "9050"), facts("127.0.0.1", "9050")))
        verdict = self.grade(ref, {S: ("passed", 100, 200)}, self.backends("10.1.2.3", "9150"), facts("10.1.2.3", "9150"))
        self.assertEqual(verdict["strict"]["differences"], {})
        self.assertEqual(verdict["strict"]["passing"], [S])

    def test_only_whole_cells_match_a_cell_fact(self):
        ref = self.reference(({S: ("passed", 100, 200)}, [[(150, q("q", [["9050", "19050"]]))]], facts("127.0.0.1", "9050")))
        verdict = self.grade(ref, {S: ("passed", 100, 200)}, [[(150, q("q", [["9150", "19150"]]))]], facts("10.1.2.3", "9150"))
        self.assertEqual(verdict["strict"]["differences"][S]["field"], "rows")

    def test_cell_fact_scoped_to_columns_leaves_other_columns_alone(self):
        def cores(n):
            return {"facts": [{"name": "be-cores", "values": [n], "match": "cell", "columns": ["CpuCores"]}]}
        cols = [f.coldef("CpuCores"), f.coldef("total")]
        ref = self.reference(({S: ("passed", 100, 200)}, [[(150, q("q", [["10", "10"]], cols))]], cores("10")))
        same = self.grade(ref, {S: ("passed", 100, 200)}, [[(150, q("q", [["4", "10"]], cols))]], cores("4"))
        self.assertEqual(same["strict"]["passing"], [S])
        wrong = self.grade(ref, {S: ("passed", 100, 200)}, [[(150, q("q", [["4", "4"]], cols))]], cores("4"))
        self.assertEqual(wrong["strict"]["differences"][S]["field"], "rows")

    def test_impersonating_the_reference_address_is_a_difference(self):
        ref = self.reference(({S: ("passed", 100, 200)}, self.backends("127.0.0.1", "9050"), facts("127.0.0.1", "9050")))
        verdict = self.grade(ref, {S: ("passed", 100, 200)}, self.backends("10.1.2.3", "9150", error_host="127.0.0.1"),
                             facts("10.1.2.3", "9150"))
        self.assertEqual(verdict["strict"]["differences"][S]["field"], "message")


def one(sql, rows):
    return [[(150, q(sql, rows))]]


class ProbeMarkTests(GradeTestCase):
    W = {S: ("passed", 100, 200)}

    def test_row_order_that_varied_on_the_reference_is_relaxed(self):
        ref = self.reference((self.W, one("q", [["1"], ["2"]])), (self.W, one("q", [["2"], ["1"]])))
        self.assertEqual(self.grade(ref, self.W, one("q", [["2"], ["1"]]))["strict"]["passing"], [S])
        self.assertEqual(self.grade(ref, self.W, one("q", [["2"], ["2"]]))["strict"]["differences"][S]["field"], "rows")

    def test_row_order_that_did_not_vary_stays_exact(self):
        ref = self.reference((self.W, one("q", [["1"], ["2"]])), (self.W, one("q", [["1"], ["2"]])))
        self.assertEqual(self.grade(ref, self.W, one("q", [["2"], ["1"]]))["strict"]["differences"][S]["field"], "rows")

    def test_only_the_column_that_varied_is_masked(self):
        ref = self.reference((self.W, one("q", [["a", "t1"]])), (self.W, one("q", [["a", "t2"]])))
        self.assertEqual(self.grade(ref, self.W, one("q", [["a", "t9"]]))["strict"]["passing"], [S])
        self.assertEqual(self.grade(ref, self.W, one("q", [["b", "t1"]]))["strict"]["differences"][S]["field"], "rows")

    def test_varying_error_span_becomes_a_pattern(self):
        def run(qid):
            return [[(150, e("select x", 1105, f"errCode = 2, query id {qid} failed"))]]
        ref = self.reference((self.W, run("1a2b")), (self.W, run("9f8e")))
        self.assertEqual(self.grade(ref, self.W, run("77aa"))["strict"]["passing"], [S])
        other = [[(150, e("select x", 1105, "errCode = 2, query id 77aa timed out"))]]
        self.assertEqual(self.grade(ref, self.W, other)["strict"]["differences"][S]["field"], "message")

    def test_varying_statement_label_becomes_a_pattern(self):
        def run(label):
            return [[(150, ([f.query(f"insert into t with label lbl_{label} values (1)")], [f.ok(affected=1)]))]]
        ref = self.reference((self.W, run("100")), (self.W, run("207")))
        self.assertEqual(self.grade(ref, self.W, run("315"))["strict"]["passing"], [S])

    def test_varying_ok_info_is_masked(self):
        def run(txn):
            return [[(150, ([f.query("insert into t values (1)")], [f.ok(affected=1, info=f"{{'txnId':'{txn}'}}".encode())]))]]
        ref = self.reference((self.W, run("10")), (self.W, run("11")))
        self.assertEqual(self.grade(ref, self.W, run("99"))["strict"]["passing"], [S])

    def test_suite_whose_exchange_count_varied_is_unstable_not_compared(self):
        ref = self.reference((self.W, one("q", [["1"]])), (self.W, [[(150, q("q", [["1"]])), (160, q("r", [["2"]]))]]))
        verdict = self.grade(ref, self.W, one("q", [["5"]]))
        self.assertEqual(verdict["strict"]["not_compared"], {"unstable": [S]})
        self.assertEqual((verdict["strict"]["passing"], verdict["strict"]["differences"]), ([], {}))

    def test_reply_whose_kind_varied_is_skipped_and_the_rest_compared(self):
        def run(first):
            return [[(150, first), (160, q("select 2", [["2"]]))]]
        ref = self.reference((self.W, run(q("select 1", [["1"]]))), (self.W, run(e("select 1", 1105, "timeout"))))
        self.assertEqual(self.grade(ref, self.W, run(q("select 1", [["7"]])))["strict"]["passing"], [S])
        self.assertIn(S, self.grade(ref, self.W, [[(150, q("select 1", [["1"]])), (160, q("select 2", [["3"]]))]])["strict"]["differences"])


class RatchetTests(GradeTestCase):
    T = "d.t.t"
    W = {S: ("passed", 100, 200), T: ("passed", 300, 400)}

    def setUp(self):
        super().setUp()
        self.ref = self.reference((self.W, [[(150, q("q", [["1"]])), (350, q("r", [["2"]]))]]))

    def scores_file(self, gate, *suites):
        with open(os.path.join(self.scores, f"epic1-{gate}.txt"), "w") as fh:
            fh.write("# suites known to pass on doris-rs\n" + "".join(s + "\n" for s in suites))

    def test_listed_suite_that_stops_passing_fails_the_check(self):
        self.scores_file("out", S, self.T)
        verdict = self.grade(self.ref, {S: ("passed", 100, 200), self.T: ("failed", 300, 400)},
                             [[(150, q("q", [["1"]]))]])
        self.assertEqual(verdict["ratchet"]["lost"]["out"], [self.T])
        self.assertFalse(verdict["ok"])

    def test_new_passes_are_reported_without_failing(self):
        self.scores_file("strict", S)
        verdict = self.grade(self.ref, self.W, [[(150, q("q", [["1"]])), (350, q("r", [["2"]]))]])
        self.assertEqual(verdict["ratchet"]["new"]["strict"], [self.T])
        self.assertEqual(verdict["ratchet"]["new"]["out"], [S, self.T])
        self.assertTrue(verdict["ok"])

    def test_strict_listed_suite_with_a_reply_difference_fails_the_check(self):
        self.scores_file("strict", S)
        verdict = self.grade(self.ref, self.W, [[(150, q("q", [["9"]])), (350, q("r", [["2"]]))]])
        self.assertEqual(verdict["ratchet"]["lost"]["strict"], [S])
        self.assertFalse(verdict["ok"])

    def test_two_numbers_per_gate(self):
        traffic = [({"protocol": "grpc", "method": "exec_plan_fragment", "kind": "request", "time_ms": 350}, b"\x00")]
        dirs = [write_run(self.dir("refbe"), self.W, [[(150, q("q", [["1"]])), (350, q("r", [["2"]]))]], traffic=traffic)]
        ref = self.dir("reference-be")
        derive.derive(dirs, ref)
        verdict = self.grade(ref, {S: ("passed", 100, 200), self.T: ("failed", 300, 400)}, [[(150, q("q", [["1"]]))]])
        self.assertEqual(verdict["out"]["numbers"], {"passing": 1, "total": 2, "passing_be": 0, "total_be": 1})


if __name__ == "__main__":
    unittest.main()
