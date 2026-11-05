"""Derive seam: recorded reference runs in, the reference products out."""
import hashlib
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "checker"))
sys.path.insert(0, os.path.dirname(__file__))
import derive  # noqa: E402
import records  # noqa: E402
from runfake import q, write_run  # noqa: E402

A, B, C = "d.a.a", "d.b.b", "d.c.c"
CONN = [[(150, q("select 1", [["1"]]))]]


def msg(method, t_ms, data=b"\x00", protocol="thrift", kind="call", service=None):
    header = {"protocol": protocol, "method": method, "kind": kind, "time_ms": t_ms}
    return ({**header, "service": service} if service else header, data)


class DeriveTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.out = os.path.join(self.tmp.name, "reference")

    def run_dir(self, i, suites, traffic=None):
        return write_run(os.path.join(self.tmp.name, f"run{i}"), suites, CONN, traffic=traffic)

    def lines(self, name):
        with open(os.path.join(self.out, name)) as fh:
            return [line.rstrip("\n") for line in fh]


class GateListTests(DeriveTestCase):
    def test_gate_is_in_scope_and_passed_in_every_run(self):
        runs = [self.run_dir(1, {A: ("passed", 100, 200), B: ("passed", 300, 400), C: ("passed", 500, 600)}),
                self.run_dir(2, {A: ("passed", 100, 200), B: ("failed", 300, 400), C: ("passed", 500, 600)}),
                self.run_dir(3, {A: ("passed", 100, 200), B: ("passed", 300, 400), C: ("passed", 500, 600)})]
        scope = {A: {"reasons": []}, B: {"reasons": []}, C: {"reasons": ["variant"]}}
        derive.derive(runs, self.out, scope=scope)
        self.assertEqual(self.lines("suites.txt"), [A])
        self.assertEqual(self.lines("flaky.txt"), [f"{B}  # flaky on reference: failed in run2"])
        with open(os.path.join(self.out, "baseline.json")) as fh:
            self.assertEqual(list(json.load(fh)["suites"]), [A])


class TrafficTests(DeriveTestCase):
    def test_suite_corpus_holds_only_what_the_suite_caused(self):
        traffic = [msg("exec_plan_fragment", 150, protocol="grpc", kind="request"), msg("heartbeat", 160),
                   msg("submit_tasks", 250), msg("fetch_data", 350, protocol="grpc", kind="request"),
                   msg("report", 360)]
        run = self.run_dir(1, {A: ("passed", 100, 200), B: ("passed", 300, 400)}, traffic=traffic)
        derive.derive([run], self.out)
        corpus = os.path.join(self.out, "traffic")
        self.assertEqual([h["method"] for h, _ in records.read(os.path.join(corpus, f"{A}.bin.gz"))], ["exec_plan_fragment"])
        self.assertEqual([h["method"] for h, _ in records.read(os.path.join(corpus, f"{B}.bin.gz"))], ["fetch_data"])
        with open(os.path.join(corpus, "manifest.json")) as fh:
            manifest = json.load(fh)
        self.assertEqual(manifest["background"], {"heartbeat": 1, "report": 1, "submit_tasks": 1})

    def test_large_http_bodies_are_kept_as_length_and_digest(self):
        body = b"1,a\n" * 20000
        request = b"PUT /api/db/t/_stream_load HTTP/1.1\r\nContent-Length: 80000\r\n\r\n" + body
        small = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}"
        traffic = [({"protocol": "http", "method": "stream_load", "kind": "request", "time_ms": 150}, request),
                   ({"protocol": "http", "method": "stream_load", "kind": "response", "time_ms": 150}, small)]
        derive.derive([self.run_dir(1, {A: ("passed", 100, 200)}, traffic=traffic)], self.out)
        (h1, d1), (h2, d2) = records.read(os.path.join(self.out, "traffic", f"{A}.bin.gz"))
        self.assertEqual(d1, b"PUT /api/db/t/_stream_load HTTP/1.1\r\nContent-Length: 80000\r\n\r\n")
        self.assertEqual(h1["body_omitted"], {"bytes": 80000, "sha256": hashlib.sha256(body).hexdigest()})
        self.assertEqual(d2, small)
        self.assertNotIn("body_omitted", h2)

    def test_grpc_frames_without_a_method_are_reported_not_kept(self):
        unnamed = ({"protocol": "grpc", "method": "?", "kind": "request", "time_ms": 150,
                    "tcp_stream": 2, "h2_stream": 8192, "frame": 7}, b"\x01")
        run = self.run_dir(1, {A: ("passed", 100, 200)},
                           traffic=[unnamed, msg("fetch_data", 160, protocol="grpc", kind="request")])
        derive.derive([run], self.out)
        corpus = os.path.join(self.out, "traffic")
        self.assertEqual([h["method"] for h, _ in records.read(os.path.join(corpus, f"{A}.bin.gz"))], ["fetch_data"])
        with open(os.path.join(corpus, "manifest.json")) as fh:
            self.assertEqual(json.load(fh)["unnamed_grpc"],
                             [{"suite": A, "tcp_stream": 2, "h2_stream": 8192, "frame": 7, "bytes": 1}])

    def test_declared_harness_http_is_not_the_suites(self):
        def http(path, kind):
            return ({"protocol": "http", "method": path, "path": path, "kind": kind, "time_ms": 150}, b"")
        traffic = [http("/api/health", "request"), http("/api/health", "response"),
                   http("/api/running_pipeline_tasks/180", "request"),
                   msg("exec_plan_fragment", 350, protocol="grpc", kind="request")]
        run = self.run_dir(1, {A: ("passed", 100, 200), B: ("passed", 300, 400)}, traffic=traffic)
        derive.derive([run], self.out)
        self.assertEqual(self.lines("reaches_be.txt"), [B])
        with open(os.path.join(self.out, "traffic", "manifest.json")) as fh:
            manifest = json.load(fh)
        self.assertEqual(manifest["harness"], {"/api/health": 2, "/api/running_pipeline_tasks/180": 1})
        self.assertEqual(manifest["without_traffic"], [A])

    def test_tablet_stat_polling_is_background(self):
        run = self.run_dir(1, {A: ("passed", 100, 200)}, traffic=[msg("get_tablet_stat", 150, kind="reply")])
        derive.derive([run], self.out)
        with open(os.path.join(self.out, "traffic", "manifest.json")) as fh:
            self.assertEqual(json.load(fh)["background"], {"get_tablet_stat": 1})

    def test_reaches_be_counts_work_sent_to_the_be(self):
        traffic = [msg("exec_plan_fragment", 150, protocol="grpc", kind="request"), msg("heartbeat", 350)]
        run = self.run_dir(1, {A: ("passed", 100, 200), B: ("passed", 300, 400)}, traffic=traffic)
        derive.derive([run], self.out)
        self.assertEqual(self.lines("reaches_be.txt"), [A])

    def test_any_request_to_the_be_reaches_it_and_calls_to_the_fe_do_not(self):
        traffic = [msg("check_storage_format", 150, service="BackendService"),
                   msg("reportExecStatus", 350, service="FrontendService")]
        run = self.run_dir(1, {A: ("passed", 100, 200), B: ("passed", 300, 400)}, traffic=traffic)
        derive.derive([run], self.out)
        self.assertEqual(self.lines("reaches_be.txt"), [A])

    def test_capture_holes_of_the_run_are_listed_in_the_manifest(self):
        run = self.run_dir(1, {A: ("passed", 100, 200)}, traffic=[msg("heartbeat", 150)])
        holes = [{"tcp_stream": 1, "from": "client", "gap": [10, 20]}]
        with open(os.path.join(run, "traffic-holes.json"), "w") as fh:
            json.dump(holes, fh)
        derive.derive([run], self.out)
        with open(os.path.join(self.out, "traffic", "manifest.json")) as fh:
            self.assertEqual(json.load(fh)["capture_holes"], holes)

    def test_missing_capture_and_suites_without_traffic_are_reported(self):
        no_capture = derive.derive([self.run_dir(1, {A: ("passed", 100, 200)})], self.out)
        self.assertEqual(no_capture["traffic"], "no capture in run 1")
        run = self.run_dir(2, {A: ("passed", 100, 200), B: ("passed", 300, 400)}, traffic=[msg("fetch_data", 150)])
        summary = derive.derive([run], self.out)
        self.assertEqual(summary["without_traffic"], 1)
        with open(os.path.join(self.out, "traffic", "manifest.json")) as fh:
            self.assertEqual(json.load(fh)["without_traffic"], [B])

    def test_overlapping_windows_are_rejected(self):
        run = self.run_dir(1, {A: ("passed", 100, 300), B: ("passed", 200, 400)}, traffic=[])
        with self.assertRaises(ValueError):
            derive.derive([run], self.out)


class NeedsRankingTests(DeriveTestCase):
    def rank(self, suites, index, supported=()):
        run = self.run_dir(1, {k: ("passed", 100 + 200 * i, 200 + 200 * i) for i, k in enumerate(suites)})
        derive.derive([run], self.out, needs_index=index, supported=set(supported))
        with open(os.path.join(self.out, "ranking.json")) as fh:
            return json.load(fh)

    def test_alone_counts_suites_blocked_by_that_feature_only(self):
        r = self.rank([A, B, C], {A: {"plan_node": ["X"]}, B: {"plan_node": ["X", "Y"]}, C: {"plan_node": ["Y"]}})
        self.assertEqual(r[0], {"feature": "plan_node:X", "alone": 1, "total": 2})

    def test_supported_features_do_not_block(self):
        self.assertEqual(self.rank([A], {A: {"plan_node": ["X"]}}, supported={"plan_node:X"}), [])

    def test_suite_needs_include_directory_load_suites(self):
        load, query = "d.load.load", "d.q.q"
        r = self.rank([load, query], {load: {"task": ["CREATE"]}, query: {"plan_node": ["X"]}}, supported={"plan_node:X"})
        self.assertIn({"feature": "task:CREATE", "alone": 2, "total": 2}, r)

    def test_a_supplement_adds_needs_the_traffic_missed(self):
        merged = derive.with_supplement({A: {"task": ["CREATE"]}},
                                        {"suites": {A: {"needs": ["data:d.load.load"], "reason": "r"}, B: {"needs": ["rpc:x.y"], "reason": "r"}}})
        self.assertEqual(merged, {A: {"task": ["CREATE"], "data": ["d.load.load"]}, B: {"rpc": ["x.y"]}})

    def test_load_suite_outside_the_gate_still_blocks_its_dependents(self):
        load, query = "d.load.load_data", "d.q.q"
        traffic = [msg("submit_tasks", 150), msg("exec_plan_fragment", 350, protocol="grpc", kind="request")]
        runs = [self.run_dir(1, {load: ("passed", 100, 200), query: ("passed", 300, 400)}, traffic=traffic),
                self.run_dir(2, {load: ("failed", 100, 200), query: ("passed", 300, 400)})]
        derive.derive(runs, self.out, needs_index={load: {"task": ["CREATE"]}, query: {"plan_node": ["X"]}},
                      supported={"plan_node:X"})
        self.assertTrue(os.path.exists(os.path.join(self.out, "traffic", f"{load}.bin.gz")))
        with open(os.path.join(self.out, "ranking.json")) as fh:
            self.assertEqual(json.load(fh), [{"feature": "task:CREATE", "alone": 1, "total": 1}])


if __name__ == "__main__":
    unittest.main()
