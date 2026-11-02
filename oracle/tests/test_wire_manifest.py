import hashlib
import json
from collections import defaultdict
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "wire", "bin"))
import thrift_split as ts  # noqa: E402

CORPUS = os.path.join(os.path.dirname(__file__), "..", "wire", "corpus")

REQUIRED_THRIFT_CALLS = [
    ("HeartbeatService", "heartbeat"),
    ("FrontendService", "report"),
    ("BackendService", "submit_tasks"),
    ("FrontendService", "finishTask"),
    ("FrontendService", "reportExecStatus"),
    ("FrontendService", "loadTxnBegin"),
    ("FrontendService", "streamLoadPut"),
    ("FrontendService", "loadTxnCommit"),
]
REQUIRED_GRPC = ["exec_plan_fragment", "fetch_data"]


def load():
    with open(os.path.join(CORPUS, "manifest.json"), encoding="utf-8") as fh:
        return json.load(fh)


class WireManifestTests(unittest.TestCase):
    def test_files_exist_and_hash(self):
        m = load()
        self.assertGreater(len(m["messages"]), 50)
        for e in m["messages"]:
            with open(os.path.join(CORPUS, e["file"]), "rb") as fh:
                data = fh.read()
            self.assertEqual(len(data), e["bytes"], e["file"])
            self.assertEqual(hashlib.sha256(data).hexdigest(), e["sha256"], e["file"])

    def test_every_thrift_file_is_exactly_one_message_with_its_recorded_name(self):
        for e in load()["messages"]:
            if e["protocol"] != "thrift":
                continue
            with open(os.path.join(CORPUS, e["file"]), "rb") as fh:
                data = fh.read()
            msgs = ts.split_messages(data)
            self.assertEqual(len(msgs), 1, e["file"])
            self.assertEqual((msgs[0].name, ts.KINDS[msgs[0].mtype]), (e["method"], e["kind"]), e["file"])
            self.assertEqual(msgs[0].end, len(data), e["file"])

    def test_required_methods_have_calls_and_replies(self):
        msgs = load()["messages"]
        seen = {(e["service"], e["method"], e["kind"]) for e in msgs}
        for service, method in REQUIRED_THRIFT_CALLS:
            self.assertIn((service, method, "call"), seen)
            self.assertIn((service, method, "reply"), seen)
        for method in REQUIRED_GRPC:
            self.assertIn(("PBackendService", method, "request"), seen)
            self.assertIn(("PBackendService", method, "response"), seen)

    def test_grpc_entries_record_the_compression_flag(self):
        for e in load()["messages"]:
            if e["protocol"] == "grpc":
                self.assertIn("compressed", e, e["file"])


    def test_every_stream_was_captured_from_its_handshake(self):
        # a stream that started before the capture cannot be split into messages
        self.assertEqual(load()["capture"]["streams_without_handshake"], [])

    def test_no_thrift_bytes_were_dropped(self):
        # every Thrift direction splits into whole messages
        self.assertEqual(load()["capture"]["trailing_bytes_dropped"], [])

    def test_only_brpc_streams_produced_no_messages(self):
        # the BE calling its own PBackendService over brpc baidu_std on 8060: not in the v1 corpus
        left = load()["capture"]["streams_without_messages"]
        for s in left:
            self.assertEqual((s["protocol"], s["server_port"]), ("baidu_std", 8060), s)
            self.assertGreater(s["bytes"], 0, s)

    def test_calls_and_replies_pair_up(self):
        # C++ clients send every call with seqid 0, so Thrift pairs by order on a connection
        thrift, grpc = defaultdict(list), defaultdict(list)
        for e in load()["messages"]:
            if e["protocol"] == "thrift":
                thrift[e["tcp_stream"]].append(e["kind"])
            else:
                grpc[(e["tcp_stream"], e["h2_stream"])].append(e["kind"])
        for s, kinds in thrift.items():
            self.assertEqual(kinds, ["call", "reply"] * (len(kinds) // 2), f"tcp stream {s}")
        for key, kinds in grpc.items():
            self.assertEqual(sorted(kinds), ["request", "response"], f"tcp/h2 stream {key}")


if __name__ == "__main__":
    unittest.main()
