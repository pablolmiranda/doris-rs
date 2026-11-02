import hashlib
import json
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


if __name__ == "__main__":
    unittest.main()
