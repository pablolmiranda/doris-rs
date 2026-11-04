"""Extraction accounting: whatever the extractor does not turn into corpus messages is reported,
never silently dropped."""
import os
import struct
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "wire", "bin"))
import extract  # noqa: E402


def message(name, mtype, seqid):
    """A strict TBinaryProtocol message with an empty struct."""
    n = name.encode()
    return struct.pack(">Ii", 0x80010000 | mtype, len(n)) + n + struct.pack(">i", seqid) + b"\x00"


def stream(client_port, server_port, client_isn=1000, server_isn=5000):
    return {"client_port": client_port, "server_port": server_port, "client_isn": client_isn, "server_isn": server_isn}


class ClassifyTests(unittest.TestCase):
    def test_protocol_is_read_from_the_clients_first_bytes(self):
        self.assertEqual(extract.classify(b"PRI * HTTP/2.0\r\n\r\nSM"), "http2")
        self.assertEqual(extract.classify(b"PRPC\x00\x00\x00\x2a"), "baidu_std")
        self.assertEqual(extract.classify(b"\x80\x01\x00\x01\x00\x00\x00\x09"), "thrift")
        self.assertEqual(extract.classify(b"GET / HTTP/1.1"), "unknown")
        self.assertEqual(extract.classify(b""), "unknown")


class ThriftAccountingTests(unittest.TestCase):
    def test_trailing_partial_message_is_reported_not_stored(self):
        call, reply, nxt = message("report", 1, 0), message("report", 2, 0), message("report", 1, 0)
        streams = {5: stream(50000, 9020)}
        segs = [
            # (frame, tcp_stream, src_port, dst_port, seq_raw, payload)
            (10, 5, 50000, 9020, 1001, call + nxt[:3]),
            (11, 5, 9020, 50000, 5001, reply),
            (12, 5, 50000, 9020, 1001 + len(call) + 3, nxt[3:5]),
        ]
        messages, dropped = extract.thrift_messages(segs, streams)
        messages.sort(key=lambda m: m["frame"])  # main() orders by frame; this returns by direction
        self.assertEqual([(m["method"], m["kind"], m["data"]) for m in messages],
                         [("report", "call", call), ("report", "reply", reply)])
        self.assertEqual(dropped, [{"tcp_stream": 5, "from": "client", "bytes": 5}])

    def test_hole_in_a_stream_keeps_the_messages_before_it_and_is_reported(self):
        first, second, third = message("report", 1, 0), message("report", 1, 0), message("report", 1, 0)
        streams = {5: stream(50000, 9020)}
        segs = [
            (10, 5, 50000, 9020, 1001, first),
            # `second` was never captured
            (12, 5, 50000, 9020, 1001 + len(first) + len(second), third),
        ]
        messages, dropped = extract.thrift_messages(segs, streams)
        self.assertEqual([m["data"] for m in messages], [first])
        self.assertEqual(dropped, [{"tcp_stream": 5, "from": "client", "bytes": len(third),
                                    "gap": [len(first), len(first) + len(second)]}])

    def test_streams_on_other_ports_are_left_to_their_own_extractor(self):
        streams = {7: stream(50001, 8060)}
        segs = [(20, 7, 50001, 8060, 1001, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")]
        self.assertEqual(extract.thrift_messages(segs, streams), ([], []))


class UnextractedStreamTests(unittest.TestCase):
    def test_streams_without_handshake_or_messages_are_reported(self):
        thrift = message("heartbeat", 1, 1)
        prpc = b"PRPC" + bytes(12) + b"meta"
        streams = {1: stream(41000, 9050), 2: stream(41001, 8060)}  # stream 3 has no handshake
        segs = [
            (1, 1, 41000, 9050, 1001, thrift),
            (2, 2, 41001, 8060, 1001, prpc),
            (3, 2, 8060, 41001, 5001, b"PRPC" + bytes(8)),
            (4, 3, 41002, 9060, 777, message("submit_tasks", 1, 3)),
        ]
        without_handshake, without_messages = extract.unextracted_streams(segs, streams, [{"tcp_stream": 1}])
        self.assertEqual(without_handshake, [{
            "tcp_stream": 3, "server_port": 9060, "bytes": len(message("submit_tasks", 1, 3)),
            "first_bytes": message("submit_tasks", 1, 3)[:16].hex(), "protocol": "thrift",
        }])
        self.assertEqual(without_messages, [{
            "tcp_stream": 2, "server_port": 8060, "bytes": len(prpc) + 12,
            "first_bytes": prpc[:16].hex(), "protocol": "baidu_std",
        }])


if __name__ == "__main__":
    unittest.main()
