import os
import struct
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "wire", "bin"))
import thrift_split as ts  # noqa: E402

# TBinaryProtocol type ids
BOOL, BYTE, DOUBLE, I16, I32, I64, STRING, STRUCT, MAP, SET, LIST = 2, 3, 4, 6, 8, 10, 11, 12, 13, 14, 15


def field(ftype, fid, payload):
    return struct.pack(">bh", ftype, fid) + payload


def tstring(s):
    b = s.encode() if isinstance(s, str) else s
    return struct.pack(">i", len(b)) + b


def message(name, mtype, seqid, body):
    return struct.pack(">I", 0x80010000 | mtype) + tstring(name) + struct.pack(">i", seqid) + body


def sample_struct():
    inner = field(LIST, 1, struct.pack(">bi", I32, 3) + struct.pack(">iii", 1, 2, 3)) + b"\x00"
    return (
        field(BOOL, 1, b"\x01")
        + field(BYTE, 2, b"\x7f")
        + field(I16, 3, struct.pack(">h", -2))
        + field(I32, 4, struct.pack(">i", 7))
        + field(I64, 5, struct.pack(">q", 1 << 40))
        + field(DOUBLE, 6, struct.pack(">d", 2.5))
        + field(STRING, 7, tstring("héllo"))
        + field(STRUCT, 8, inner)
        + field(MAP, 9, struct.pack(">bbi", STRING, I64, 2) + tstring("a") + struct.pack(">q", 1) + tstring("b") + struct.pack(">q", 2))
        + field(SET, 10, struct.pack(">bi", I16, 2) + struct.pack(">hh", 5, 6))
        + b"\x00"
    )


class SplitTests(unittest.TestCase):
    def test_read_message_returns_header_and_bounds(self):
        msg = message("heartbeat", 1, 7, sample_struct())
        m = ts.read_message(msg, 0)
        self.assertEqual((m.name, m.mtype, m.seqid, m.start, m.end), ("heartbeat", 1, 7, 0, len(msg)))
        self.assertEqual(ts.KINDS[m.mtype], "call")

    def test_split_back_to_back_messages(self):
        a = message("report", 1, 1, sample_struct())
        b = message("report", 2, 1, field(STRUCT, 0, b"\x00") + b"\x00")
        msgs = ts.split_messages(a + b)
        self.assertEqual([(m.name, m.mtype, m.start, m.end) for m in msgs], [("report", 1, 0, len(a)), ("report", 2, len(a), len(a) + len(b))])

    def test_trailing_partial_message_is_dropped(self):
        a = message("report", 1, 1, sample_struct())
        b = message("report", 1, 2, sample_struct())
        msgs = ts.split_messages(a + b[:-3])
        self.assertEqual(len(msgs), 1)
        self.assertEqual(msgs[0].end, len(a))

    def test_incomplete_message_raises(self):
        msg = message("heartbeat", 1, 7, sample_struct())
        with self.assertRaises(ts.Incomplete):
            ts.read_message(msg[:-1], 0)

    def test_non_strict_header_is_rejected(self):
        # an old non-strict message starts with the name's length, a small positive i32
        with self.assertRaises(ValueError):
            ts.read_message(tstring("heartbeat") + b"\x01" + struct.pack(">i", 1) + b"\x00", 0)

    def test_unknown_field_type_is_rejected(self):
        with self.assertRaises(ValueError):
            ts.read_message(message("x", 1, 1, field(99, 1, b"")), 0)


if __name__ == "__main__":
    unittest.main()
