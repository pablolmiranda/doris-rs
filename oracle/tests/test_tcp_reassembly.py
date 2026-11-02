import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "wire", "bin"))
import extract  # noqa: E402


class ReassemblyTests(unittest.TestCase):
    def test_in_order_segments_concatenate(self):
        self.assertEqual(extract.reassemble([(100, b"ab"), (102, b"cd")], 100), b"abcd")

    def test_retransmitted_segment_is_not_duplicated(self):
        segs = [(100, b"ab"), (102, b"cd"), (102, b"cd"), (104, b"e")]
        self.assertEqual(extract.reassemble(segs, 100), b"abcde")

    def test_overlapping_retransmission_keeps_each_byte_once(self):
        segs = [(100, b"abcd"), (102, b"cdef")]
        self.assertEqual(extract.reassemble(segs, 100), b"abcdef")

    def test_out_of_order_segments_are_sorted(self):
        self.assertEqual(extract.reassemble([(103, b"de"), (100, b"abc")], 100), b"abcde")

    def test_gap_raises(self):
        with self.assertRaises(extract.Gap):
            extract.reassemble([(100, b"ab"), (105, b"xy")], 100)

    def test_sequence_number_wraparound(self):
        start = 2**32 - 3
        self.assertEqual(extract.reassemble([(0, b"def"), (start, b"abc")], start), b"abcdef")

    def test_grpc_messages_split_on_length_prefix(self):
        stream = b"\x00\x00\x00\x00\x02hi" + b"\x01\x00\x00\x00\x01z"
        self.assertEqual(extract.split_grpc(stream), [(False, b"hi"), (True, b"z")])


if __name__ == "__main__":
    unittest.main()
