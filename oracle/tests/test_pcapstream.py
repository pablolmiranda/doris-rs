"""Streaming TCP reassembly from a pcap file: whole connections per direction, timed, with holes
reported, without holding the capture in memory."""
import os
import struct
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "checker"))
import pcapstream  # noqa: E402

SYN, ACK, PSH = 0x02, 0x10, 0x08


def ip4(a):
    return bytes(int(x) for x in a.split("."))


def packet(t_us, src, sport, dst, dport, seq, flags, payload=b""):
    tcp = struct.pack(">HHIIBBHHH", sport, dport, seq, 0, 5 << 4, flags, 65535, 0, 0) + payload
    ip = struct.pack(">BBHHHBBH4s4s", 0x45, 0, 20 + len(tcp), 0, 0, 64, 6, 0, ip4(src), ip4(dst)) + tcp
    frame = b"\x00" * 12 + b"\x08\x00" + ip
    return struct.pack("<IIII", t_us // 1_000_000, t_us % 1_000_000, len(frame), len(frame)) + frame


def pcap(packets):
    return struct.pack("<IHHiIII", 0xA1B2C3D4, 2, 4, 0, 0, 65535, 1) + b"".join(packets)


C, S = "127.0.0.1", "127.0.0.1"


class StreamTests(unittest.TestCase):
    def write(self, packets):
        fd, path = tempfile.mkstemp(suffix=".pcap")
        os.write(fd, pcap(packets))
        os.close(fd)
        self.addCleanup(os.remove, path)
        return path

    def test_connection_is_reassembled_per_direction_with_times(self):
        path = self.write([
            packet(1_000_000, C, 40000, S, 9060, 999, SYN),
            packet(1_000_100, S, 9060, C, 40000, 4999, SYN | ACK),
            packet(1_001_000, C, 40000, S, 9060, 1000, PSH | ACK, b"hello "),
            packet(1_002_000, S, 9060, C, 40000, 5000, PSH | ACK, b"hi"),
            # out of order and overlapping retransmission
            packet(1_004_000, C, 40000, S, 9060, 1009, PSH | ACK, b"ld"),
            packet(1_003_000, C, 40000, S, 9060, 1006, PSH | ACK, b"wor"),
            packet(1_005_000, C, 40000, S, 9060, 1006, PSH | ACK, b"wor"),
        ])
        conns = list(pcapstream.connections(path, ports={9060}))
        self.assertEqual(len(conns), 1)
        c = conns[0]
        self.assertEqual((c.client_port, c.server_port), (40000, 9060))
        self.assertEqual(c.client.data, b"hello world")
        self.assertEqual(c.server.data, b"hi")
        self.assertIsNone(c.client.gap)
        self.assertEqual(c.client.time_at(0), 1001)
        self.assertEqual(c.client.time_at(7), 1003)

    def test_hole_is_reported_and_data_stops_before_it(self):
        path = self.write([
            packet(1_000_000, C, 40001, S, 9060, 99, SYN),
            packet(1_000_100, S, 9060, C, 40001, 499, SYN | ACK),
            packet(1_001_000, C, 40001, S, 9060, 100, PSH | ACK, b"abc"),
            packet(1_002_000, C, 40001, S, 9060, 110, PSH | ACK, b"xyz"),
        ])
        c = next(pcapstream.connections(path, ports={9060}))
        self.assertEqual(c.client.data, b"abc")
        self.assertEqual(c.client.gap, (3, 10))
        self.assertEqual(c.client.after_gap, 3)

    def test_connections_without_handshake_and_other_ports_are_counted_not_returned(self):
        path = self.write([
            packet(1_000_000, C, 40002, S, 9060, 100, PSH | ACK, b"mid-stream"),
            packet(1_000_000, C, 40003, S, 1234, 99, SYN),
        ])
        stats = {}
        self.assertEqual(list(pcapstream.connections(path, ports={9060}, stats=stats)), [])
        self.assertEqual(stats["without_handshake"], 1)


if __name__ == "__main__":
    unittest.main()
