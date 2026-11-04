"""Streaming TCP reassembly from a pcap file, standard library only.

connections(path, ports) reads the capture once, packet by packet, keeping payload bytes in a
temporary spool file and only a small index in memory, then yields every connection on one of
the ports whose handshake was captured, each direction reassembled by sequence number. An
hour-long suite run captures tens of gigabytes; nothing here holds the capture in memory.

A direction stops at its first hole (a byte range that was never captured): unframed protocols
cannot be split again after one, so `gap` and `after_gap` say what was lost instead.
"""
import bisect
import collections
import os
import struct
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "wire", "bin"))
import extract  # noqa: E402

SYN, ACK = 0x02, 0x10


class Direction:
    def __init__(self):
        self.isn = None
        self.pieces = []  # (relative seq, spool offset, length, t_ms)
        self.data = b""
        self.gap = None
        self.after_gap = 0
        self._starts, self._times = [], []

    def time_at(self, offset):
        """t_ms of the segment that first carried byte `offset` of the reassembled data."""
        i = bisect.bisect_right(self._starts, offset) - 1
        return self._times[i] if i >= 0 else None

    def reassemble(self, spool):
        pieces = sorted(self.pieces)
        uses, self.gap, self.after_gap = extract.until_gap([(off, n) for off, _, n, _ in pieces])
        out = bytearray()
        for i, skip in uses:
            _, at, n, t = pieces[i]
            spool.seek(at)
            self._starts.append(len(out))
            self._times.append(t)
            out += spool.read(n)[skip:]
        self.data = bytes(out)
        self.pieces = []


class Connection:
    def __init__(self, client, server, first_ms):
        self.client_addr, self.client_port = client
        self.server_addr, self.server_port = server
        self.first_ms = first_ms
        self.client, self.server = Direction(), Direction()


def _links(linktype, frame):
    """The IP packet inside a link-layer frame, or None."""
    if linktype == 1:  # Ethernet
        ethertype, pos = struct.unpack(">H", frame[12:14])[0], 14
        while ethertype == 0x8100:
            ethertype, pos = struct.unpack(">H", frame[pos + 2:pos + 4])[0], pos + 4
        return frame[pos:] if ethertype in (0x0800, 0x86DD) else None
    if linktype == 113:  # Linux cooked
        return frame[16:] if struct.unpack(">H", frame[14:16])[0] in (0x0800, 0x86DD) else None
    if linktype == 276:  # Linux cooked v2
        return frame[20:] if struct.unpack(">H", frame[0:2])[0] in (0x0800, 0x86DD) else None
    if linktype == 0:  # BSD loopback
        return frame[4:]
    if linktype in (12, 101):  # raw IP
        return frame
    return None


def _tcp(ip):
    """(src, dst, sport, dport, seq, flags, payload) of a TCP packet, or None."""
    if not ip:
        return None
    version = ip[0] >> 4
    if version == 4:
        ihl, total = (ip[0] & 0x0F) * 4, struct.unpack(">H", ip[2:4])[0]
        if ip[9] != 6:
            return None
        src, dst, seg = ip[12:16], ip[16:20], ip[ihl:total or len(ip)]
    elif version == 6:
        if ip[6] != 6:
            return None
        src, dst, seg = ip[8:24], ip[24:40], ip[40:40 + struct.unpack(">H", ip[4:6])[0]]
    else:
        return None
    if len(seg) < 20:
        return None
    sport, dport, seq = struct.unpack(">HHI", seg[:8])
    offset, flags = (seg[12] >> 4) * 4, seg[13]
    return src, dst, sport, dport, seq, flags, seg[offset:]


def packets(path):
    """(t_ms, src, dst, sport, dport, seq, flags, payload) for every TCP packet in the capture."""
    with open(path, "rb") as fh:
        header = fh.read(24)
        magic = struct.unpack("<I", header[:4])[0]
        endian = "<" if magic in (0xA1B2C3D4, 0xA1B23C4D) else ">"
        nanos = magic in (0xA1B23C4D, 0x4D3CB2A1)
        linktype = struct.unpack(endian + "I", header[20:24])[0] & 0x0FFFFFFF
        rec = struct.Struct(endian + "IIII")
        while True:
            h = fh.read(16)
            if len(h) < 16:
                return
            sec, frac, incl, _ = rec.unpack(h)
            frame = fh.read(incl)
            parsed = _tcp(_links(linktype, frame))
            if parsed is None:
                continue
            t_ms = sec * 1000 + (frac // 1_000_000 if nanos else frac // 1000)
            yield (t_ms, *parsed)


def connections(path, ports, stats=None):
    """Yield every handshaken connection whose server port is in `ports`, in order of their SYN,
    each direction reassembled. stats, if given, receives counts of what was not returned."""
    stats = {} if stats is None else stats
    stats.setdefault("without_handshake", 0)
    live, done, orphans = {}, [], set()
    with tempfile.TemporaryFile() as spool:
        for t_ms, src, dst, sport, dport, seq, flags, payload in packets(path):
            if sport not in ports and dport not in ports:
                continue
            key = (src, sport, dst, dport)
            rkey = (dst, dport, src, sport)
            if flags & SYN and not flags & ACK:
                if key in live:
                    done.append(live.pop(key))
                c = Connection((src, sport), (dst, dport), t_ms)
                c.client.isn = seq
                live[key] = c
                continue
            if key in live:
                c, d = live[key], live[key].client
            elif rkey in live:
                c, d = live[rkey], live[rkey].server
            else:
                if payload:
                    orphans.add(frozenset((key, rkey)))
                continue
            if flags & SYN and flags & ACK:
                d.isn = seq
                continue
            if payload and d.isn is not None:
                spool.seek(0, os.SEEK_END)
                at = spool.tell()
                spool.write(payload)
                d.pieces.append(((seq - d.isn - 1) % 2**32, at, len(payload), t_ms))
        stats["without_handshake"] = len(orphans)
        done.extend(live.values())
        live.clear()
        # Drop each connection from this list as it is yielded, so its reassembled bytes are
        # freed once the caller lets go of it (a run has thousands of connections).
        queue = collections.deque(sorted(done, key=lambda c: c.first_ms))
        done.clear()
        while queue:
            c = queue.popleft()
            if c.server_port not in ports or c.server.isn is None:
                continue
            spool.flush()
            c.client.reassemble(spool)
            c.server.reassemble(spool)
            yield c
