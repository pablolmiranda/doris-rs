"""A run's FE<->BE messages, from its packet capture.

    traffic.py PCAP OUT.bin.gz

Thrift (9020, 9050, 9060) and BE HTTP (8040, stream load) connections are reassembled in one
streaming pass (pcapstream.py: payloads spooled to disk, so a run's tens of gigabytes never sit
in memory); gRPC (8060) needs tshark's HPACK decoding and is cut out with tcpdump first. Every
message gets the epoch time of the segment carrying its first byte (time_ms), which derive.py
uses to give it to the suite that was running. Holes in the capture are written next to the
output (traffic-holes.json) and printed, never dropped silently.
"""
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "wire", "bin"))
import extract  # noqa: E402
import pcapstream  # noqa: E402
import records  # noqa: E402
import thrift_split  # noqa: E402

json_array_items = extract.json_array_items
HTTP_PORT = 8040


class Incomplete(Exception):
    """A stream ends inside an HTTP message."""


def _head(data, pos):
    end = data.find(b"\r\n\r\n", pos)
    if end < 0:
        raise Incomplete(f"no end of headers after byte {pos}")
    lines = data[pos:end].split(b"\r\n")
    headers = {}
    for line in lines[1:]:
        k, _, v = line.partition(b":")
        headers[k.strip().lower().decode("latin-1")] = v.strip().decode("latin-1")
    return lines[0].decode("latin-1"), headers, end + 4


def _body_end(data, pos, headers):
    if headers.get("transfer-encoding", "").lower() == "chunked":
        while True:
            line_end = data.find(b"\r\n", pos)
            if line_end < 0:
                raise Incomplete("chunk size line cut off")
            size = int(data[pos:line_end].split(b";")[0], 16)
            pos = line_end + 2 + size + 2
            if pos > len(data):
                raise Incomplete("chunk cut off")
            if size == 0:
                return pos
    end = pos + int(headers.get("content-length", "0"))
    if end > len(data):
        raise Incomplete("body shorter than Content-Length")
    return end


def http_exchanges(c2s, s2c, complete_only=False):
    """[{method, path, status, request, response}] of one HTTP/1.1 connection, both directions
    reassembled. Interim 1xx responses stay with the final response they precede. With
    complete_only, an exchange cut off by the end of the data (a capture hole) ends the list
    instead of raising Incomplete."""
    out, cpos, spos = [], 0, 0
    while cpos < len(c2s):
        try:
            start = cpos
            line, headers, cpos = _head(c2s, cpos)
            cpos = _body_end(c2s, cpos, headers)
            method, path = (line.split(" ") + ["", ""])[:2]
            rstart, status = spos, None
            while spos < len(s2c):
                sline, sheaders, spos = _head(s2c, spos)
                status = int(sline.split(" ")[1])
                if 100 <= status < 200:
                    continue
                spos = _body_end(s2c, spos, sheaders)
                break
        except Incomplete:
            if complete_only:
                return out
            raise
        if complete_only and status is None:
            return out
        out.append({"method": method, "path": path, "status": status,
                    "request": c2s[start:cpos], "response": s2c[rstart:spos]})
    return out


def http_records(conn, stream_id):
    """(records, damaged): a request and a response record for every complete exchange on one
    BE HTTP connection, timed by the request's first byte, and its directions with a hole."""
    damaged = [{"tcp_stream": stream_id, "from": side, "gap": list(d.gap)}
               for side, d in (("client", conn.client), ("server", conn.server)) if d.gap]
    out, at = [], 0
    for ex in http_exchanges(conn.client.data, conn.server.data, complete_only=True):
        kind = "stream_load" if ex["path"].endswith("/_stream_load") else ex["path"].split("?")[0]
        header = {"protocol": "http", "service": "http", "method": kind, "path": ex["path"],
                  "status": ex["status"], "tcp_stream": stream_id, "time_ms": conn.client.time_at(at)}
        out.append(({**header, "kind": "request"}, ex["request"]))
        out.append(({**header, "kind": "response"}, ex["response"]))
        at += len(ex["request"])
    return out, damaged


def thrift_records(conn, stream_id):
    """(records, holes): the Thrift messages of both directions of one connection, each timed by
    its first byte; a direction with a hole or a partial message at its end is reported."""
    out, holes = [], []
    for side, d, kinds in (("client", conn.client, "to_server"), ("server", conn.server, "to_client")):
        msgs = thrift_split.split_messages(d.data)
        left = len(d.data) - (msgs[-1].end if msgs else 0) + d.after_gap
        if left or d.gap:
            entry = {"tcp_stream": stream_id, "from": side, "bytes": left}
            if d.gap:
                entry["gap"] = list(d.gap)
            holes.append(entry)
        for m in msgs:
            out.append(({"protocol": "thrift", "port": conn.server_port,
                         "service": extract.THRIFT_PORTS[conn.server_port], "method": m.name,
                         "kind": thrift_split.KINDS[m.mtype], "seqid": m.seqid, "tcp_stream": stream_id,
                         "time_ms": d.time_at(m.start)}, d.data[m.start:m.end]))
    return out, holes


def grpc_records(pcap, tmp_dir):
    """gRPC (8060) messages, read by tshark (HPACK headers name the method) from a capture cut
    down to that port with tcpdump, so tshark never reads the whole run."""
    small = os.path.join(tmp_dir, "grpc.pcap")
    subprocess.run(["tcpdump", "-r", pcap, "-w", small, f"tcp port {extract.GRPC_PORT}"],
                   check=True, capture_output=True)
    streams = extract.syn_streams(small)
    times = extract.frame_times(small, [extract.GRPC_PORT])
    out = []
    for m in extract.grpc_messages(small, streams):
        data = m.pop("data")
        m["time_ms"] = times.get(m["frame"])
        out.append((m, data))
    return out


def extract_run(pcap, out_path):
    stats = {}
    items, holes = [], []
    for i, conn in enumerate(pcapstream.connections(pcap, {*extract.THRIFT_PORTS, HTTP_PORT}, stats)):
        if conn.server_port == HTTP_PORT:
            recs, damaged = http_records(conn, i)
        else:
            recs, damaged = thrift_records(conn, i)
        items += recs
        holes += [d for d in damaged if "gap" in d]
    with tempfile.TemporaryDirectory() as tmp:
        items += grpc_records(pcap, tmp)
    items.sort(key=lambda it: (it[0]["time_ms"] or 0))
    records.write(out_path, items)
    with open(os.path.join(os.path.dirname(os.path.abspath(out_path)), "traffic-holes.json"), "w", encoding="utf-8") as fh:
        json.dump(holes, fh, indent=1)
    if holes:
        print(f"{len(holes)} connection directions with a capture hole: {holes[:5]}", file=sys.stderr)
    return {"messages": len(items), "holes": len(holes), "thrift_or_http_without_handshake": stats["without_handshake"]}


def main(argv):
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    print(extract_run(argv[1], argv[2]))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
