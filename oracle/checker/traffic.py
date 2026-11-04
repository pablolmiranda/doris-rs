"""A run's FE<->BE messages, from its packet capture.

    traffic.py PCAP OUT.bin.gz

Thrift (9020, 9050, 9060) and BE HTTP (8040, stream load) connections are reassembled in one
streaming pass (pcapstream.py: payloads spooled to disk, so a run's tens of gigabytes never sit
in memory); gRPC (8060) needs tshark's HPACK decoding and is cut out with tcpdump first. Every
message gets the epoch time of the segment carrying its first byte (time_ms), which derive.py
uses to give it to the suite that was running. Directions that lost bytes (a hole in the
capture, or a message cut off at its end) are written next to the output (traffic-holes.json)
and printed, never dropped silently.
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


def _routes():
    with open(os.path.join(HERE, "..", "wire", "be_http_routes.txt"), encoding="utf-8") as fh:
        return [line.strip().split("/") for line in fh if line.strip() and not line.startswith("#")]


ROUTES = _routes()


def route(path):
    """The BE route template a request path hits (`/api/meta/{op}/{tablet_id}`), so requests to
    one endpoint share a name whatever ids or table names they carry; the bare path when no
    route matches. Literal segments win over placeholders."""
    bare = path.split("?")[0]
    segments = bare.split("/")
    best, literals = None, -1
    for template in ROUTES:
        if len(template) != len(segments):
            continue
        if all(t == s or t.startswith("{") for t, s in zip(template, segments)):
            n = sum(not t.startswith("{") for t in template)
            if n > literals:
                best, literals = template, n
    return "/".join(best) if best else bare


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


def damage(stream_id, side, direction, trailing=0):
    """What one direction lost, or None: its capture hole (`gap`) if any, and `bytes`, the
    captured bytes no record holds (a partial message at the end, plus everything past a hole)."""
    if not (direction.gap or trailing):
        return None
    entry = {"tcp_stream": stream_id, "from": side, "bytes": trailing + direction.after_gap}
    if direction.gap:
        entry["gap"] = list(direction.gap)
    return entry


def http_records(conn, stream_id):
    """(records, damaged): a request and a response record for every complete exchange on one
    BE HTTP connection, timed by the request's first byte, and its directions with a hole."""
    damaged = [e for e in (damage(stream_id, "client", conn.client), damage(stream_id, "server", conn.server)) if e]
    out, at = [], 0
    for ex in http_exchanges(conn.client.data, conn.server.data, complete_only=True):
        template = route(ex["path"])
        kind = "stream_load" if template == "/api/{db}/{table}/_stream_load" else template
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
    for side, d in (("client", conn.client), ("server", conn.server)):
        msgs = thrift_split.split_messages(d.data)
        entry = damage(stream_id, side, d, trailing=len(d.data) - (msgs[-1].end if msgs else 0))
        if entry:
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
        holes += damaged
    with tempfile.TemporaryDirectory() as tmp:
        items += grpc_records(pcap, tmp)
    items.sort(key=lambda it: (it[0]["time_ms"] or 0))
    records.write(out_path, items)
    with open(os.path.join(os.path.dirname(os.path.abspath(out_path)), "traffic-holes.json"), "w", encoding="utf-8") as fh:
        json.dump(holes, fh, indent=1)
    if holes:
        print(f"{len(holes)} connection directions lost bytes (a capture hole or a cut-off message): {holes[:5]}",
              file=sys.stderr)
    return {"messages": len(items), "holes": len(holes), "thrift_or_http_without_handshake": stats["without_handshake"]}


def main(argv):
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    print(extract_run(argv[1], argv[2]))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
