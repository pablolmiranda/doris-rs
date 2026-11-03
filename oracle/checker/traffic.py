"""A run's FE<->BE messages, from its packet capture.

    traffic.py PCAP OUT.bin.gz

Thrift (9020, 9050, 9060) and gRPC (8060) messages come from the wire extractor
(oracle/wire/bin/extract.py); BE HTTP on 8040 (stream load) is split into request/response
exchanges here. Every message gets the epoch time of its first segment (time_ms), which
derive.py uses to give it to the suite that was running. Nothing is dropped silently: streams
the extractor could not split are printed.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "wire", "bin"))
import extract  # noqa: E402
import records  # noqa: E402

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


def http_messages(segs, streams, times):
    """(records, damaged): records for every complete exchange on the BE HTTP port, timed by the
    request's first segment, and the directions with a capture hole (exchanges from the hole on
    are not recorded)."""
    out, damaged = [], []
    by_direction = {}
    for frame, s, sport, _, seq, payload in segs:
        if s in streams and streams[s]["server_port"] == HTTP_PORT:
            by_direction.setdefault((s, sport == streams[s]["client_port"]), []).append((seq, payload, frame))
    for s in sorted({s for s, _ in by_direction}):
        info = streams[s]
        data = {}
        for to_server, isn in ((True, info["client_isn"]), (False, info["server_isn"])):
            items = by_direction.get((s, to_server), [])
            data[to_server], gap, _ = extract.reassemble_until_gap([(q, p) for q, p, _ in items], isn + 1)
            if gap:
                damaged.append({"tcp_stream": s, "from": "client" if to_server else "server", "gap": list(gap)})
        first = min((f for _, _, f in by_direction.get((s, True), [])), default=0)
        for ex in http_exchanges(data[True], data[False], complete_only=True):
            kind = "stream_load" if ex["path"].endswith("/_stream_load") else ex["path"].split("?")[0]
            header = {"protocol": "http", "service": "http", "method": kind, "path": ex["path"],
                      "status": ex["status"], "tcp_stream": s, "time_ms": times.get(first)}
            out.append(({**header, "kind": "request"}, ex["request"]))
            out.append(({**header, "kind": "response"}, ex["response"]))
    return out, damaged


def extract_run(pcap, out_path):
    ports = [*extract.CAPTURED_PORTS, HTTP_PORT]
    streams = extract.syn_streams(pcap)
    segs = extract.segments(pcap, ports)
    times = extract.frame_times(pcap, ports)
    thrift, dropped = extract.thrift_messages(segs, streams)
    grpc = extract.grpc_messages(pcap, streams)
    items = []
    for m in sorted(thrift + grpc, key=lambda m: (m["frame"], m["protocol"], m["tcp_stream"])):
        data = m.pop("data")
        m["time_ms"] = times.get(m["frame"])
        items.append((m, data))
    http, damaged = http_messages(segs, streams, times)
    items += http
    items.sort(key=lambda it: (it[0]["time_ms"] or 0))
    records.write(out_path, items)
    without_handshake, without_messages = extract.unextracted_streams(
        [s for s in segs if s[3] != HTTP_PORT and s[2] != HTTP_PORT], streams, thrift + grpc)
    for label, entries in (("without handshake", without_handshake), ("without messages", without_messages),
                           ("ending inside a Thrift message or at a capture hole", dropped),
                           ("of BE HTTP with a capture hole", damaged)):
        if entries:
            print(f"{len(entries)} streams {label}: {entries[:5]}", file=sys.stderr)
    holes = [d for d in dropped if "gap" in d] + damaged
    with open(os.path.join(os.path.dirname(os.path.abspath(out_path)), "traffic-holes.json"), "w", encoding="utf-8") as fh:
        json.dump(holes, fh, indent=1)
    return {"messages": len(items), "streams": len(streams),
            "holes": sum(1 for d in dropped if "gap" in d) + len(damaged)}


def main(argv):
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    print(extract_run(argv[1], argv[2]))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
