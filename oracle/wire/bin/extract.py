#!/usr/bin/env python3
"""Turn a wire capture into the committed wire corpus.

    extract.py [PCAP]        (default: the newest pcap in oracle/wire/captures/)

Writes oracle/wire/corpus/messages/{thrift,grpc}/<index>-<method>-<kind>.bin and manifest.json.
Only TCP streams whose SYN is in the capture are used: FE<->BE Thrift is unframed, so a stream
can be split into messages only from its first byte.

- Thrift (9020 FrontendService, 9050 HeartbeatService, 9060 BackendService): tshark exports
  every TCP segment; each direction is reassembled by sequence number and split with
  thrift_split. Client-to-server messages are calls, the reverse are replies.
- gRPC (8060 PBackendService): tshark's HTTP/2 dissector supplies each DATA frame's payload,
  stream id and request path; payloads are concatenated per stream and direction and split on
  the 5-byte gRPC prefix (compressed flag, big-endian length).
"""
import hashlib
import json
import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import thrift_split  # noqa: E402

WIRE = os.path.abspath(os.path.join(HERE, ".."))
ORACLE = os.path.abspath(os.path.join(WIRE, ".."))
CORPUS = os.path.join(WIRE, "corpus")
TSHARK_IMAGE = os.environ.get("CAPTURE_IMAGE", "nicolaka/netshoot:v0.16")
THRIFT_PORTS = {9020: "FrontendService", 9050: "HeartbeatService", 9060: "BackendService"}
GRPC_PORT = 8060
CAPTURED_PORTS = sorted({*THRIFT_PORTS, GRPC_PORT})
# A stream's protocol, recognised from the first bytes its client sent.
PREFIXES = [(b"PRI * HTTP/2.0", "http2"), (b"PRPC", "baidu_std"), (b"\x80\x01", "thrift")]


class Gap(Exception):
    """Bytes are missing from a reassembled stream."""


def until_gap(pieces):
    """pieces: (offset, length) of one direction's segments, sorted by offset. Returns (uses,
    gap, after): uses lists (index, skip) for each piece that adds bytes, skip being how many of
    its leading bytes are already covered; gap is the first (start, end) range never captured,
    or None; after counts the captured bytes past it."""
    uses, end = [], 0
    for i, (off, n) in enumerate(pieces):
        if off > end:
            return uses, (end, off), sum(length for _, length in pieces[i:])
        if off + n > end:
            uses.append((i, end - off))
            end = off + n
    return uses, None, 0


def _pieces(segments, start_seq):
    return sorted(((seq - start_seq) % 2**32, payload) for seq, payload in segments)


def reassemble(segments, start_seq):
    """Concatenate (seq_raw, payload) segments of one direction starting at `start_seq`
    (the ISN + 1), modulo 2^32, keeping each byte once and failing on a hole."""
    data, gap, _ = reassemble_until_gap(segments, start_seq)
    if gap:
        raise Gap(f"bytes {gap[0]}..{gap[1]} missing")
    return data


def reassemble_until_gap(segments, start_seq):
    """(bytes up to the first hole, (hole start, hole end) or None, captured bytes after it).
    Unframed Thrift cannot be split again after a hole, so a direction keeps its prefix only."""
    pieces = _pieces(segments, start_seq)
    uses, gap, after = until_gap([(off, len(p)) for off, p in pieces])
    return b"".join(pieces[i][1][skip:] for i, skip in uses), gap, after


def split_grpc(stream):
    """Split one direction of a gRPC stream into (compressed, message) pairs; a trailing
    partial message is dropped."""
    out, pos = [], 0
    while pos + 5 <= len(stream):
        flag = stream[pos]
        length = int.from_bytes(stream[pos + 1:pos + 5], "big")
        if pos + 5 + length > len(stream):
            break
        out.append((flag != 0, stream[pos + 5:pos + 5 + length]))
        pos += 5 + length
    return out


def json_array_items(chunks, object_pairs_hook=None):
    """Yield the items of a JSON array read as byte chunks, each as soon as it is complete, so a
    capture's dissection never has to fit in memory at once."""
    decoder = json.JSONDecoder(object_pairs_hook=object_pairs_hook)
    buf, started = "", False
    for chunk in chunks:
        buf += chunk.decode("utf-8", "replace")
        pos = 0
        while True:
            while pos < len(buf) and buf[pos] in " \t\r\n,":
                pos += 1
            if not started and pos < len(buf):
                if buf[pos] != "[":
                    raise ValueError("expected a JSON array")
                started, pos = True, pos + 1
                continue
            if pos >= len(buf) or buf[pos] == "]":
                break
            try:
                item, end = decoder.raw_decode(buf, pos)
            except json.JSONDecodeError:
                break
            yield item
            pos = end
        buf = buf[pos:]


def tshark_packets(pcap, args):
    """tshark -T json output, one packet at a time."""
    d, f = os.path.dirname(os.path.abspath(pcap)), os.path.basename(pcap)
    cmd = ["docker", "run", "--rm", "-v", f"{d}:/c:ro", TSHARK_IMAGE, "tshark", "-r", f"/c/{f}", *args, "-T", "json"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    try:
        yield from json_array_items(iter(lambda: proc.stdout.read(1 << 20), b""), object_pairs_hook=lambda p: p)
    finally:
        proc.stdout.close()
        if proc.wait() != 0:
            raise subprocess.CalledProcessError(proc.returncode, cmd)


def tshark(pcap, args):
    d, f = os.path.dirname(os.path.abspath(pcap)), os.path.basename(pcap)
    cmd = ["docker", "run", "--rm", "-v", f"{d}:/c:ro", TSHARK_IMAGE, "tshark", "-r", f"/c/{f}", *args]
    return subprocess.run(cmd, check=True, capture_output=True).stdout.decode("utf-8", "replace")


def fields(pcap, display_filter, names, extra=()):
    out = tshark(pcap, [*extra, "-Y", display_filter, "-T", "fields", "-E", "separator=/t", *sum((["-e", n] for n in names), [])])
    return [line.split("\t") for line in out.splitlines() if line]


def syn_streams(pcap):
    """tcp.stream -> {client_port, server_port, client_isn, server_isn} for streams opened in the capture."""
    streams = {}
    for stream, sport, dport, seq in fields(pcap, "tcp.flags.syn == 1 && tcp.flags.ack == 0", ["tcp.stream", "tcp.srcport", "tcp.dstport", "tcp.seq_raw"]):
        streams[int(stream)] = {"client_port": int(sport), "server_port": int(dport), "client_isn": int(seq)}
    for stream, seq in fields(pcap, "tcp.flags.syn == 1 && tcp.flags.ack == 1", ["tcp.stream", "tcp.seq_raw"]):
        if int(stream) in streams:
            streams[int(stream)]["server_isn"] = int(seq)
    return {s: v for s, v in streams.items() if "server_isn" in v}


def frame_times(pcap, ports=CAPTURED_PORTS):
    """{frame number: epoch milliseconds} for every payload-carrying segment on the ports."""
    plist = ", ".join(str(p) for p in ports)
    return {int(f): round(float(t) * 1000) for f, t in
            fields(pcap, f"tcp.len > 0 && tcp.port in {{{plist}}}", ["frame.number", "frame.time_epoch"])}


def segments(pcap, ports=CAPTURED_PORTS):
    """Every TCP segment carrying payload on the given ports, in capture order:
    (frame, tcp_stream, src_port, dst_port, seq_raw, payload)."""
    ports = ", ".join(str(p) for p in ports)
    rows = fields(pcap, f"tcp.len > 0 && tcp.port in {{{ports}}}",
                  ["frame.number", "tcp.stream", "tcp.srcport", "tcp.dstport", "tcp.seq_raw", "tcp.payload"])
    return [(int(f), int(s), int(sp), int(dp), int(seq), bytes.fromhex(p)) for f, s, sp, dp, seq, p in rows]


def classify(first_bytes):
    """Protocol of a stream from the first bytes its client sent."""
    return next((name for prefix, name in PREFIXES if first_bytes.startswith(prefix)), "unknown")


def thrift_messages(segs, streams):
    """(messages, dropped): the Thrift messages of every handshaken stream on a Thrift port, and
    each direction whose reassembled bytes end inside a message (those bytes are not stored)."""
    by_direction = {}
    for frame, s, sport, _, seq, payload in segs:
        if s not in streams or streams[s]["server_port"] not in THRIFT_PORTS:
            continue
        to_server = sport == streams[s]["client_port"]
        by_direction.setdefault((s, to_server), []).append((seq, payload, frame))
    out, dropped = [], []
    for (s, to_server), items in sorted(by_direction.items()):
        info = streams[s]
        start = (info["client_isn"] if to_server else info["server_isn"]) + 1
        data, gap, after = reassemble_until_gap([(seq, p) for seq, p, _ in items], start)
        frames = sorted(((seq - start) % 2**32, frame) for seq, _, frame in items)
        msgs = thrift_split.split_messages(data)
        left = len(data) - (msgs[-1].end if msgs else 0) + after
        if left or gap:
            entry = {"tcp_stream": s, "from": "client" if to_server else "server", "bytes": left}
            if gap:
                entry["gap"] = list(gap)
            dropped.append(entry)
        for m in msgs:
            first_frame = max(f for off, f in frames if off <= m.start) if frames else 0
            out.append({
                "protocol": "thrift",
                "port": info["server_port"],
                "service": THRIFT_PORTS[info["server_port"]],
                "method": m.name,
                "kind": thrift_split.KINDS[m.mtype],
                "seqid": m.seqid,
                "tcp_stream": s,
                "frame": first_frame,
                "data": data[m.start:m.end],
            })
    return out, dropped


def unextracted_streams(segs, streams, messages):
    """(without_handshake, without_messages): streams on a captured port whose SYN and SYN-ACK
    were not captured, and handshaken streams that produced no message, with their payload size
    and the first bytes their client sent."""
    seen = {}
    for _, s, sport, dport, _, payload in segs:
        server = streams[s]["server_port"] if s in streams else (dport if dport in CAPTURED_PORTS else sport)
        e = seen.setdefault(s, {"server_port": server, "bytes": 0, "first": b""})
        e["bytes"] += len(payload)
        if not e["first"] and sport != server:
            e["first"] = payload[:16]
    extracted = {m["tcp_stream"] for m in messages}
    without_handshake, without_messages = [], []
    for s, e in sorted(seen.items()):
        entry = {"tcp_stream": s, "server_port": e["server_port"], "bytes": e["bytes"],
                 "first_bytes": e["first"].hex(), "protocol": classify(e["first"])}
        if s not in streams:
            without_handshake.append(entry)
        elif s not in extracted:
            without_messages.append(entry)
    return without_handshake, without_messages


def _pairs(obj, key):
    return [v for k, v in obj if k == key]


def grpc_messages(pcap, streams):
    packets = tshark_packets(pcap, ["-d", f"tcp.port=={GRPC_PORT},http2", "-Y", "http2.type == 0"])
    flows, uris, first_frame = {}, {}, {}
    for pkt in packets:
        layers = dict(dict(pkt)["_source"])["layers"]
        frame = int(dict(_pairs(layers, "frame")[0])["frame.number"])
        tcp = dict(_pairs(layers, "tcp")[0])
        s = int(tcp["tcp.stream"])
        if s not in streams:
            continue
        to_server = int(tcp["tcp.srcport"]) == streams[s]["client_port"]
        for h2 in _pairs(layers, "http2"):
            for fr in _pairs(h2, "http2.stream"):
                f = dict(fr)
                if f.get("http2.type") != "0":
                    continue
                key = (s, int(f["http2.streamid"]), to_server)
                payload = f.get("http2.data.data", "")
                flows.setdefault(key, bytearray()).extend(bytes.fromhex(payload.replace(":", "")))
                first_frame.setdefault(key, frame)
                if "http2.request.full_uri" in f:
                    uris[key[:2]] = f["http2.request.full_uri"]
    out = []
    for (s, h2_stream, to_server), data in sorted(flows.items()):
        uri = uris.get((s, h2_stream), "")
        service, method = uri.rsplit("/", 2)[-2:] if uri.count("/") >= 2 else ("?", "?")
        for compressed, msg in split_grpc(bytes(data)):
            out.append({
                "protocol": "grpc",
                "port": GRPC_PORT,
                "service": service.split(".")[-1],
                "method": method,
                "kind": "request" if to_server else "response",
                "h2_stream": h2_stream,
                "tcp_stream": s,
                "compressed": compressed,
                "frame": first_frame[(s, h2_stream, to_server)],
                "data": msg,
            })
    return out


def reference_identity():
    values = {}
    with open(os.path.join(ORACLE, "versions.env"), encoding="utf-8") as fh:
        for line in fh:
            if "=" in line and not line.startswith("#"):
                k, v = line.strip().split("=", 1)
                values[k] = v
    return {"image": values.get("DORIS_IMAGE", ""), "version": values.get("DORIS_VERSION", ""), "src_tag": values.get("DORIS_SRC_TAG", "")}


def main(argv):
    if len(argv) > 1:
        pcap = argv[1]
    else:
        caps = sorted(f for f in os.listdir(os.path.join(WIRE, "captures")) if f.endswith(".pcap"))
        if not caps:
            raise SystemExit("no capture; run `make wire-capture`")
        pcap = os.path.join(WIRE, "captures", caps[-1])
    streams = syn_streams(pcap)
    segs = segments(pcap)
    thrift, dropped = thrift_messages(segs, streams)
    messages = thrift + grpc_messages(pcap, streams)
    without_handshake, without_messages = unextracted_streams(segs, streams, messages)
    messages.sort(key=lambda m: (m["frame"], m["protocol"], m["tcp_stream"]))
    shutil.rmtree(os.path.join(CORPUS, "messages"), ignore_errors=True)
    entries = []
    for i, m in enumerate(messages):
        rel = f"messages/{m['protocol']}/{i:04d}-{m['method']}-{m['kind']}.bin"
        path = os.path.join(CORPUS, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        data = m.pop("data")
        with open(path, "wb") as fh:
            fh.write(data)
        entries.append({"file": rel, **m, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    with open(pcap, "rb") as fh:
        pcap_sha = hashlib.sha256(fh.read()).hexdigest()
    manifest = {
        "corpus_version": 1,
        "reference": reference_identity(),
        "capture": {
            "pcap": os.path.basename(pcap),
            "sha256": pcap_sha,
            "streams": len(streams),
            # nothing below is in the corpus; the manifest tests decide what is acceptable
            "streams_without_handshake": without_handshake,
            "streams_without_messages": without_messages,
            "trailing_bytes_dropped": dropped,
        },
        "messages": entries,
    }
    with open(os.path.join(CORPUS, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
        fh.write("\n")
    counts = {}
    for e in entries:
        counts[(e["protocol"], e["kind"])] = counts.get((e["protocol"], e["kind"]), 0) + 1
    used = len({e["tcp_stream"] for e in entries})
    print(f"{len(entries)} messages from {used} of {len(streams)} streams: " + ", ".join(f"{p} {k} {n}" for (p, k), n in sorted(counts.items())))
    for label, items in (("without handshake (not split)", without_handshake),
                         ("without messages", without_messages)):
        for e in items:
            print(f"  stream {e['tcp_stream']} port {e['server_port']} {label}: {e['bytes']} bytes, {e['protocol']}")
    for d in dropped:
        print(f"  stream {d['tcp_stream']} {d['from']} side: {d['bytes']} trailing bytes dropped (partial message)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
