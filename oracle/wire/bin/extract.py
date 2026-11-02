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


class Gap(Exception):
    """Bytes are missing from a reassembled stream."""


def reassemble(segments, start_seq):
    """Concatenate (seq_raw, payload) segments of one direction starting at `start_seq`
    (the ISN + 1), modulo 2^32, keeping each byte once and failing on a hole."""
    pieces = sorted(((seq - start_seq) % 2**32, payload) for seq, payload in segments)
    out = bytearray()
    for off, payload in pieces:
        if off > len(out):
            raise Gap(f"bytes {len(out)}..{off} missing")
        end = off + len(payload)
        if end > len(out):
            out += payload[len(out) - off:]
    return bytes(out)


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


def thrift_messages(pcap, streams):
    ports = ", ".join(str(p) for p in THRIFT_PORTS)
    segs = {}
    for frame, stream, sport, seq, payload in fields(pcap, f"tcp.len > 0 && tcp.port in {{{ports}}}", ["frame.number", "tcp.stream", "tcp.srcport", "tcp.seq_raw", "tcp.payload"]):
        s = int(stream)
        if s not in streams:
            continue
        to_server = int(sport) == streams[s]["client_port"]
        segs.setdefault((s, to_server), []).append((int(seq), bytes.fromhex(payload), int(frame)))
    out = []
    for (s, to_server), items in sorted(segs.items()):
        info = streams[s]
        start = (info["client_isn"] if to_server else info["server_isn"]) + 1
        data = reassemble([(seq, p) for seq, p, _ in items], start)
        frames = sorted(((seq - start) % 2**32, frame) for seq, _, frame in items)
        for m in thrift_split.split_messages(data):
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
    return out


def _pairs(obj, key):
    return [v for k, v in obj if k == key]


def grpc_messages(pcap, streams):
    text = tshark(pcap, ["-d", f"tcp.port=={GRPC_PORT},http2", "-Y", "http2.type == 0", "-T", "json"])
    packets = json.loads(text, object_pairs_hook=lambda p: p) if text.strip() else []
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
    messages = thrift_messages(pcap, streams) + grpc_messages(pcap, streams)
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
        "capture": {"pcap": os.path.basename(pcap), "sha256": pcap_sha, "streams": len(streams)},
        "messages": entries,
    }
    with open(os.path.join(CORPUS, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
        fh.write("\n")
    counts = {}
    for e in entries:
        counts[(e["protocol"], e["kind"])] = counts.get((e["protocol"], e["kind"]), 0) + 1
    print(f"{len(entries)} messages from {len(streams)} streams: " + ", ".join(f"{p} {k} {n}" for (p, k), n in sorted(counts.items())))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
