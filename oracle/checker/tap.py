"""A transparent TCP tap in front of a MySQL server, recording every session it forwards.

    tap.py --listen HOST:PORT --upstream HOST:PORT --out DIR

Each connection is written to DIR/<n>.jsonl, one line per forwarded chunk:
{"t_ms": <epoch ms>, "dir": "c2s" | "s2c", "hex": <bytes>}. The only change made to the traffic
is that CLIENT_SSL is cleared in the server greeting, so the client stays on plain text and the
session can be recorded; the log holds the bytes as forwarded. Standard library only.
"""
import argparse
import asyncio
import itertools
import json
import os
import signal
import time

CLIENT_SSL = 0x0800


def now_ms():
    return time.time_ns() // 1_000_000


def clear_ssl(greeting):
    """The greeting packet (header included) with CLIENT_SSL cleared in the lower capabilities."""
    data = bytearray(greeting)
    version_end = data.index(b"\x00", 5)
    caps_at = version_end + 1 + 4 + 8 + 1
    caps = int.from_bytes(data[caps_at:caps_at + 2], "little") & ~CLIENT_SSL
    data[caps_at:caps_at + 2] = caps.to_bytes(2, "little")
    return bytes(data)


class Log:
    def __init__(self, path):
        self.fh = open(path, "w", encoding="utf-8")

    def write(self, direction, data):
        self.fh.write(json.dumps({"t_ms": now_ms(), "dir": direction, "hex": data.hex()}) + "\n")
        self.fh.flush()

    def close(self):
        self.fh.close()


async def pump(reader, writer, log, direction, first_packet_hook=None):
    pending = b""
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            if first_packet_hook is not None:
                pending += data
                if len(pending) < 4:
                    continue
                need = 4 + int.from_bytes(pending[:3], "little")
                if len(pending) < need:
                    continue
                data = first_packet_hook(pending[:need]) + pending[need:]
                first_packet_hook, pending = None, b""
            log.write(direction, data)
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.IncompleteReadError):
        pass
    finally:
        try:
            writer.close()
        except Exception:  # noqa: BLE001 - the peer may already be gone
            pass


async def serve(listen_host, listen_port, upstream_host, upstream_port, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    counter = itertools.count(1)

    async def handle(client_reader, client_writer):
        n = next(counter)
        log = Log(os.path.join(out_dir, f"{os.getpid()}-{n:06d}.jsonl"))
        try:
            server_reader, server_writer = await asyncio.open_connection(upstream_host, upstream_port)
        except OSError:
            client_writer.close()
            log.close()
            return
        await asyncio.gather(
            pump(client_reader, server_writer, log, "c2s"),
            pump(server_reader, client_writer, log, "s2c", first_packet_hook=clear_ssl),
        )
        log.close()

    return await asyncio.start_server(handle, listen_host, listen_port)


def read_log(path):
    """(c2s bytes, s2c bytes, client_times) of one connection log. client_times lists
    (byte offset in c2s, t_ms) for every client chunk."""
    c2s, s2c, times = bytearray(), bytearray(), []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            rec = json.loads(line)
            data = bytes.fromhex(rec["hex"])
            if rec["dir"] == "c2s":
                times.append((len(c2s), rec["t_ms"]))
                c2s += data
            else:
                s2c += data
    return bytes(c2s), bytes(s2c), times


async def main_async(args):
    lh, lp = args.listen.rsplit(":", 1)
    uh, up = args.upstream.rsplit(":", 1)
    server = await serve(lh, int(lp), uh, int(up), args.out)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    print(f"tap listening on {args.listen} -> {args.upstream}, logs in {args.out}", flush=True)
    await stop.wait()
    server.close()
    await server.wait_closed()


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--listen", required=True)
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--out", required=True)
    asyncio.run(main_async(parser.parse_args()))


if __name__ == "__main__":
    main()
