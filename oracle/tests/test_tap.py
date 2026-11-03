"""The MySQL tap forwards a session unchanged except for CLIENT_SSL, and records what it forwarded."""
import asyncio
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "checker"))
sys.path.insert(0, os.path.dirname(__file__))
import mysql_wire  # noqa: E402
import mysqlfake as f  # noqa: E402
import tap  # noqa: E402


async def fake_server(reader, writer):
    writer.write(f.frame(0, f.greeting(f.BASE_CAPS | f.CLIENT_SSL)))
    await writer.drain()
    await reader.readexactly(4 + len(f.login()))
    writer.write(f.frame(2, f.ok()))
    await writer.drain()
    header = await reader.readexactly(4)
    await reader.readexactly(int.from_bytes(header[:3], "little"))
    writer.write(f.frames(1, f.resultset([f.coldef("a")], [["1"]])))
    await writer.drain()
    await reader.read()
    writer.close()


async def scenario(out_dir):
    server = await asyncio.start_server(fake_server, "127.0.0.1", 0)
    upstream = server.sockets[0].getsockname()
    proxy = await tap.serve("127.0.0.1", 0, upstream[0], upstream[1], out_dir)
    host, port = proxy.sockets[0].getsockname()[:2]
    reader, writer = await asyncio.open_connection(host, port)
    seen = bytearray(await reader.readexactly(4 + len(f.greeting())))
    writer.write(f.frame(1, f.login()))
    seen += await reader.readexactly(4 + len(f.ok()))
    writer.write(f.frame(0, f.query("select 1")))
    expected = f.frames(1, f.resultset([f.coldef("a")], [["1"]]))
    seen += await reader.readexactly(len(expected))
    writer.close()
    await writer.wait_closed()
    await asyncio.sleep(0.1)
    proxy.close()
    await proxy.wait_closed()
    server.close()
    await server.wait_closed()
    return bytes(seen)


class TapTests(unittest.TestCase):
    def test_tap_clears_client_ssl_and_records_what_it_forwarded(self):
        with tempfile.TemporaryDirectory() as out:
            seen = asyncio.run(scenario(out))
            greeting_caps = int.from_bytes(seen[4 + 1 + 7 + 4 + 9:4 + 1 + 7 + 4 + 9 + 2], "little")
            self.assertEqual(greeting_caps & f.CLIENT_SSL, 0)
            logs = os.listdir(out)
            self.assertEqual(len(logs), 1)
            c2s, s2c, client_times = tap.read_log(os.path.join(out, logs[0]))
            self.assertEqual(s2c, seen)
            exchanges = mysql_wire.decode_session(c2s, s2c, client_times)
            self.assertEqual(exchanges[1]["statement"], "select 1")
            self.assertGreaterEqual(exchanges[1]["t_ms"], exchanges[0]["t_ms"])
            self.assertGreater(exchanges[0]["t_ms"], 1_700_000_000_000)
            self.assertEqual(exchanges[1]["reply"]["rows"], [["31"]])


if __name__ == "__main__":
    unittest.main()
