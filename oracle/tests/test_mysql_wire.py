"""MySQL decoding seam: recorded client and server bytes in, canonical exchanges out."""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "checker"))
sys.path.insert(0, os.path.dirname(__file__))
import mysql_wire  # noqa: E402
import mysqlfake as f  # noqa: E402


class TextProtocolTests(unittest.TestCase):
    def test_query_returns_canonical_resultset(self):
        c2s, s2c = f.session([([f.query("select 1, null")],
                               f.resultset([f.coldef("1", f.MYSQL_TYPE_LONGLONG, 20), f.coldef("NULL")], [["1", None]]))])
        exchanges = mysql_wire.decode_session(c2s, s2c)
        self.assertEqual([e["command"] for e in exchanges], ["handshake", "COM_QUERY"])
        reply = exchanges[1]["reply"]
        self.assertEqual(exchanges[1]["statement"], "select 1, null")
        self.assertEqual(reply["type"], "resultset")
        self.assertEqual(reply["rows"], [["31", None]])
        self.assertEqual(reply["columns"][0], {
            "catalog": "def", "schema": "db", "table": "t", "org_table": "t", "name": "1", "org_name": "1",
            "charset": 33, "length": 20, "col_type": f.MYSQL_TYPE_LONGLONG, "flags": 0, "decimals": 0})

    def test_err_reply(self):
        c2s, s2c = f.session([([f.query("select x")], [f.err(1105, "HY000", "errCode = 2, detailMessage = x")])])
        reply = mysql_wire.decode_session(c2s, s2c)[1]["reply"]
        self.assertEqual(reply, {"type": "err", "code": 1105, "sqlstate": "HY000", "message": "errCode = 2, detailMessage = x"})

    def test_ok_reply(self):
        c2s, s2c = f.session([([f.query("insert into t values (1)")], [f.ok(affected=1, info=b"{'label':'x'}")])])
        reply = mysql_wire.decode_session(c2s, s2c)[1]["reply"]
        self.assertEqual((reply["type"], reply["affected_rows"], reply["info"]), ("ok", 1, "{'label':'x'}"))

    def test_ok_info_is_length_encoded_under_session_track(self):
        caps = f.BASE_CAPS | 0x00800000
        info = b"{'label':'x', 'txnId':'42'}"
        c2s, s2c = f.session([([f.query("insert into t values (1)")], [f.ok(affected=1, info=f.lenenc_str(info))])], caps=caps)
        self.assertEqual(mysql_wire.decode_session(c2s, s2c)[1]["reply"]["info"], info.decode())

    def test_deprecate_eof_gives_the_same_reply_as_classic_eof(self):
        cols, rows = [f.coldef("a")], [["x"], [None]]
        classic = f.session([([f.query("q")], f.resultset(cols, rows))])
        caps = f.BASE_CAPS | f.CLIENT_DEPRECATE_EOF
        modern = f.session([([f.query("q")], f.resultset(cols, rows, deprecate_eof=True))], caps=caps)
        self.assertEqual(mysql_wire.decode_session(*classic)[1], mysql_wire.decode_session(*modern)[1])


class UnusualReplyTests(unittest.TestCase):
    def test_large_packet_is_joined(self):
        big = "x" * (0xFFFFFF + 10)
        c2s, s2c = f.session([([f.query("select big")], f.resultset([f.coldef("b")], [[big]]))])
        rows = mysql_wire.decode_session(c2s, s2c)[1]["reply"]["rows"]
        self.assertEqual(len(rows[0][0]), 2 * len(big))

    def test_more_results_exists_gives_one_multi_reply(self):
        more = f.STATUS_AUTOCOMMIT | f.SERVER_MORE_RESULTS_EXISTS
        server = (f.resultset([f.coldef("a")], [["1"]], status=more)
                  + f.resultset([f.coldef("b")], [["2"]], status=more) + [f.ok()])
        c2s, s2c = f.session([([f.query("select 1; select 2; set x=1")], server)])
        reply = mysql_wire.decode_session(c2s, s2c)[1]["reply"]
        self.assertEqual(reply["type"], "multi")
        self.assertEqual([p["type"] for p in reply["parts"]], ["resultset", "resultset", "ok"])
        self.assertEqual(reply["parts"][1]["rows"], [["32"]])

    def test_local_infile_exchange(self):
        client = [f.query("load data local infile 'a.csv' into table t"), b"1,a\n", b"2,b\n", b""]
        server = [b"\xfb" + b"a.csv", f.ok(affected=2)]
        c2s = f.frame(1, f.login()) + f.frame(0, client[0]) + f.frames(2, client[1:])
        s2c = f.frame(0, f.greeting()) + f.frame(2, f.ok()) + f.frame(1, server[0]) + f.frame(5, server[1])
        exchanges = mysql_wire.decode_session(c2s, s2c)
        self.assertEqual(len(exchanges), 2)
        self.assertEqual(exchanges[1]["reply"], {"type": "local_infile", "file": "a.csv", "then": mysql_wire.parse_ok(f.ok(affected=2))})

    def test_truncated_stream_is_an_error_not_a_hang(self):
        c2s, s2c = f.session([([f.query("select 1")], f.resultset([f.coldef("a")], [["1"]]))])
        with self.assertRaises(mysql_wire.DecodeError):
            mysql_wire.decode_session(c2s, s2c[:-3])

    def test_last_command_the_server_never_answered_is_no_reply(self):
        # A target that hangs until the client gives up: the command is sent, nothing comes back.
        c2s, s2c = f.session([([f.query("select 1")], f.resultset([f.coldef("a")], [["1"]])),
                              ([f.query("delete from t where a = 1")], [])])
        exchanges = mysql_wire.decode_session(c2s, s2c)
        self.assertEqual(exchanges[-1]["statement"], "delete from t where a = 1")
        self.assertEqual(exchanges[-1]["reply"], {"type": "no_reply"})


class PreparedStatementTests(unittest.TestCase):
    def test_execute_carries_statement_params_and_binary_rows(self):
        import struct
        col = f.coldef("r", f.MYSQL_TYPE_LONGLONG, 20)
        param = f.coldef("?", f.MYSQL_TYPE_LONG, 11)
        dt = f.coldef("d", f.MYSQL_TYPE_DATETIME, 23, decimals=3)
        commands = [
            ([f.prepare("select ? + 1, now(3)")], f.prepare_ok(9, [col, dt], [param])),
            ([f.execute(9, [(f.MYSQL_TYPE_LONG, struct.pack("<i", 41))])],
             f.binary_resultset([col, dt], [[struct.pack("<q", 42), bytes([7]) + struct.pack("<HBBBBB", 2026, 11, 2, 6, 11, 48)],
                                            [None, None]])),
            ([b"\x19" + struct.pack("<I", 9)], []),
        ]
        exchanges = mysql_wire.decode_session(*f.session(commands))
        prep, ex, close = exchanges[1], exchanges[2], exchanges[3]
        self.assertEqual(prep["command"], "COM_STMT_PREPARE")
        self.assertEqual(prep["reply"]["type"], "prepare_ok")
        self.assertNotIn("statement_id", prep["reply"])
        self.assertEqual(ex["statement"], "select ? + 1, now(3)")
        self.assertEqual(ex["params"], [{"type": f.MYSQL_TYPE_LONG, "value": "29000000"}])
        self.assertEqual(ex["reply"]["protocol"], "binary")
        self.assertEqual(ex["reply"]["rows"], [["2a00000000000000", "07ea070b02060b30"], [None, None]])
        self.assertEqual((close["command"], close["reply"]), ("COM_STMT_CLOSE", None))


if __name__ == "__main__":
    unittest.main()
