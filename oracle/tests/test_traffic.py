"""Run-level traffic extraction: the pieces that turn a capture into FE<->BE message records."""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "checker"))
import traffic  # noqa: E402

PUT1 = b"PUT /api/db/t/_stream_load HTTP/1.1\r\nContent-Length: 4\r\nlabel: a\r\n\r\n1,a\n"
PUT2 = (b"PUT /api/db/t/_stream_load HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n"
        b"2\r\n2,\r\n2\r\nb\n\r\n0\r\n\r\n")
OK1 = b'HTTP/1.1 200 OK\r\nContent-Length: 17\r\n\r\n{"Status": "Ok"}\n'
OK2 = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n"


class HttpTests(unittest.TestCase):
    def test_requests_and_responses_pair_up_with_both_body_framings(self):
        exchanges = traffic.http_exchanges(PUT1 + PUT2, OK1 + OK2)
        self.assertEqual([(e["method"], e["path"], e["status"]) for e in exchanges],
                         [("PUT", "/api/db/t/_stream_load", 200)] * 2)
        self.assertEqual(exchanges[0]["request"], PUT1)
        self.assertEqual(exchanges[1]["request"], PUT2)
        self.assertEqual(exchanges[1]["response"], OK2)

    def test_expect_100_continue_interim_response_is_skipped(self):
        put = b"PUT /api/db/t/_stream_load HTTP/1.1\r\nExpect: 100-continue\r\nContent-Length: 1\r\n\r\nx"
        cont = b"HTTP/1.1 100 Continue\r\n\r\n"
        exchanges = traffic.http_exchanges(put, cont + OK1)
        self.assertEqual(exchanges[0]["status"], 200)
        self.assertEqual(exchanges[0]["response"], cont + OK1)

    def test_cut_off_request_is_reported_not_guessed(self):
        with self.assertRaises(traffic.Incomplete):
            traffic.http_exchanges(PUT1[:-2], b"")


class HttpGapTests(unittest.TestCase):
    def test_hole_keeps_complete_exchanges_before_it(self):
        streams = {7: {"client_port": 40000, "server_port": 8040, "client_isn": 100, "server_isn": 500}}
        segs = [(1, 7, 40000, 8040, 101, PUT1), (2, 7, 8040, 40000, 501, OK1),
                # the first bytes of PUT2 were never captured
                (3, 7, 40000, 8040, 101 + len(PUT1) + 10, PUT2[10:])]
        items, damaged = traffic.http_messages(segs, streams, {1: 1000})
        self.assertEqual([(h["kind"], d) for h, d in items], [("request", PUT1), ("response", OK1)])
        self.assertEqual(damaged, [{"tcp_stream": 7, "from": "client", "gap": [len(PUT1), len(PUT1) + 10]}])


class JsonStreamTests(unittest.TestCase):
    def test_array_items_are_yielded_as_they_complete(self):
        chunks = [b'[\n  {"a": "x}"', b', "b": [1, {"c": 2}]},\n  {"a": "y"}', b"\n]\n"]
        self.assertEqual(list(traffic.json_array_items(iter(chunks))), [{"a": "x}", "b": [1, {"c": 2}]}, {"a": "y"}])

    def test_empty_array(self):
        self.assertEqual(list(traffic.json_array_items(iter([b"[\n]\n"]))), [])


if __name__ == "__main__":
    unittest.main()
