"""Decode a recorded MySQL session (client bytes and server bytes) into canonical exchanges.

An exchange is one client command and the server's whole reply, in the canonical form the
strict gate compares (Epic 1 spec §4.5): OK, ERR and result-set fields, rows as hex of the value
bytes or None for NULL.
"""
import struct

CLIENT_SSL = 0x00000800
CLIENT_DEPRECATE_EOF = 0x01000000
CLIENT_SESSION_TRACK = 0x00800000
SERVER_SESSION_STATE_CHANGED = 0x4000
SERVER_MORE_RESULTS_EXISTS = 0x0008

COMMANDS = {
    0x01: "COM_QUIT", 0x02: "COM_INIT_DB", 0x03: "COM_QUERY", 0x04: "COM_FIELD_LIST",
    0x0E: "COM_PING", 0x16: "COM_STMT_PREPARE", 0x17: "COM_STMT_EXECUTE",
    0x18: "COM_STMT_SEND_LONG_DATA", 0x19: "COM_STMT_CLOSE", 0x1A: "COM_STMT_RESET",
    0x1B: "COM_SET_OPTION",
}
NO_REPLY = {"COM_QUIT", "COM_STMT_CLOSE", "COM_STMT_SEND_LONG_DATA"}


class DecodeError(Exception):
    pass


def packets(stream):
    """[(sequence_id, payload)] of a byte stream; a payload split across 0xFFFFFF-byte frames
    is joined. A stream that ends inside a frame is an error."""
    return [(seq, payload) for seq, payload, _ in packets_at(stream)]


def packets_at(stream):
    """Like packets(), with the byte offset where each packet starts."""
    out, pos, pending = [], 0, None
    while pos < len(stream):
        if pos + 4 > len(stream):
            raise DecodeError(f"stream ends inside a packet header at byte {pos}")
        length = int.from_bytes(stream[pos:pos + 3], "little")
        seq = stream[pos + 3]
        body = stream[pos + 4:pos + 4 + length]
        if len(body) < length:
            raise DecodeError(f"stream ends inside a packet at byte {pos}")
        if pending is None:
            pending = (seq, bytearray(body), pos)
        else:
            pending[1].extend(body)
        pos += 4 + length
        if length < 0xFFFFFF:
            out.append((pending[0], bytes(pending[1]), pending[2]))
            pending = None
    if pending is not None:
        raise DecodeError("stream ends inside a split packet")
    return out


class Reader:
    """Cursor over one payload."""

    def __init__(self, data):
        self.data, self.pos = data, 0

    def take(self, n):
        if self.pos + n > len(self.data):
            raise DecodeError("payload too short")
        b = self.data[self.pos:self.pos + n]
        self.pos += n
        return b

    def int(self, n):
        return int.from_bytes(self.take(n), "little")

    def lenenc(self):
        first = self.int(1)
        if first < 0xFB:
            return first
        if first == 0xFC:
            return self.int(2)
        if first == 0xFD:
            return self.int(3)
        if first == 0xFE:
            return self.int(8)
        raise DecodeError(f"bad length-encoded integer 0x{first:02x}")

    def lenenc_bytes(self):
        return self.take(self.lenenc())

    def nul_str(self):
        end = self.data.index(b"\x00", self.pos)
        s = self.data[self.pos:end]
        self.pos = end + 1
        return s

    def rest(self):
        return self.take(len(self.data) - self.pos)

    def done(self):
        return self.pos >= len(self.data)


def text(b):
    return b.decode("utf-8", errors="surrogateescape")


class ServerStream:
    def __init__(self, payloads):
        self.payloads, self.i = payloads, 0

    def next(self):
        if self.i >= len(self.payloads):
            raise DecodeError("server stream ends before the reply does")
        p = self.payloads[self.i][1]
        self.i += 1
        return p

    def peek(self):
        return self.payloads[self.i][1] if self.i < len(self.payloads) else None


def is_eof(p):
    return p[:1] == b"\xfe" and len(p) < 9


def parse_ok(p, caps=0):
    """OK packet (or the OK-style end of a result set). Under CLIENT_SESSION_TRACK the info is
    length-encoded and may be followed by session-state changes."""
    r = Reader(p)
    r.take(1)
    out = {"type": "ok", "affected_rows": r.lenenc(), "last_insert_id": r.lenenc(),
           "status": r.int(2), "warnings": r.int(2)}
    if caps & CLIENT_SESSION_TRACK:
        out["info"] = text(r.lenenc_bytes()) if not r.done() else ""
        if out["status"] & SERVER_SESSION_STATE_CHANGED and not r.done():
            out["session_state"] = r.lenenc_bytes().hex()
    else:
        out["info"] = text(r.rest())
    return out


def parse_err(p):
    r = Reader(p)
    r.take(1)
    code = r.int(2)
    sqlstate = None
    if r.data[r.pos:r.pos + 1] == b"#":
        r.take(1)
        sqlstate = text(r.take(5))
    return {"type": "err", "code": code, "sqlstate": sqlstate, "message": text(r.rest())}


def parse_eof(p):
    r = Reader(p)
    r.take(1)
    warnings, status = r.int(2), r.int(2)
    return {"status": status, "warnings": warnings}


def parse_coldef(p):
    r = Reader(p)
    col = {k: text(r.lenenc_bytes()) for k in ("catalog", "schema", "table", "org_table", "name", "org_name")}
    r.lenenc()
    col["charset"], col["length"], col["col_type"], col["flags"], col["decimals"] = struct.unpack("<HIBHB", r.take(10))
    return col


def parse_text_row(p):
    r, row = Reader(p), []
    while not r.done():
        if r.data[r.pos] == 0xFB:
            r.take(1)
            row.append(None)
        else:
            row.append(r.lenenc_bytes().hex())
    return row


FIXED_BINARY = {0x01: 1, 0x02: 2, 0x0D: 2, 0x03: 4, 0x09: 4, 0x04: 4, 0x08: 8, 0x05: 8, 0x06: 0}
LENGTH_PREFIXED = {0x07, 0x0A, 0x0B, 0x0C}  # TIMESTAMP, DATE, TIME, DATETIME: one length byte


def binary_value(r, col_type):
    """Raw bytes of one binary-protocol value, including any length prefix, so the canonical
    form keeps the server's exact encoding."""
    start = r.pos
    if col_type in FIXED_BINARY:
        r.take(FIXED_BINARY[col_type])
    elif col_type in LENGTH_PREFIXED:
        r.take(r.int(1))
    else:
        r.lenenc_bytes()
    return r.data[start:r.pos]


def parse_binary_row(p, columns):
    r = Reader(p)
    r.take(1)
    bitmap = r.take((len(columns) + 7 + 2) // 8)
    row = []
    for i, col in enumerate(columns):
        bit = i + 2
        if bitmap[bit // 8] & (1 << (bit % 8)):
            row.append(None)
        else:
            row.append(binary_value(r, col["col_type"]).hex())
    return row


class Session:
    def __init__(self, c2s, s2c, client_times=None):
        self.client = packets_at(c2s)
        self.client_times = client_times or []
        self.server = ServerStream(packets(s2c))
        self.caps = 0

    def handshake(self):
        greet = self.server.next()
        r = Reader(greet)
        r.take(1)
        version = text(r.nul_str())
        r.take(4)
        r.take(9)
        caps = r.int(2)
        r.take(1)
        r.take(2)
        caps |= r.int(2) << 16
        login = self.client[0][1] if self.client else b""
        client_caps = int.from_bytes(login[:4], "little") if len(login) >= 4 else 0
        self.caps = caps & client_caps
        reply = self.reply_simple()
        return {"command": "handshake", "statement": None, "params": None,
                "reply": {"type": "handshake", "server_version": version, "server_caps": caps, "login": reply}}

    def reply_simple(self):
        p = self.server.next()
        if p[:1] == b"\x00":
            return parse_ok(p, self.caps)
        if p[:1] == b"\xff":
            return parse_err(p)
        if p[:1] == b"\xfe":
            return {"type": "auth_switch"}
        raise DecodeError(f"unexpected packet 0x{p[0]:02x}")

    @property
    def deprecate_eof(self):
        return bool(self.caps & CLIENT_DEPRECATE_EOF)

    def reply_query(self, binary=False):
        if self.server.peek() is None and self.ci >= len(self.client):
            # The last command, and the server sent nothing back: it hung until the client
            # gave up. A reply cut off part way is still a DecodeError.
            return {"type": "no_reply"}
        parts = [self.reply_part(binary)]
        while parts[-1].get("status", 0) & SERVER_MORE_RESULTS_EXISTS:
            parts.append(self.reply_part(binary))
        return parts[0] if len(parts) == 1 else {"type": "multi", "parts": parts}

    def reply_part(self, binary=False):
        p = self.server.peek()
        if p is None:
            raise DecodeError("server stream ends before the reply does")
        if p[:1] in (b"\x00", b"\xff"):
            return self.reply_simple()
        if p[:1] == b"\xfb":
            self.server.next()
            # The client sends the file as packets ending with an empty one; they are not part
            # of the reply and are not compared.
            while True:
                if self.ci >= len(self.client):
                    raise DecodeError("client stream ends inside a LOCAL INFILE upload")
                chunk = self.client[self.ci][1]
                self.ci += 1
                if not chunk:
                    break
            return {"type": "local_infile", "file": text(p[1:]), "then": self.reply_simple()}
        count = Reader(self.server.next()).lenenc()
        columns = [parse_coldef(self.server.next()) for _ in range(count)]
        if not self.deprecate_eof:
            self.server.next()
        rows = []
        while True:
            p = self.server.next()
            if p[:1] == b"\xff":
                return {"type": "err_after_rows", "columns": columns, "rows": rows, "error": parse_err(p)}
            if (self.deprecate_eof and p[:1] == b"\xfe" and len(p) < 0xFFFFFF) or (not self.deprecate_eof and is_eof(p)):
                end = parse_ok(p, self.caps) if self.deprecate_eof else parse_eof(p)
                out = {"type": "resultset", "columns": columns, "rows": rows,
                       "status": end["status"], "warnings": end["warnings"]}
                if binary:
                    out["protocol"] = "binary"
                return out
            rows.append(parse_binary_row(p, columns) if binary else parse_text_row(p))

    def reply_prepare(self, statement):
        p = self.server.next()
        if p[:1] == b"\xff":
            return parse_err(p)
        r = Reader(p)
        r.take(1)
        stmt_id, num_columns, num_params = r.int(4), r.int(2), r.int(2)
        r.take(1)
        warnings = r.int(2) if not r.done() else 0
        groups = {}
        for key, n in (("params", num_params), ("columns", num_columns)):
            groups[key] = [parse_coldef(self.server.next()) for _ in range(n)]
            if n and not self.deprecate_eof:
                self.server.next()
        self.statements[stmt_id] = {"statement": statement, "num_params": num_params}
        # The statement id is assigned by the server and varies, so it is not kept.
        return {"type": "prepare_ok", "warnings": warnings, **groups}

    def execute_params(self, payload, stmt):
        num_params = stmt.get("num_params", 0)
        if not num_params:
            return []
        r = Reader(payload)
        r.take(10)
        bitmap = r.take((num_params + 7) // 8)
        if r.int(1) == 1:
            stmt["param_types"] = [r.int(2) & 0xFF for _ in range(num_params)]
        types = stmt.get("param_types", [])
        out = []
        for i in range(num_params):
            t = types[i] if i < len(types) else 0xFD
            if bitmap[i // 8] & (1 << (i % 8)):
                out.append({"type": t, "value": None})
            else:
                out.append({"type": t, "value": binary_value(r, t).hex()})
        return out

    def time_at(self, offset):
        """t_ms of the client chunk holding byte `offset`, or None without timing."""
        t = None
        for start, t_ms in self.client_times:
            if start > offset:
                break
            t = t_ms
        return t

    def exchanges(self):
        self.statements = {}
        out = [self.handshake()]
        out[0]["t_ms"] = self.time_at(0)
        self.ci = 1
        while self.ci < len(self.client):
            _, payload, offset = self.client[self.ci]
            self.ci += 1
            name = COMMANDS.get(payload[0], f"COM_0x{payload[0]:02x}")
            ex = {"command": name, "statement": None, "params": None, "t_ms": self.time_at(offset)}
            if name == "COM_STMT_PREPARE":
                ex["statement"] = text(payload[1:])
                ex["reply"] = self.reply_prepare(ex["statement"])
            elif name == "COM_STMT_EXECUTE":
                stmt = self.statements.get(int.from_bytes(payload[1:5], "little"), {})
                ex["statement"] = stmt.get("statement")
                ex["params"] = self.execute_params(payload, stmt)
                ex["reply"] = self.reply_query(binary=True)
            elif name == "COM_QUERY":
                ex["statement"] = text(payload[1:])
                ex["reply"] = self.reply_query()
            elif name in NO_REPLY:
                ex["reply"] = None
            else:
                ex["reply"] = self.reply_simple()
            out.append(ex)
        return out


def decode_session(c2s, s2c, client_times=None):
    """Exchanges of one connection. client_times, as tap.read_log returns them, gives each
    exchange the time its command was sent ("t_ms"); without it t_ms is None."""
    return Session(c2s, s2c, client_times).exchanges()
