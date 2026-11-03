"""Builds MySQL protocol byte streams for tests: a greeting, a login, and the packets of common
commands and replies, so decoding can be checked without a server."""
import struct

CLIENT_PROTOCOL_41 = 0x00000200
CLIENT_SSL = 0x00000800
CLIENT_SECURE_CONNECTION = 0x00008000
CLIENT_MULTI_RESULTS = 0x00020000
CLIENT_PLUGIN_AUTH = 0x00080000
CLIENT_DEPRECATE_EOF = 0x01000000
BASE_CAPS = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_MULTI_RESULTS | CLIENT_PLUGIN_AUTH

SERVER_MORE_RESULTS_EXISTS = 0x0008
STATUS_AUTOCOMMIT = 0x0002

MYSQL_TYPE_DECIMAL_NEW = 0xF6
MYSQL_TYPE_LONG = 0x03
MYSQL_TYPE_LONGLONG = 0x08
MYSQL_TYPE_DATETIME = 0x0C
MYSQL_TYPE_VAR_STRING = 0xFD


def lenenc_int(n):
    if n < 251:
        return bytes([n])
    if n < 1 << 16:
        return b"\xfc" + struct.pack("<H", n)
    if n < 1 << 24:
        return b"\xfd" + struct.pack("<I", n)[:3]
    return b"\xfe" + struct.pack("<Q", n)


def lenenc_str(b):
    if isinstance(b, str):
        b = b.encode()
    return lenenc_int(len(b)) + b


def frame(seq, payload):
    """One packet; payloads of 0xFFFFFF bytes or more are split as the protocol requires."""
    out = b""
    while True:
        chunk, payload = payload[:0xFFFFFF], payload[0xFFFFFF:]
        out += struct.pack("<I", len(chunk))[:3] + bytes([seq & 0xFF]) + chunk
        seq += 1
        if len(chunk) < 0xFFFFFF:
            return out


def frames(seq, payloads):
    out = b""
    for p in payloads:
        out += frame(seq, p)
        seq += 1
    return out


def greeting(caps=BASE_CAPS):
    return (b"\x0a" + b"5.7.99\x00" + struct.pack("<I", 7) + b"abcdefgh\x00"
            + struct.pack("<H", caps & 0xFFFF) + b"\x21" + struct.pack("<H", STATUS_AUTOCOMMIT)
            + struct.pack("<H", caps >> 16) + b"\x15" + b"\x00" * 10 + b"ijklmnopqrst\x00"
            + b"mysql_native_password\x00")


def login(caps=BASE_CAPS):
    return (struct.pack("<I", caps) + struct.pack("<I", 1 << 24) + b"\x21" + b"\x00" * 23
            + b"root\x00" + b"\x00" + b"mysql_native_password\x00")


def ok(affected=0, last_id=0, status=STATUS_AUTOCOMMIT, warnings=0, info=b"", header=b"\x00"):
    return header + lenenc_int(affected) + lenenc_int(last_id) + struct.pack("<HH", status, warnings) + info


def err(code, sqlstate, message):
    return b"\xff" + struct.pack("<H", code) + b"#" + sqlstate.encode() + message.encode()


def eof(status=STATUS_AUTOCOMMIT, warnings=0):
    return b"\xfe" + struct.pack("<HH", warnings, status)


def coldef(name, col_type=MYSQL_TYPE_VAR_STRING, length=255, flags=0, decimals=0, charset=33,
           table="t", schema="db"):
    return (lenenc_str("def") + lenenc_str(schema) + lenenc_str(table) + lenenc_str(table)
            + lenenc_str(name) + lenenc_str(name) + b"\x0c" + struct.pack("<HIBHB", charset, length, col_type, flags, decimals)
            + b"\x00\x00")


def text_row(values):
    return b"".join(b"\xfb" if v is None else lenenc_str(v) for v in values)


def resultset(columns, rows, deprecate_eof=False, status=STATUS_AUTOCOMMIT, warnings=0):
    """Server payloads of a text-protocol result set. columns: list of coldef() payloads."""
    out = [lenenc_int(len(columns)), *columns]
    if not deprecate_eof:
        out.append(eof(status))
    out += [text_row(r) for r in rows]
    out.append(ok(status=status, warnings=warnings, header=b"\xfe") if deprecate_eof else eof(status, warnings))
    return out


def query(sql):
    return b"\x03" + sql.encode()


def session(commands, caps=BASE_CAPS, server_caps=None):
    """(c2s, s2c) bytes: greeting and login, then each (client_payloads, server_payloads)
    command, sequence ids restarting at 0 for every command."""
    server_caps = caps if server_caps is None else server_caps
    c2s = frame(1, login(caps))
    s2c = frame(0, greeting(server_caps)) + frame(2, ok())
    for client, server in commands:
        c2s += frames(0, client)
        s2c += frames(len(client), server)
    return c2s, s2c


def prepare(sql):
    return b"\x16" + sql.encode()


def prepare_ok(stmt_id, columns, params, deprecate_eof=False):
    """Server payloads of a COM_STMT_PREPARE reply. columns/params: lists of coldef() payloads."""
    out = [b"\x00" + struct.pack("<IHHBH", stmt_id, len(columns), len(params), 0, 0)]
    for group in (params, columns):
        if group:
            out += group
            if not deprecate_eof:
                out.append(eof())
    return out


def execute(stmt_id, params):
    """COM_STMT_EXECUTE with params [(mysql_type, value_bytes or None)], types sent."""
    body = b"\x17" + struct.pack("<IBI", stmt_id, 0, 1)
    if params:
        bitmap = bytearray((len(params) + 7) // 8)
        for i, (_, v) in enumerate(params):
            if v is None:
                bitmap[i // 8] |= 1 << (i % 8)
        body += bytes(bitmap) + b"\x01" + b"".join(struct.pack("<BB", t, 0) for t, _ in params)
        body += b"".join(v for _, v in params if v is not None)
    return body


def binary_row(values):
    """values: list of encoded value bytes or None."""
    bitmap = bytearray((len(values) + 7 + 2) // 8)
    for i, v in enumerate(values):
        if v is None:
            bitmap[(i + 2) // 8] |= 1 << ((i + 2) % 8)
    return b"\x00" + bytes(bitmap) + b"".join(v for v in values if v is not None)


def binary_resultset(columns, rows, deprecate_eof=False):
    out = [lenenc_int(len(columns)), *columns]
    if not deprecate_eof:
        out.append(eof())
    out += [binary_row(r) for r in rows]
    out.append(ok(header=b"\xfe") if deprecate_eof else eof())
    return out
