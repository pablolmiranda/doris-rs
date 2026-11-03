#!/usr/bin/env python3
"""Schema-less splitter for strict TBinaryProtocol messages, as the Doris FE and BE exchange them
unframed over TCP. It reads a message header and skips the struct by field type, so it finds
message boundaries without the IDL; decoding with the schema is the Rust side's job."""
import struct
from dataclasses import dataclass

KINDS = {1: "call", 2: "reply", 3: "exception", 4: "oneway"}
VERSION_MASK = 0xFFFF0000
VERSION_1 = 0x80010000
MAX_DEPTH = 64

# TBinaryProtocol type ids and fixed sizes
T_STOP, T_BOOL, T_BYTE, T_DOUBLE, T_I16, T_I32, T_I64 = 0, 2, 3, 4, 6, 8, 10
T_STRING, T_STRUCT, T_MAP, T_SET, T_LIST, T_UUID = 11, 12, 13, 14, 15, 16
FIXED = {T_BOOL: 1, T_BYTE: 1, T_DOUBLE: 8, T_I16: 2, T_I32: 4, T_I64: 8, T_UUID: 16}


class Incomplete(Exception):
    """The buffer ends before the message does."""


@dataclass(frozen=True)
class ThriftMessage:
    name: str
    mtype: int
    seqid: int
    start: int
    end: int


class _Reader:
    def __init__(self, buf: bytes, pos: int):
        self.buf, self.pos = buf, pos

    def take(self, n: int) -> bytes:
        if n < 0:
            raise ValueError(f"negative length {n} at {self.pos}")
        if self.pos + n > len(self.buf):
            raise Incomplete(f"need {n} bytes at {self.pos}, have {len(self.buf) - self.pos}")
        chunk = self.buf[self.pos:self.pos + n]
        self.pos += n
        return chunk

    def i8(self) -> int:
        return struct.unpack(">b", self.take(1))[0]

    def i32(self) -> int:
        return struct.unpack(">i", self.take(4))[0]

    def skip(self, ttype: int, depth: int) -> None:
        if depth > MAX_DEPTH:
            raise ValueError("nesting deeper than any Doris message")
        if ttype in FIXED:
            self.take(FIXED[ttype])
        elif ttype == T_STRING:
            self.take(self.i32())
        elif ttype == T_STRUCT:
            self.skip_struct(depth + 1)
        elif ttype == T_MAP:
            ktype, vtype, size = self.i8(), self.i8(), self.i32()
            if size < 0:
                raise ValueError(f"negative map size {size}")
            for _ in range(size):
                self.skip(ktype, depth + 1)
                self.skip(vtype, depth + 1)
        elif ttype in (T_SET, T_LIST):
            etype, size = self.i8(), self.i32()
            if size < 0:
                raise ValueError(f"negative list size {size}")
            for _ in range(size):
                self.skip(etype, depth + 1)
        else:
            raise ValueError(f"unknown field type {ttype} at {self.pos}")

    def skip_struct(self, depth: int) -> None:
        while True:
            ftype = self.i8()
            if ftype == T_STOP:
                return
            self.take(2)  # field id
            self.skip(ftype, depth)


def read_message(buf: bytes, pos: int = 0) -> ThriftMessage:
    """Read one strict message starting at `pos`. Raises Incomplete if the buffer ends first."""
    r = _Reader(buf, pos)
    version = struct.unpack(">I", r.take(4))[0]
    if version & VERSION_MASK != VERSION_1:
        raise ValueError(f"not a strict TBinaryProtocol header: {version:#010x}")
    mtype = version & 0xFF
    if mtype not in KINDS:
        raise ValueError(f"unknown message type {mtype}")
    name = r.take(r.i32()).decode("utf-8")
    seqid = r.i32()
    r.skip_struct(0)
    return ThriftMessage(name, mtype, seqid, pos, r.pos)


def split_messages(buf: bytes) -> list:
    """Every complete message in `buf`, in order. A trailing partial message is dropped."""
    out, pos = [], 0
    while pos < len(buf):
        try:
            m = read_message(buf, pos)
        except Incomplete:
            break
        out.append(m)
        pos = m.end
    return out
