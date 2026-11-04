"""FE<->BE message files: gzip of records, each a little-endian u32 header length, a JSON header,
a little-endian u32 data length, and the raw message bytes. Read by doris-wire
(src/suites.rs) too, so the format stays this simple."""
import gzip
import json
import struct


def write(path, items):
    """items: iterable of (header dict, data bytes)."""
    # No time or name in the gzip header: the same items give the same bytes, so a re-derive
    # changes only the files whose content changed.
    with open(path, "wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as fh:
        for header, data in items:
            h = json.dumps(header, sort_keys=True).encode()
            fh.write(struct.pack("<I", len(h)) + h + struct.pack("<I", len(data)) + data)


def write_text_gz(path, text):
    """text, gzipped with the same fixed header as write."""
    with open(path, "wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as fh:
        fh.write(text.encode("utf-8"))


def read(path):
    """[(header dict, data bytes)]."""
    out = []
    with gzip.open(path, "rb") as fh:
        while True:
            n = fh.read(4)
            if not n:
                return out
            header = json.loads(fh.read(struct.unpack("<I", n)[0]))
            data = fh.read(struct.unpack("<I", fh.read(4))[0])
            out.append((header, data))
