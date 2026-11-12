#!/usr/bin/env python3
"""Regenerates crates/doris-exec/src/tld.txt from the C++ BE's
be/src/exprs/function/url/tldLookup.generated.cpp: its wordlist[] strings, octal escapes
decoded, empty ones dropped, sorted, one per line.

Usage: scripts/gen-tld.py [path to tldLookup.generated.cpp]
"""
import re
import sys

src_path = sys.argv[1] if len(sys.argv) > 1 else ".cache/doris-src/be/src/exprs/function/url/tldLookup.generated.cpp"
src = open(src_path, encoding="latin-1").read()
start = src.index("wordlist[] = {")
body = src[start : src.index("};", start)]


def unescape(w):
    out = bytearray()
    k = 0
    while k < len(w):
        if w[k] == "\\":
            m = re.match(r"[0-7]{1,3}", w[k + 1 :])
            if m:
                out.append(int(m.group(), 8))
                k += 1 + len(m.group())
                continue
            out.append(ord(w[k + 1]))
            k += 2
            continue
        out.append(ord(w[k]))
        k += 1
    return bytes(out)


words = sorted({unescape(w) for w in re.findall(r'"((?:[^"\\]|\\.)*)"', body) if w})
open("crates/doris-exec/src/tld.txt", "wb").write(b"\n".join(words) + b"\n")
print(len(words), "entries")
