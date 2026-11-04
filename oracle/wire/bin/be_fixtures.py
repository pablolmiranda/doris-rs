"""Turn an M2 capture into the BE fixtures: the wire corpus layout under OUT, plus an index of the
agent tasks it carries (task types, created and dropped tablets) in manifest.json.

    python3 wire/bin/be_fixtures.py <pcap> <out>

The scenario (wire/scenarios/m2.sh) already copied headers/ and layout.txt into <out>.
"""
import json
import os
import re
import struct
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import extract  # noqa: E402
import thrift_split as ts  # noqa: E402

TYPES_THRIFT = os.path.join(HERE, "..", "..", "..", ".cache", "doris-src", "gensrc", "thrift", "Types.thrift")


def task_type_names():
    with open(TYPES_THRIFT, encoding="utf-8") as fh:
        text = fh.read()
    body = re.search(r"enum TTaskType \{(.*?)\}", text, re.S).group(1)
    return {int(v): k for k, v in re.findall(r"(\w+)\s*=\s*(\d+)", body)}


class _Values(ts._Reader):
    """Reads Thrift binary values: a struct becomes {field id: value}."""

    def value(self, ttype, depth=0):
        if ttype == 2 or ttype == 3:
            return self.i8()
        if ttype == 4:
            return struct.unpack(">d", self.take(8))[0]
        if ttype == 6:
            return struct.unpack(">h", self.take(2))[0]
        if ttype == 8:
            return self.i32()
        if ttype == 10:
            return struct.unpack(">q", self.take(8))[0]
        if ttype == 11:
            return self.take(self.i32())
        if ttype == 12:
            return self.struct(depth + 1)
        if ttype == 13:
            k, v, n = self.i8(), self.i8(), self.i32()
            return [(self.value(k, depth + 1), self.value(v, depth + 1)) for _ in range(n)]
        if ttype in (14, 15):
            e, n = self.i8(), self.i32()
            return [self.value(e, depth + 1) for _ in range(n)]
        raise ValueError(f"unknown field type {ttype}")

    def struct(self, depth=0):
        out = {}
        while True:
            ftype = self.i8()
            if ftype == 0:
                return out
            fid = struct.unpack(">h", self.take(2))[0]
            out[fid] = self.value(ftype, depth)


def call_args(data):
    r = _Values(data, 0)
    r.take(4)
    r.take(r.i32())
    r.i32()
    return r.struct()


def task_index(submit_calls):
    """Task types (names, first-seen order) and tablet ids from submit_tasks call bytes."""
    names = task_type_names()
    types, created, dropped = [], [], []
    for data in submit_calls:
        for task in call_args(data).get(1, []):
            name = names.get(task[2], str(task[2]))
            if name not in types:
                types.append(name)
            if 5 in task:
                created.append(task[5][1])
            if 6 in task:
                dropped.append(task[6][1])
    return {"task_types": types, "created_tablets": created, "dropped_tablets": dropped}


def main(argv):
    pcap, out = argv[1], os.path.abspath(argv[2])
    extract.CORPUS = out
    extract.main([argv[0], pcap])
    path = os.path.join(out, "manifest.json")
    with open(path, encoding="utf-8") as fh:
        manifest = json.load(fh)
    calls = []
    for e in manifest["messages"]:
        if e["method"] == "submit_tasks" and e["kind"] == "call":
            with open(os.path.join(out, e["file"]), "rb") as fh:
                calls.append(fh.read())
    manifest.update(task_index(calls))
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
        fh.write("\n")
    print("tasks:", ", ".join(manifest["task_types"]), f"; {len(manifest['created_tablets'])} created")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
