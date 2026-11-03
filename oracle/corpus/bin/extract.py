#!/usr/bin/env python3
"""Copy every `golden` table's tablet files and metadata out of the reference BE and write
manifest.json.

Layout written:
    data/<table>/<rowset_id>_<segment_id>.dat   raw segment files (sorted by name = version order)
    data/<table>/tablet_meta.json               GET /api/meta/header/<tablet_id> on the BE, verbatim
    manifest.json                               reference identity + per-table tablet id, schema
                                                hash, row count, file sha256 and size
Requires the reference cluster from compose/reference-single.yml to be up and `golden` loaded.

    extract.py              extract every table, rewriting manifest.json
    extract.py t1 t2 ...    extract only the named tables and merge them into manifest.json,
                            leaving other tables' committed files and entries untouched
"""
import hashlib
import json
import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORPUS = os.path.abspath(os.path.join(HERE, ".."))
ORACLE = os.path.abspath(os.path.join(CORPUS, ".."))
COMPOSE = ["docker", "compose", "-f", os.path.join(ORACLE, "compose", "reference-single.yml")]
STORAGE_DATA = "/opt/apache-doris/be/storage/data"


def sh(args, binary=False):
    out = subprocess.run(args, check=True, capture_output=True)
    return out.stdout if binary else out.stdout.decode("utf-8")


def in_container(cmd, binary=False):
    return sh(COMPOSE + ["exec", "-T", "doris", "bash", "-c", cmd], binary=binary)


def mysql(query):
    """Run a query, return (header, rows) with rows as lists of strings."""
    text = in_container("mysql -uroot -h127.0.0.1 -P9030 --batch --raw -e " + json.dumps(query))
    lines = [l for l in text.split("\n") if l != ""]
    header = lines[0].split("\t")
    return header, [l.split("\t") for l in lines[1:]]


def container_name():
    return sh(COMPOSE + ["ps", "-q", "doris"]).strip()


def reference_identity():
    header, rows = mysql("SHOW FRONTENDS")
    version = rows[0][header.index("Version")]
    image = ""
    with open(os.path.join(ORACLE, "versions.env"), encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("DORIS_IMAGE="):
                image = line.strip().split("=", 1)[1]
    return {"image": image, "version": version, "commit": version.rsplit("-", 1)[-1]}


def tablet_of(table):
    header, rows = mysql(f"SHOW TABLETS FROM golden.{table}")
    if len(rows) != 1:
        raise SystemExit(f"{table}: expected exactly one tablet (BUCKETS 1, one replica), got {len(rows)}")
    row = dict(zip(header, rows[0]))
    return int(row["TabletId"]), row["MetaUrl"]


def extract_table(table, cid):
    tablet_id, meta_url = tablet_of(table)
    tablet_dir = in_container(f"find {STORAGE_DATA} -maxdepth 3 -type d -name {tablet_id}").strip()
    if not tablet_dir:
        raise SystemExit(f"{table}: tablet {tablet_id} directory not found under {STORAGE_DATA}")
    schema_dirs = in_container(f"ls {tablet_dir}").split()
    if len(schema_dirs) != 1:
        raise SystemExit(f"{table}: expected one schema-hash dir in {tablet_dir}, got {schema_dirs}")
    schema_hash = int(schema_dirs[0])
    dest = os.path.join(CORPUS, "data", table)
    shutil.rmtree(dest, ignore_errors=True)
    os.makedirs(dest)
    sh(["docker", "cp", f"{cid}:{tablet_dir}/{schema_hash}/.", dest])
    meta = in_container(f"curl -s {meta_url}", binary=True)
    with open(os.path.join(dest, "tablet_meta.json"), "wb") as fh:
        fh.write(meta)
    files = []
    for name in sorted(os.listdir(dest)):
        if not name.endswith(".dat"):
            continue
        with open(os.path.join(dest, name), "rb") as fh:
            data = fh.read()
        files.append({
            "path": f"data/{table}/{name}",
            "bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        })
    with open(os.path.join(CORPUS, "expected", table + ".count"), encoding="utf-8") as fh:
        rows = int(fh.read().strip())
    return {"tablet_id": tablet_id, "schema_hash": schema_hash, "rows": rows, "files": files}


def main():
    cid = container_name()
    if not cid:
        raise SystemExit("reference container is not running; run `make up` first")
    manifest_path = os.path.join(CORPUS, "manifest.json")
    if len(sys.argv) > 1:
        tables = sorted(sys.argv[1:])
        with open(manifest_path, encoding="utf-8") as fh:
            manifest = json.load(fh)
        if manifest["reference"] != reference_identity():
            raise SystemExit("reference identity differs from manifest.json; do a full rebuild")
    else:
        _, rows = mysql("SHOW TABLES FROM golden")
        tables = sorted(r[0] for r in rows)
        manifest = {"corpus_version": 1, "reference": reference_identity(), "tables": {}}
    for table in tables:
        manifest["tables"][table] = extract_table(table, cid)
        t = manifest["tables"][table]
        print(f"{table:16s} tablet={t['tablet_id']} files={len(t['files'])} rows={t['rows']} "
              f"bytes={sum(f['bytes'] for f in t['files'])}")
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=True)
        fh.write("\n")
    print("manifest.json written")


if __name__ == "__main__":
    sys.exit(main())
