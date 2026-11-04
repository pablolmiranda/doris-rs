"""The deployment facts of a run: a committed template (clusters/*.facts.json) with the facts that
depend on the host (`from_backends`: a SHOW BACKENDS column, such as the cores and memory the BE
reports) filled from the running cluster, so nothing machine-specific is committed.

    facts.py TEMPLATE FE_CONTAINER OUT
"""
import copy
import json
import subprocess
import sys


def fill(template, show_backends):
    """The template with each `from_backends` fact's values taken from SHOW BACKENDS output
    (tab-separated, header line first)."""
    lines = [line.split("\t") for line in show_backends.strip("\n").split("\n")]
    header, rows = lines[0], lines[1:]
    if not rows:
        raise ValueError("SHOW BACKENDS returned no backend")
    out = copy.deepcopy(template)
    for fact in out["facts"]:
        column = fact.pop("from_backends", None)
        if column is None:
            continue
        if column not in header:
            raise ValueError(f"fact {fact['name']}: SHOW BACKENDS has no column {column}")
        i = header.index(column)
        fact["values"] = sorted({row[i] for row in rows})
    return out


def show_backends(fe_container):
    return subprocess.run(["docker", "exec", fe_container, "mysql", "-uroot", "-h127.0.0.1", "-P9030",
                           "--batch", "-e", "SHOW BACKENDS"], check=True, capture_output=True, text=True).stdout


def main(argv):
    if len(argv) != 4:
        print(__doc__, file=sys.stderr)
        return 2
    with open(argv[1], encoding="utf-8") as fh:
        template = json.load(fh)
    filled = fill(template, show_backends(argv[2]))
    with open(argv[3], "w", encoding="utf-8") as fh:
        json.dump(filled, fh, indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
