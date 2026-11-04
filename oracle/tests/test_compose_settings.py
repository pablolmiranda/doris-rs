"""The split reference cluster keeps every FE and BE setting of the single one; it may only add
lines (its deliberate deployment differences) and lower the BE memory limit."""
import os
import unittest

COMPOSE = os.path.join(os.path.dirname(__file__), "..", "compose")


def block(path, name):
    lines = open(path, encoding="utf-8").read().splitlines()
    i = next(j for j, line in enumerate(lines) if line.strip() == f"{name}: |")
    indent = len(lines[i + 1]) - len(lines[i + 1].lstrip())
    out = []
    for line in lines[i + 1:]:
        if not line.startswith(" " * indent):
            break
        out.append(line.strip())
    return [x for x in out if x and not x.startswith("#")]


class SplitSettingsTests(unittest.TestCase):
    def test_split_keeps_every_single_setting(self):
        single = os.path.join(COMPOSE, "reference-single.yml")
        split = os.path.join(COMPOSE, "reference-split.yml")
        for name in ("FE_CONFIG_EXTRA", "BE_CONFIG_EXTRA"):
            ours = set(block(split, name))
            for line in block(single, name):
                if line.startswith("mem_limit"):
                    continue
                self.assertIn(line, ours, f"{name}: reference-split.yml lacks '{line}'")


class RustClusterSettingsTests(unittest.TestCase):
    single = os.path.join(COMPOSE, "reference-single.yml")

    def test_fe_only_keeps_every_fe_setting(self):
        ours = set(block(os.path.join(COMPOSE, "fe-only.yml"), "FE_CONFIG_EXTRA"))
        for line in block(self.single, "FE_CONFIG_EXTRA"):
            self.assertIn(line, ours, f"fe-only.yml lacks '{line}'")

    def test_rust_be_conf_keeps_every_be_setting(self):
        path = os.path.join(COMPOSE, "..", "rust", "be.conf")
        ours = {line.strip() for line in open(path, encoding="utf-8")}
        for line in block(self.single, "BE_CONFIG_EXTRA"):
            if line.startswith("mem_limit"):
                continue
            self.assertIn(line, ours, f"rust/be.conf lacks '{line}'")


if __name__ == "__main__":
    unittest.main()
