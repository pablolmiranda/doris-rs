import hashlib
import json
import os
import unittest

CORPUS = os.path.join(os.path.dirname(__file__), "..", "corpus")
MANIFEST = os.path.join(CORPUS, "manifest.json")
TABLES = ["scalar_ints", "scalar_others", "zstd_ints", "empty_table", "lz4f_big", "zstd_big",
          "strings_fallback", "scalar_more"]


def load_manifest():
    with open(MANIFEST, encoding="utf-8") as fh:
        return json.load(fh)


class ManifestTests(unittest.TestCase):
    def test_manifest_lists_every_table_with_reference_identity(self):
        m = load_manifest()
        self.assertEqual(sorted(m["tables"]), sorted(TABLES))
        for key in ("image", "version", "commit"):
            self.assertTrue(m["reference"][key], f"reference.{key} empty")
        self.assertIn("4.1.5", m["reference"]["version"])

    def test_manifest_files_exist_and_hash(self):
        m = load_manifest()
        for name, t in m["tables"].items():
            self.assertIsInstance(t["tablet_id"], int)
            self.assertIsInstance(t["schema_hash"], int)
            self.assertTrue(os.path.exists(os.path.join(CORPUS, "data", name, "tablet_meta.json")))
            for f in t["files"]:
                path = os.path.join(CORPUS, f["path"])
                self.assertTrue(os.path.exists(path), path)
                with open(path, "rb") as fh:
                    data = fh.read()
                self.assertEqual(len(data), f["bytes"], path)
                self.assertEqual(hashlib.sha256(data).hexdigest(), f["sha256"], path)
                self.assertTrue(f["path"].endswith(".dat"), path)

    def test_manifest_rows_match_expected_counts_and_rowsets(self):
        m = load_manifest()
        for name, t in m["tables"].items():
            with open(os.path.join(CORPUS, "expected", name + ".count"), encoding="utf-8") as fh:
                self.assertEqual(t["rows"], int(fh.read().strip()), name)
        # scalar_ints was loaded with three INSERTs: three rowsets, each one segment.
        self.assertEqual(len(m["tables"]["scalar_ints"]["files"]), 3)
        # a zero-row table may legitimately have no segment files (ruling in the ledger)
        self.assertEqual(m["tables"]["empty_table"]["rows"], 0)

    def test_corpus_total_size_under_2mb(self):
        m = load_manifest()
        total = sum(f["bytes"] for t in m["tables"].values() for f in t["files"])
        self.assertLess(total, 2_000_000)
        self.assertGreater(total, 10_000)


if __name__ == "__main__":
    unittest.main()
