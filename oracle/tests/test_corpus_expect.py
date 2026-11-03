import glob
import os
import unittest

CORPUS = os.path.join(os.path.dirname(__file__), "..", "corpus")
EXPECTED = os.path.join(CORPUS, "expected")
TABLES = ["scalar_ints", "scalar_others", "zstd_ints", "empty_table", "lz4f_big", "zstd_big"]


class ExpectedRowsTests(unittest.TestCase):
    def test_every_corpus_table_has_expected_files(self):
        for table in TABLES:
            for suffix in (".tsv", ".count", ".ddl.sql"):
                self.assertTrue(
                    os.path.exists(os.path.join(EXPECTED, table + suffix)),
                    f"missing expected/{table}{suffix}",
                )

    def test_expected_tsv_is_sorted_and_matches_count(self):
        for path in glob.glob(os.path.join(EXPECTED, "*.tsv")):
            with open(path, encoding="utf-8") as fh:
                lines = fh.read().split("\n")
            if lines and lines[-1] == "":
                lines.pop()
            rows = [line.split("\t") for line in lines]
            # Every corpus table has a unique INT first column `k`, and the reference sorts
            # numerically, so the file order must be strictly increasing k.
            keys = [int(r[0]) for r in rows]
            self.assertEqual(keys, sorted(set(keys)), f"{path} is not in strictly increasing k order")
            with open(path[: -len(".tsv")] + ".count", encoding="utf-8") as fh:
                self.assertEqual(len(rows), int(fh.read().strip()), f"{path} row count")

    def test_nulls_are_the_literal_word_null(self):
        # The reference's mysql client prints SQL NULL as the bare word NULL in batch mode.
        # No corpus string value equals "NULL", so the marker is unambiguous.
        with open(os.path.join(EXPECTED, "scalar_ints.tsv"), encoding="utf-8") as fh:
            text = fh.read()
        self.assertIn("\tNULL\t", text)
        self.assertEqual(text.count("9001\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL"), 1)


if __name__ == "__main__":
    unittest.main()
