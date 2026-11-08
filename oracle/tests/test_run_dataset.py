import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "swap"))
import run_dataset  # noqa: E402


class RunDatasetTest(unittest.TestCase):
    def test_stream_load_lines_become_curl_calls(self):
        step = run_dataset.parse_line("-- stream_load m4.big big.csv column_separator=, columns=c,k,v", 3)
        self.assertEqual(step.kind, "stream_load")
        self.assertEqual(step.file, "big.csv")
        # the body goes on stdin to curl inside the FE container (-T -), through the FE's redirect
        self.assertEqual(
            step.argv,
            [
                "curl", "-sS", "--location-trusted", "-u", "root:",
                "-H", "Expect:100-continue",
                "-H", "label:m4_big_3",
                "-H", "column_separator:,",
                "-H", "columns:c,k,v",
                "-T", "-",
                "http://127.0.0.1:8030/api/m4/big/_stream_load",
            ],
        )

    def test_statements_split_on_semicolons_outside_quotes(self):
        steps = run_dataset.parse_script("CREATE DATABASE m4;\nINSERT INTO t VALUES ('a;b'), (1);\n-- a comment\n")
        self.assertEqual([s.kind for s in steps], ["sql", "sql"])
        self.assertEqual(steps[1].sql, "INSERT INTO t VALUES ('a;b'), (1)")

    def test_queries_are_one_per_line_with_names(self):
        qs = run_dataset.parse_queries("all_types_point: SELECT 1\n\n# c\nbig_count_unordered: SELECT COUNT(*) FROM m4.big\n")
        self.assertEqual(qs, [("all_types_point", "SELECT 1"), ("big_count_unordered", "SELECT COUNT(*) FROM m4.big")])


if __name__ == "__main__":
    unittest.main()
