"""Static scope rules: which p0-core suites Epic 1 owns."""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "checker"))
import scope  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures", "scope")


class ScopeTests(unittest.TestCase):
    def test_keys_match_results_format_and_sql_files_count(self):
        result = scope.scan(FIXTURES, ["d", "e"])
        self.assertEqual(result["d.suite.suite"], {"file": "d/suite.groovy", "reasons": ["aggregate_key"]})
        self.assertEqual(result["e.load.load"], {"file": "e/load.groovy", "reasons": []})

    def test_sql_files_under_sql_directories_are_suites_of_their_own(self):
        result = scope.scan(FIXTURES, ["e"])
        self.assertEqual(result["e.sql.q1.q1"], {"file": "e/sql/q1.sql", "reasons": []})
        self.assertEqual(result["e.sql.q2.q2"], {"file": "e/sql/q2.sql", "reasons": ["complex_types"]})
        self.assertNotIn("complex_types", result["e.load.load"]["reasons"])

    def test_groovy_suites_only_read_sql_files_they_name(self):
        self.assertEqual(scope.scan(FIXTURES, ["e"])["e.load.load"]["reasons"], [])

    def test_rules(self):
        cases = {
            "k1 VARIANT NULL": ["variant"],
            "INDEX idx (c) USING INVERTED": ["text_index"],
            "INDEX idx (c) USING NGRAM_BF": ["text_index"],
            "where c MATCH_ANY 'x'": ["text_index"],
            "v BITMAP BITMAP_UNION": ["agg_types"],
            "select to_bitmap(1)": ["agg_types"],
            "h HLL HLL_UNION": ["agg_types"],
            "q QUANTILE_STATE": ["agg_types"],
            "a AGG_STATE<sum(int)>": ["agg_types"],
            "AGGREGATE KEY(k1)": ["aggregate_key"],
            "c ARRAY<INT>": ["complex_types"],
            "c MAP<STRING, INT>": ["complex_types"],
            "c STRUCT<a:INT>": ["complex_types"],
            "j JSON NULL": ["complex_types"],
            "j JSONB,": ["complex_types"],
            'arrow_flight_sql "select 1"': ["arrow_flight"],
            "select AI_GENERATE('x')": ["ai_functions"],
            "INDEX v (emb) USING ANN": ["vector"],
            "select l2_distance(a, b)": ["vector"],
            "CREATE CATALOG hive PROPERTIES": ["external_catalog"],
            "GetDebugPoint().enableDebugPointForAllBEs('x')": ["debug_points"],
        }
        for text, want in cases.items():
            self.assertEqual(scope.exclusion_reasons(text), want, text)

    def test_no_false_positives(self):
        for text in ["create table test_variant_like (k INT)", '"format" = "json"', "set format json",
                     "select bitmap_count_x from t", "DUPLICATE KEY(k1)", "UNIQUE KEY(k1)",
                     "select json_extract(s, '$.a')", "sql 'select 1' // map of results",
                     "def array = [1, 2]", "streamLoad { set 'format', 'json' }"]:
            self.assertEqual(scope.exclusion_reasons(text), [], text)

    def test_exclusion_report_groups_suites_by_reason_with_counts(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            lst = os.path.join(d, "list.txt")
            open(lst, "w").write("d\ne\n")
            out = [os.path.join(d, n) for n in ("scope.json", "dirs.txt", "exclusions.txt")]
            self.assertEqual(scope.main(["scope", FIXTURES, lst, *out]), 0)
            report = open(out[2]).read().splitlines()
        self.assertIn("in scope: 2 of 4 suites", report)
        self.assertIn("aggregate_key: 1", report)
        self.assertIn("  d.suite.suite", report)


if __name__ == "__main__":
    unittest.main()
