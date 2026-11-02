import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bin"))
import override_conf as oc  # noqa: E402

BASE = '''// header
defaultDb = "regression_test"
jdbcUrl = "jdbc:mysql://127.0.0.1:9030/?a=b"
feHttpAddress = "127.0.0.1:8030"
excludeDockerTest = true
suites = {
    //test_suite_1 {
    //    key1 = "val1"
    //}
}
'''


class OverrideTests(unittest.TestCase):
    def test_replaces_only_named_keys(self):
        out = oc.apply_overrides(BASE, {"jdbcUrl": '"jdbc:mysql://doris:9030/?a=b"'})
        self.assertIn('jdbcUrl = "jdbc:mysql://doris:9030/?a=b"', out)
        self.assertIn('feHttpAddress = "127.0.0.1:8030"', out)
        self.assertIn('defaultDb = "regression_test"', out)

    def test_does_not_touch_commented_or_nested_lines(self):
        out = oc.apply_overrides(BASE, {"key1": '"zzz"'})
        self.assertIn('//    key1 = "val1"', out)
        self.assertTrue(out.rstrip().endswith('key1 = "zzz"'))

    def test_missing_keys_are_appended(self):
        out = oc.apply_overrides(BASE, {"brandNew": "42"})
        self.assertIn("// added by doris-rs oracle harness\nbrandNew = 42", out)

    def test_parse_pairs_keeps_equals_in_value(self):
        self.assertEqual(oc.parse_pairs(["a=x=y"]), {"a": "x=y"})
        with self.assertRaises(ValueError):
            oc.parse_pairs(["novalue"])


if __name__ == "__main__":
    unittest.main()
