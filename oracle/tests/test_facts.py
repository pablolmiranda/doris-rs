"""Deployment facts of a run: the committed template plus the values the cluster reports for the
facts that depend on the host (SHOW BACKENDS)."""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "checker"))
import facts  # noqa: E402

BACKENDS = ("BackendId\tHost\tCpuCores\tMemory\tAlive\n"
            "10002\t172.30.42.20\t8\t7.50 GB\ttrue\n")

TEMPLATE = {
    "cluster": "split",
    "facts": [
        {"name": "be-host", "values": ["172.30.42.20"], "match": "substring"},
        {"name": "be-cores", "from_backends": "CpuCores", "match": "cell", "columns": ["CpuCores"]},
        {"name": "be-memory", "from_backends": "Memory", "match": "cell", "columns": ["Memory"]},
    ],
    "not_varied": [{"fact": "be-cores", "reason": "r"}],
}


class LiveFactsTests(unittest.TestCase):
    def test_host_dependent_facts_take_the_values_the_cluster_reports(self):
        filled = facts.fill(TEMPLATE, BACKENDS)
        self.assertEqual([f["values"] for f in filled["facts"]], [["172.30.42.20"], ["8"], ["7.50 GB"]])
        self.assertNotIn("from_backends", filled["facts"][1])
        self.assertEqual(filled["not_varied"], TEMPLATE["not_varied"])

    def test_a_column_the_cluster_does_not_report_is_an_error(self):
        template = {"facts": [{"name": "x", "from_backends": "NoSuchColumn", "match": "cell"}]}
        with self.assertRaises(ValueError):
            facts.fill(template, BACKENDS)

    def test_no_backend_is_an_error(self):
        with self.assertRaises(ValueError):
            facts.fill(TEMPLATE, "BackendId\tHost\tCpuCores\tMemory\n")


if __name__ == "__main__":
    unittest.main()
