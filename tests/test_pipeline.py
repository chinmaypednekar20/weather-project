"""
Integration tests for Extreme Weather Analytics System.
Verifies HDFS data existence, output JSONs, and data schema sanity.
"""
import os
import json
import unittest

PROJECT_ROOT = "/home/chinmay/bigdata-project/weather-project"
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")


class TestBigDataPipeline(unittest.TestCase):

    def test_historical_summary_exists_and_valid(self):
        fpath = os.path.join(OUTPUT_DIR, "historical_summary.json")
        self.assertTrue(os.path.exists(fpath), f"Missing {fpath}")
        with open(fpath) as f:
            data = json.load(f)
        self.assertIn("yearly_trends", data)
        self.assertIn("top_event_types", data)
        self.assertEqual(len(data["yearly_trends"]), 5, "Expected 5 years of trends (2020-2024)")

    def test_spatial_summary_exists_and_valid(self):
        fpath = os.path.join(OUTPUT_DIR, "spatial_summary.json")
        self.assertTrue(os.path.exists(fpath), f"Missing {fpath}")
        with open(fpath) as f:
            data = json.load(f)
        self.assertIn("state_rankings", data)
        self.assertIn("hotspot_sample_points", data)
        self.assertGreater(len(data["state_rankings"]), 45, "Expected states list")
        self.assertGreater(len(data["hotspot_sample_points"]), 1000, "Expected hotspot points")

    def test_temporal_summary_exists_and_valid(self):
        fpath = os.path.join(OUTPUT_DIR, "temporal_summary.json")
        self.assertTrue(os.path.exists(fpath), f"Missing {fpath}")
        with open(fpath) as f:
            data = json.load(f)
        self.assertIn("monthly_patterns", data)
        self.assertIn("seasonal_patterns", data)
        self.assertEqual(len(data["monthly_patterns"]), 12, "Expected 12 months")
        self.assertEqual(len(data["seasonal_patterns"]), 4, "Expected 4 seasons")

    def test_impact_summary_exists_and_valid(self):
        fpath = os.path.join(OUTPUT_DIR, "impact_summary.json")
        self.assertTrue(os.path.exists(fpath), f"Missing {fpath}")
        with open(fpath) as f:
            data = json.load(f)
        self.assertIn("casualty_impact", data)
        self.assertIn("economic_damage", data)
        self.assertIn("top_damaging_events", data)
        self.assertGreater(len(data["casualty_impact"]), 0)


if __name__ == "__main__":
    unittest.main()
