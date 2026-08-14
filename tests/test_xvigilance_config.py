import unittest
from unittest.mock import MagicMock, patch
import os
import sys
from datetime import datetime, timedelta, timezone

SRC_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../service_factory/services/linkx-graph-maintenance/src"))
WORKER_SRC_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../service_factory/services/linkx-worker/src"))
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)
if WORKER_SRC_PATH not in sys.path:
    sys.path.insert(0, WORKER_SRC_PATH)

from linkx_xvigilance.config import get_xvigilance_config


class TestXvigilanceConfig(unittest.TestCase):
    def test_config_resolution_defaults(self):
        with patch.dict(
            "os.environ",
            {
                "LINKX_ELASTIC_API_BASE_URL": "http://172.27.23.43:5000",
                "LINKX_DATE_COLUMN": "transactiondate",
            },
            clear=False,
        ):
            config = get_xvigilance_config()
            self.assertEqual(config["elastic_base_url"], "http://172.27.23.43:5000")
            self.assertEqual(config["date_column"], "transactiondate")
            self.assertIn("TRANSACTIONID", config["fetch_columns"])
            self.assertEqual(config["page_size"], 50000)

    def test_hourly_window_overrun_and_rest_calculation(self):
        # Window [10:00 - 11:00]
        window_start = datetime(2026, 8, 14, 10, 0, 0, tzinfo=timezone.utc)
        window_end = datetime(2026, 8, 14, 11, 0, 0, tzinfo=timezone.utc)

        # Case 1: Finished early at 10:15 (now_utc < window_end) -> Rest required
        now_early = datetime(2026, 8, 14, 10, 15, 0, tzinfo=timezone.utc)
        self.assertTrue(now_early < window_end)
        rest_seconds = (window_end - now_early).total_seconds()
        self.assertEqual(rest_seconds, 45 * 60)  # 45 minutes rest

        # Case 2: Analysis finished late at 11:25 (now_utc >= window_end) -> No rest, continue
        now_late = datetime(2026, 8, 14, 11, 25, 0, tzinfo=timezone.utc)
        self.assertTrue(now_late >= window_end)
        self.assertFalse(now_late < window_end)


if __name__ == "__main__":
    unittest.main()
