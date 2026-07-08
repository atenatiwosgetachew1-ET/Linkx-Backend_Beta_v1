import sys
import types
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from batch_manager.utils import graph_status_events


class _Cursor:
    def __init__(self, owner):
        self.owner = owner

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self.owner.calls.append((query, params))

    def fetchone(self):
        return self.owner.rows.pop(0) if self.owner.rows else None


class _Connection:
    def __init__(self, owner):
        self.owner = owner

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return _Cursor(self.owner)


class _Psycopg:
    def __init__(self, rows):
        self.rows = list(rows)
        self.calls = []
        self.connect_count = 0

    def connect(self, *args, **kwargs):
        self.connect_count += 1
        return _Connection(self)


class GraphStatusEventsTest(unittest.TestCase):
    def setUp(self):
        graph_status_events._EVENT_CACHE.clear()
        graph_status_events._ACTIVE_JOB_CACHE.clear()

    def test_latest_event_cache_reuses_postgres_result(self):
        fake_psycopg = _Psycopg([
            (5, {"phase": "inserted"}, datetime(2026, 7, 8, 12, 0, 0)),
        ])

        with patch.dict(sys.modules, {"psycopg": fake_psycopg}):
            with patch("batch_manager.utils.graph_status_events._database_url", return_value="postgres://test"):
                with patch("batch_manager.utils.graph_status_events._ensure_lookup_indexes"):
                    first = graph_status_events.latest_graph_metadata_event("1_618421", 0)
                    second = graph_status_events.latest_graph_metadata_event("1_618421", 0)
                    already_seen = graph_status_events.latest_graph_metadata_event("1_618421", 5)

        self.assertEqual(first["event_id"], 5)
        self.assertEqual(second["event_id"], 5)
        self.assertIsNone(already_seen)
        self.assertEqual(fake_psycopg.connect_count, 1)

    def test_active_job_cache_reuses_postgres_result(self):
        fake_psycopg = _Psycopg([(1,)])

        with patch.dict(sys.modules, {"psycopg": fake_psycopg}):
            with patch("batch_manager.utils.graph_status_events._database_url", return_value="postgres://test"):
                with patch("batch_manager.utils.graph_status_events._ensure_lookup_indexes"):
                    first = graph_status_events.has_active_graph_session_job("1_618421")
                    second = graph_status_events.has_active_graph_session_job("1_618421")

        self.assertTrue(first)
        self.assertTrue(second)
        self.assertEqual(fake_psycopg.connect_count, 1)

    def test_lookup_index_helper_runs_once(self):
        fake_psycopg = _Psycopg([])

        with patch.dict(sys.modules, {"psycopg": fake_psycopg}):
            graph_status_events._LOOKUP_INDEXES_CHECKED = False
            graph_status_events._ensure_lookup_indexes("postgres://test")
            graph_status_events._ensure_lookup_indexes("postgres://test")

        self.assertEqual(fake_psycopg.connect_count, 1)
        executed_sql = "\n".join(query for query, _params in fake_psycopg.calls)
        self.assertIn("idx_job_events_session_type_id_desc", executed_sql)
        self.assertIn("idx_jobs_session_status_finished", executed_sql)


if __name__ == "__main__":
    unittest.main()
