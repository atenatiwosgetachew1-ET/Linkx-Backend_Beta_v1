import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

_fake_globals = types.ModuleType("globals")
_fake_globals._session_store = {}
sys.modules.setdefault("globals", _fake_globals)

from batch_manager.utils.database_utils import _RELATIONSHIP_STATUS_CACHE, _fetch_relationship_status, get_graph_metadata


class _Record(dict):
    def single(self):
        return self


class _Session:
    def __init__(self, results):
        self._results = list(results)
        self.calls = []

    def run(self, query, **params):
        self.calls.append((query, params))
        return self._results.pop(0)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _Driver:
    def __init__(self, factory):
        self._factory = factory

    def session(self):
        return self._factory()


class GraphMetadataTest(unittest.TestCase):
    def test_metadata_counts_use_exact_session_batch_and_run_scope(self):
        query_session = _Session([
            _Record({"run_id": "run-123"}),
        ])
        metadata_session = _Session([
            _Record({"name": "neo4j"}),
            _Record({"versions": ["2026.05.0"]}),
            _Record({
                "total_nodes": 7,
                "total_relationships": 5,
                "relationship_labels": ["HAS_RELATIONSHIP"],
            }),
            [{"key": "NodeId"}, {"key": "batch_id"}],
        ])
        sessions = [query_session, metadata_session]
        driver = _Driver(lambda: sessions.pop(0))

        with patch("batch_manager.utils.database_utils._session_store", {"1_618421": {"live_analysis": None}}):
            metadata = get_graph_metadata(driver, "1_618421", {"username": "neo4j"})

        self.assertEqual(metadata["total_nodes"], 7)
        self.assertEqual(metadata["total_relationships"], 5)
        self.assertEqual(metadata["relationship_labels"], ["HAS_RELATIONSHIP"])
        self.assertEqual(metadata["property_keys"], ["NodeId", "batch_id"])
        self.assertEqual(metadata["neo4j_version"], "2026.05.0")

        summary_query, summary_params = metadata_session.calls[2]
        property_query, property_params = metadata_session.calls[3]
        self.assertIn("MATCH (n)", summary_query)
        self.assertIn("MATCH ()-[r]->()", summary_query)
        self.assertEqual(summary_params["session_id"], "1_618421")
        self.assertEqual(summary_params["batch_prefix"], "1_618421_")
        self.assertEqual(summary_params["run_id"], "run-123")
        self.assertEqual(property_params["session_id"], "1_618421")
        self.assertEqual(property_params["batch_prefix"], "1_618421_")
        self.assertEqual(property_params["run_id"], "run-123")

    def test_metadata_uses_static_and_schema_caches_when_available(self):
        query_session = _Session([
            _Record({"run_id": "run-123"}),
        ])
        metadata_session = _Session([
            _Record({
                "total_nodes": 9,
                "total_relationships": 6,
                "relationship_labels": ["FUND_FLOW"],
            }),
        ])
        sessions = [query_session, metadata_session]
        driver = _Driver(lambda: sessions.pop(0))

        metadata = get_graph_metadata(
            driver,
            "1_618421",
            {"username": "neo4j"},
            static_cache={"database": "neo4j", "user": "neo4j", "neo4j_version": "2026.05.0"},
            schema_cache={"property_keys": ["NodeId", "batch_id"]},
            refresh_schema=False,
        )

        self.assertEqual(metadata["total_nodes"], 9)
        self.assertEqual(metadata["total_relationships"], 6)
        self.assertEqual(metadata["relationship_labels"], ["FUND_FLOW"])
        self.assertEqual(metadata["property_keys"], ["NodeId", "batch_id"])
        self.assertEqual(len(metadata_session.calls), 1)
        self.assertIn("RETURN total_nodes, total_relationships, relationship_labels", metadata_session.calls[0][0])

    def test_relationship_status_uses_exact_session_batch_and_run_scope(self):
        metadata_session = _Session([
            [
                {"id": "rel-1", "type": "FUND_FLOW", "color": "#123", "bgcolor": "#abc"},
                {"id": "rel-2", "type": "CIRCULAR_FLOW", "color": None, "bgcolor": None},
            ],
        ])
        driver = _Driver(lambda: metadata_session)
        _RELATIONSHIP_STATUS_CACHE.clear()

        relationships = _fetch_relationship_status(driver, "1_618421", run_id="run-123", cache_seconds=10)

        self.assertEqual(relationships, [
            {"id": "rel-1", "type": "FUND_FLOW", "color": "#123", "bgcolor": "#abc"},
            {"id": "rel-2", "type": "CIRCULAR_FLOW", "color": "#333", "bgcolor": "#DDD"},
        ])
        query, params = metadata_session.calls[0]
        self.assertIn("coalesce(r.batch_id, '') STARTS WITH $batch_prefix", query)
        self.assertIn("r.run_id = $run_id", query)
        self.assertIn("min(elementId(r))", query)
        self.assertNotIn("collect(r)", query)
        self.assertEqual(params["session_id"], "1_618421")
        self.assertEqual(params["batch_prefix"], "1_618421_")
        self.assertEqual(params["run_id"], "run-123")

    def test_relationship_status_cache_reuses_lookup(self):
        metadata_session = _Session([
            [{"id": "rel-1", "type": "FUND_FLOW", "color": "#123", "bgcolor": "#abc"}],
        ])
        driver = _Driver(lambda: metadata_session)
        _RELATIONSHIP_STATUS_CACHE.clear()

        first = _fetch_relationship_status(driver, "1_618421", run_id="run-123", cache_seconds=10)
        second = _fetch_relationship_status(driver, "1_618421", run_id="run-123", cache_seconds=10)

        self.assertEqual(first, second)
        self.assertEqual(len(metadata_session.calls), 1)


if __name__ == "__main__":
    unittest.main()
