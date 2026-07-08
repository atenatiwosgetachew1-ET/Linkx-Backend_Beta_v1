import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from batch_manager.utils.database_utils import get_graph_metadata


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
            _Record({"total_nodes": 7}),
            _Record({"total_relationships": 5}),
            _Record({"labels": ["HAS_RELATIONSHIP"]}),
            [{"key": "NodeId"}, {"key": "batch_id"}],
            _Record({"versions": ["2026.05.0"]}),
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

        node_query_params = metadata_session.calls[1][1]
        rel_query_params = metadata_session.calls[2][1]
        self.assertEqual(node_query_params["session_id"], "1_618421")
        self.assertEqual(node_query_params["batch_prefix"], "1_618421_")
        self.assertEqual(node_query_params["run_id"], "run-123")
        self.assertEqual(rel_query_params["session_id"], "1_618421")
        self.assertEqual(rel_query_params["batch_prefix"], "1_618421_")
        self.assertEqual(rel_query_params["run_id"], "run-123")


if __name__ == "__main__":
    unittest.main()
