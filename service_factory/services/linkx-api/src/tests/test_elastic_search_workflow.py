import unittest
from unittest.mock import patch

import pandas as pd

from batch_manager.batch_data_manager import batch_data_manager


class ElasticSearchWorkflowTest(unittest.TestCase):
    def _config(self):
        return {
            "active_storage_address": "172.27.23.43",
            "active_storage_host": "172.27.23.43",
            "api_port": "5000",
            "date_column": "transactiondate",
            "search_api_endpoint_es_strict": "api/search/ui",
            "search_api_endpoint_es_fuzzy": "api/search/individual",
            "search_columns_strict": ["accountno", "benaccountno"],
            "search_columns_fuzzy": ["entity_name", "othername"],
            "elastic_scroll_limit": 1000000,
            "elastic_scroll_batch_size": 10000,
            "dataframes_limit": 1000000,
        }

    def test_strict_search_uses_configured_columns_when_request_uses_default_column(self):
        elastic_response = {"results": [{"size": 1}], "message": "1 results found"}
        with patch("batch_manager.batch_data_manager.load_temp_config", side_effect=lambda key, session_id: self._config().get(key)):
            with patch("batch_manager.batch_data_manager.es_keyword_search", return_value=elastic_response) as search_mock:
                result = batch_data_manager({
                    "id": "search",
                    "session_id": "session-1",
                    "keyword": "5642153",
                    "hybrid": True,
                    "strict": True,
                    "search_column": "transactionid",
                    "storage": "172.27.23.43",
                })

        self.assertEqual(result["message"], "1 results found")
        self.assertEqual(search_mock.call_args.args[3], ["accountno", "benaccountno"])
        self.assertTrue(search_mock.call_args.args[4])

    def test_fuzzy_search_uses_configured_columns_when_request_omits_column(self):
        elastic_response = {"results": [{"size": 1}], "message": "1 results found"}
        with patch("batch_manager.batch_data_manager.load_temp_config", side_effect=lambda key, session_id: self._config().get(key)):
            with patch("batch_manager.batch_data_manager.es_keyword_search", return_value=elastic_response) as search_mock:
                result = batch_data_manager({
                    "id": "search",
                    "session_id": "session-1",
                    "keyword": "alice",
                    "hybrid": True,
                    "strict": False,
                    "storage": "172.27.23.43",
                })

        self.assertEqual(result["message"], "1 results found")
        self.assertEqual(search_mock.call_args.args[3], ["entity_name", "othername"])
        self.assertFalse(search_mock.call_args.args[4])

    def test_strict_dataframe_creation_uses_configured_columns_when_column_is_missing(self):
        strict_df = pd.DataFrame({"ACCOUNTNO": ["5642153"], "TRANSACTIONDATE": ["2024-01-01"]})
        with patch("batch_manager.batch_data_manager.load_temp_config", side_effect=lambda key, session_id: self._config().get(key)):
            with patch("batch_manager.batch_data_manager.es_keyword_search", return_value=strict_df) as search_mock:
                result = batch_data_manager({
                    "id": "load_sourceData",
                    "session_id": "session-1",
                    "type": "array",
                    "kind": "hybrid",
                    "value": [{"type": "elastic", "keyword": "5642153", "strict": True}],
                })

        self.assertEqual(len(result), 1)
        self.assertEqual(search_mock.call_args.args[3], ["accountno", "benaccountno"])
        self.assertTrue(search_mock.call_args.args[4])

    def test_fuzzy_dataframe_creation_uses_configured_columns_when_column_is_missing(self):
        fuzzy_df = pd.DataFrame({"ENTITY_NAME": ["alice"], "TRANSACTIONDATE": ["2024-01-01"]})
        with patch("batch_manager.batch_data_manager.load_temp_config", side_effect=lambda key, session_id: self._config().get(key)):
            with patch("batch_manager.batch_data_manager.es_keyword_search", return_value=fuzzy_df) as search_mock:
                result = batch_data_manager({
                    "id": "load_sourceData",
                    "session_id": "session-1",
                    "type": "array",
                    "kind": "hybrid",
                    "value": [{"type": "elastic", "keyword": "alice", "strict": False}],
                })

        self.assertEqual(len(result), 1)
        self.assertEqual(search_mock.call_args.args[3], ["entity_name", "othername"])
        self.assertFalse(search_mock.call_args.args[4])


if __name__ == "__main__":
    unittest.main()
