import unittest
from unittest.mock import patch


class STRLinkAnalysisApiTest(unittest.TestCase):
    def setUp(self):
        import main

        self.client = main.app.test_client()

    def test_graph_link_accepts_cached_str_report_status(self):
        import globals

        globals.str_report_status_registry.clear()
        globals.str_report_status_registry["str_report_test"] = {
            "metadata": {"session_id": "str_report_test", "type": "metadata", "data": {"message": "success!"}},
        }

        response = self.client.post(
            "/graph_link",
            json={"id": "link", "source_id": "str_report_test"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"message": "success!"})

    def test_STR_link_analysis_creates_dataframe_when_elastic_has_results(self):
        elastic_response = {
            "results": [
                {
                    "name": "Results found for '5642153'",
                    "keyword": "5642153",
                    "size": 1,
                    "strict": True,
                    "type": "elastic",
                    "column": "accountno",
                }
            ],
            "message": "1 results found",
        }

        with patch("api.STR_link_analysis._prepare_session", return_value=True), \
             patch("api.STR_link_analysis.load_temp_config", side_effect=lambda key, session_id: {
                 "date_column": "transactiondate",
                 "active_storage_address": "172.20.137.129",
                 "api_port": "5000",
                 "search_api_endpoint_es_strict": "api/search/ui",
             }.get(key)), \
             patch("api.STR_link_analysis.es_keyword_search", return_value=elastic_response) as search_mock, \
             patch("api.STR_link_analysis.create_dataframe_response", return_value=("response", 200)) as dataframe_mock, \
             patch("api.STR_link_analysis._ingest_dataframe_to_neo4j", return_value=True) as ingest_mock, \
             patch("api.STR_link_analysis._analysis_summary", return_value={
                 "total_nodes": 1,
                 "flagged_nodes": 1,
                 "clean_nodes": 0,
                 "flagged_relationships": 1,
                 "metrics": {"degree": {"min": 1, "max": 1, "avg": 1}},
             }), \
             patch("api.STR_link_analysis._relationship_panel_payload", return_value=[{"id": "rel_1", "type": "SMURFING", "textcolor": "#111827", "bgcolor": "#d5d276"}]), \
             patch("api.STR_link_analysis.emit_str_report_link_analysis") as open_emit_mock, \
             patch("api.STR_link_analysis.emit_status_payload") as status_emit_mock:
            response = self.client.post(
                "/api/STR_link_analysis",
                json={
                    "entity": "bank",
                    "type": "account_number",
                    "value": "5642153",
                    "session_id": "499767",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {
            "message": "success!",
            "session_id": "499767",
            "wait_for_prepare": False,
            "socket_emit": [],
        })
        open_emit_mock.assert_called_once_with({
            "message": "success!",
            "session_id": "499767",
            "wait_for_prepare": False,
            "socket_emit": [],
        }, "499767")
        self.assertEqual(status_emit_mock.call_count, 2)
        metadata_payload = status_emit_mock.call_args_list[0].args[0]
        relationships_payload = status_emit_mock.call_args_list[1].args[0]
        self.assertEqual(metadata_payload["type"], "metadata")
        self.assertEqual(metadata_payload["data"]["message"], "success!")
        self.assertEqual(metadata_payload["data"]["status"], "flagged")
        self.assertEqual(metadata_payload["data"]["summary"]["flagged_nodes"], 1)
        self.assertEqual(relationships_payload["type"], "relationships")
        self.assertEqual(relationships_payload["data"][0]["type"], "SMURFING")
        search_mock.assert_called_once()
        search_args = search_mock.call_args.args
        self.assertEqual(search_args[0], "search")
        self.assertEqual(search_args[2], "5642153")
        self.assertEqual(search_args[3], "accountno")
        self.assertTrue(search_args[4])
        dataframe_mock.assert_called_once()
        dataframe_payload, session_id = dataframe_mock.call_args.args
        self.assertEqual(session_id, "499767")
        self.assertEqual(dataframe_payload["kind"], "hybrid")
        self.assertEqual(dataframe_payload["type"], "array")
        self.assertEqual(dataframe_payload["value"][0]["type"], "elastic")
        self.assertEqual(dataframe_payload["value"][0]["column"], "accountno")
        self.assertEqual(dataframe_payload["value"][0]["keyword"], "5642153")
        ingest_mock.assert_called_once_with("499767", "bank")

    def test_success_response_marks_clean_when_no_flagged_nodes(self):
        with patch("api.STR_link_analysis._analysis_summary", return_value={
            "total_nodes": 3,
            "flagged_nodes": 0,
            "clean_nodes": 3,
            "flagged_relationships": 0,
            "all_relationships": 1,
            "total_relationship_edges": 2,
            "metrics": {
                "degree": {"min": 0, "max": 0, "avg": 0},
                "pagerank": {"min": 0.15, "max": 0.15, "avg": 0.15},
            },
        }), \
             patch("api.STR_link_analysis.emit_str_report_link_analysis"), \
             patch("api.STR_link_analysis.emit_status_payload") as status_emit_mock:
            payload = __import__("api.STR_link_analysis", fromlist=["_success_response"])._success_response("499767", "bank")

        self.assertEqual(payload["message"], "success!")
        self.assertEqual(payload["session_id"], "499767")
        self.assertFalse(payload["wait_for_prepare"])
        metadata_payload = status_emit_mock.call_args_list[0].args[0]
        relationships_payload = status_emit_mock.call_args_list[1].args[0]
        self.assertEqual(metadata_payload["data"]["status"], "clean")
        self.assertEqual(metadata_payload["data"]["summary"]["clean_nodes"], 3)
        self.assertEqual(metadata_payload["data"]["summary"]["flagged_relationships"], 0)
        self.assertEqual(metadata_payload["data"]["summary"]["all_relationships"], 1)
        self.assertEqual(metadata_payload["data"]["summary"]["total_relationship_edges"], 2)
        self.assertEqual(metadata_payload["data"]["summary"]["metrics"]["pagerank"]["avg"], 0.15)
        self.assertEqual(relationships_payload["data"], [{
            "id": "rel_transacts_to",
            "type": "TRANSACTS_TO",
            "textcolor": "#ffffff",
            "bgcolor": "#750b8c",
        }])

    def test_STR_link_analysis_generates_str_report_session_id(self):
        elastic_response = {"results": [{"size": 1}]}

        with patch("api.STR_link_analysis._prepare_session", return_value=True), \
             patch("api.STR_link_analysis.load_temp_config", return_value=None), \
             patch("api.STR_link_analysis.es_keyword_search", return_value=elastic_response), \
             patch("api.STR_link_analysis.create_dataframe_response", return_value=("response", 200)), \
             patch("api.STR_link_analysis._ingest_dataframe_to_neo4j", return_value=True), \
             patch("api.STR_link_analysis._success_response", return_value={"message": "success!", "session_id": "ok", "wait_for_prepare": False, "socket_emit": []}) as success_mock:
            response = self.client.post(
                "/api/STR_link_analysis",
                json={"entity": "bank", "type": "account_number", "value": "5642153"},
            )

        self.assertEqual(response.status_code, 200)
        generated_session_id = success_mock.call_args.args[0]
        self.assertTrue(generated_session_id.startswith("str_report_"))

    def test_STR_link_analysis_uses_request_str_id_as_session_id(self):
        elastic_response = {"results": [{"size": 1}]}

        with patch("api.STR_link_analysis._prepare_session", return_value=True), \
             patch("api.STR_link_analysis.load_temp_config", return_value=None), \
             patch("api.STR_link_analysis.es_keyword_search", return_value=elastic_response), \
             patch("api.STR_link_analysis.create_dataframe_response", return_value=("response", 200)), \
             patch("api.STR_link_analysis._ingest_dataframe_to_neo4j", return_value=True), \
             patch("api.STR_link_analysis._success_response", return_value={"message": "success!", "session_id": "str_report_existing", "wait_for_prepare": False, "socket_emit": []}) as success_mock:
            response = self.client.post(
                "/api/STR_link_analysis",
                json={
                    "entity": "bank",
                    "type": "account_number",
                    "value": "5642153",
                    "str_id": "str_report_existing",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(success_mock.call_args.args[0], "str_report_existing")

    def test_status_payload_is_replayable_for_late_subscribers(self):
        import globals
        from api.STR_link_analysis import _success_response
        from batch_manager.utils.notification_utils import flush_status_pending

        globals.str_report_status_registry.clear()

        with patch("api.STR_link_analysis._analysis_summary", return_value={
            "total_nodes": 3,
            "flagged_nodes": 0,
            "clean_nodes": 3,
            "flagged_relationships": 0,
            "all_relationships": 1,
            "total_relationship_edges": 2,
            "metrics": {"degree": {"min": 0, "max": 0, "avg": 0}},
        }), \
             patch("api.STR_link_analysis.emit_str_report_link_analysis"), \
             patch("batch_manager.utils.notification_utils._socketio") as socketio_mock:
            _success_response("str_report_test", "bank")
            emitted = flush_status_pending("str_report_test", "socket-1")

        self.assertEqual(emitted, 2)
        self.assertEqual(socketio_mock.emit.call_count, 2)
        first_payload = socketio_mock.emit.call_args_list[0].args[1]
        second_payload = socketio_mock.emit.call_args_list[1].args[1]
        self.assertEqual(first_payload["type"], "metadata")
        self.assertEqual(first_payload["data"]["status"], "clean")
        self.assertEqual(second_payload["type"], "relationships")
        self.assertEqual(second_payload["data"], [{
            "id": "rel_transacts_to",
            "type": "TRANSACTS_TO",
            "textcolor": "#ffffff",
            "bgcolor": "#750b8c",
        }])

    def test_STR_link_analysis_sends_created_dataframe_to_analyzer(self):
        default_config = {
            "active_storage_address": "172.20.137.129",
            "spark_port": "4040",
            "active_tool": "neo4j",
            "active_tool_protocol": "neo4j://172.21.22.88",
            "tool_protocol_port": "7687",
            "active_tool_username": "neo4j",
            "active_tool_password": "test-neo4j-password",
            "default_source_col": "accountno",
            "default_target_col": "benaccountno",
            "default_relationship": "TRANSACTS_TO",
        }

        with patch("api.STR_link_analysis.load_temp_config", return_value=None), \
             patch("api.STR_link_analysis.get_default_session_config", return_value=default_config), \
             patch("api.STR_link_analysis.save_temp_config"), \
             patch("api.STR_link_analysis.analyzer", return_value=True) as analyzer_mock:
            result = __import__("api.STR_link_analysis", fromlist=["_ingest_dataframe_to_neo4j"])._ingest_dataframe_to_neo4j("499767", "bank")

        self.assertTrue(result)
        self.assertEqual(analyzer_mock.call_count, 2)
        link_payload = analyzer_mock.call_args_list[0].args[0]
        relationship_payload = analyzer_mock.call_args_list[1].args[0]
        self.assertEqual(link_payload["id"], "batch_data")
        self.assertEqual(link_payload["type"], "new")
        self.assertEqual(link_payload["session_id"], "499767")
        self.assertEqual(link_payload["dataframe_dir"], "public/temp_dfParts/merged_dfpart_499767/")
        self.assertEqual(link_payload["tool"], "neo4j")
        self.assertEqual(link_payload["tool_credentials"]["url"], "neo4j://172.21.22.88:7687")
        self.assertEqual(link_payload["tool_credentials"]["password"], "test-neo4j-password")
        self.assertEqual(link_payload["action"], "Link Analysis")
        self.assertEqual(link_payload["rule"], "bank transactions")
        self.assertEqual(relationship_payload["action"], "Source / Target Relationship")
        self.assertEqual(relationship_payload["source"], "accountno")
        self.assertEqual(relationship_payload["target"], "benaccountno")
        self.assertEqual(relationship_payload["relationship"], "TRANSACTS_TO")

    def test_STR_link_analysis_uses_configured_source_target_columns(self):
        default_config = {
            "active_storage_address": "172.20.137.129",
            "spark_port": "4040",
            "active_tool": "neo4j",
            "active_tool_protocol": "neo4j://172.21.22.88",
            "tool_protocol_port": "7687",
            "active_tool_username": "neo4j",
            "active_tool_password": "test-neo4j-password",
            "default_source_col": "sender_account",
            "default_target_col": "receiver_account",
            "default_relationship": "SENDS_TO",
        }

        with patch("api.STR_link_analysis.load_temp_config", return_value=None), \
             patch("api.STR_link_analysis.get_default_session_config", return_value=default_config), \
             patch("api.STR_link_analysis.save_temp_config"), \
             patch("api.STR_link_analysis.analyzer", return_value=True) as analyzer_mock:
            result = __import__("api.STR_link_analysis", fromlist=["_ingest_dataframe_to_neo4j"])._ingest_dataframe_to_neo4j("499767", "bank")

        self.assertTrue(result)
        relationship_payload = analyzer_mock.call_args_list[1].args[0]
        self.assertEqual(relationship_payload["source"], "sender_account")
        self.assertEqual(relationship_payload["target"], "receiver_account")
        self.assertEqual(relationship_payload["relationship"], "SENDS_TO")

    def test_STR_link_analysis_fails_when_dataframe_creation_fails(self):
        elastic_response = {"results": [{"size": 1}]}

        with patch("api.STR_link_analysis._prepare_session", return_value=True), \
             patch("api.STR_link_analysis.load_temp_config", return_value=None), \
             patch("api.STR_link_analysis.es_keyword_search", return_value=elastic_response), \
             patch("api.STR_link_analysis.create_dataframe_response", return_value=("response", 400)):
            response = self.client.post(
                "/api/STR_link_analysis",
                json={"entity": "bank", "type": "account_number", "value": "5642153"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"message": "failed!"})

    def test_STR_link_analysis_returns_not_found_when_elastic_has_no_results(self):
        with patch("api.STR_link_analysis._prepare_session", return_value=True), \
             patch("api.STR_link_analysis.load_temp_config", return_value=None), \
             patch("api.STR_link_analysis.es_keyword_search", return_value=None), \
             patch("api.STR_link_analysis.create_dataframe_response") as dataframe_mock:
            response = self.client.post(
                "/api/STR_link_analysis",
                json={"entity": "bank", "type": "account_number", "value": "5642153"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"message": "Not found!"})
        dataframe_mock.assert_not_called()

    def test_STR_link_analysis_requires_json_body(self):
        response = self.client.post("/api/STR_link_analysis", data="not json")

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json(), {"message": "failed!"})

    def test_STR_link_analysis_requires_three_fields(self):
        response = self.client.post(
            "/api/STR_link_analysis",
            json={"entity": "bank", "type": "account_number"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json(), {"message": "failed!"})

    def test_STR_link_analysis_rejects_unsupported_bank_type(self):
        response = self.client.post(
            "/api/STR_link_analysis",
            json={"entity": "bank", "type": "phone_number", "value": "0911000000"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json(), {"message": "failed!"})


if __name__ == "__main__":
    unittest.main()
