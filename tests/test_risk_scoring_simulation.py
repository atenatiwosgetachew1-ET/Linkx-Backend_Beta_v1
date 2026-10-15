import os
import sys
import unittest
import tempfile
import importlib.util
import types
import pandas as pd
from unittest.mock import MagicMock, patch

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
worker_src = os.path.join(BASE_DIR, "service_factory", "services", "linkx-worker", "src")
api_src = os.path.join(BASE_DIR, "service_factory", "services", "linkx-api", "src")

# 1. Setup worker path and api path for risk_scoring services
if worker_src not in sys.path:
    sys.path.insert(0, worker_src)
if api_src not in sys.path:
    sys.path.append(api_src)

from batch_manager.services.risk_scoring_kafka_service import execute_formal_link_analysis
from batch_manager.services.unified_risk_scoring_service import process_unified_job

# 2. Dynamically load unified_risk_scoring_api from linkx-api
api_file = os.path.join(api_src, "api", "unified_risk_scoring_api.py")
mock_orch = types.ModuleType("service_orchestration")
mock_orch.connect = MagicMock()
mock_orch.enqueue_worker_job = MagicMock()
mock_orch.get_worker_job = MagicMock()
old_orch = sys.modules.get("service_orchestration")
sys.modules["service_orchestration"] = mock_orch

spec = importlib.util.spec_from_file_location("unified_risk_scoring_api_mod", api_file)
unified_api_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(unified_api_mod)
_get_time_window = unified_api_mod._get_time_window
_check_unified_cache = unified_api_mod._check_unified_cache


class TestRiskScoringSimulation(unittest.TestCase):
    """
    Simulation test suite verifying both Full-History and Scoped-Window flows
    for the Risk Scoring Link Analysis service.
    """

    def test_get_time_window_handling(self):
        """Verifies that time window extraction accurately distinguishes omitted vs provided windows."""
        # 1. When omitted or None -> must return None (Full-History Mode)
        self.assertIsNone(_get_time_window(None))
        self.assertIsNone(_get_time_window({}))
        self.assertIsNone(_get_time_window({"start_date": ""}))
        self.assertIsNone(_get_time_window("invalid_type"))

        # 2. When valid window provided -> must return normalized window (Scoped Window Mode)
        window = {"start_date": " 2025-10-15 ", "end_date": "2026-10-15"}
        expected = {"start_date": "2025-10-15", "end_date": "2026-10-15"}
        self.assertEqual(_get_time_window(window), expected)

    def test_check_unified_cache_sql_branching(self):
        """Verifies that cache queries properly distinguish between scoped and full-history requests."""
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_cur.fetchone.return_value = ({"cached": True},)

        mock_orch.connect.return_value = mock_conn

        # Case A: Full-History (time_window is None) -> checks for NULL / unscoped time_window
        _check_unified_cache("1000698739723", None)
        sql_unscoped = mock_cur.execute.call_args[0][0]
        args_unscoped = mock_cur.execute.call_args[0][1]
        self.assertIn("time_window' IS NULL", sql_unscoped)
        self.assertEqual(args_unscoped, ("1000698739723",))

        # Case B: Scoped (time_window provided) -> checks exact start_date and end_date
        mock_cur.reset_mock()
        scoped_win = {"start_date": "2025-10-15", "end_date": "2026-10-15"}
        _check_unified_cache("1000698739723", scoped_win)
        sql_scoped = mock_cur.execute.call_args[0][0]
        args_scoped = mock_cur.execute.call_args[0][1]
        self.assertIn("start_date' = %s", sql_scoped)
        self.assertIn("end_date' = %s", sql_scoped)
        self.assertEqual(args_scoped, ("1000698739723", "2025-10-15", "2026-10-15"))

    def test_full_history_flow_bypasses_pandas_date_filtering(self):
        """
        Simulation Case 1: External request WITHOUT time_window.
        Verifies that pandas date filtering is completely bypassed, preserving all rows
        and preventing actual_rows vs expected_dataframe_rows mismatch.
        """
        payload = {
            "meta": {
                "trace_id": "sim-trace-full-history-001",
                "aggregation_key": {"type": "accountno", "value": "1000698739723"}
            },
            "data": {
                "entity_id": "1000698739723",
                "analysis_type": "transaction_graph",
                "time_window": None  # Omitted / Full History
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a mock dataframe with rows spanning 2021 to 2026
            df = pd.DataFrame({
                "accountno": ["1000698739723"] * 3,
                "transactiondate": ["2021-05-01", "2024-08-10", "2026-09-01"],
                "amount": [100.0, 250.0, 500.0]
            })
            pq_path = os.path.join(tmpdir, "data.parquet")
            df.to_parquet(pq_path)

            mock_saved_configs = {
                "active_dataframe_dir": tmpdir,
                "active_dataframe_rows": 3,
                "active_storage_address": "172.27.23.43",
                "active_column": "accountno",
                "active_strict_mood": "normal"
            }

            def mock_load_temp(key, sid=None):
                return mock_saved_configs.get(key)

            def mock_save_temp(key, val, sid=None):
                mock_saved_configs[key] = val

            with patch("batch_manager.services.risk_scoring_kafka_service._prepare_session", return_value=True), \
                 patch("batch_manager.services.risk_scoring_kafka_service.es_keyword_search", return_value={"results": [{"size": 100}]}), \
                 patch("batch_manager.services.risk_scoring_kafka_service.load_temp_config", side_effect=mock_load_temp), \
                 patch("batch_manager.services.risk_scoring_kafka_service.save_temp_config", side_effect=mock_save_temp), \
                 patch("batch_manager.services.risk_scoring_kafka_service.create_dataframe_result", return_value=({}, 200)), \
                 patch("batch_manager.services.risk_scoring_kafka_service._ingest_dataframe_to_neo4j", return_value=True), \
                 patch("batch_manager.services.risk_scoring_kafka_service._analysis_summary", return_value={"flagged_nodes": 0, "flagged_relationships": 0}), \
                 patch("batch_manager.services.risk_scoring_kafka_service._get_linked_entities_from_neo4j", return_value=([], 0)), \
                 patch("batch_manager.utils.postgres_utils.get_postgres_connection"):

                event, status = execute_formal_link_analysis(payload)

                # Verify status is success
                self.assertEqual(status, "success")
                self.assertIsNotNone(event)
                # Verify that active_dataframe_rows was NOT touched by any date filter
                self.assertEqual(mock_saved_configs["active_dataframe_rows"], 3)
                # Verify parquet file was untouched
                reloaded = pd.read_parquet(pq_path)
                self.assertEqual(len(reloaded), 3)

    def test_scoped_window_flow_with_epoch_timestamps_and_row_sync(self):
        """
        Simulation Case 2: External request WITH time_window and Epoch Millisecond timestamps.
        Verifies:
        1. Epoch ms timestamps (1787786873000) are parsed without NaT error.
        2. Filtered dataframe keeps matching rows and formats date as %Y-%m-%d string.
        3. active_dataframe_rows is synchronized to the filtered count so Neo4j integrity passes.
        """
        payload = {
            "meta": {
                "trace_id": "sim-trace-scoped-window-002",
                "aggregation_key": {"type": "accountno", "value": "1000698739723"}
            },
            "data": {
                "entity_id": "1000698739723",
                "analysis_type": "transaction_graph",
                "time_window": {
                    "start_date": "2025-10-15",
                    "end_date": "2026-10-15"
                }
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            # 1. 1787786873000 ms is ~2026-08-26 (INSIDE WINDOW)
            # 2. 1609459200000 ms is 2021-01-01 (OUTSIDE WINDOW)
            # 3. '2026-02-15' ISO string is 2026-02-15 (INSIDE WINDOW)
            df = pd.DataFrame({
                "accountno": ["1000698739723", "1000698739723", "1000698739723"],
                "transactiondate": ["1787786873000", "1609459200000", "2026-02-15"],
                "amount": [1000.0, 2000.0, 3000.0]
            })
            pq_path = os.path.join(tmpdir, "data.parquet")
            df.to_parquet(pq_path)

            mock_saved_configs = {
                "active_dataframe_dir": tmpdir,
                "active_dataframe_rows": 3,  # Initial count before date filtering
                "active_storage_address": "172.27.23.43",
                "active_column": "accountno",
                "active_strict_mood": "normal"
            }

            def mock_load_temp(key, sid=None):
                return mock_saved_configs.get(key)

            def mock_save_temp(key, val, sid=None):
                mock_saved_configs[key] = val

            ingest_payloads_captured = []

            def mock_ingest(session_id, custom_source=None, custom_target=None, custom_rel=None):
                # Verify that at time of ingestion, active_dataframe_rows is synchronized
                ingest_payloads_captured.append(mock_saved_configs["active_dataframe_rows"])
                return True

            with patch("batch_manager.services.risk_scoring_kafka_service._prepare_session", return_value=True), \
                 patch("batch_manager.services.risk_scoring_kafka_service.es_keyword_search", return_value={"results": [{"size": 100}]}), \
                 patch("batch_manager.services.risk_scoring_kafka_service.load_temp_config", side_effect=mock_load_temp), \
                 patch("batch_manager.services.risk_scoring_kafka_service.save_temp_config", side_effect=mock_save_temp), \
                 patch("batch_manager.services.risk_scoring_kafka_service.create_dataframe_result", return_value=({}, 200)), \
                 patch("batch_manager.services.risk_scoring_kafka_service._ingest_dataframe_to_neo4j", side_effect=mock_ingest), \
                 patch("batch_manager.services.risk_scoring_kafka_service._analysis_summary", return_value={"flagged_nodes": 0, "flagged_relationships": 0}), \
                 patch("batch_manager.services.risk_scoring_kafka_service._get_linked_entities_from_neo4j", return_value=([], 0)), \
                 patch("batch_manager.utils.postgres_utils.get_postgres_connection"):

                event, status = execute_formal_link_analysis(payload)

                self.assertEqual(status, "success")
                self.assertIsNotNone(event)
                # Verify that 1 row was excluded (2021) and 2 rows kept
                self.assertEqual(mock_saved_configs["active_dataframe_rows"], 2)
                self.assertEqual(ingest_payloads_captured, [2])

                # Check rewritten parquet file
                reloaded = pd.read_parquet(pq_path)
                self.assertEqual(len(reloaded), 2)
                # Dates should be formatted string %Y-%m-%d
                self.assertIn("2026-08-26", reloaded["transactiondate"].values)
                self.assertIn("2026-02-15", reloaded["transactiondate"].values)

    def test_scoped_window_zero_activity_handling(self):
        """
        Simulation Case 3: External request WITH time_window that matches 0 transactions.
        Verifies that zero activity returns a clean 0-score success response immediately
        without calling Neo4j ingestion or tripping any neo4j_ingestion_failed errors.
        """
        payload = {
            "meta": {
                "trace_id": "sim-trace-zero-activity-003",
                "aggregation_key": {"type": "accountno", "value": "1000698739723"}
            },
            "data": {
                "entity_id": "1000698739723",
                "analysis_type": "transaction_graph",
                "time_window": {
                    "start_date": "2010-01-01",
                    "end_date": "2010-01-31"  # No transactions in 2010
                }
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame({
                "accountno": ["1000698739723"],
                "transactiondate": ["2026-05-01"],
                "amount": [5000.0]
            })
            pq_path = os.path.join(tmpdir, "data.parquet")
            df.to_parquet(pq_path)

            mock_saved_configs = {
                "active_dataframe_dir": tmpdir,
                "active_dataframe_rows": 1,
                "active_storage_address": "172.27.23.43",
                "active_column": "accountno",
                "active_strict_mood": "normal"
            }

            def mock_load_temp(key, sid=None):
                return mock_saved_configs.get(key)

            def mock_save_temp(key, val, sid=None):
                mock_saved_configs[key] = val

            mock_ingest = MagicMock(return_value=True)

            with patch("batch_manager.services.risk_scoring_kafka_service._prepare_session", return_value=True), \
                 patch("batch_manager.services.risk_scoring_kafka_service.es_keyword_search", return_value={"results": [{"size": 100}]}), \
                 patch("batch_manager.services.risk_scoring_kafka_service.load_temp_config", side_effect=mock_load_temp), \
                 patch("batch_manager.services.risk_scoring_kafka_service.save_temp_config", side_effect=mock_save_temp), \
                 patch("batch_manager.services.risk_scoring_kafka_service.create_dataframe_result", return_value=({}, 200)), \
                 patch("batch_manager.services.risk_scoring_kafka_service._ingest_dataframe_to_neo4j", mock_ingest), \
                 patch("batch_manager.services.risk_scoring_kafka_service._get_linked_entities_from_neo4j", return_value=([], 0)), \
                 patch("batch_manager.utils.postgres_utils.get_postgres_connection"):

                event, status = execute_formal_link_analysis(payload)

                # Must return success cleanly
                self.assertEqual(status, "success")
                self.assertIsNotNone(event)
                # Ingest must NOT have been called
                mock_ingest.assert_not_called()
                # Row count updated to 0
                self.assertEqual(mock_saved_configs["active_dataframe_rows"], 0)

                # Response metrics must be 0-score
                data = event["data"]
                self.assertEqual(data["flagged_entity_links"], 0)
                self.assertEqual(data["linked_accounts_count"], 0)
                self.assertEqual(data["network_centrality_score"], 0.0)

    def test_process_unified_job_end_to_end_simulation(self):
        """
        Simulation Case 4: End-to-end worker execution of process_unified_job.
        Verifies that v2.0 schema response is generated with success=True,
        error=None, and delivered to the webhook.
        """
        job_payload = {
            "job_id": "job-sim-8bfcf4bb",
            "account_no": "1000698739723",
            "time_window": None  # Full-history flow
        }

        mock_event = {
            "meta": {"session_id": "test_session_123"},
            "data": {
                "flagged_count": 0,
                "linked_count": 5,
                "network_centrality_score": 0.42,
                "max_path_length": 2
            }
        }

        mock_webhook = MagicMock()

        with patch("batch_manager.services.risk_scoring_kafka_service.execute_formal_link_analysis", return_value=(mock_event, "success")), \
             patch("batch_manager.services.unified_risk_scoring_service._send_webhook", mock_webhook), \
             patch("batch_manager.services.unified_risk_scoring_service.get_postgres_connection"):

            result = process_unified_job(job_payload)

            self.assertEqual(result["status"], "success")
            self.assertEqual(result["account_no"], "1000698739723")

            # Verify the v2 payload passed to the webhook
            mock_webhook.assert_called_once()
            call_args = mock_webhook.call_args[0]
            delivered_payload = call_args[2]

            self.assertEqual(delivered_payload["schema_version"], "2.0")
            self.assertEqual(delivered_payload["job_id"], "job-sim-8bfcf4bb")
            self.assertTrue(delivered_payload["success"])
            self.assertIsNone(delivered_payload["error"])
            self.assertEqual(delivered_payload["data"]["linked_accounts_count"], 5)
            self.assertEqual(delivered_payload["data"]["network_centrality_score"], 0.42)

    def test_sync_analysis_endpoint_time_window_flow(self):
        """
        Simulation Case 5: Direct HTTP call to POST /api/risk_scoring/sync_analysis.
        Verifies that time_window (provided vs omitted) is cleanly parsed and forwarded
        to the worker job queue, returning 200 OK.
        """
        from flask import Flask
        # Dynamically load risk_scoring_sync_api
        sync_api_file = os.path.join(api_src, "api", "risk_scoring_sync_api.py")
        spec_sync = importlib.util.spec_from_file_location("risk_scoring_sync_api_mod", sync_api_file)
        sync_mod = importlib.util.module_from_spec(spec_sync)
        spec_sync.loader.exec_module(sync_mod)

        app = Flask(__name__)
        app.register_blueprint(sync_mod.risk_scoring_sync_api, url_prefix="/api/risk_scoring")
        client = app.test_client()

        os.environ["LINKX_RISK_SCORING_API_KEY"] = "test-secret-key"

        mock_sync_conn = MagicMock()
        mock_sync_conn.__enter__.return_value = mock_sync_conn
        mock_sync_cur = MagicMock()
        mock_sync_cur.fetchone.return_value = None
        mock_sync_conn.cursor.return_value.__enter__.return_value = mock_sync_cur
        mock_orch.connect.return_value = mock_sync_conn

        # Case A: Scoped window provided
        with patch.object(sync_mod, "enqueue_worker_job", return_value={"job_id": "job-scoped-1"}) as mock_enq, \
             patch.object(sync_mod, "get_worker_job", return_value={"status": "succeeded", "result": {"source": "link", "data": {"network_centrality_score": 0.25}}}):
            
            resp = client.post(
                "/api/risk_scoring/sync_analysis",
                headers={"X-API-Key": "test-secret-key"},
                json={
                    "account_no": "1000698739723",
                    "time_window": {"start_date": "2025-10-15", "end_date": "2026-10-15"}
                }
            )
            self.assertEqual(resp.status_code, 200)
            json_data = resp.get_json()
            self.assertTrue(json_data["success"])
            self.assertEqual(json_data["account_no"], "1000698739723")
            enq_payload = mock_enq.call_args[1]["payload"]
            self.assertEqual(enq_payload["time_window"], {"start_date": "2025-10-15", "end_date": "2026-10-15"})

        # Case B: Full history (time_window omitted)
        with patch.object(sync_mod, "enqueue_worker_job", return_value={"job_id": "job-unscoped-2"}) as mock_enq2, \
             patch.object(sync_mod, "get_worker_job", return_value={"status": "succeeded", "result": {"source": "link", "data": {"network_centrality_score": 0.10}}}):
            
            resp2 = client.post(
                "/api/risk_scoring/sync_analysis",
                headers={"X-API-Key": "test-secret-key"},
                json={"account_no": "1000698739723"}
            )
            self.assertEqual(resp2.status_code, 200)
            json_data2 = resp2.get_json()
            self.assertTrue(json_data2["success"])
            enq_payload2 = mock_enq2.call_args[1]["payload"]
            self.assertIsNone(enq_payload2["time_window"])

    def test_sync_worker_job_execution_with_time_window(self):
        """
        Simulation Case 6: Worker processing of risk_scoring_sync job.
        Verifies that process_sync_job forwards time_window down to execute_formal_link_analysis.
        """
        from batch_manager.services.risk_scoring_sync_service import process_sync_job

        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_cur.fetchone.return_value = None

        with patch("batch_manager.services.risk_scoring_kafka_service.execute_formal_link_analysis") as mock_exec, \
             patch("batch_manager.services.risk_scoring_sync_service.get_postgres_connection", return_value=mock_conn), \
             patch("batch_manager.analyzing.LA_graphs_script.fetch_graph", return_value={"nodes": [], "edges": []}):

            mock_exec.return_value = ({
                "meta": {"session_id": "sync-session-999"},
                "data": {"network_centrality_score": 0.35, "max_path_length": 3, "linked_count": 8, "flagged_count": 0}
            }, "success")

            # 1. Scoped
            scoped_result = process_sync_job({
                "entity_id": "1000698739723",
                "entity_type": "accountno",
                "time_window": {"start_date": "2025-10-15", "end_date": "2026-10-15"}
            })
            self.assertEqual(scoped_result["status"], "success")
            self.assertEqual(scoped_result["data"]["network_centrality_score"], 0.35)
            call_payload = mock_exec.call_args[0][0]
            self.assertEqual(call_payload["data"]["time_window"], {"start_date": "2025-10-15", "end_date": "2026-10-15"})

            # 2. Unscoped / Full History
            unscoped_result = process_sync_job({
                "entity_id": "1000698739723",
                "entity_type": "accountno",
                "time_window": None
            })
            self.assertEqual(unscoped_result["status"], "success")
            call_payload2 = mock_exec.call_args[0][0]
            self.assertIsNone(call_payload2["data"]["time_window"])

    def test_claim_job_preserves_external_job_id(self):
        """
        Verifies that runner.py claim_job preserves the caller's external job_id
        and sets db_job_id to the database UUID rather than overwriting the caller's ID.
        """
        from linkx_worker.runner import claim_job

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        caller_job_id = "job-ec80db27-d908-463f-a3a0-11b94a53a244"
        db_uuid = "76fed3b7-162e-496b-bdd6-a857a871c49b"

        # Mock DB returning row: (id, session_id, run_id, job_type, queue_name, payload, attempts, max_attempts)
        mock_cur.fetchone.return_value = (
            db_uuid,
            None,
            None,
            "unified_link_analysis",
            "analysis",
            {"job_id": caller_job_id, "account_no": "1000698739723", "time_window": None},
            0,
            3,
        )

        with patch("linkx_worker.runner.emit_event"):
            claimed = claim_job(mock_conn, ["analysis"], "test-worker-1")

        self.assertIsNotNone(claimed)
        payload = claimed["payload"]
        self.assertEqual(payload["job_id"], caller_job_id)
        self.assertEqual(payload["external_job_id"], caller_job_id)
        self.assertEqual(payload["db_job_id"], db_uuid)

    def test_process_unified_job_webhook_preserves_caller_job_id(self):
        """
        Verifies that process_unified_job dispatches the webhook using the caller's
        external_job_id so the external Risk Scoring Aggregator can match the callback.
        """
        caller_job_id = "job-ec80db27-d908-463f-a3a0-11b94a53a244"
        job_payload = {
            "job_id": caller_job_id,
            "external_job_id": caller_job_id,
            "db_job_id": "76fed3b7-162e-496b-bdd6-a857a871c49b",
            "account_no": "1000698739723",
            "time_window": None
        }

        mock_event = {
            "meta": {"session_id": "session-123"},
            "data": {
                "flagged_count": 0,
                "linked_count": 4,
                "network_centrality_score": 0.22,
                "max_path_length": 2
            }
        }

        mock_send = MagicMock(return_value={"success": True, "status_code": 200})

        with patch("batch_manager.services.risk_scoring_kafka_service.execute_formal_link_analysis", return_value=(mock_event, "success")), \
             patch("batch_manager.services.unified_risk_scoring_service._send_webhook", mock_send), \
             patch("batch_manager.services.unified_risk_scoring_service.get_postgres_connection"):

            res = process_unified_job(job_payload)

            self.assertEqual(res["status"], "success")
            self.assertEqual(res["webhook"]["status_code"], 200)

            # Webhook must receive caller_job_id as the correlation and payload job_id
            mock_send.assert_called_once()
            args = mock_send.call_args[0]
            self.assertEqual(args[1], caller_job_id)
            self.assertEqual(args[2]["job_id"], caller_job_id)

    def test_send_webhook_https_pool_and_fallback(self):
        """
        Verifies _send_webhook uses direct HTTPSConnectionPool with SNI server_hostname,
        and falls back to requests.post on pool exception.
        """
        from batch_manager.services.unified_risk_scoring_service import _send_webhook

        payload = {"job_id": "test-job-999", "account_no": "1000698739723"}

        # Case 1: Pool succeeds
        mock_pool_cls = MagicMock()
        mock_pool = MagicMock()
        mock_res = MagicMock(status=200, data=b'{"status":"received"}')
        mock_pool.urlopen.return_value = mock_res
        mock_pool_cls.return_value = mock_pool

        with patch("urllib3.connectionpool.HTTPSConnectionPool", mock_pool_cls):
            out = _send_webhook("1000698739723", "test-job-999", payload)
            self.assertTrue(out["success"])
            self.assertEqual(out["status_code"], 200)
            mock_pool_cls.assert_called_once_with(
                host="172.27.23.46",
                port=443,
                server_hostname="risk-platform.local",
                cert_reqs="CERT_NONE",
                assert_hostname=False,
                timeout=10.0
            )

        # Case 2: Pool raises, falls back to requests.post
        mock_pool_fail = MagicMock(side_effect=Exception("Pool connection reset"))
        mock_req_post = MagicMock()
        mock_req_post.return_value.ok = True
        mock_req_post.return_value.status_code = 200

        with patch("urllib3.connectionpool.HTTPSConnectionPool", mock_pool_fail), \
             patch("requests.post", mock_req_post):
            out2 = _send_webhook("1000698739723", "test-job-999", payload)
            self.assertTrue(out2["success"])
            self.assertEqual(out2["status_code"], 200)
            mock_req_post.assert_called_once()


if __name__ == "__main__":
    unittest.main()
