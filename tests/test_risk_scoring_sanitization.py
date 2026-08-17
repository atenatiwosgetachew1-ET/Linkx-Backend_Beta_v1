import os
import sys
import unittest

worker_src = os.path.join(os.path.dirname(__file__), "..", "service_factory", "services", "linkx-worker", "src")
if os.path.exists(worker_src) and worker_src not in sys.path:
    sys.path.insert(0, os.path.abspath(worker_src))

from batch_manager.services.risk_scoring_kafka_service import (
    sanitize_risk_scoring_request,
)


class TestRiskScoringSanitization(unittest.TestCase):
    def test_sanitization_filters_bulk_and_preserves_required(self):
        bloated_payload = {
            "schema_version": "1.0",
            "success": True,
            "event_type": "score.calculated",
            "message": "Risk score calculated: composite=6.2, level=LOW",
            "data": {
                "transaction_id": "074f93ca-8350-4227-a072-ae8774da9063",
                "entity_id": "ACC83393",
                "is_entity": False,
                "composite_score": 6.2,
                "residual_risk_score": 0.1,
                "total_score": 6.2,
                "risk_level": "LOW",
                "risky": False,
                "scoring_status": "completed",
                "risk_profile_id": 9,
                "profile_version": "v1",
                "profile_config_hash": "7ae14171c6e5450c",
                "risk_narrative": "Low-risk standard transaction within normal behavioral baseline",
                "recommended_actions": [],
                "category_breakdown": {
                    "transaction_risk": {"score": 24.8, "weight": 0.3, "weighted_score": 7.43},
                    "behavioral_risk": {"score": 23.1, "weight": 0.2, "weighted_score": 4.61},
                    "ml_risk": {"score": 24.6, "weight": 0.2, "weighted_score": 4.92},
                    "link_risk": {"score": 8.8, "weight": 0.15, "weighted_score": 1.33},
                    "rule_risk": {"score": 2.3, "weight": 0.15, "weighted_score": 0.34},
                },
                "triggered_rules": [],
                "alerts_triggered": [],
                "scored_at": "2026-08-15T12:51:35.939Z",
                "source": "realtime",
            },
            "meta": {
                "trace_id": "06c2a42bc9ce44928287b62e7b01ec47",
                "span_id": "5343df9a444a422b",
                "traceparent": "00-06c2a42bc9ce44928287b62e7b01ec47-5343df9a444a422b-01",
                "correlation_id": "e1804ee6-fef9-4d7b-9beb-1a7b6b0cd429",
                "timestamp": "2026-08-15T12:51:35.939Z",
                "service": {
                    "name": "risk-scoring-ms",
                    "version": "1.0.0",
                    "namespace": "risk-decision-platform",
                },
                "messaging": {
                    "system": "kafka",
                    "destination_name": "dev.scoring.score.calculated.v1",
                    "operation_name": "publish",
                },
                "source_id": "scoring",
                "aggregation_key": {
                    "type": "accountno",
                    "value": "ACC83393",
                },
                "processing": {
                    "duration_ms": 141.1,
                },
            },
            "error": None,
        }

        sanitized = sanitize_risk_scoring_request(bloated_payload)

        # Verify only required fields remain in data
        self.assertEqual(sanitized["event_type"], "score.calculated")
        self.assertEqual(sanitized["data"]["entity_id"], "ACC83393")
        self.assertFalse(sanitized["data"]["is_entity"])
        self.assertEqual(sanitized["data"]["transaction_id"], "074f93ca-8350-4227-a072-ae8774da9063")
        self.assertNotIn("category_breakdown", sanitized["data"])
        self.assertNotIn("risk_narrative", sanitized["data"])
        self.assertNotIn("triggered_rules", sanitized["data"])
        self.assertNotIn("recommended_actions", sanitized["data"])

        # Verify tracing context in meta
        self.assertEqual(sanitized["meta"]["trace_id"], "06c2a42bc9ce44928287b62e7b01ec47")
        self.assertEqual(sanitized["meta"]["span_id"], "5343df9a444a422b")
        self.assertEqual(sanitized["meta"]["correlation_id"], "e1804ee6-fef9-4d7b-9beb-1a7b6b0cd429")
        self.assertEqual(sanitized["meta"]["aggregation_key"]["value"], "ACC83393")

    def test_sanitization_preserves_dynamic_aggregation_key_type(self):
        phone_payload = {
            "event_type": "score.calculated",
            "data": {
                "transaction_id": "tx_phone_123",
                "entity_id": "0911223344",
                "is_entity": False,
            },
            "meta": {
                "trace_id": "trace_phone_1",
                "aggregation_key": {
                    "type": "businessmobileno",
                    "value": "0911223344",
                },
            },
        }
        sanitized = sanitize_risk_scoring_request(phone_payload)
        self.assertEqual(sanitized["data"]["entity_id"], "0911223344")
        self.assertEqual(sanitized["meta"]["aggregation_key"]["type"], "businessmobileno")
        self.assertEqual(sanitized["meta"]["aggregation_key"]["value"], "0911223344")

    def test_sanitization_raises_on_missing_entity_id(self):
        invalid_payload = {"data": {}, "meta": {}}
        with self.assertRaises(ValueError):
            sanitize_risk_scoring_request(invalid_payload)


if __name__ == "__main__":
    unittest.main()
