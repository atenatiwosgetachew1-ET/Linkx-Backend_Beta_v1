"""Backward compatibility shim for risk_scoring_kafka_service.

All functionality has been generalized into external_link_analysis_service.py
to serve all sibling service accounts (Risk Scoring, xVigilance, AML Monitoring, etc.).
"""

from batch_manager.services.external_link_analysis_service import *  # noqa: F401, F403
