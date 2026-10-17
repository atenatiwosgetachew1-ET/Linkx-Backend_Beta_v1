"""Backward compatibility shim for risk_scoring_sync_api.

The synchronous analysis endpoint has been generalized to link_analysis_sync_api.py
to serve all sibling service accounts (Risk Scoring, xVigilance, etc.).
"""

from api.link_analysis_sync_api import link_analysis_sync_api as risk_scoring_sync_api  # noqa: F401
