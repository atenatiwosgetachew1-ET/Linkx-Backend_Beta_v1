"""Backward compatibility shim for risk_scoring_sync_api.

The synchronous analysis endpoint has been generalized to link_analysis_sync_api.py
to serve all sibling service accounts (Risk Scoring, xVigilance, etc.).
"""

from flask import Blueprint
from api.link_analysis_sync_api import sync_analysis

risk_scoring_sync_api = Blueprint("risk_scoring_sync_api", __name__)
risk_scoring_sync_api.add_url_rule('/sync_analysis', view_func=sync_analysis, methods=['POST'])
risk_scoring_sync_api.add_url_rule('/sync', view_func=sync_analysis, methods=['POST'])
