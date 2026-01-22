# Ext_APIs.py
from flask import Blueprint, request, abort, jsonify

ext_api = Blueprint("ext_api", __name__)

API_KEY = "LinkxAnalyser"

@ext_api.before_request
def check_key():
    if request.headers.get("x-api-key") != API_KEY:
        abort(401)

@ext_api.route('/riskScoreRequests', methods=['POST'])
def riskScoreRequests():
    print("External request triggered")
    data = request.json
    return jsonify({"status": "OK", "received": data})

@ext_api.route('/riskDatabaseRequests', methods=['POST'])
def riskDatabaseRequests():
    print("External riskDatabase request triggered")
    data = request.json
    return jsonify({"status": "OK", "received": data})

@ext_api.route('/strRequests', methods=['POST'])
def strRequests():
    print("External request triggered")
    data = request.json
    return jsonify({"status": "OK", "received": data})