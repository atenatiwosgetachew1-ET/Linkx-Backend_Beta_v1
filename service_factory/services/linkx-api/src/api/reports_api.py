from flask import Blueprint, request, jsonify
from auth.decorators import permission_required, current_actor_from_request
from auth.repository import record_security_event
from batch_manager.utils.reports_utils import insert_report, get_reports

reports_api = Blueprint('reports_api', __name__, url_prefix='/api/v1/reports')

def _audit(event_type, target_id=None, success=True, metadata=None):
    actor = current_actor_from_request()
    try:
        record_security_event(
            event_type,
            actor=actor,
            target_type="report",
            target_id=target_id,
            success=success,
            metadata=metadata or {}
        )
    except Exception as e:
        print(f"Failed to write audit log: {e}")

def _get_paginated_reports(report_type):
    try:
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
        status = request.args.get('status')
        
        reports, total_count = get_reports(report_type=report_type, status=status, limit=limit, offset=offset)
        
        _audit(f"reports.list_{report_type.lower()}", success=True, metadata={"status": status, "limit": limit, "offset": offset})
        
        # Return structured pagination format
        return jsonify({
            "data": reports,
            "limit": limit,
            "offset": offset,
            "count": total_count
        }), 200
    except Exception as e:
        _audit(f"reports.list_{report_type.lower()}", success=False, metadata={"error": str(e)})
        return jsonify({"error": str(e)}), 500

@reports_api.route('/parent', methods=['GET'])
@permission_required('reports:read')
def list_parent_reports():
    return _get_paginated_reports('PARENT_RECEIVED')

@reports_api.route('/xvigilance', methods=['GET'])
@permission_required('reports:read')
def list_xvigilance_reports():
    return _get_paginated_reports('XVIGILANCE_FINDING')

@reports_api.route('/evidence', methods=['GET'])
@permission_required('reports:read')
def list_evidence_reports():
    return _get_paginated_reports('SERVICE_EVIDENCE')

@reports_api.route('/import-parent', methods=['POST'])
@permission_required('reports:read')
def import_parent_report():
    data = request.json
    if not data or 'external_reference_id' not in data:
        return jsonify({"error": "Missing external_reference_id in payload"}), 400
    
    try:
        report_id = insert_report(
            report_type='PARENT_RECEIVED',
            source_system='parent_ctms',
            payload=data,
            external_reference_id=data['external_reference_id'],
            status='NEW'
        )
        _audit("reports.import_parent", target_id=str(report_id), success=True)
        return jsonify({"message": "Report imported successfully", "report_id": report_id}), 202
    except Exception as e:
        _audit("reports.import_parent", success=False, metadata={"error": str(e)})
        return jsonify({"error": str(e)}), 500

@reports_api.route('/<report_id>/bind-workspace', methods=['POST'])
@permission_required('reports:read')
def bind_workspace(report_id):
    data = request.json
    workspace_id = data.get('workspace_id')
    if not workspace_id:
        return jsonify({"error": "Missing workspace_id"}), 400
        
    try:
        _audit("reports.bind_workspace", target_id=str(report_id), success=True, metadata={"workspace_id": workspace_id})
        return jsonify({"message": f"Report {report_id} bound to workspace {workspace_id}"}), 200
    except Exception as e:
        _audit("reports.bind_workspace", target_id=str(report_id), success=False, metadata={"error": str(e)})
        return jsonify({"error": str(e)}), 500
