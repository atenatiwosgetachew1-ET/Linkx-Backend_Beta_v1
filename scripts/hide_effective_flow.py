import re

def hide_effective_flow(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    # 1. Remove r.reason from EFFECTIVE_FLOW
    # In the query: SET r.intermediary ..., r.reason = '...', r.edge_semantic = ...
    content = re.sub(r"r\.reason = 'funds flow through trusted intermediary',\s*", "", content)

    # 2. In promote_anomalies_to_postgres, before dumping nodes, overwrite the UI fields
    # We want to replace n_props["BENACCOUNTNO"] with n_props["LOGICAL_BENACCOUNTNO"] if it exists
    # Wait, the node building is:
    # graphs[anomaly_type]["nodes"][n_id] = {
    #     "id": n_id,
    #     "label": n_props.get("NodeId", n_id),
    #     **n_props
    # }
    
    node_builder = """
                if n_props.get("LOGICAL_BENACCOUNTNO"):
                    n_props["BENACCOUNTNO"] = n_props["LOGICAL_BENACCOUNTNO"]
                    n_props["IS_LOGICAL_PASSTHROUGH"] = True
                    
                graphs[anomaly_type]["nodes"][n_id] = {
                    "id": n_id,
                    "label": n_props.get("NodeId", n_id),
                    **n_props
                }
"""
    content = re.sub(r'graphs\[anomaly_type\]\["nodes"\]\[n_id\] = \{\s*"id": n_id,\s*"label": n_props\.get\("NodeId", n_id\),\s*\*\*n_props\s*\}', node_builder.strip(), content)

    node_builder_m = """
                if m_props.get("LOGICAL_BENACCOUNTNO"):
                    m_props["BENACCOUNTNO"] = m_props["LOGICAL_BENACCOUNTNO"]
                    m_props["IS_LOGICAL_PASSTHROUGH"] = True
                    
                graphs[anomaly_type]["nodes"][m_id] = {
                    "id": m_id,
                    "label": m_props.get("NodeId", m_id),
                    **m_props
                }
"""
    content = re.sub(r'graphs\[anomaly_type\]\["nodes"\]\[m_id\] = \{\s*"id": m_id,\s*"label": m_props\.get\("NodeId", m_id\),\s*\*\*m_props\s*\}', node_builder_m.strip(), content)

    with open(file_path, "w") as f:
        f.write(content)

hide_effective_flow("/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py")
print("Done")
