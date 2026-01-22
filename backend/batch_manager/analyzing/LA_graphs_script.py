from globals import create_file,save_temp_config,load_temp_config
from connection_utils import tools
import json
from flask import jsonify

def build_node_properties_full(node):
    # node is a Neo4j Node object
    if not node:
        return ""
    if hasattr(node, 'properties'):
        node_props = node.properties
    else:
        # fallback if node is a dict (unlikely in your case)
        node_props = node.get('properties', {})
    if not node_props:
        return ""
    lines = []
    for key, value in node_props.items():
        lines.append(f"{key}: {value}")
    return '\n'.join(lines)

def prepare_graph_data_full(records):
    nodes_dict = {}
    edges = []

    for rec in records:
        a_node = rec['a']
        b_node = rec['b']
        rel = rec['r']

        # Access properties directly from Neo4j Node objects
        a_props = a_node.properties if hasattr(a_node, 'properties') else {}
        b_props = b_node.properties if hasattr(b_node, 'properties') else {}

        a_id = a_props.get('element_id') or getattr(a_node, 'element_id', None)
        b_id = b_props.get('element_id') or getattr(b_node, 'element_id', None)

        # Use 'element_id' property or fallback to node ID
        a_element_id = a_props.get('element_id') or getattr(a_node, 'element_id', None)
        b_element_id = b_props.get('element_id') or getattr(b_node, 'element_id', None)

        # Make sure IDs are valid
        if not a_element_id or not b_element_id:
            continue

        # Build nodes
        if a_element_id not in nodes_dict:
            title_str = build_node_properties_full(a_node)
            label_value = a_props.get('BENACCOUNTNO', a_element_id)
            nodes_dict[a_element_id] = {
                'id': a_element_id,
                'label': label_value,
                'title': title_str,
                'color': '#97C2FC'
            }

        if b_element_id not in nodes_dict:
            title_str_b = build_node_properties_full(b_node)
            label_value_b = b_props.get('BENACCOUNTNO', b_element_id)
            nodes_dict[b_element_id] = {
                'id': b_element_id,
                'label': label_value_b,
                'title': title_str_b,
                'color': '#97C2FC'
            }

        # Build edges
        edges.append({'from': a_element_id, 'to': b_element_id})

    return list(nodes_dict.values()), edges

def fetch_graph(id,action,source_id,value,type):
	# print("relationship:",id,action,source_id,value,type)
	# if id == "status":
	# 	tool=load_temp_config("tool",source_id)
	# 	tool_credentials=load_temp_config("tool_credentials",source_id)	
	# 	driver = tools(tool.lower(), "check", {"session_id": source_id})
	# 	try:
	# 		with driver.session() as session:
	# 			# Get database info
	# 			result_db_info = session.run("CALL db.info()")
	# 			db_info = result_db_info.consume()
	# 			result_db_info = session.run("CALL db.info()")					
	# 			# Extract database name
	# 			db_info_record = result_db_info.single()
	# 			database_name = db_info_record['name']
	# 			# Get current user
	# 			#result_user = session.run("CALL dbms.security.getUser()")
	# 			#user_info = result_user.consume()
	# 			user_info = tool_credentials["username"]
	# 			# Get total nodes
	# 			total_nodes_result = session.run("MATCH (n) RETURN count(n) AS total_nodes")
	# 			total_nodes = total_nodes_result.single()["total_nodes"]
	# 			# Get total relationships
	# 			total_rels_result = session.run("MATCH ()-[r]->() RETURN count(r) AS total_relationships")
	# 			total_relationships = total_rels_result.single()["total_relationships"]
	# 			# Get total relationship labels
	# 			total_rels_labels = session.run("MATCH ()-[r]->() RETURN COLLECT(DISTINCT type(r)) AS relationship_labels")
	# 			total_relationship_labels = total_rels_labels.single()["relationship_labels"]
	# 			# Get all property keys
	# 			property_keys_result = session.run("MATCH (n) UNWIND keys(n) AS key RETURN DISTINCT key")
	# 			property_keys = [record["key"] for record in property_keys_result]
	# 			# Get Neo4j version
	# 			version_result = session.run("CALL dbms.components() YIELD name, versions WHERE name CONTAINS 'Neo4j' RETURN name, versions")
	# 			version_info = version_result.single()
	# 			try:
	# 				return {
	# 					"sourceId": source_id,
	# 					"databaseName": database_name,
	# 					"user": user_info,
	# 					"nodes": total_nodes,
	# 					"relationships": total_relationships,
	# 					"allRelationships": total_relationship_labels,
	# 					"propertyKeys": property_keys,
	# 					"appVersion": version_info["versions"]
	# 			    };
	# 			except Exception as e:
	# 				print(e)
	# 				return None		
	# 	except Exception as e:
	# 		print(e)
	# 	finally:
	# 		driver.close()
			
	if id == "relationship":
		try:
			tool = load_temp_config("tool", source_id)
			driver = tools(tool.lower(), "check", {"session_id": source_id})

			nodes = {}
			edges = []
			rel_type = value

			if not rel_type:
				print("No relationship type provided")
				return {"nodes": [], "edges": [], "error": "No relationship type provided"}

			query = f"""
					MATCH (a)-[r:{rel_type}]->(b)
					RETURN a, r, b
					LIMIT 100000
					"""


			with driver.session() as session:
				for record in session.run(query):
					a = record["a"]
					b = record["b"]
					r = record["r"]

					# include all Neo4j node properties
					nodes[a.id] = {
						"id": a.id,
						"label": a.get("NodeId", str(a.id)),
						**dict(a)  # flatten all node properties
					}
					nodes[b.id] = {
						"id": b.id,
						"label": b.get("NodeId", str(b.id)),
						**dict(b)
					}

					        # include all relationship properties
					edges.append({
						"from": a.id,
						"to": b.id,
						"label": r.type,  # or type(r).__name__
						**dict(r)  # flatten all relationship properties
					})

			return {
				"nodes": list(nodes.values()),
				"edges": edges
			}
		except Exception as e:
			print("Relationship graph error:", e)
			return {"nodes": [], "edges": [], "error": str(e)}
		finally:
			driver.close()

	if id == "uploads":
		pass
