#!/usr/bin/env python3
"""
Unit test to verify the component-preserving, amount-sorted soft ~1,000 evidence extraction logic.
"""
from collections import defaultdict

def _extract_amount(val):
    if val is None:
        return 0.0
    try:
        a = float(val)
        return a if a > 0 else 0.0
    except (ValueError, TypeError):
        return 0.0

def _node_amount(node):
    for k in ("TRANSFERAMOUNT", "AMOUNT", "AMOUNTINBIRR", "amount", "transferamount"):
        if k in node:
            amt = _extract_amount(node[k])
            if amt > 0:
                return amt
    return 0.0

def _edge_amount(edge):
    for k in ("total_amount", "amount", "TRANSFERAMOUNT", "AMOUNT", "AMOUNTINBIRR"):
        if k in edge:
            amt = _extract_amount(edge[k])
            if amt > 0:
                return amt
    return 0.0

def extract_evidence_subgraph(nodes_list, edges_list, soft_target_nodes=1000):
    node_map = {n["id"]: n for n in nodes_list}
    
    adj = defaultdict(set)
    for edge in edges_list:
        adj[edge["from"]].add(edge["to"])
        adj[edge["to"]].add(edge["from"])

    all_node_ids = set(node_map.keys())
    visited = set()
    components = []

    for nid in all_node_ids:
        if nid not in visited:
            comp_nodes = set()
            queue = [nid]
            visited.add(nid)
            while queue:
                curr = queue.pop(0)
                comp_nodes.add(curr)
                for neighbor in adj[curr]:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        queue.append(neighbor)

            comp_edges = [
                e for e in edges_list
                if e["from"] in comp_nodes and e["to"] in comp_nodes
            ]
            comp_node_objs = [node_map[i] for i in comp_nodes if i in node_map]

            comp_total_vol = sum(_node_amount(n) for n in comp_node_objs)
            comp_max_node_amt = max((_node_amount(n) for n in comp_node_objs), default=0.0)
            comp_max_edge_amt = max((_edge_amount(e) for e in comp_edges), default=0.0)
            comp_max_amt = max(comp_max_node_amt, comp_max_edge_amt)

            components.append({
                "node_ids": comp_nodes,
                "nodes": comp_node_objs,
                "edges": comp_edges,
                "node_count": len(comp_nodes),
                "edge_count": len(comp_edges),
                "total_volume": comp_total_vol,
                "max_amount": comp_max_amt
            })

    # Sort components by transaction amount / volume descending
    components.sort(key=lambda c: (c["total_volume"], c["max_amount"], c["node_count"]), reverse=True)

    selected_components = []
    accumulated_nodes = 0

    for comp in components:
        if not selected_components:
            selected_components.append(comp)
            accumulated_nodes += comp["node_count"]
        else:
            if accumulated_nodes < soft_target_nodes:
                selected_components.append(comp)
                accumulated_nodes += comp["node_count"]
            else:
                break

    render_nodes = []
    render_edges = []
    for comp in selected_components:
        render_nodes.extend(comp["nodes"])
        render_edges.extend(comp["edges"])

    if len(render_nodes) > 1200 and len(selected_components) == 1:
        top_edges_sorted = sorted(render_edges, key=lambda e: _edge_amount(e), reverse=True)
        sub_node_ids = set()
        sub_edges = []
        for e in top_edges_sorted:
            sub_node_ids.add(e["from"])
            sub_node_ids.add(e["to"])
            sub_edges.append(e)
            if len(sub_node_ids) >= soft_target_nodes:
                break
        sub_edges_set = {e["id"] for e in sub_edges}
        for e in render_edges:
            if e["id"] not in sub_edges_set and e["from"] in sub_node_ids and e["to"] in sub_node_ids:
                sub_edges.append(e)
        render_nodes = [n for n in render_nodes if n["id"] in sub_node_ids]
        render_edges = sub_edges

    render_nodes.sort(key=lambda n: _node_amount(n), reverse=True)
    render_edges.sort(key=lambda e: _edge_amount(e), reverse=True)

    is_truncated = (len(components) > len(selected_components) or len(nodes_list) > len(render_nodes))
    
    return render_nodes, render_edges, components, selected_components, is_truncated

def run_tests():
    print("Testing Component-Preserving & Amount-Sorted Extraction...")
    
    # 1. Create 3 clusters of varying size and amounts
    # Cluster A: 400 nodes, transactions of 5,000 each = 2,000,000 total
    # Cluster B: 350 nodes, transactions of 2,000 each = 700,000 total
    # Cluster C: 350 nodes, transactions of 1,000 each = 350,000 total
    # Cluster D: 200 nodes, transactions of 100 each = 20,000 total
    
    nodes = []
    edges = []
    
    # Cluster A
    for i in range(400):
        nodes.append({"id": f"A_{i}", "TRANSFERAMOUNT": 5000})
        if i > 0:
            edges.append({"id": f"e_A_{i}", "from": f"A_{i-1}", "to": f"A_{i}", "amount": 5000})
            
    # Cluster B
    for i in range(350):
        nodes.append({"id": f"B_{i}", "TRANSFERAMOUNT": 2000})
        if i > 0:
            edges.append({"id": f"e_B_{i}", "from": f"B_{i-1}", "to": f"B_{i}", "amount": 2000})

    # Cluster C
    for i in range(350):
        nodes.append({"id": f"C_{i}", "TRANSFERAMOUNT": 1000})
        if i > 0:
            edges.append({"id": f"e_C_{i}", "from": f"C_{i-1}", "to": f"C_{i}", "amount": 1000})

    # Cluster D
    for i in range(200):
        nodes.append({"id": f"D_{i}", "TRANSFERAMOUNT": 100})
        if i > 0:
            edges.append({"id": f"e_D_{i}", "from": f"D_{i-1}", "to": f"D_{i}", "amount": 100})
            
    r_nodes, r_edges, comps, sel_comps, truncated = extract_evidence_subgraph(nodes, edges, 1000)
    
    # Assertions
    # 1. Cluster A (400) + Cluster B (350 = 750) + Cluster C (350 = 1100).
    # Since 750 < 1000, Cluster C is added COMPLETELY! Total = 1,100 nodes. Cluster D is excluded.
    assert len(sel_comps) == 3, f"Expected 3 components, got {len(sel_comps)}"
    assert len(r_nodes) == 1100, f"Expected 1100 nodes (soft ~1000), got {len(r_nodes)}"
    assert truncated == True, "Expected truncated == True"
    
    # 2. Check that no node in Cluster C is missing its neighbors
    # For every edge in Cluster A, B, C: both endpoints must be in r_nodes
    r_node_ids = {n["id"] for n in r_nodes}
    for e in r_edges:
        assert e["from"] in r_node_ids, f"Edge endpoint {e['from']} missing!"
        assert e["to"] in r_node_ids, f"Edge endpoint {e['to']} missing!"
        
    # Check that EVERY node in Cluster C is in r_nodes (not cut mid-cluster)
    c_nodes_in_result = [nid for nid in r_node_ids if nid.startswith("C_")]
    assert len(c_nodes_in_result) == 350, f"Cluster C was severed! Expected 350 nodes, got {len(c_nodes_in_result)}"
    
    # 3. Check sorting: highest amount first
    assert r_nodes[0]["TRANSFERAMOUNT"] >= r_nodes[-1]["TRANSFERAMOUNT"], "Nodes not sorted by amount DESC!"
    assert r_edges[0]["amount"] >= r_edges[-1]["amount"], "Edges not sorted by amount DESC!"
    
    print("✅ All verification assertions passed successfully!")

if __name__ == "__main__":
    run_tests()
