import re

def rewrite_effective_flow(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    # We need to replace the entire Cypher block for EFFECTIVE_FLOW
    # with a Python-based matcher.
    
    python_block = """
        if pass_through_accounts:
            t_eff = time.time()
            try:
                with driver.session() as s:
                    # 1. Fetch inbound
                    inbound_res = s.run(f"MATCH (n:{label}) WHERE ($session_id IS NULL OR n.session_id = $session_id) AND n.BENACCOUNTNO IN $pt AND n.ACCOUNTNO IS NOT NULL AND n.ACCOUNTNO <> '' RETURN id(n) AS id, n.ACCOUNTNO AS acc, n.BENACCOUNTNO AS ben, n.TRANSACTIONDATE AS date, n.TRANSACTIONTIME AS time, coalesce(toFloat(n.AMOUNTINBIRR), toFloat(n.AMOUNT), toFloat(n.amount), toFloat(n.LOCAL_AMOUNT), 0.0) AS amt", session_id=sp, pt=pass_through_accounts)
                    inbounds = [dict(r) for r in inbound_res]
                    
                    # 2. Fetch outbound
                    outbound_res = s.run(f"MATCH (n:{label}) WHERE ($session_id IS NULL OR n.session_id = $session_id) AND n.ACCOUNTNO IN $pt AND n.BENACCOUNTNO IS NOT NULL AND n.BENACCOUNTNO <> '' RETURN id(n) AS id, n.ACCOUNTNO AS acc, n.BENACCOUNTNO AS ben, n.TRANSACTIONDATE AS date, n.TRANSACTIONTIME AS time, coalesce(toFloat(n.AMOUNTINBIRR), toFloat(n.AMOUNT), toFloat(n.amount), toFloat(n.LOCAL_AMOUNT), 0.0) AS amt", session_id=sp, pt=pass_through_accounts)
                    outbounds = [dict(r) for r in outbound_res]
                
                # 3. Match in Python (O(N))
                # Group outbounds by (acc, date)
                from collections import defaultdict
                out_map = defaultdict(list)
                for o in outbounds:
                    out_map[(o["acc"], o["date"])].append(o)
                
                # Sort outbounds by time to allow fast sequential matching or just finding the first
                for k in out_map:
                    out_map[k].sort(key=lambda x: str(x["time"]))
                
                edges_to_create = []
                for i in inbounds:
                    candidates = out_map.get((i["ben"], i["date"]), [])
                    for o in candidates:
                        if o["ben"] != i["acc"] and str(o["time"]) >= str(i["time"]):
                            if i["amt"] > 0 and o["amt"] > 0 and abs(o["amt"] - i["amt"]) <= (i["amt"] * 0.1):
                                edges_to_create.append({
                                    "in_id": i["id"],
                                    "out_id": o["id"],
                                    "intermediary": i["ben"],
                                    "in_amt": i["amt"],
                                    "out_amt": o["amt"],
                                    "fee": i["amt"] - o["amt"],
                                    "sender": i["acc"],
                                    "receiver": o["ben"],
                                    "date": i["date"]
                                })
                                break # LIMIT 1 equivalent
                
                # 4. Write back to Neo4j
                if edges_to_create:
                    with driver.session() as s:
                        s.run(f"UNWIND $edges AS e MATCH (inbound) WHERE id(inbound) = e.in_id MATCH (outbound) WHERE id(outbound) = e.out_id MERGE (inbound)-[r:EFFECTIVE_FLOW {{session_id:$session_id}}]->(outbound) SET r.intermediary = e.intermediary, r.hop_count = 2, r.in_amount = e.in_amt, r.out_amount = e.out_amt, r.fee_delta = e.fee, r.effective_sender = e.sender, r.effective_receiver = e.receiver, r.tx_date = e.date, r.bgcolor = '#9b59b6', r.textcolor = '#eeeeee', r.provisional = false, r.edge_semantic = 'EFFECTIVE_FLOW', r.financial_flow = true, r.directed_display = true", session_id=sp, edges=edges_to_create)
                
                rules_completed.append("EFFECTIVE_FLOW")
                print(f"  [Rule] EFFECTIVE_FLOW ✓ (Python matched {len(edges_to_create)} edges in {time.time() - t_eff:.1f}s)", flush=True)
            except Exception as e:
                rules_failed.append(("EFFECTIVE_FLOW", str(e)[:100]))
                print(f"  [Rule] EFFECTIVE_FLOW ✗ {str(e)[:100]}", flush=True)
"""
    
    # We replace the try/except block for EFFECTIVE_FLOW
    pattern = re.compile(r"try:\n\s+with driver\.session\(\) as s:\n\s+s\.run\(f\"\"\"\n\s+MATCH \(inbound:.*?except Exception as e:\n\s+rules_failed\.append\(\(\"EFFECTIVE_FLOW\", str\(e\)\[:100\]\)\)\n\s+print\(f\"  \[Rule\] EFFECTIVE_FLOW ✗ \{str\(e\)\[:100\]\}\", flush=True\)", re.DOTALL)
    
    new_content = pattern.sub(python_block.strip("\n"), content)
    
    with open(file_path, "w") as f:
        f.write(new_content)

rewrite_effective_flow("/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py")
print("Done")
