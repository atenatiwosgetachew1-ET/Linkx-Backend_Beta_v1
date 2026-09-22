import re

def rewrite_effective_flow_la(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    python_block = """
        if pass_through_accounts:
            log_writer(log_file, f"[{datetime.now()}] [Info] Starting EFFECTIVE_FLOW rule (Python accelerated)")
            try:
                # 1. Fetch inbound
                inbound_res = session.run(f"MATCH (n:{label}) WHERE {_session_scope_clause('n')} AND n.BENACCOUNTNO IN $pt AND n.ACCOUNTNO IS NOT NULL AND n.ACCOUNTNO <> '' RETURN id(n) AS id, n.ACCOUNTNO AS acc, n.BENACCOUNTNO AS ben, n.TRANSACTIONDATE AS date, n.TRANSACTIONTIME AS time, coalesce(toFloat(n.AMOUNTINBIRR), toFloat(n.AMOUNT), toFloat(n.amount), toFloat(n.LOCAL_AMOUNT), 0.0) AS amt", session_param=session_param, pt=pass_through_accounts)
                inbounds = [dict(r) for r in inbound_res]
                
                # 2. Fetch outbound
                outbound_res = session.run(f"MATCH (n:{label}) WHERE {_session_scope_clause('n')} AND n.ACCOUNTNO IN $pt AND n.BENACCOUNTNO IS NOT NULL AND n.BENACCOUNTNO <> '' RETURN id(n) AS id, n.ACCOUNTNO AS acc, n.BENACCOUNTNO AS ben, n.TRANSACTIONDATE AS date, n.TRANSACTIONTIME AS time, coalesce(toFloat(n.AMOUNTINBIRR), toFloat(n.AMOUNT), toFloat(n.amount), toFloat(n.LOCAL_AMOUNT), 0.0) AS amt", session_param=session_param, pt=pass_through_accounts)
                outbounds = [dict(r) for r in outbound_res]
                
                from collections import defaultdict
                out_map = defaultdict(list)
                for o in outbounds:
                    out_map[(o["acc"], o["date"])].append(o)
                    
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
                                break
                                
                if edges_to_create:
                    session.run(f"UNWIND $edges AS e MATCH (inbound) WHERE id(inbound) = e.in_id MATCH (outbound) WHERE id(outbound) = e.out_id MERGE (inbound)-[r:EFFECTIVE_FLOW {{session_id:$session_id}}]->(outbound) SET r.intermediary = e.intermediary, r.hop_count = 2, r.in_amount = e.in_amt, r.out_amount = e.out_amt, r.fee_delta = e.fee, r.effective_sender = e.sender, r.effective_receiver = e.receiver, r.tx_date = e.date, r.bgcolor = '#9b59b6', r.textcolor = '#eeeeee', r.provisional = false, r.edge_semantic = 'EFFECTIVE_FLOW', r.financial_flow = true, r.directed_display = true", session_id=session_param, edges=edges_to_create)
                log_writer(log_file, f"[{datetime.now()}] [Info] EFFECTIVE_FLOW rule completed. Python matched {len(edges_to_create)} edges.")
            except Exception as e:
                log_writer(log_file, f"[{datetime.now()}] [Error] EFFECTIVE_FLOW rule failed: {e}")
"""
    
    pattern = re.compile(r"session\.run\(f\"\"\"\n\s+MATCH \(inbound:.*?log_writer\(log_file, f\"\[\{datetime\.now\(\)\}\] \[Info\] EFFECTIVE_FLOW rule completed\"\)", re.DOTALL)
    new_content = pattern.sub(python_block.strip("\n"), content)

    with open(file_path, "w") as f:
        f.write(new_content)

rewrite_effective_flow_la("/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py")
print("Done")
