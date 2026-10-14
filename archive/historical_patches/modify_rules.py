import re

file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py'

with open(file_path, 'r') as f:
    content = f.read()

scores = {
    'SMURFING': 0.3,
    'CIRCULAR_FLOW': 0.6,
    'FUND_FLOW': 0.5,
    'DORMANT_TO_ACTIVE': 0.4,
    'ABNORMAL_BALANCE_CHANGE': 0.3,
    'HUB_AND_SPOKE': 0.5,
    'SHARED_IDENTIFIER': 0.8,
    'RAPID_WITHDRAWAL': 0.4,
    'ACCOUNT_ACTIVITY_SPIKE': 0.3,
    'HIGH_RISK_LINK': 0.7,
    'PEP_INVOLVED': 0.8,
    'SANCTIONED_ENTITY_MATCH': 1.0,
    'LATE_NIGHT_TX': 0.3,
    'JUST_BELOW_THRESHOLD': 0.3
}

# The pattern is: MERGE (something)-[r:RELATION_TYPE...]->(something)
# optionally followed by some lines, then: SET r.bgcolor = 
# Since python regex with re.sub is hard for multiline, let's just process line by line.

lines = content.split('\n')
new_lines = []
current_rel = None

for line in lines:
    # Check if this line is a MERGE with a relation we care about
    merge_match = re.search(r'MERGE\s+\([^\)]+\)-\[\s*([a-zA-Z0-9_]+)\s*:\s*([A-Z_]+)', line)
    if merge_match:
        rel_type = merge_match.group(2)
        if rel_type in scores:
            current_rel = rel_type
    
    # Check if this line is a SET for bgcolor
    set_match = re.search(r'SET\s+([a-zA-Z0-9_]+)\.bgcolor\s*=\s*(.*)', line)
    if set_match and current_rel:
        var_name = set_match.group(1)
        score = scores.get(current_rel, 0.5)
        # Inject the evidence flags
        line = line.replace(f"SET {var_name}.bgcolor =", f"SET {var_name}.is_evidence = true, {var_name}.anomaly_score = {score}, {var_name}.bgcolor =")
        
    new_lines.append(line)

with open(file_path, 'w') as f:
    f.write('\n'.join(new_lines))

print("Modified SET clauses in LA_rules_script.py")
