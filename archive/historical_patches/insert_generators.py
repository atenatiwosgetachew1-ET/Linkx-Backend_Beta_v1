with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'r') as f:
    lines = f.readlines()

with open('/var/www/linkx-backend/generator_funcs.py', 'r') as f:
    gen_code = f.read()

out_lines = []
for line in lines:
    if line.startswith('def _create_transaction_indexes'):
        out_lines.append("# --- RULE GENERATORS (PHASE 1) ---\n")
        out_lines.append(gen_code + "\n\n")
        out_lines.append(line)
    else:
        out_lines.append(line)

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'w') as f:
    f.writelines(out_lines)
