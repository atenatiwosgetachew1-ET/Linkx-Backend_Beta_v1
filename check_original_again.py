with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'r') as f:
    text = f.read()
import re
# Print the original if we can find it in git history
import subprocess
try:
    old_text = subprocess.check_output(['git', 'show', 'HEAD^:service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py']).decode('utf-8')
    pattern = r"def get_circular_flow_query\(.*?\):\n.*?return f\"\"\"\n.*?(?=\n\s*\"\"\")\n\s*\"\"\""
    print(re.search(pattern, old_text, flags=re.DOTALL).group(0))
except Exception as e:
    print(e)
