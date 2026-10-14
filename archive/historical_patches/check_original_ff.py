import subprocess
import re
try:
    old_text = subprocess.check_output(['git', 'show', 'HEAD^:service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py']).decode('utf-8')
    pattern = r"def get_fund_flow_query\(.*?\):\n.*?return f\"\"\"\n.*?(?=\n\s*\"\"\")\n\s*\"\"\""
    print(re.search(pattern, old_text, flags=re.DOTALL).group(0))
except Exception as e:
    print(e)
