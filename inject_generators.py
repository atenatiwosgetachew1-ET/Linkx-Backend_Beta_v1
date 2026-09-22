import re

with open('scratch_queries.py', 'r') as f:
    text = f.read()

# We need to find each s.run(f""" ... """)
blocks = re.findall(r's\.run\(f\"\"\"(.*?)\"\"\",', text, re.DOTALL)

print(f"Found {len(blocks)} blocks.")
