import re

fetcher_path = "/var/www/linkx-backend/service_factory/services/Linkx_xmaintenance/src/linkx_xvigilance/fetcher.py"
with open(fetcher_path, "r") as f:
    code = f.read()

# Replace the yield line to only yield once and return
old_yield = """            yield page
            scroll_id = response.get("_scroll_id")"""
new_yield = """            # DEV LIMIT: ONLY YIELD EXACTLY ONE PAGE OF 500
            yield page[:500]
            print("[fetcher] DEV LIMIT: Stopping after 500 records.", flush=True)
            break
            # scroll_id = response.get("_scroll_id")"""

code = code.replace(old_yield, new_yield)

with open(fetcher_path, "w") as f:
    f.write(code)
