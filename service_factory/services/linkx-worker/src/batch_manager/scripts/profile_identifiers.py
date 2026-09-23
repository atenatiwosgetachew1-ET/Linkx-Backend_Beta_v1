import sys
import os

try:
    from neo4j import GraphDatabase
except ImportError:
    print("Error: neo4j python driver is not installed in this environment.")
    sys.exit(1)

# Try to find the running daemon's environment variables
url = os.getenv("LINKX_NEO4J_URL")
username = os.getenv("LINKX_NEO4J_USERNAME")
password = os.getenv("LINKX_NEO4J_PASSWORD")

if not url:
    try:
        import subprocess
        # Find daemon PID
        pid = subprocess.check_output(["pgrep", "-f", "xvigilance_consumer.py"]).decode().split('\n')[0]
        env_raw = subprocess.check_output(["sudo", "cat", f"/proc/{pid}/environ"]).decode()
        env_dict = {kv.split('=')[0]: kv.split('=')[1] for kv in env_raw.split('\0') if '=' in kv}
        
        url = env_dict.get("LINKX_NEO4J_URL", "bolt://localhost:7687")
        username = env_dict.get("LINKX_NEO4J_USERNAME", "neo4j")
        password = env_dict.get("LINKX_NEO4J_PASSWORD", "password")
    except Exception as e:
        print(f"Warning: Could not read daemon environment. Fallback to localhost. ({e})")
        url = "bolt://localhost:7687"
        username = "neo4j"
        password = "password"

print(f"Connecting to Neo4j at {url} as {username}...")

try:
    driver = GraphDatabase.driver(url, auth=(username, password))
except Exception as e:
    print(f"Failed to connect: {e}")
    sys.exit(1)

with driver.session() as s:
    print("\n--- Top 20 Device IDs ---")
    res1 = s.run("""
        MATCH (t:`bank_transactions_xvigilance-daemon`)
        WHERE t.DEVICE_ID IS NOT NULL AND t.DEVICE_ID <> '' AND toLower(t.DEVICE_ID) <> 'null'
        RETURN t.DEVICE_ID AS device, count(t) AS cnt
        ORDER BY cnt DESC LIMIT 20
    """)
    for r in res1:
        print(f"{r['cnt']:>6} uses -> '{r['device']}'")
        
    print("\n--- Top 20 IP Addresses ---")
    res2 = s.run("""
        MATCH (t:`bank_transactions_xvigilance-daemon`)
        WHERE t.IP_ADDRESS IS NOT NULL AND t.IP_ADDRESS <> '' AND toLower(t.IP_ADDRESS) <> 'null'
        RETURN t.IP_ADDRESS AS ip, count(t) AS cnt
        ORDER BY cnt DESC LIMIT 20
    """)
    for r in res2:
        print(f"{r['cnt']:>6} uses -> '{r['ip']}'")

driver.close()
print("\nProfiling complete.")
