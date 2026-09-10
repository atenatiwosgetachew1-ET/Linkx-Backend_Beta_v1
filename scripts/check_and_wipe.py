import os
from neo4j import GraphDatabase

# Load .env
with open('/opt/linkx-worker/.env') as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            os.environ.setdefault(k.strip(), v.strip())

# Get Neo4j credentials from env
neo4j_uri = os.getenv('NEO4J_URI') or os.getenv('LINKX_NEO4J_URI') or 'bolt://172.27.23.85:7687'
neo4j_user = os.getenv('NEO4J_USER') or os.getenv('LINKX_NEO4J_USER') or 'neo4j'
neo4j_pass = os.getenv('NEO4J_PASSWORD') or os.getenv('LINKX_NEO4J_PASSWORD') or ''

print(f'Connecting to Neo4j at {neo4j_uri} as {neo4j_user}...')
driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_pass))
with driver.session() as s:
    result = s.run("MATCH (n) RETURN count(n) AS c")
    print(f'Nodes before wipe: {result.single()["c"]}')
    s.run("MATCH (n) DETACH DELETE n")
    result2 = s.run("MATCH (n) RETURN count(n) AS c")
    print(f'Nodes after wipe: {result2.single()["c"]}')
print('Neo4j wiped clean!')
driver.close()
