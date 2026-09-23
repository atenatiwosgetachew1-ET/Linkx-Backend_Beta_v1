import sys
sys.path.append("/var/www/linkx-backend/service_factory/services/linkx-worker/src")
from batch_manager.services.risk_scoring_kafka_service import create_neo4j_driver
import json

driver = create_neo4j_driver()
with driver.session() as s:
    result = s.run("MATCH (n) RETURN count(n) AS cnt")
    for r in result:
        print(f"Total nodes: {r['cnt']}")
driver.close()
