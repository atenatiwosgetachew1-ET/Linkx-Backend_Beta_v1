import re

def fix_fstring(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    # We need to replace CALL { with CALL {{ and } with }}
    # But ONLY in the EFFECTIVE_FLOW query
    
    # Actually, we can just replace 'CALL {' with 'CALL {{'
    # And 'LIMIT 1\n                    }' with 'LIMIT 1\n                    }}'
    
    content = content.replace("CALL {\n                        WITH inbound", "CALL {{\n                        WITH inbound")
    content = content.replace("LIMIT 1\n                    }\n\n                    MERGE", "LIMIT 1\n                    }}\n\n                    MERGE")

    with open(file_path, "w") as f:
        f.write(content)

fix_fstring("/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py")
fix_fstring("/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py")
print("Done")
