import sys
import os

# Ensure we can import the backend modules
sys.path.append(os.path.join(os.path.dirname(__file__), 'service_factory/services/linkx-api/src'))

from batch_manager.config_defaults import _fetch_global_entities, update_global_entities

def seed():
    global_entities = _fetch_global_entities()
    if not global_entities:
        print("No global entities found in the database.")
        return
        
    trusted_entities = global_entities.get("trusted_entities", [])
    updated_count = 0
    
    pass_through_categories = {"WALLET", "BANK", "PAYMENT_PROCESSOR", "TELECOM", "AGENT"}
    
    for entity in trusted_entities:
        if isinstance(entity, dict):
            category = str(entity.get("category", "")).upper()
            if "pass_through" not in entity:
                if category in pass_through_categories:
                    entity["pass_through"] = True
                    updated_count += 1
                else:
                    entity["pass_through"] = False
                    updated_count += 1
                    
    if updated_count > 0:
        update_global_entities({"trusted_entities": trusted_entities}, {"username": "system_seeder"})
        print(f"Successfully seeded {updated_count} trusted entities with pass_through flags.")
    else:
        print("All entities already have pass_through flags. No updates needed.")

if __name__ == "__main__":
    seed()
