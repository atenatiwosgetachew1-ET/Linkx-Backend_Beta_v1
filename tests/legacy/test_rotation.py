import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'service_factory/services/linkx-api/src'))
from batch_manager.config_defaults import _auto_load_dotenv
_auto_load_dotenv()

from session_config_store import create_session_config, save_session_config, load_session_config, _connect, ensure_schema
import json

def test():
    ensure_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM session_configs WHERE session_id IN ('111', '222')")
            cur.execute("DELETE FROM analysis_sessions WHERE session_id IN ('111', '222')")
        conn.commit()

    conf1 = create_session_config("111", actor={"id": "admin", "actor_type": "user"}, default_config={"theme": "dark"})
    
    save_session_config("1_111", {"theme": "light", "custom_setting": "yes"})
    
    copied_config = create_session_config("222", actor={"id": "admin", "actor_type": "user"}, existing_session_id="111")
    print("Copied Config during rotation:")
    print(json.dumps(copied_config, indent=2))
    
    loaded = load_session_config("1_222")
    print("\nLoaded Config for window 1 of new session:")
    print(json.dumps(loaded, indent=2))

if __name__ == "__main__":
    test()
