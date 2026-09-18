import sys
import os
import json
import time

correct_path = os.path.join(os.path.dirname(__file__), 'service_factory/services/linkx-api/src')
sys.path.insert(0, correct_path)

from batch_manager.config_defaults import _auto_load_dotenv
_auto_load_dotenv()

from auth.repository import bind_analysis_session_actor
from session_config_store import save_session_config, create_session_config, duplicate_window_config, _connect

def run_test():
    valid_user_id = None
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users LIMIT 1")
            row = cur.fetchone()
            if row:
                valid_user_id = row[0]
                
    if not valid_user_id:
        return
        
    actor = {'id': valid_user_id, 'actor_type': 'user'}
    
    parent_session = '777777'
    
    bind_analysis_session_actor(parent_session, actor)
    
    # 1. Base config gets our survivor identifier
    save_session_config(parent_session, {'test_identifier': 'ROTATION_SURVIVOR'})
    
    # 2. Wait 1 second so the timestamp is definitively newer
    time.sleep(1)
    
    # 3. Simulate the UI duplicating a window, which creates a new row with {} and a NEWER timestamp!
    duplicate_window_config(parent_session, '2')
    
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE analysis_sessions SET created_at = NOW() - INTERVAL '13 hours' WHERE session_id = %s", (parent_session,))
        conn.commit()
    
    new_session_id = '666666'
    bind_analysis_session_actor(new_session_id, actor)
    
    copied_config = create_session_config(
        session_id=new_session_id,
        actor=actor,
        default_config={"default_theme": "dark"},
        existing_session_id=parent_session
    )
    
    print("Config applied to the new rotated session:")
    print(json.dumps(copied_config, indent=2))
    
    if copied_config and "test_identifier" in copied_config:
        print("\n✅ SUCCESS: The identifier survived!")
    else:
        print("\n❌ BUG VERIFIED: The identifier was WIPED OUT! It grabbed the empty window row instead.")

if __name__ == "__main__":
    run_test()
