import sys
import os
import json

correct_path = os.path.join(os.path.dirname(__file__), 'service_factory/services/linkx-api/src')
sys.path.insert(0, correct_path)

from batch_manager.config_defaults import _auto_load_dotenv
_auto_load_dotenv()

from auth.repository import bind_analysis_session_actor
from session_config_store import save_session_config, create_session_config, _connect

def run_test():
    valid_user_id = None
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users LIMIT 1")
            row = cur.fetchone()
            if row:
                valid_user_id = row[0]
                
    if not valid_user_id:
        print("No users found in the database. Cannot run test.")
        return
        
    actor = {'id': valid_user_id, 'actor_type': 'user'}
    
    print("--- STEP 1: SETUP ---")
    print(f"Using valid user ID: {valid_user_id}")
    
    parent_session = '999999'
    window_session = f'1_{parent_session}'
    
    bind_analysis_session_actor(parent_session, actor)
    
    save_session_config(parent_session, {'test_identifier': 'ROTATION_SURVIVOR'})
    print("Saved parent config: {'test_identifier': 'ROTATION_SURVIVOR'}")
    
    save_session_config(window_session, {})
    print(f"Simulated opening a window ({window_session}) with an empty config.")
    
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE analysis_sessions 
                SET created_at = NOW() - INTERVAL '13 hours' 
                WHERE session_id = %s
            """, (parent_session,))
        conn.commit()
    print("Artificially aged parent session by 13 hours.")
    
    print("\n--- STEP 2: ROTATION TRIGGER ---")
    new_session_id = '888888'
    
    # WE MUST CREATE THE NEW SESSION IN DB FIRST (just like main.py does)
    bind_analysis_session_actor(new_session_id, actor)
    
    print(f"Triggering rotation from '{parent_session}' -> '{new_session_id}'...")
    
    copied_config = create_session_config(
        session_id=new_session_id,
        actor=actor,
        default_config={"default_theme": "dark"},
        existing_session_id=parent_session
    )
    
    print("\n--- RESULT ---")
    print("Config applied to the new rotated session:")
    print(json.dumps(copied_config, indent=2))
    
    if copied_config and "test_identifier" in copied_config:
        print("\n✅ SUCCESS: The identifier survived the rotation! (The bug is fixed)")
    else:
        print("\n❌ BUG VERIFIED: The identifier was WIPED OUT! It grabbed the empty window config instead.")

if __name__ == "__main__":
    run_test()
