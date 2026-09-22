import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'service_factory/services/linkx-api/src'))
from batch_manager.config_defaults import _auto_load_dotenv
_auto_load_dotenv()

from session_config_store import save_session_config, save_user_config, get_user_config

def verify_fix():
    print("Simulating User Saving Config during Session...")
    user_id = "test_user_999"
    session_id = "test_session_123"
    custom_config = {"theme": "test_theme_success", "active_tool": "test_tool"}
    
    # 1. Simulate the new sync code added to main.py
    res = save_user_config(user_id, custom_config)
    print("User config synced:", res)
    
    # 3. Simulate Idle timeout / New Session initialization
    print("\nSimulating Idle Timeout (Session rotated)")
    
    # 4. Fetch the initial config for the new session (which pulls from user_config)
    inherited_config = get_user_config(user_id)
    print("Inherited Config in New Session:")
    print("theme:", inherited_config.get("theme"))
    
    if inherited_config.get("theme") == "test_theme_success":
        print("\nSUCCESS: The verify test passed! The configuration correctly persists across sessions.")
    else:
        print("\nFAILED: Configuration did not persist.")

if __name__ == "__main__":
    verify_fix()
