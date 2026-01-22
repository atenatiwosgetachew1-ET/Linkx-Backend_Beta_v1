import json
import os
import ast
from datetime import datetime, timedelta
from werkzeug.utils import secure_filename


session_thread_registry = {}
_session_store = {}
fetch_thread_registry = {}
sockets_registry = {}

def create_file(path, name, type, instant_text):
    output_file = os.path.join(path, f'{name}.{type}')
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if isinstance(instant_text, dict) or isinstance(instant_text, list):
        data_content = instant_text
    else:
        data_content = {"value": instant_text}

    json_obj = {
        "data": data_content,
        "Last modified": current_time
    }

    # Create directory if not exists
    try:
        if not os.path.exists(path):
            os.makedirs(path)
        with open(output_file, 'w') as f:
            json.dump(json_obj, f, indent=4)
        return True
    except Exception as e:
        print(e)
        return False

def save_uploaded_file(uploaded_file, save_dir, filename_prefix, session_id):
    # Ensure directory exists
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    
    # Secure filename
    filename = secure_filename(uploaded_file.filename)
    ext = filename.rsplit('.', 1)[1].lower()
    save_path = os.path.join(save_dir, f"{filename_prefix}_{filename}")
    
    try:
        uploaded_file.save(save_path)
        return save_path
    except Exception as e:
        print(f"Error saving file: {e}")
        return None

def load_temp_config(key, session_id):
    config_path = os.path.join("public", "temp_config", f"{session_id}_temp_config.json")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        if key == "all":
            return config
        if key == "data":
            return config.get("data")
        # Look inside 'data' first
        return config.get("data", {}).get(key)
    except Exception as e:
        print(f"[load_temp_config] Error: {e}")
        return None


def save_temp_config(key, value, session_id):
    """
    Save a key-value pair into the temp configuration for a session.
    If key == "all", merge the provided dictionary into the existing 'data' instead of replacing it.
    """
    config_path = os.path.join("public", "temp_config", f"{session_id}_temp_config.json")

    # Load existing config, or create base structure
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            file_content = json.load(f)
    else:
        file_content = {"data": {}, "Last modified": datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

    if "data" not in file_content or not isinstance(file_content["data"], dict):
        file_content["data"] = {}

    # Merge instead of replace
    if key == "all":
        if not isinstance(value, dict):
            raise ValueError("When key='all', value must be a dict to merge.")
        # Update only the keys in value, keep existing ones
        file_content["data"].update(value)
    else:
        file_content["data"][key] = value

    # Update timestamp
    file_content["Last modified"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Write back
    try:
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(file_content, f, indent=2)
    except Exception as e:
        print(f"[save_temp_config] Error saving config: {e}")
        raise

def get_or_create_socket_entry(sid):
    if sid not in sockets_registry:
        sockets_registry[sid] = {}
    return sockets_registry[sid]
