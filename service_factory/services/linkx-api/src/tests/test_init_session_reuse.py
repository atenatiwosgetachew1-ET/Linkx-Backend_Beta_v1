import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


def _load_api_main():
    src_dir = Path(__file__).resolve().parents[1]
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))
    module_path = src_dir / "main.py"
    spec = importlib.util.spec_from_file_location("linkx_api_main", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class InitSessionReuseTest(unittest.TestCase):
    def test_reuses_latest_active_parent_session_when_fresh(self):
        main = _load_api_main()

        actor = {"actor_type": "user", "id": 7}
        session_info = {"session_id": "452162", "created_at": None, "last_seen_at": None}

        with patch.object(main, "get_actor_main_session_info", return_value=session_info), \
             patch.object(main, "bind_analysis_session_actor", return_value=True), \
             patch.object(main, "load_temp_config", return_value={"trusted_entities": [{"ACCOUNTNO": "ACC1"}]}), \
             patch.object(main, "_normalize_configuration", side_effect=lambda value: value), \
             patch.object(main, "_session_age_seconds", return_value=60), \
             patch.object(main, "_session_rotation_seconds", return_value=3600):
            result = main._load_reusable_parent_session(actor, None)

        self.assertEqual(result["session_id"], "452162")
        self.assertTrue(result["reused_existing_session"])
        self.assertFalse(result["session_rotated"])
        self.assertEqual(result["configuration"]["trusted_entities"][0]["ACCOUNTNO"], "ACC1")

    def test_rotates_parent_session_when_active_session_is_too_old(self):
        main = _load_api_main()

        actor = {"actor_type": "user", "id": 7}
        session_info = {"session_id": "452162", "created_at": object(), "last_seen_at": None}

        with patch.object(main, "get_actor_main_session_info", return_value=session_info), \
             patch.object(main, "_session_age_seconds", return_value=7200), \
             patch.object(main, "_session_rotation_seconds", return_value=3600), \
             patch.object(main, "_new_parent_session_id", return_value="778899"):
            result = main._load_reusable_parent_session(actor, None)

        self.assertEqual(result["session_id"], "778899")
        self.assertFalse(result["reused_existing_session"])
        self.assertTrue(result["session_rotated"])
        self.assertEqual(result["rotated_from_session"], "452162")
        self.assertIsNone(result["configuration"])


if __name__ == "__main__":
    unittest.main()
