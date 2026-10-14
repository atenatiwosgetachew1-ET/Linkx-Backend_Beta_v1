#!/usr/bin/env python3
"""Quick verification of CDR session configuration loaded from PostgreSQL."""

import os
import sys

for env_path in [
    "/opt/linkx-backend-api/.env",
    "/opt/linkx-worker/.env",
    "/opt/linkx-control-data/.env",
    ".env",
]:
    if os.path.isfile(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip("'\"")
                    if k and k not in os.environ:
                        os.environ[k] = v
        break

sys.path.insert(0, "/opt/linkx-backend-api/src")
sys.path.insert(0, "/opt/linkx-worker/src")

try:
    from session_config_store import get_user_config

    cfg = get_user_config(1)
    print("========================================")
    print("LinkX CDR Live Configuration Check")
    print("========================================")
    print("Active Rule :", cfg.get("active_rule"))
    print("Source Col  :", cfg.get("default_source_col"))
    print("Target Col  :", cfg.get("default_target_col"))
    print("Database    :", cfg.get("active_storage_database"))
    print("Tables      :", cfg.get("active_storage_tables"))
    print("========================================")

    if cfg.get("active_rule") == ["call data records"]:
        print("PASS: Verified fresh CDR session configuration.")
    else:
        print("WARN: Active rule is not Call Data Records:", cfg.get("active_rule"))
except Exception as exc:
    print(f"FAIL: Could not query session config: {exc}")
    sys.exit(1)
