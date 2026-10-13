-- ==============================================================================
-- LinkX CDR Database Fresh Session Configuration Seed
-- Purges old session states and initializes user configurations with native CDR defaults
-- ==============================================================================

-- 1. Wipe existing session configs to clear any previous session state
TRUNCATE session_configs CASCADE;

-- 2. Seed clean CDR defaults into user_configs for all existing users
INSERT INTO user_configs (user_id, config, version, updated_at)
SELECT id, '{
  "active_rule": ["call data records"],
  "rule_names": ["call data records", "social media (tweeter)", "bank transactions"],
  "rule_file_names": ["call_data_records_rules", "social_media_(tweeter)_rules", "bank_transactions_rules"],
  "default_source_col": "CALLING_NO",
  "default_target_col": "CALLED_NO",
  "default_relationship": "CALLED_TO",
  "date_column": "START_TIME",
  "storage_path": "user/cdr/cleaned_partitioned",
  "storage_databases": ["cdrdb", "cdr_db"],
  "storage_tables": ["call_records", "sms_records"],
  "active_storage_database": "cdrdb",
  "active_storage_tables": ["call_records", "sms_records"],
  "search_columns_strict": [
    "call_id",
    "calling_no",
    "called_no",
    "imei",
    "imsi",
    "start_time",
    "duration",
    "location_id"
  ],
  "search_columns_fuzzy": [
    "caller_name",
    "callee_name",
    "location_name",
    "call_type",
    "city",
    "cell_tower_id"
  ],
  "fetch_columns": [
    "CALL_ID",
    "CALLING_NO",
    "CALLED_NO",
    "START_TIME",
    "DURATION",
    "LOCATION_ID",
    "CALL_TYPE",
    "IMEI",
    "IMSI",
    "CALL_STATUS"
  ],
  "column_mapping": {
    "CALLINGNUMBER": "CALLING_NO",
    "CALLEDNUMBER": "CALLED_NO",
    "STARTTIME": "START_TIME",
    "CALLDURATION": "DURATION",
    "CELLID": "LOCATION_ID",
    "LOCATIONID": "LOCATION_ID",
    "CALL_TIME": "START_TIME",
    "TIMESTAMP": "START_TIME",
    "SOURCE_PHONE": "CALLING_NO",
    "DEST_PHONE": "CALLED_NO"
  },
  "tools": ["neo4j"],
  "active_tool": "neo4j",
  "active_tool_protocol": "neo4j://172.21.13.67:7687",
  "active_tool_username": "neo4j",
  "active_tool_database": "",
  "active_tool_tables": [],
  "tool_protocol_port": "7687",
  "tool_web_port": "7474",
  "automation": "true",
  "remote": "false",
  "dataframes_limit": 1000000
}'::jsonb, 1, NOW()
FROM users
ON CONFLICT (user_id) DO UPDATE
SET config = EXCLUDED.config, version = user_configs.version + 1, updated_at = NOW();

-- 3. Verification query
SELECT user_id, config->'active_rule' AS active_rule, config->'default_source_col' AS source, config->'default_target_col' AS target FROM user_configs;
