#!/bin/bash
echo "1. Initializing fresh session..."
RESPONSE=$(curl -s -X POST http://127.0.0.1:8000/init -H "Content-Type: application/json" -d '{}')
SESSION_ID=$(echo $RESPONSE | grep -o '"results":[^,]*' | awk -F':' '{print $2}' | tr -d ' "}')
echo "Session ID: $SESSION_ID"

echo "2. Saving custom config to the session (Setting theme to 'test_theme_123')..."
curl -s -X POST http://127.0.0.1:8000/configuration -H "Content-Type: application/json" -d "{
  \"id\": \"save\",
  \"session_id\": \"$SESSION_ID\",
  \"theme\": \"test_theme_123\"
}" > /dev/null

echo "3. Initializing a brand NEW session (simulating old session rotation)..."
RESPONSE2=$(curl -s -X POST http://127.0.0.1:8000/init -H "Content-Type: application/json" -d '{}')
NEW_SESSION_ID=$(echo $RESPONSE2 | grep -o '"results":[^,]*' | awk -F':' '{print $2}' | tr -d ' "}')
echo "New Session ID: $NEW_SESSION_ID"

echo "4. Checking if the new session inherited 'test_theme_123'..."
THEME=$(echo $RESPONSE2 | grep -o '"theme":[^,]*' | awk -F':' '{print $2}' | tr -d ' "}')
echo "Inherited theme: $THEME"
if [ "$THEME" = "test_theme_123" ]; then
    echo "SUCCESS: The configuration was successfully persisted!"
else
    echo "FAILED: The configuration was not persisted."
fi
