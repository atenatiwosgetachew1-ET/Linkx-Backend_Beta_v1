import os
import sys

file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/test_phase2_architecture.py'

with open(file_path, 'r') as f:
    content = f.read()

mock_code = """
import os
import sys
from unittest.mock import MagicMock

# Mock third-party dependencies that aren't in the global environment
sys.modules['flask_socketio'] = MagicMock()
sys.modules['py4j'] = MagicMock()
sys.modules['py4j.java_gateway'] = MagicMock()
"""

if "sys.modules['flask_socketio']" not in content:
    content = content.replace("import sys\n", mock_code)

with open(file_path, 'w') as f:
    f.write(content)

print("Fixed test script.")
