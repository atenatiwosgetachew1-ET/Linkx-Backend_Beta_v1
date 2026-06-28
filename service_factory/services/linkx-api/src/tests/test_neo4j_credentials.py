import sys
import unittest
from pathlib import Path
from unittest.mock import patch


SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class _SecretStoreStub:
    MASKED_SECRET = "***"

    @staticmethod
    def decrypt_secret(value):
        return value

    @staticmethod
    def encrypt_secret(value):
        return value

    @staticmethod
    def is_sensitive_key(key):
        return "password" in str(key or "").lower()

    @staticmethod
    def should_store_secret(value):
        return value not in (None, "", "***")


sys.modules.setdefault('security.secret_store', _SecretStoreStub())


class FakeCursor:
    def __init__(self, rows):
        self._rows = list(rows)

    def execute(self, query, params):
        self.last_query = query
        self.last_params = params

    def fetchone(self):
        if self._rows:
            return self._rows.pop(0)
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self, rows):
        self._cursor = FakeCursor(rows)

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class Neo4jCredentialTests(unittest.TestCase):
    def test_resolve_neo4j_credentials_uses_managed_secret(self):
        from batch_manager.utils.neo4j_utils import resolve_neo4j_credentials

        with patch('batch_manager.utils.neo4j_utils.db_enabled', return_value=True),              patch('batch_manager.utils.neo4j_utils._connect', return_value=FakeConn([])),              patch('batch_manager.utils.neo4j_utils._load_managed_secret', return_value='resolved-secret'):
            resolved = resolve_neo4j_credentials({
                'url': 'neo4j://graph',
                'username': 'neo4j',
                'password': '***',
                'password_ref': 'secret-123',
            })

        self.assertEqual(resolved['password'], 'resolved-secret')
        self.assertEqual(resolved['_credential_source'], 'managed_secret')

    def test_load_session_config_preserves_nested_password_ref(self):
        import session_config_store

        base_config = {'tool_credentials': {'password': '***', 'password_ref': 'secret-123', 'username': 'neo4j'}}
        window_config = {'tool_credentials': {'password': '***'}}

        with patch('session_config_store.db_enabled', return_value=True),              patch('session_config_store.ensure_schema'),              patch('session_config_store._connect', return_value=FakeConn([(base_config,), (window_config,)])),              patch('session_config_store._resolve_config_secrets', side_effect=lambda value, cur: value):
            loaded = session_config_store.load_session_config('window_session', window_id='window')

        self.assertEqual(loaded['tool_credentials']['password'], '***')
        self.assertEqual(loaded['tool_credentials']['password_ref'], 'secret-123')
        self.assertEqual(loaded['tool_credentials']['username'], 'neo4j')

    def test_connect_to_tool_schema_accepts_session_id_and_password_ref(self):
        from security.payload_validation import COMMON_SCHEMAS, validate_payload

        payload = validate_payload({
            'tool_name': 'neo4j',
            'url': 'bolt://172.27.23.85:7687',
            'username': 'neo4j',
            'password': '***',
            'password_ref': 'secret-123',
            'database': '',
            'source_id': '1_196295',
            'session_id': '1_196295',
        }, COMMON_SCHEMAS['connect_to_tool'])

        self.assertEqual(payload['session_id'], '1_196295')
        self.assertEqual(payload['password_ref'], 'secret-123')
        self.assertEqual(payload['database'], '')

    def test_resolve_neo4j_credentials_rejects_masked_password_without_ref(self):
        from batch_manager.utils.neo4j_utils import Neo4jCredentialConfigError, resolve_neo4j_credentials

        with self.assertRaises(Neo4jCredentialConfigError) as exc:
            resolve_neo4j_credentials({
                'url': 'bolt://172.27.23.85:7687',
                'username': 'neo4j',
                'password': '***',
                'session_id': '1_196295',
            })

        self.assertIn('password_ref', str(exc.exception))

    def test_analyzer_does_not_fallback_when_payload_credentials_are_masked(self):
        import batch_manager.analyzing.analyzer as analyzer_module
        from batch_manager.utils.neo4j_utils import Neo4jCredentialConfigError

        payload = {
            'id': 'batch_data',
            'type': 'new',
            'tool': 'neo4j',
            'session_id': 'sess-1',
            'tool_credentials': {'url': 'neo4j://graph', 'username': 'neo4j', 'password': '***'},
            'log_file': 'test.log',
            'dataframe_dir': '/tmp/df.parquet',
        }

        with patch('batch_manager.analyzing.analyzer.load_file', return_value=[]),              patch('batch_manager.analyzing.analyzer.create_neo4j_driver', side_effect=Neo4jCredentialConfigError('masked')),              patch('batch_manager.analyzing.analyzer.tools') as tools_mock:
            result = analyzer_module.analyzer(payload)

        self.assertFalse(result)
        tools_mock.assert_not_called()


if __name__ == '__main__':
    unittest.main()
