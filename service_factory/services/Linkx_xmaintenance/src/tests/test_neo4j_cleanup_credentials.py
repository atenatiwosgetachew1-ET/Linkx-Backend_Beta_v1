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


sys.modules.setdefault('security.secret_store', _SecretStoreStub())


class CleanupNeo4jCredentialTests(unittest.TestCase):
    def test_resolve_neo4j_credentials_uses_password_ref(self):
        from batch_manager.utils.neo4j_utils import resolve_neo4j_credentials

        with patch('batch_manager.utils.neo4j_utils._load_managed_secret', return_value='resolved-secret'):
            resolved = resolve_neo4j_credentials({
                'neo4j_url': 'bolt://172.27.23.85:7687',
                'neo4j_username': 'neo4j',
                'neo4j_password_ref': 'secret-123',
                'neo4j_database': 'neo4j',
            })

        self.assertEqual(resolved['password'], 'resolved-secret')
        self.assertEqual(resolved['_credential_source'], 'managed_secret')

    def test_cleanup_neo4j_session_returns_invalid_credentials_without_retrying_auth(self):
        from batch_manager.utils.neo4j_utils import Neo4jCredentialConfigError
        from linkx_xcleanup.tasks import cleanup_neo4j_session

        with patch('linkx_xcleanup.tasks._create_neo4j_driver_with_retry', side_effect=Neo4jCredentialConfigError('Neo4j password is masked but no password_ref is available')) as retry_mock:
            result = cleanup_neo4j_session('1_196295', payload={
                'neo4j_url': 'bolt://172.27.23.85:7687',
                'neo4j_username': 'neo4j',
                'neo4j_password': '***',
            })

        self.assertEqual(retry_mock.call_count, 1)
        self.assertEqual(result['neo4j'], 'invalid_credentials')
        self.assertIn('password_ref', result['error'])


if __name__ == '__main__':
    unittest.main()
