import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
API_SRC = REPO_ROOT / "service_factory/services/linkx-api/src"
GRAPH_SRC = REPO_ROOT / "service_factory/services/linkx-graph-maintenance/src"

if str(API_SRC) not in sys.path:
    sys.path.insert(0, str(API_SRC))


class SecurityRegressionTests(unittest.TestCase):
    def read(self, relative):
        return (REPO_ROOT / relative).read_text()

    def test_redaction_masks_nested_sensitive_values(self):
        from security.redaction import redact_value

        payload = {
            "username": "analyst",
            "password": "super-secret",
            "nested": {
                "Authorization": "Bearer token",
                "items": [{"client_secret": "client-secret"}],
            },
        }

        redacted = redact_value(payload)

        self.assertEqual(redacted["username"], "analyst")
        self.assertEqual(redacted["password"], "***")
        self.assertEqual(redacted["nested"]["Authorization"], "***")
        self.assertEqual(redacted["nested"]["items"][0]["client_secret"], "***")
        self.assertNotIn("super-secret", repr(redacted))
        self.assertNotIn("client-secret", repr(redacted))
        self.assertNotIn("Bearer token", repr(redacted))

    def test_str_analyzer_payload_logging_stays_redacted(self):
        content = self.read("service_factory/services/linkx-api/src/api/STR_link_analysis.py")

        self.assertIn("redact_value", content)
        self.assertNotIn('print(f"STR link analysis {step_name} analyzer payload:", payload)', content)

    def test_cleanup_neo4j_credential_logging_stays_metadata_only(self):
        content = self.read("service_factory/services/linkx-graph-maintenance/src/linkx_cleanup/tasks.py")

        self.assertIn("Neo4j credential source", content)
        self.assertNotIn("creds=", content)

    def test_token_revocation_markers_remain_present(self):
        tokens = self.read("service_factory/services/linkx-api/src/auth/tokens.py")
        routes = self.read("service_factory/services/linkx-api/src/auth/routes.py")
        repository = self.read("service_factory/services/linkx-api/src/auth/repository.py")

        self.assertIn('"jti"', tokens)
        self.assertIn("is_token_jti_revoked", tokens)
        self.assertIn("_revoke_current_bearer_token", routes)
        self.assertIn("token_invalidated", routes)
        self.assertIn("CREATE TABLE IF NOT EXISTS token_revocations", repository)
        self.assertIn("auth.token_revoke", routes)

    def test_ai_permissions_stay_granular(self):
        ai_service = self.read("service_factory/services/linkx-api/src/api/ai_service.py")
        repository = self.read("service_factory/services/linkx-api/src/auth/repository.py")

        for permission in (
            "ai:session:read",
            "ai:artifact:read",
            "ai:cleanup:read",
            "ai:graph:metadata:read",
        ):
            self.assertIn(permission, ai_service)
            self.assertIn(permission, repository)

    def test_offhost_backup_sync_keeps_ssh_key_separate_from_rsync_opts(self):
        content = self.read("service_factory/scripts/sync-backups-offhost.sh")

        self.assertIn("LINKX_BACKUP_SSH_KEY", content)
        self.assertIn("LINKX_BACKUP_SSH_OPTS", content)
        self.assertIn('rsync_cmd+=(-e "ssh -i ${SSH_KEY} ${SSH_OPTS}")', content)

    def test_vulnerable_nltk_textblob_path_is_not_pinned(self):
        requirement_files = (
            "requirements.txt",
            "service_factory/requirements.txt",
            "service_factory/services/linkx-api/src/requirements.txt",
            "service_factory/services/linkx-worker/src/requirements.txt",
            "service_factory/services/linkx-graph-maintenance/src/requirements.txt",
        )
        analysis_files = (
            "batch_manager/analyzing/LA_rules_script.py",
            "service_factory/batch_manager/analyzing/LA_rules_script.py",
            "service_factory/services/linkx-api/src/batch_manager/analyzing/LA_rules_script.py",
            "service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py",
        )

        for relative in requirement_files:
            content = self.read(relative)
            self.assertNotIn("nltk==", content)
            self.assertNotIn("textblob==", content)

        for relative in analysis_files:
            content = self.read(relative)
            self.assertNotIn("from textblob import TextBlob", content)
            self.assertNotIn("import nltk", content)


if __name__ == "__main__":
    unittest.main()
