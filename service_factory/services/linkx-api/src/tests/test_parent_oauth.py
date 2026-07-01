import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from flask import Flask

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from auth.routes import auth_api, _map_parent_roles_to_linkx  # noqa: E402


class ParentOAuthExchangeTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.secret_key = "test-secret"
        self.app.register_blueprint(auth_api, url_prefix="/auth")
        self.client = self.app.test_client()

    def test_exchange_uses_userinfo_and_issues_linkx_token(self):
        token_data = {
            "access_token": "parent-access-token",
            "refresh_token": "parent-refresh-token",
            "token_type": "Bearer",
            "expires_in": 3600,
            "scope": "openid profile roles permissions",
        }
        userinfo = {
            "sub": "550e8400-e29b-41d4-a716-446655440000",
            "username": "analyst.one",
            "full_name": "Analyst One",
            "role": "ANALYST",
            "is_active": True,
            "permissions": ["InvestigationRead", "InvestigationCreate"],
            "entity_id": "entity-1",
        }
        user = {
            "id": 7,
            "actor_type": "user",
            "username": "parent:550e8400-e29b-41d4-a716-446655440000",
            "display_name": "Analyst One",
            "roles": ["analyst"],
            "permissions": ["analysis:run"],
        }

        with patch("auth.routes.exchange_authorization_code", return_value=token_data) as exchange_mock, \
             patch("auth.routes.fetch_userinfo", return_value=userinfo) as userinfo_mock, \
             patch("auth.routes.upsert_external_user", return_value=user) as upsert_mock, \
             patch("auth.routes.upsert_parent_oauth_session") as session_mock, \
             patch("auth.routes.create_access_token", return_value="linkx-token"), \
             patch("auth.routes.public_actor", return_value={"id": 7, "roles": ["analyst"]}), \
             patch("auth.routes.record_security_event"):
            response = self.client.post(
                "/auth/exchange",
                json={
                    "code": "auth-code-123",
                    "code_verifier": "v" * 43,
                    "redirect_uri": "http://linkx.example/auth/callback",
                },
            )

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertEqual(body["token"], "linkx-token")
        self.assertEqual(body["access_token"], "linkx-token")
        self.assertEqual(body["parent"]["mapped_roles"], ["analyst"])
        exchange_mock.assert_called_once_with(
            "auth-code-123",
            "v" * 43,
            redirect_uri="http://linkx.example/auth/callback",
        )
        userinfo_mock.assert_called_once_with("parent-access-token")
        upsert_mock.assert_called_once_with(
            "parent:550e8400-e29b-41d4-a716-446655440000",
            display_name="Analyst One",
            parent_roles=["analyst"],
        )
        session_mock.assert_called_once()

    def test_parent_role_mapping_is_generic_and_deterministic(self):
        self.assertEqual(_map_parent_roles_to_linkx(["HIGHER_OFFICIAL"], []), ["admin"])
        self.assertEqual(_map_parent_roles_to_linkx(["DIRECTOR"], []), ["manager"])
        self.assertEqual(_map_parent_roles_to_linkx(["ANALYST"], []), ["analyst"])
        self.assertEqual(_map_parent_roles_to_linkx(["DATA_ENCODER"], []), ["viewer"])
        self.assertEqual(_map_parent_roles_to_linkx(["RECEIVING_OFFICER"], []), [])
        self.assertEqual(_map_parent_roles_to_linkx([], ["InvestigationCreate"]), ["analyst"])
        self.assertEqual(_map_parent_roles_to_linkx([], ["InvestigationRead"]), ["viewer"])


if __name__ == "__main__":
    unittest.main()
