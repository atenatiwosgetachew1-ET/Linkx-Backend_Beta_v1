import uuid
import unittest
from unittest.mock import patch


class SSOExchangeTest(unittest.TestCase):
    def test_sso_exchange_maps_parent_roles_and_rejects_replay(self):
        import main

        client = main.app.test_client()
        code = f"code-{uuid.uuid4().hex}"
        payload = {
            "code": code,
            "state": "state-123",
            "client": "linkx_frontend",
            "redirect_uri": "https://linkx.example.com/path",
        }

        class ParentResponse:
            status_code = 200

            def json(self):
                return {
                    "valid": True,
                    "state": "state-123",
                    "user": {
                        "username": "sso.user@example.com",
                        "display_name": "SSO User",
                    },
                    "roles": ["team_leader"],
                }

        with patch.dict(
            "os.environ",
            {
                "LINKX_PARENT_SSO_EXCHANGE_URL": "https://parent.example.com/sso/exchange",
                "LINKX_PARENT_SSO_CLIENT_ID": "linkx_backend",
                "LINKX_PARENT_SSO_CLIENT_SECRET": "secret",
            },
            clear=False,
        ), patch("auth.routes.requests.post", return_value=ParentResponse()) as post_mock:
            response = client.post("/auth/sso/exchange", json=payload)
            self.assertEqual(response.status_code, 200)
            body = response.get_json()
            self.assertEqual(body["message"], "success!")
            self.assertIn("token", body)
            self.assertEqual(body["user"]["username"], "sso.user@example.com")
            self.assertIn("admin", body["user"]["roles"])
            self.assertIn("users:manage", body["user"]["permissions"])
            self.assertEqual(post_mock.call_count, 1)
            _, kwargs = post_mock.call_args
            self.assertEqual(kwargs["json"]["code"], code)
            self.assertEqual(kwargs["headers"]["X-Linkx-Client-Id"], "linkx_backend")

            replay = client.post("/auth/sso/exchange", json=payload)
            self.assertEqual(replay.status_code, 409)
            self.assertEqual(replay.get_json()["message"], "sso_code_already_used")
            self.assertEqual(post_mock.call_count, 1)


if __name__ == "__main__":
    unittest.main()
