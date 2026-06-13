import io
import os
import shutil
import tempfile
import unittest


class FilePipelineSmokeTest(unittest.TestCase):
    def setUp(self):
        self.project_cwd = os.getcwd()
        self.temp_dir = tempfile.mkdtemp(prefix="linkx-pipeline-")
        os.chdir(self.temp_dir)

    def tearDown(self):
        os.chdir(self.project_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_upload_then_create_dataframe_writes_session_parquet(self):
        import main

        client = main.app.test_client()
        login_response = client.post("/auth/login", json={"username": "admin", "password": "Admin@12345"})
        self.assertEqual(login_response.status_code, 200)
        token = login_response.get_json()["token"]
        init_response = client.post("/init", json={}, headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(init_response.status_code, 200)
        session_id = str(init_response.get_json()["results"])

        upload_response = client.post(
            "/upload_batch_files",
            data={
                "session_id": session_id,
                "file": (io.BytesIO(b"name,amount\nAlice,10\nBob,20\n"), "sample.csv"),
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(upload_response.status_code, 200)

        create_response = client.post(
            "/live_batch_files",
            json={
                "id": "create_DF",
                "session_id": session_id,
                "kind": "files",
                "type": "array",
                "value": ["sample.csv"],
            },
        )
        self.assertEqual(create_response.status_code, 200)

        payload = create_response.get_json()
        self.assertEqual(payload["message"], "success!")
        self.assertEqual(payload["results"]["num_rows"], 2)
        self.assertTrue(
            os.path.exists(f"public/temp_dfParts/merged_dfpart_{session_id}/merged_dfpart_{session_id}.parquet")
        )


if __name__ == "__main__":
    unittest.main()
