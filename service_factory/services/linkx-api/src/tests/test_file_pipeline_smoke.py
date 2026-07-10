import io
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch


class FilePipelineSmokeTest(unittest.TestCase):
    def setUp(self):
        self.project_cwd = os.getcwd()
        self.temp_dir = tempfile.mkdtemp(prefix="linkx-pipeline-")
        os.chdir(self.temp_dir)

    def tearDown(self):
        os.chdir(self.project_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_upload_then_create_dataframe_enqueues_worker_job(self):
        import main

        client = main.app.test_client()
        init_response = client.post("/init", json={})
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

        job = {"job_id": "df-job-1", "status": "queued", "queue": "dataframe", "job_type": "create_DF", "session_id": session_id}
        with patch("main._async_worker_jobs_enabled", return_value=True):
            with patch("main.enqueue_worker_job", return_value=job) as enqueue_mock:
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
        self.assertEqual(create_response.status_code, 202)

        payload = create_response.get_json()
        self.assertEqual(payload["message"], "success")
        self.assertEqual(payload["job_id"], "df-job-1")
        self.assertEqual(payload["results"]["poll_url"], "/jobs/df-job-1")
        enqueue_mock.assert_called_once()
        self.assertEqual(enqueue_mock.call_args.args[:2], ("dataframe", "create_DF"))



if __name__ == "__main__":
    unittest.main()
