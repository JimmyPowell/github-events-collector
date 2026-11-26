import gzip
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from collector import (
    BoundedDedupSet,
    HourlyArchiveWriter,
    TokenPool,
    obfuscate_emails,
)


class FakeUploader:
    def __init__(self, fail_times: int = 0) -> None:
        self.fail_times = fail_times
        self.calls: List[str] = []

    def build_object_name(self, filename: str) -> str:
        return f"raw/{filename}"

    def gcs_uri(self, filename: str) -> str:
        return f"gs://test-bucket/raw/{filename}"

    def upload_file(self, local_path: Path) -> str:
        self.calls.append(local_path.name)
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("simulated network error")
        return self.gcs_uri(local_path.name)


class CollectorTests(unittest.TestCase):
    def test_obfuscate_emails(self) -> None:
        event = {
            "id": "1",
            "payload": {
                "commits": [
                    {"author": {"email": "alice@example.com", "name": "Alice"}},
                    {"author": {"email": "", "name": "Empty"}},
                ]
            },
        }
        obfuscate_emails(event)
        obf = event["payload"]["commits"][0]["author"]["email"]
        self.assertTrue(obf.endswith("@example.com"))
        self.assertNotEqual(obf, "alice@example.com")
        self.assertEqual(len(obf.split("@")[0]), 40)
        self.assertEqual(event["payload"]["commits"][1]["author"]["email"], "")

    def test_dedup_bounded(self) -> None:
        d = BoundedDedupSet(max_size=3)
        self.assertTrue(d.add_if_new("1"))
        self.assertTrue(d.add_if_new("2"))
        self.assertTrue(d.add_if_new("3"))
        self.assertFalse(d.add_if_new("2"))
        self.assertTrue(d.add_if_new("4"))
        self.assertEqual(len(d), 3)
        self.assertTrue(d.add_if_new("1"))

    def test_token_pool_rotation_and_backoff(self) -> None:
        pool = TokenPool(["tokA", "tokB"])
        s1, w1 = pool.acquire()
        s2, w2 = pool.acquire()
        self.assertEqual(w1, 0.0)
        self.assertEqual(w2, 0.0)
        self.assertNotEqual(s1.token, s2.token)

        pool.update_from_headers(s1, {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "9999999999"})
        s3, w3 = pool.acquire()
        self.assertEqual(s3.token, s2.token)
        self.assertEqual(w3, 0.0)

    def test_hourly_rotation_gzip_and_upload_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            uploader = FakeUploader(fail_times=0)
            writer = HourlyArchiveWriter(
                data_dir=data_dir,
                retention_days=7,
                max_disk_usage_percent=99,
                uploader=uploader,
            )
            t1 = datetime(2026, 11, 26, 6, 59, 0, tzinfo=timezone.utc)
            t2 = datetime(2026, 11, 26, 7, 0, 5, tzinfo=timezone.utc)

            writer.write_events([{"id": "100", "type": "PushEvent"}], now=t1)
            writer.write_events([{"id": "101", "type": "WatchEvent"}], now=t2)
            writer.close()

            gz_file = data_dir / "2026-11-26-06.jsonl.gz"
            cur_file = data_dir / "2026-11-26-07.jsonl"
            status_file = data_dir / "upload_status.json"
            self.assertTrue(gz_file.exists())
            self.assertFalse((data_dir / "2026-11-26-06.jsonl").exists())
            self.assertTrue(cur_file.exists())
            self.assertTrue(status_file.exists())

            with gzip.open(gz_file, "rt", encoding="utf-8") as f:
                lines = [json.loads(line) for line in f]
            self.assertEqual(lines, [{"id": "100", "type": "PushEvent"}])

            status = json.loads(status_file.read_text(encoding="utf-8"))
            entry = status["2026-11-26-06.jsonl.gz"]
            self.assertEqual(entry["status"], "uploaded")
            self.assertEqual(entry["gcs_uri"], "gs://test-bucket/raw/2026-11-26-06.jsonl.gz")
            self.assertIsNone(entry["last_error"])

    def test_upload_failure_and_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            uploader = FakeUploader(fail_times=1)
            writer = HourlyArchiveWriter(
                data_dir=data_dir,
                retention_days=7,
                max_disk_usage_percent=99,
                uploader=uploader,
            )
            t1 = datetime(2026, 11, 26, 6, 59, 0, tzinfo=timezone.utc)
            t2 = datetime(2026, 11, 26, 7, 0, 5, tzinfo=timezone.utc)

            writer.write_events([{"id": "100", "type": "PushEvent"}], now=t1)
            writer.write_events([{"id": "101", "type": "WatchEvent"}], now=t2)
            writer.close()

            rec = writer.status_store.get("2026-11-26-06.jsonl.gz")
            self.assertIsNotNone(rec)
            self.assertEqual(rec["status"], "failed")
            self.assertIn("simulated network error", rec["last_error"])

            # Retry should now succeed
            retried = writer.retry_pending_uploads()
            self.assertEqual(retried, 1)
            rec2 = writer.status_store.get("2026-11-26-06.jsonl.gz")
            self.assertEqual(rec2["status"], "uploaded")
            self.assertEqual(rec2["attempts"], 2)


if __name__ == "__main__":
    unittest.main()
