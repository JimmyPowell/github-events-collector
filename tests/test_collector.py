import gzip
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from collector import (
    BoundedDedupSet,
    HourlyArchiveWriter,
    TokenPool,
    obfuscate_emails,
)


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
        self.assertTrue(d.add_if_new("4"))  # evicts "1"
        self.assertEqual(len(d), 3)
        self.assertTrue(d.add_if_new("1"))

    def test_token_pool_rotation_and_backoff(self) -> None:
        pool = TokenPool(["tokA", "tokB"])
        s1, w1 = pool.acquire()
        s2, w2 = pool.acquire()
        self.assertEqual(w1, 0.0)
        self.assertEqual(w2, 0.0)
        self.assertNotEqual(s1.token, s2.token)

        # Exhaust tokA
        pool.update_from_headers(s1, {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "9999999999"})
        s3, w3 = pool.acquire()
        self.assertEqual(s3.token, s2.token)
        self.assertEqual(w3, 0.0)

    def test_hourly_rotation_and_gzip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            writer = HourlyArchiveWriter(data_dir=data_dir, retention_days=7, max_disk_usage_percent=99)
            t1 = datetime(2026, 11, 26, 6, 59, 0, tzinfo=timezone.utc)
            t2 = datetime(2026, 11, 26, 7, 0, 5, tzinfo=timezone.utc)

            writer.write_events([{"id": "100", "type": "PushEvent"}], now=t1)
            writer.write_events([{"id": "101", "type": "WatchEvent"}], now=t2)
            writer.close()

            gz_file = data_dir / "2026-11-26-06.jsonl.gz"
            cur_file = data_dir / "2026-11-26-07.jsonl"
            self.assertTrue(gz_file.exists())
            self.assertFalse((data_dir / "2026-11-26-06.jsonl").exists())
            self.assertTrue(cur_file.exists())

            with gzip.open(gz_file, "rt", encoding="utf-8") as f:
                lines = [json.loads(line) for line in f]
            self.assertEqual(lines, [{"id": "100", "type": "PushEvent"}])


if __name__ == "__main__":
    unittest.main()
