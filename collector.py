#!/usr/bin/env python3
"""
GitHub Global /events Collector

Inspired by GH Archive (2025 UTC-rollover fix) and OpenDigger's multi-token pool:
- Polls https://api.github.com/events?per_page=100
- Multi-token rotation with automatic X-RateLimit-* backoff
- Conditional requests via ETag / If-None-Match (304 consumes 0 quota)
- Bounded LRU event ID deduplication (constant memory footprint)
- Hourly UTC NDJSON files (YYYY-MM-DD-HH.jsonl) auto-compressed to .jsonl.gz
- Automatic upload of completed .jsonl.gz archives to Google Cloud Storage (GCS)
- Persistent upload ledger (data/upload_status.json) tracking success/failure + auto-retry
- Disk usage & retention guard to keep small VPS disks safe
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import os
import shutil
import signal
import sys
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Set, TextIO

import requests

EVENTS_API_URL = "https://api.github.com/events"
USER_AGENT = "github-events-collector/1.1"


def load_dotenv(env_path: Path) -> None:
    """Minimal .env loader without external dependencies."""
    if not env_path.is_file():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def file_md5_hex(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def obfuscate_emails(obj: Any) -> Any:
    """Recursively SHA1-hash email local parts, matching GH Archive's obfuscate.rb."""
    if isinstance(obj, dict):
        email = obj.get("email")
        if isinstance(email, str) and email:
            if "@" in email:
                local, host = email.split("@", 1)
                hashed = hashlib.sha1(local.encode("utf-8")).hexdigest()
                obj["email"] = f"{hashed}@{host}"
            else:
                obj["email"] = hashlib.sha1(email.encode("utf-8")).hexdigest()
        for v in obj.values():
            if isinstance(v, (dict, list)):
                obfuscate_emails(v)
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)):
                obfuscate_emails(item)
    return obj


@dataclass
class TokenState:
    token: Optional[str]
    label: str
    remaining: int = 5000
    reset_at: float = 0.0

    def is_available(self, now: float) -> bool:
        return self.remaining > 1 or now >= self.reset_at


class TokenPool:
    """Manages one or more GitHub tokens (or anonymous fallback) with rate-limit awareness."""

    def __init__(self, tokens: List[str]) -> None:
        cleaned = [t.strip() for t in tokens if t.strip()]
        if cleaned:
            self.states = [
                TokenState(token=t, label=f"token#{i + 1}", remaining=5000)
                for i, t in enumerate(cleaned)
            ]
        else:
            self.states = [TokenState(token=None, label="anonymous", remaining=60)]
        self._cursor = 0

    @property
    def is_anonymous(self) -> bool:
        return len(self.states) == 1 and self.states[0].token is None

    def acquire(self) -> tuple[TokenState, float]:
        """Return (TokenState, wait_seconds_before_use)."""
        now = time.time()
        n = len(self.states)
        for offset in range(n):
            idx = (self._cursor + offset) % n
            state = self.states[idx]
            if state.is_available(now):
                self._cursor = (idx + 1) % n
                return state, 0.0

        earliest = min(self.states, key=lambda s: s.reset_at)
        wait_s = max(1.0, earliest.reset_at - now + 1.0)
        return earliest, wait_s

    @staticmethod
    def update_from_headers(state: TokenState, headers: Dict[str, str]) -> None:
        rem = headers.get("X-RateLimit-Remaining")
        rst = headers.get("X-RateLimit-Reset")
        if rem is not None:
            try:
                state.remaining = int(rem)
            except ValueError:
                pass
        if rst is not None:
            try:
                state.reset_at = float(rst)
            except ValueError:
                pass


class BoundedDedupSet:
    """Constant-memory LRU set for deduplicating GitHub event IDs."""

    def __init__(self, max_size: int = 20000) -> None:
        self.max_size = max_size
        self._store: OrderedDict[str, None] = OrderedDict()

    def add_if_new(self, event_id: str) -> bool:
        if event_id in self._store:
            self._store.move_to_end(event_id)
            return False
        self._store[event_id] = None
        if len(self._store) > self.max_size:
            self._store.popitem(last=False)
        return True

    def __len__(self) -> int:
        return len(self._store)


class UploadStatusStore:
    """
    Thread-safe JSON ledger persisted at <DATA_DIR>/upload_status.json.
    Tracks whether each compressed hourly archive was successfully uploaded to GCS.
    """

    def __init__(self, status_path: Path) -> None:
        self.status_path = status_path
        self._lock = threading.Lock()
        self._records: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        if not self.status_path.is_file():
            return
        try:
            data = json.loads(self.status_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                self._records = data
        except Exception:
            self._records = {}

    def _save_locked(self) -> None:
        self.status_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.status_path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(self._records, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        tmp.replace(self.status_path)

    def get(self, filename: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            rec = self._records.get(filename)
            return dict(rec) if rec is not None else None

    def is_uploaded(self, filename: str, current_md5: Optional[str] = None) -> bool:
        with self._lock:
            rec = self._records.get(filename)
            if not rec or rec.get("status") != "uploaded":
                return False
            if current_md5 and rec.get("md5") and rec.get("md5") != current_md5:
                return False
            return True

    def record_success(self, filename: str, gcs_uri: str, size_bytes: int, md5: str) -> None:
        with self._lock:
            prev = self._records.get(filename, {})
            now_iso = utc_now_iso()
            self._records[filename] = {
                "status": "uploaded",
                "gcs_uri": gcs_uri,
                "size_bytes": size_bytes,
                "md5": md5,
                "attempts": int(prev.get("attempts", 0)) + 1,
                "uploaded_at": now_iso,
                "last_attempt_at": now_iso,
                "last_error": None,
            }
            self._save_locked()

    def record_failure(self, filename: str, gcs_uri: str, size_bytes: int, md5: str, error: str) -> None:
        with self._lock:
            prev = self._records.get(filename, {})
            self._records[filename] = {
                "status": "failed",
                "gcs_uri": gcs_uri,
                "size_bytes": size_bytes,
                "md5": md5,
                "attempts": int(prev.get("attempts", 0)) + 1,
                "uploaded_at": prev.get("uploaded_at"),
                "last_attempt_at": utc_now_iso(),
                "last_error": error[:500],
            }
            self._save_locked()


class ArchiveUploader(Protocol):
    def build_object_name(self, filename: str) -> str: ...
    def upload_file(self, local_path: Path) -> str: ...


class GCSUploader:
    """Uploads compressed hourly archives to Google Cloud Storage with retries."""

    def __init__(
        self,
        bucket_name: str,
        prefix: str = "raw",
        credentials_file: Optional[str] = None,
        max_retries: int = 3,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.bucket_name = bucket_name.replace("gs://", "").strip("/")
        self.prefix = prefix.strip("/")
        self.credentials_file = credentials_file
        self.max_retries = max_retries
        self.logger = logger or logging.getLogger(__name__)
        self._client = None
        self._bucket = None

    def _get_bucket(self) -> Any:
        if self._bucket is None:
            from google.cloud import storage  # Lazy import so GCS stays optional

            if self.credentials_file:
                self._client = storage.Client.from_service_account_json(self.credentials_file)
            else:
                self._client = storage.Client()
            self._bucket = self._client.bucket(self.bucket_name)
        return self._bucket

    def build_object_name(self, filename: str) -> str:
        """Partition path as <prefix>/YYYY/MM/DD/YYYY-MM-DD-HH.jsonl.gz when possible."""
        stem = filename.replace(".jsonl.gz", "")
        parts = stem.split("-")
        if len(parts) == 4 and len(parts[0]) == 4 and len(parts[1]) == 2 and len(parts[2]) == 2:
            subpath = f"{parts[0]}/{parts[1]}/{parts[2]}/{filename}"
        else:
            subpath = filename
        return f"{self.prefix}/{subpath}" if self.prefix else subpath

    def gcs_uri(self, filename: str) -> str:
        return f"gs://{self.bucket_name}/{self.build_object_name(filename)}"

    def upload_file(self, local_path: Path) -> str:
        object_name = self.build_object_name(local_path.name)
        uri = f"gs://{self.bucket_name}/{object_name}"
        bucket = self._get_bucket()

        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                blob = bucket.blob(object_name)
                blob.content_type = "application/gzip"
                blob.upload_from_filename(str(local_path), timeout=120)
                return uri
            except Exception as exc:
                last_exc = exc
                if attempt < self.max_retries:
                    backoff = 2.0 ** attempt
                    self.logger.warning(
                        "GCS upload attempt %d/%d failed for %s: %s (retrying in %.0fs)",
                        attempt,
                        self.max_retries,
                        local_path.name,
                        exc,
                        backoff,
                    )
                    time.sleep(backoff)
        assert last_exc is not None
        raise last_exc

    def self_test(self) -> str:
        """Verify write + delete permissions on the configured bucket."""
        bucket = self._get_bucket()
        test_obj = f"{self.prefix}/_healthcheck.txt" if self.prefix else "_healthcheck.txt"
        blob = bucket.blob(test_obj)
        blob.upload_from_string(f"ok {utc_now_iso()}\n", content_type="text/plain", timeout=30)
        blob.delete(timeout=30)
        return f"gs://{self.bucket_name}/{test_obj}"


class HourlyArchiveWriter:
    """
    Writes NDJSON events into UTC hourly files (YYYY-MM-DD-HH.jsonl) based on
    collection time, compresses past-hour files into .jsonl.gz, optionally uploads
    them to GCS, and records every archive's upload status in upload_status.json.
    """

    def __init__(
        self,
        data_dir: Path,
        retention_days: int = 7,
        max_disk_usage_percent: int = 80,
        uploader: Optional[ArchiveUploader] = None,
        delete_local_after_upload: bool = False,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.retention_days = retention_days
        self.max_disk_usage_percent = max_disk_usage_percent
        self.uploader = uploader
        self.delete_local_after_upload = delete_local_after_upload
        self.logger = logger or logging.getLogger(__name__)
        self.status_store = UploadStatusStore(self.data_dir / "upload_status.json")
        self._current_hour: Optional[str] = None
        self._file: Optional[TextIO] = None
        self._bg_threads: List[threading.Thread] = []

    @staticmethod
    def hour_key(dt: Optional[datetime] = None) -> str:
        dt = dt or datetime.now(timezone.utc)
        return dt.strftime("%Y-%m-%d-%H")

    def _open_for_hour(self, hour_key: str) -> TextIO:
        if self._current_hour == hour_key and self._file is not None:
            return self._file

        old_path: Optional[Path] = None
        if self._file is not None:
            old_path = Path(self._file.name)
            self._file.flush()
            self._file.close()
            self._file = None

        target_path = self.data_dir / f"{hour_key}.jsonl"
        self._file = target_path.open("a", encoding="utf-8", buffering=1)
        self._current_hour = hour_key
        self.logger.info("Active archive file: %s", target_path)

        if old_path is not None and old_path.exists():
            self._schedule_finalize(old_path)

        # Also retry any previously failed uploads in background on rollover
        if self.uploader is not None:
            self._schedule_retry_pending()

        self.cleanup_old_archives()
        return self._file

    def write_events(self, events: List[Dict[str, Any]], now: Optional[datetime] = None) -> int:
        if not events:
            return 0
        key = self.hour_key(now)
        fh = self._open_for_hour(key)
        for ev in events:
            fh.write(json.dumps(ev, ensure_ascii=False, separators=(",", ":")) + "\n")
        fh.flush()
        return len(events)

    def compress_stale_files(self, now: Optional[datetime] = None) -> None:
        """Compress any leftover past-hour .jsonl files from previous runs and upload pending .jsonl.gz."""
        current_name = f"{self.hour_key(now)}.jsonl"
        for path in sorted(self.data_dir.glob("*.jsonl")):
            if path.name != current_name:
                self._finalize_file(path)
        if self.uploader is not None:
            self.retry_pending_uploads()

    def _schedule_finalize(self, path: Path) -> None:
        t = threading.Thread(target=self._finalize_file, args=(path,), daemon=False)
        t.start()
        self._bg_threads = [th for th in self._bg_threads if th.is_alive()]
        self._bg_threads.append(t)

    def _schedule_retry_pending(self) -> None:
        t = threading.Thread(target=self.retry_pending_uploads, daemon=False)
        t.start()
        self._bg_threads = [th for th in self._bg_threads if th.is_alive()]
        self._bg_threads.append(t)

    def _finalize_file(self, src: Path) -> None:
        gz_path = self._compress_file(src)
        if gz_path is not None and self.uploader is not None:
            self._upload_archive(gz_path)

    def _compress_file(self, src: Path) -> Optional[Path]:
        if not src.exists():
            return None
        gz_path = src.with_suffix(".jsonl.gz")
        tmp_path = src.with_suffix(".jsonl.gz.tmp")
        try:
            mode = "ab" if gz_path.exists() else "wb"
            target = gz_path if mode == "ab" else tmp_path
            with src.open("rb") as f_in, gzip.open(target, mode, compresslevel=6) as f_out:
                shutil.copyfileobj(f_in, f_out, length=1024 * 1024)
            if target == tmp_path:
                tmp_path.replace(gz_path)
            src.unlink()
            self.logger.info("Compressed archive: %s (%d bytes)", gz_path.name, gz_path.stat().st_size)
            return gz_path
        except Exception:
            self.logger.exception("Failed to compress %s", src)
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            return None

    def _upload_archive(self, gz_path: Path) -> bool:
        if self.uploader is None or not gz_path.exists():
            return False
        size_bytes = gz_path.stat().st_size
        md5 = file_md5_hex(gz_path)
        if self.status_store.is_uploaded(gz_path.name, current_md5=md5):
            return True

        expected_uri = (
            self.uploader.gcs_uri(gz_path.name)
            if hasattr(self.uploader, "gcs_uri")
            else self.uploader.build_object_name(gz_path.name)
        )
        try:
            uri = self.uploader.upload_file(gz_path)
            self.status_store.record_success(
                filename=gz_path.name,
                gcs_uri=uri,
                size_bytes=size_bytes,
                md5=md5,
            )
            self.logger.info("Uploaded archive to %s (%d bytes, md5=%s)", uri, size_bytes, md5)
            if self.delete_local_after_upload:
                gz_path.unlink(missing_ok=True)
                self.logger.info("Deleted local archive after upload: %s", gz_path.name)
            return True
        except Exception as exc:
            self.status_store.record_failure(
                filename=gz_path.name,
                gcs_uri=expected_uri,
                size_bytes=size_bytes,
                md5=md5,
                error=str(exc),
            )
            self.logger.error("Failed to upload %s to %s: %s", gz_path.name, expected_uri, exc)
            return False

    def retry_pending_uploads(self) -> int:
        """Upload any local .jsonl.gz files that have not yet been marked uploaded."""
        if self.uploader is None:
            return 0
        uploaded_count = 0
        for gz_path in sorted(self.data_dir.glob("*.jsonl.gz")):
            if not self.status_store.is_uploaded(gz_path.name):
                if self._upload_archive(gz_path):
                    uploaded_count += 1
        return uploaded_count

    def cleanup_old_archives(self) -> None:
        """Enforce retention_days and max_disk_usage_percent on .jsonl.gz files."""
        gz_files = sorted(self.data_dir.glob("*.jsonl.gz"))
        if not gz_files:
            return

        if self.retention_days > 0:
            cutoff = datetime.now(timezone.utc) - timedelta(days=self.retention_days)
            cutoff_prefix = cutoff.strftime("%Y-%m-%d-%H")
            remaining_files: List[Path] = []
            for f in gz_files:
                stem = f.name.replace(".jsonl.gz", "")
                if stem < cutoff_prefix:
                    # Keep un-uploaded files if uploader is enabled and disk isn't full
                    if self.uploader is not None and not self.status_store.is_uploaded(f.name):
                        remaining_files.append(f)
                        continue
                    self.logger.info("Removing expired archive (>%dd): %s", self.retention_days, f.name)
                    f.unlink(missing_ok=True)
                else:
                    remaining_files.append(f)
            gz_files = remaining_files

        if self.max_disk_usage_percent > 0:
            # Prefer deleting already-uploaded files first if disk pressure occurs
            if self.uploader is not None:
                gz_files.sort(key=lambda p: (0 if self.status_store.is_uploaded(p.name) else 1, p.name))
            while gz_files:
                usage = shutil.disk_usage(self.data_dir)
                pct = (usage.used / usage.total) * 100.0
                if pct < self.max_disk_usage_percent:
                    break
                oldest = gz_files.pop(0)
                self.logger.warning(
                    "Disk usage %.1f%% >= %d%%; removing oldest archive: %s",
                    pct,
                    self.max_disk_usage_percent,
                    oldest.name,
                )
                oldest.unlink(missing_ok=True)

    def close(self) -> None:
        if self._file is not None:
            try:
                self._file.flush()
                self._file.close()
            finally:
                self._file = None
        for t in self._bg_threads:
            t.join(timeout=60)


@dataclass
class CollectorConfig:
    data_dir: Path = Path("data")
    tokens: List[str] = field(default_factory=list)
    poll_interval_seconds: float = 1.0
    per_page: int = 100
    obfuscate_email: bool = True
    event_types: Set[str] = field(default_factory=set)
    dedup_size: int = 20000
    retention_days: int = 7
    max_disk_usage_percent: int = 80
    request_timeout: float = 10.0
    gcs_bucket: str = ""
    gcs_prefix: str = "raw"
    gcs_credentials_file: str = ""
    delete_local_after_upload: bool = False

    @classmethod
    def from_env(cls) -> "CollectorConfig":
        raw_tokens = os.getenv("GITHUB_TOKENS", "") or os.getenv("GITHUB_TOKEN", "")
        tokens = [t.strip() for t in raw_tokens.split(",") if t.strip()]
        raw_types = os.getenv("EVENT_TYPES", "").strip()
        event_types = {t.strip() for t in raw_types.split(",") if t.strip()} if raw_types else set()

        default_interval = "1.0" if tokens else "60.0"
        return cls(
            data_dir=Path(os.getenv("DATA_DIR", "data")),
            tokens=tokens,
            poll_interval_seconds=float(os.getenv("POLL_INTERVAL_SECONDS", default_interval)),
            per_page=min(100, max(1, int(os.getenv("PER_PAGE", "100")))),
            obfuscate_email=os.getenv("OBFUSCATE_EMAIL", "true").lower() in ("1", "true", "yes"),
            event_types=event_types,
            dedup_size=int(os.getenv("DEDUP_SIZE", "20000")),
            retention_days=int(os.getenv("MAX_RETENTION_DAYS", "7")),
            max_disk_usage_percent=int(os.getenv("MAX_DISK_USAGE_PERCENT", "80")),
            request_timeout=float(os.getenv("REQUEST_TIMEOUT", "10.0")),
            gcs_bucket=os.getenv("GCS_BUCKET", "").strip(),
            gcs_prefix=os.getenv("GCS_PREFIX", "raw").strip(),
            gcs_credentials_file=(
                os.getenv("GCS_CREDENTIALS_FILE", "").strip()
                or os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
            ),
            delete_local_after_upload=os.getenv("DELETE_LOCAL_AFTER_UPLOAD", "false").lower()
            in ("1", "true", "yes"),
        )


class GitHubEventsCollector:
    def __init__(self, config: CollectorConfig, logger: Optional[logging.Logger] = None) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.token_pool = TokenPool(config.tokens)
        self.dedup = BoundedDedupSet(max_size=config.dedup_size)
        self.uploader: Optional[GCSUploader] = None
        if config.gcs_bucket:
            self.uploader = GCSUploader(
                bucket_name=config.gcs_bucket,
                prefix=config.gcs_prefix,
                credentials_file=config.gcs_credentials_file or None,
                logger=self.logger,
            )
        self.writer = HourlyArchiveWriter(
            data_dir=config.data_dir,
            retention_days=config.retention_days,
            max_disk_usage_percent=config.max_disk_usage_percent,
            uploader=self.uploader,
            delete_local_after_upload=config.delete_local_after_upload,
            logger=self.logger,
        )
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "User-Agent": USER_AGENT,
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )
        self._etag: Optional[str] = None
        self._stop_event = threading.Event()
        self._total_written = 0
        self._total_polls = 0
        self._total_304 = 0
        self._window_written = 0
        self._last_stats_log = time.time()

    def stop(self) -> None:
        self._stop_event.set()

    def poll_once(self) -> float:
        """Perform a single poll. Returns recommended sleep duration in seconds."""
        state, wait_s = self.token_pool.acquire()
        if wait_s > 0:
            self.logger.warning("Rate limit reached (%s). Sleeping %.1fs until reset.", state.label, wait_s)
            return min(wait_s, 60.0)

        headers: Dict[str, str] = {}
        if state.token:
            headers["Authorization"] = f"Bearer {state.token}"
        if self._etag:
            headers["If-None-Match"] = self._etag

        self._total_polls += 1
        try:
            resp = self.session.get(
                EVENTS_API_URL,
                params={"per_page": self.config.per_page},
                headers=headers,
                timeout=self.config.request_timeout,
            )
        except requests.RequestException as exc:
            self.logger.warning("HTTP request failed: %s", exc)
            return max(2.0, self.config.poll_interval_seconds)

        self.token_pool.update_from_headers(state, resp.headers)

        if resp.status_code == 304:
            self._total_304 += 1
            return self._compute_sleep(resp.headers, state)

        if resp.status_code in (403, 429):
            retry_after = resp.headers.get("Retry-After")
            wait_sec = float(retry_after) if retry_after and retry_after.isdigit() else 15.0
            self.logger.warning(
                "GitHub API %d (%s), remaining=%d. Backing off %.1fs.",
                resp.status_code,
                state.label,
                state.remaining,
                wait_sec,
            )
            return wait_sec

        if resp.status_code != 200:
            self.logger.warning("Unexpected status %d: %s", resp.status_code, resp.text[:200])
            return max(2.0, self.config.poll_interval_seconds)

        new_etag = resp.headers.get("ETag")
        if new_etag:
            self._etag = new_etag

        try:
            payload = resp.json()
        except ValueError as exc:
            self.logger.warning("Invalid JSON payload: %s", exc)
            return self.config.poll_interval_seconds

        if not isinstance(payload, list):
            self.logger.warning("Unexpected JSON structure (expected list)")
            return self.config.poll_interval_seconds

        new_events: List[Dict[str, Any]] = []
        for event in reversed(payload):
            if not isinstance(event, dict):
                continue
            event_id = str(event.get("id", ""))
            if not event_id or not self.dedup.add_if_new(event_id):
                continue
            if self.config.event_types and event.get("type") not in self.config.event_types:
                continue
            if self.config.obfuscate_email:
                obfuscate_emails(event)
            new_events.append(event)

        written = self.writer.write_events(new_events)
        self._total_written += written
        self._window_written += written

        if written >= self.config.per_page:
            self.logger.warning("Batch full (%d events); some events may have been missed.", written)

        self._maybe_log_stats(state)
        return self._compute_sleep(resp.headers, state)

    def _compute_sleep(self, headers: Dict[str, str], state: TokenState) -> float:
        base = self.config.poll_interval_seconds
        if self.token_pool.is_anonymous:
            poll_hdr = headers.get("X-Poll-Interval")
            if poll_hdr and poll_hdr.isdigit():
                return max(base, float(poll_hdr))
        return base

    def _maybe_log_stats(self, state: TokenState) -> None:
        now = time.time()
        if now - self._last_stats_log >= 60.0 or self._total_polls == 1:
            usage = shutil.disk_usage(self.config.data_dir)
            disk_pct = (usage.used / usage.total) * 100.0
            self.logger.info(
                "Stats | total_written=%d (+%d) | polls=%d (304=%d) | %s remaining=%d | disk=%.1f%%",
                self._total_written,
                self._window_written,
                self._total_polls,
                self._total_304,
                state.label,
                state.remaining,
                disk_pct,
            )
            self._window_written = 0
            self._last_stats_log = now

    def run(self) -> None:
        if self.token_pool.is_anonymous:
            self.logger.warning(
                "Running in ANONYMOUS mode (60 req/hr). Set GITHUB_TOKENS in .env for 5000 req/hr per token!"
            )
        else:
            self.logger.info(
                "Starting collector with %d token(s), poll_interval=%.2fs, data_dir=%s, gcs=%s",
                len(self.token_pool.states),
                self.config.poll_interval_seconds,
                self.config.data_dir.resolve(),
                f"gs://{self.uploader.bucket_name}/{self.uploader.prefix}" if self.uploader else "disabled",
            )

        self.writer.compress_stale_files()

        try:
            while not self._stop_event.is_set():
                sleep_s = self.poll_once()
                self._stop_event.wait(timeout=sleep_s)
        finally:
            self.logger.info("Shutting down collector gracefully (total_written=%d)...", self._total_written)
            self.writer.close()
            self.session.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="GitHub Global /events Collector")
    parser.add_argument(
        "--check-gcs",
        action="store_true",
        help="Verify GCS write/delete permissions using current .env config and exit",
    )
    args = parser.parse_args()

    load_dotenv(Path(".env"))
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    logging.Formatter.converter = time.gmtime

    config = CollectorConfig.from_env()

    if args.check_gcs:
        if not config.gcs_bucket:
            logging.getLogger(__name__).error("GCS_BUCKET is not configured.")
            return 1
        uploader = GCSUploader(
            bucket_name=config.gcs_bucket,
            prefix=config.gcs_prefix,
            credentials_file=config.gcs_credentials_file or None,
        )
        uri = uploader.self_test()
        logging.getLogger(__name__).info("GCS self-test PASSED (verified write+delete at %s)", uri)
        return 0

    collector = GitHubEventsCollector(config)

    def handle_signal(signum: int, _frame: Any) -> None:
        logging.getLogger(__name__).info("Received signal %d, stopping...", signum)
        collector.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    collector.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
