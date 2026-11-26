#!/usr/bin/env python3
"""
GitHub Global /events Collector

Inspired by GH Archive (2025 UTC-rollover fix) and OpenDigger's multi-token pool:
- Polls https://api.github.com/events?per_page=100
- Multi-token rotation with automatic X-RateLimit-* backoff
- Conditional requests via ETag / If-None-Match (304 consumes 0 quota)
- Bounded LRU event ID deduplication (constant memory footprint)
- Hourly UTC NDJSON files (YYYY-MM-DD-HH.jsonl) auto-compressed to .jsonl.gz
- Disk usage & retention guard to keep small VPS disks safe
"""

from __future__ import annotations

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
from typing import Any, Dict, List, Optional, Set, TextIO

import requests

EVENTS_API_URL = "https://api.github.com/events"
USER_AGENT = "github-events-collector/1.0"


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

        # All tokens exhausted: pick the one that resets earliest
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


class HourlyArchiveWriter:
    """
    Writes NDJSON events into UTC hourly files (YYYY-MM-DD-HH.jsonl) based on
    collection time, and compresses past-hour files into .jsonl.gz.
    """

    def __init__(
        self,
        data_dir: Path,
        retention_days: int = 7,
        max_disk_usage_percent: int = 80,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.retention_days = retention_days
        self.max_disk_usage_percent = max_disk_usage_percent
        self.logger = logger or logging.getLogger(__name__)
        self._current_hour: Optional[str] = None
        self._file: Optional[TextIO] = None
        self._compress_threads: List[threading.Thread] = []

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
            self._schedule_compress(old_path)

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
        """Compress any leftover past-hour .jsonl files from previous runs."""
        current_name = f"{self.hour_key(now)}.jsonl"
        for path in sorted(self.data_dir.glob("*.jsonl")):
            if path.name != current_name:
                self._compress_file(path)

    def _schedule_compress(self, path: Path) -> None:
        t = threading.Thread(target=self._compress_file, args=(path,), daemon=False)
        t.start()
        self._compress_threads = [th for th in self._compress_threads if th.is_alive()]
        self._compress_threads.append(t)

    def _compress_file(self, src: Path) -> None:
        if not src.exists():
            return
        gz_path = src.with_suffix(".jsonl.gz")
        tmp_path = src.with_suffix(".jsonl.gz.tmp")
        try:
            # If gz already exists (e.g., restart within same past hour), append stream
            mode = "ab" if gz_path.exists() else "wb"
            target = gz_path if mode == "ab" else tmp_path
            with src.open("rb") as f_in, gzip.open(target, mode, compresslevel=6) as f_out:
                shutil.copyfileobj(f_in, f_out, length=1024 * 1024)
            if target == tmp_path:
                tmp_path.replace(gz_path)
            src.unlink()
            self.logger.info("Compressed archive: %s (%d bytes)", gz_path.name, gz_path.stat().st_size)
        except Exception:
            self.logger.exception("Failed to compress %s", src)
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

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
                    self.logger.info("Removing expired archive (>%dd): %s", self.retention_days, f.name)
                    f.unlink(missing_ok=True)
                else:
                    remaining_files.append(f)
            gz_files = remaining_files

        if self.max_disk_usage_percent > 0:
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
        for t in self._compress_threads:
            t.join(timeout=30)


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

    @classmethod
    def from_env(cls) -> "CollectorConfig":
        raw_tokens = os.getenv("GITHUB_TOKENS", "") or os.getenv("GITHUB_TOKEN", "")
        tokens = [t.strip() for t in raw_tokens.split(",") if t.strip()]
        raw_types = os.getenv("EVENT_TYPES", "").strip()
        event_types = {t.strip() for t in raw_types.split(",") if t.strip()} if raw_types else set()

        # Default poll interval: 1.0s with token(s), 60.0s in anonymous mode
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
        )


class GitHubEventsCollector:
    def __init__(self, config: CollectorConfig, logger: Optional[logging.Logger] = None) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.token_pool = TokenPool(config.tokens)
        self.dedup = BoundedDedupSet(max_size=config.dedup_size)
        self.writer = HourlyArchiveWriter(
            data_dir=config.data_dir,
            retention_days=config.retention_days,
            max_disk_usage_percent=config.max_disk_usage_percent,
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

        # Oldest-first ordering inside each batch for cleaner chronological logs
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
        # In anonymous mode, respect X-Poll-Interval so we don't burn 60 req/hr
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
                "Starting collector with %d token(s), poll_interval=%.2fs, data_dir=%s",
                len(self.token_pool.states),
                self.config.poll_interval_seconds,
                self.config.data_dir.resolve(),
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
    load_dotenv(Path(".env"))
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    logging.Formatter.converter = time.gmtime

    config = CollectorConfig.from_env()
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
