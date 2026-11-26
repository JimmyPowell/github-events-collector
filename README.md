# github-events-collector

Lightweight Python collector for the global GitHub `/events` stream, designed for small VPS instances (e.g. GCP `e2-micro` 1GB RAM / 30GB disk).

## Design Highlights

- **Global `/events` polling**: Polls `https://api.github.com/events?per_page=100` with `ETag` / `If-None-Match` conditional requests (`304 Not Modified` costs 0 rate limit).
- **Multi-token pool**: Supports comma-separated `GITHUB_TOKENS` with automatic `X-RateLimit-*` tracking and backoff (also works in anonymous fallback mode at 60 req/hr).
- **GH Archive-compatible hourly NDJSON**: Writes raw events to `data/YYYY-MM-DD-HH.jsonl` using current UTC collection time (avoiding late-event writes to already-archived hours), and automatically compresses past hours to `.jsonl.gz`.
- **Automatic GCS Archiving & Upload Ledger**: After hourly gzip compression, automatically uploads to `gs://<GCS_BUCKET>/<GCS_PREFIX>/YYYY/MM/DD/YYYY-MM-DD-HH.jsonl.gz` with retries, and tracks every archive's upload status (`uploaded` / `failed`, `gcs_uri`, `md5`, `size_bytes`, `attempts`, `last_error`) in `data/upload_status.json`. Failed uploads are automatically retried on startup and hourly rollover.
- **Constant memory & disk safety**: Bounded LRU deduplication set + automatic retention days (`MAX_RETENTION_DAYS`) and disk usage threshold (`MAX_DISK_USAGE_PERCENT`) cleanup.
- **Privacy**: Optional SHA1 obfuscation of email local parts (`OBFUSCATE_EMAIL=true`), matching GH Archive.

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Edit .env: add GITHUB_TOKENS and optional GCS_BUCKET / GCS_CREDENTIALS_FILE

# Optional: verify GCS bucket write permissions
python collector.py --check-gcs

python collector.py
```

## Deploy as a `systemd` Service

```bash
sudo cp deploy/github-events-collector.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now github-events-collector
sudo journalctl -u github-events-collector -f
```
