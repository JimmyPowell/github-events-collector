# github-events-collector

Lightweight Python collector for the global GitHub `/events` stream, designed for small VPS instances (e.g. GCP `e2-micro` 1GB RAM / 30GB disk).

## Design Highlights

- **Global `/events` polling**: Polls `https://api.github.com/events?per_page=100` with `ETag` / `If-None-Match` conditional requests (`304 Not Modified` costs 0 rate limit).
- **Multi-token pool**: Supports comma-separated `GITHUB_TOKENS` with automatic `X-RateLimit-*` tracking and backoff (also works in anonymous fallback mode at 60 req/hr).
- **GH Archive-compatible hourly NDJSON**: Writes raw events to `data/YYYY-MM-DD-HH.jsonl` using current UTC collection time (avoiding late-event writes to already-archived hours), and automatically compresses past hours to `.jsonl.gz`.
- **Constant memory & disk safety**: Bounded LRU deduplication set + automatic retention days (`MAX_RETENTION_DAYS`) and disk usage threshold (`MAX_DISK_USAGE_PERCENT`) cleanup.
- **Privacy**: Optional SHA1 obfuscation of email local parts (`OBFUSCATE_EMAIL=true`), matching GH Archive.

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Edit .env and add your GITHUB_TOKENS (recommended for 5000 req/hr per token)

python collector.py
```

## Deploy as a `systemd` Service

```bash
sudo cp deploy/github-events-collector.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now github-events-collector
sudo journalctl -u github-events-collector -f
```
