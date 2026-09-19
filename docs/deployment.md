# Deployment guide

## Local Windows

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt pytest
Copy-Item .env.example .env
.\.venv\Scripts\uvicorn app.main:app --reload
```

Open `http://127.0.0.1:8000`. The in-process scheduler and SQLite repository
require one application worker against one database file.

## Docker Compose

```powershell
Copy-Item .env.example .env
docker compose up --build
```

The compose file mounts `./data` at `/app/data`; that mount is the persistence
boundary for SQLite. Do not run multiple replicas against this file.

## Render Blueprint

1. Create a Render Blueprint from this repository.
2. Keep `render.yaml` on the deployment branch and use its single worker.
3. Set optional alert secrets in Render's environment settings, never in Git.
4. Verify `/api/health` after each automatic deployment.

The Blueprint uses Render Free only as a demonstration environment. Its local
filesystem is ephemeral and an idle service can spin down, so SQLite signal
history is lost when the instance restarts or redeploys. The scanner remains
usable but historic analytics are not durable there. A durable deployment
needs a persistent volume or an external database adapter before relying on
history for operational decisions.

## Production guardrails

- Use exactly one worker/instance until persistence and scheduling are moved
  to shared infrastructure.
- Keep `DEBUG_TOKEN` set in any internet-facing deployment. `/api/debug` is
  intentionally unavailable when the token is absent.
- Keep CORS blank for the bundled same-origin dashboard. Add only exact HTTPS
  origins if a separate dashboard is deployed.
- Alert URLs and Telegram credentials are optional. They send candidate
  observations labelled `NO_TRADE`; they never place orders.


## Render Free daily-history note

Render Free uses an ephemeral filesystem, so every deploy or restart clears the stored `daily_equity_bars` history. The application automatically runs this bounded backfill once at startup when history is empty: `python -m collector.backfill --days 60 --max-downloads 60`. No Render Shell access is required. Until the backfill completes, stock rows use the clearly labelled minute-level fallback; index rows remain available through the NSE `allIndices` previous-close source. The startup warning identifies this condition, and `/api/health` reports the quick check at `daily_equity_data.bars`.


## Optional free backup for Render Free

Render Free has an ephemeral filesystem. To preserve signal events and analytics across redeploys, configure an S3-compatible or Supabase Storage upload endpoint through `NSE_OI_BACKUP_URL` and its secret `NSE_OI_BACKUP_TOKEN`. After the 18:10 IST daily ingestion, the app uploads one gzip-compressed SQLite snapshot and retains the latest seven manifest entries. On startup, when local daily history is empty, the newest valid snapshot is restored before the normal bounded market-data backfills. Upload or restore failures are logged and never block ingestion or startup. Leave both variables unset to use the existing ephemeral-disk behavior.

For the simplest paid alternative, attach a Render persistent disk mounted at `/data` and set `NSE_OI_DATABASE=/data/nse_oi.db`.

## Live Render service safety

The existing service is not Blueprint-managed, so changes to `render.yaml` do **not** change its live settings. In the Render dashboard, open **Settings → Build & Deploy**, set **Auto-Deploy: Off**, and set **Health Check Path: `/api/health`**. Deploy only outside the weekday market window, preferably between **16:00 and 08:30 IST**. The manual GitHub Actions workflow refuses 09:00–15:45 IST unless explicitly forced.

Rotate the Render API key that was exposed during setup. Never paste broker credentials, Render tokens, backup tokens, or debug tokens into source, issues, logs, or chat. Run `python tools/angel_login_check.py` locally with environment variables to validate Angel credentials; the script prints only `status`, `errorcode`, and a safe message.

For free durable snapshots, create a **private** GitHub data repository and a fine-grained token limited to Contents: write for that repository. Configure `NSE_OI_BACKUP_GITHUB_REPO`, `NSE_OI_BACKUP_GITHUB_TOKEN`, and `NSE_OI_BACKUP_BRANCH=data` in Render. Review the generated seed pull request before merging, and deploy only after the seed contains at least 60 daily bars for at least 90% of the F&O universe.

A free external pinger may call `/api/health` every five minutes during 08:45–15:45 IST to reduce Render spin-downs. This does not replace the app's freshness and source-health checks.
