# Troubleshooting

## No live signals or option chain

NSE can rate-limit, change, or temporarily block public endpoints. The client
refreshes a Chrome-impersonating session and retries bounded requests. Check
`/api/health` for the last refresh and use the dashboard's refresh controls;
do not increase the polling interval below the documented minimum without
revalidating public-source limits.

## Empty technical indicators

EMA50 and the daily validation checks need sufficient stored NSE bhavcopy
history. Run the bounded 60-day backfill command from the README, then wait
for the daily 18:10 IST ingestion job. The app intentionally reports missing
data instead of inferring intraday values from daily bars.

## History disappeared after a Render deployment

This is expected on a Render Free web service: its filesystem is ephemeral.
SQLite history persists locally and through Docker Compose only when the data
directory is backed by a persistent volume.

**Free durable backup (recommended):** create a private GitHub repository
(e.g. `VIGNESH6579/nse-oi-data`), a fine-grained PAT with Contents read/write
on that repo only, and set these Render environment variables:

```
NSE_OI_BACKUP_GITHUB_REPO=VIGNESH6579/nse-oi-data
NSE_OI_BACKUP_GITHUB_TOKEN=<fine-grained-token>
NSE_OI_BACKUP_BRANCH=data
```

The app uploads a gzipped SQLite snapshot every few minutes during market hours
and on shutdown, and restores it on cold start when local history is thin.
Leave the variables unset to keep pure ephemeral behaviour.

## startup_state stays BLOCKED while readiness is READY

Free-tier cold starts often boot outside market hours. A transient pre_open
self-test failure (universe not yet loaded, bars still downloading) used to
leave `startup_state` permanently BLOCKED even after a later post_open test
succeeded. Current builds recover automatically: off-hours pre_open BLOCKED is
recorded as DEGRADED, and the 5-minute readiness recheck promotes
`startup_state` to READY as soon as any self-test stage reports READY.

If you still see a stuck BLOCKED after deploying this fix, restart the service
once outside 09:00–15:45 IST and confirm `/api/health` shows both
`readiness` and `startup_state` as READY.

## Alerts not arriving

Leave all alert settings blank to disable them. For ntfy or webhook delivery,
verify an HTTPS destination. For Telegram, set both
`NSE_OI_TELEGRAM_BOT_TOKEN` and `NSE_OI_TELEGRAM_CHAT_ID`; the application
rejects partial Telegram configuration at startup. Alert delivery results are
audited without storing credential values.

## Git push crashes on this Windows host

The reported `git-remote-https.exe` memory-read dialog is a local Git for
Windows transport crash, independent of the FastAPI service. Dismiss the
dialog and update/repair Git for Windows. The working tree and local tests are
not corrupted by that dialog.
