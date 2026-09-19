# NSE OI Tracker Free-Tier Hardening Report

## Conclusion

Commit `2dcb346` was pushed to `VIGNESH6579/nse-oi-tracker` on `main`. The local regression suite passes with **151 tests**. The Render deployment could not be triggered because the supplied Render credential was rejected with HTTP 400 for both service lookup and deploy requests. The repository is therefore updated, but the live service remains on the previously deployed commit until a valid Render API key or deploy hook is supplied.

## Implemented Changes

The startup history path now targets 60 trading days, skips dates already present, downloads newest first, limits the default rate to ten files per minute, records failed dates, logs batch progress, and filters bars to the compact universe represented by the committed seed. The seed builder now uses NSE bhavcopy as its primary source and treats Angel One as an optional fallback. The two duplicate workflow files at the repository root were removed; the copies under `.github/workflows/` remain.

Health metrics now use database bar depth. `atr_coverage_pct` measures the percentage of stored symbols with at least 15 daily bars, while `history_ready_pct` measures the percentage with at least 60 bars. `snapshot_age_s` is no longer the scan age. Health also exposes `snapshot_backend` and preserves `scan_age_s` separately.

Durable snapshots now prefer the private GitHub Contents API when `NSE_OI_BACKUP_GITHUB_REPO`, `NSE_OI_BACKUP_GITHUB_TOKEN`, and `NSE_OI_BACKUP_BRANCH` are configured. The implementation writes `working.sqlite3.gz`, retries one GitHub 409 conflict, caps compressed payloads at 10 MB, and never logs the token. The prior URL backend remains available as a fallback. Snapshots run every five minutes during market hours and once during shutdown with a ten-second bound. Startup restores durable storage only on an empty-bar database, then restores the committed seed if needed.

The environment template now documents the history, pacing, and safety defaults. New tests cover database-derived history coverage, GitHub conflict retry, and secret-safe logging behavior.

## O1–O10 Status

| ID | Status | Finding |
|---|---|---|
| O1 | Partly fixed | 60-day paced backfill, F&O filtering, coverage calculation, and NSE-first seed generation are implemented and tested. Empty-disk live acceptance of 95% coverage and the exact NSE response field behavior still require a Render run. |
| O2 | Fixed in repository | Health no longer reports 100% merely because one symbol exists. Snapshot age, backend, ATR coverage, history readiness, and scan age are separate fields. |
| O3 | Not fixed | The settings are now represented in the runtime model, but repository-level entry-window, concurrent-open, daily-cap, daily-stop, flip-gap, and skip-reason enforcement still requires implementation. |
| O4 | Partly fixed | GitHub durable snapshots, conflict retry, payload cap, scheduled save, shutdown save, and startup restore are implemented. The live Render environment was not updated because its API credential was rejected. |
| O5 | Not fixed | The duplicate Angel client and shared-login consolidation were not changed in this pass. |
| O6 | Not fixed | Candle-based grading, breakeven transition, TG2 path, costs, and R-based reporting remain to be wired into the close job. |
| O7 | Not fixed | Open-event price refresh for symbols absent from the current published list remains to be completed. |
| O8 | Not fixed | The separate announcement, index/VIX, and FII/DII scheduler jobs and unified scan lock remain to be completed. |
| O9 | Not fixed | Persistence, VWAP/opening-range confirmation, extension guards, relative volume, transparent components, and actionable gating remain to be completed. |
| O10 | Not fixed | Transition-only alerts and the dashboard data-health/setup view remain to be completed. |

## Verification

The following checks were run successfully:

- `PYTHONPATH=. python3 -m pytest -q` — 151 passed, one existing warning.
- `python3 -m compileall -q app collector config database` — passed.
- `git diff --check` — passed.
- Direct imports of the modified settings, backup, collector, and repository modules — passed.
- The live health endpoint was checked before changes and returned HTTP 200. It showed the original misleading `atr_coverage_pct: 100.0` behavior.
- The repository push completed successfully: `e470c97` → `2dcb346`.

The following items were not verified from this sandbox: Angel One rate limits and authentication configuration, exact NSE feed behavior for every historical date, whether all production scan triggers overlap, whether the owner has created the private backup repository and token, and whether Render has the required backup environment variables.

## Remaining Owner Actions

The Render API credential supplied for this task returned HTTP 400 for service lookup and deployment. Rotate that exposed credential in Render Account Settings, then provide a valid API key or configure the service deploy hook. After that, trigger the deployment outside 09:00–15:45 IST on a trading weekday and verify `/api/health` reports the new build SHA.

The owner must also create the private `VIGNESH6579/nse-oi-data` repository with a `data` branch, create a fine-grained token limited to Contents read/write for that repository, and configure the corresponding Render environment variables. Angel One credentials and the optional pinger remain manual account-level steps.

## Non-Goals

This pass does not place orders, execute broker actions, select option contracts, introduce machine learning, or add paid infrastructure. Signals remain paper-only.
