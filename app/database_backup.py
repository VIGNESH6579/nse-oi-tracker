"""Bounded SQLite snapshots for ephemeral Render storage."""
from __future__ import annotations

import base64
import gzip
import io
import json
import logging
import os
import shutil
import sqlite3
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)
_TIMEOUT = 15
_MAX_PAYLOAD = 10 * 1024 * 1024
_KEEP = 7
_MAX_BAR_DATES = 80
_last_successful_snapshot_at: datetime | None = None
_last_snapshot_bytes: int | None = None
_last_snapshot_error: str | None = None
_PUBLIC_SEED_URL = "https://raw.githubusercontent.com/VIGNESH6579/nse-oi-tracker/data/data/seed_bhavcopy.sqlite3.gz"
_PUBLIC_SEED_MAX_BYTES = 10 * 1024 * 1024


def last_snapshot_info() -> dict:
    """Non-secret snapshot diagnostics for /api/health."""
    return {"snapshot_bytes": _last_snapshot_bytes, "snapshot_last_error": _last_snapshot_error}


def _config() -> tuple[str, str]:
    base = os.getenv("NSE_OI_BACKUP_URL", "").strip().rstrip("/")
    token = os.getenv("NSE_OI_BACKUP_TOKEN", "").strip()
    return base, token


def _github_config() -> tuple[str, str, str]:
    repo = os.getenv("NSE_OI_BACKUP_GITHUB_REPO", "").strip().strip("/")
    token = os.getenv("NSE_OI_BACKUP_GITHUB_TOKEN", "").strip()
    branch = os.getenv("NSE_OI_BACKUP_BRANCH", "data").strip() or "data"
    return repo, token, branch


def _request(url: str, *, method: str = "GET", body: bytes | None = None, token: str = "", headers: dict[str, str] | None = None) -> bytes:
    request_headers = {"User-Agent": "nse-oi-tracker-backup/2", **(headers or {})}
    if token:
        request_headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, data=body, headers=request_headers, method=method)
    with urllib.request.urlopen(request, timeout=_TIMEOUT) as response:
        return response.read()


def last_snapshot_age_s(now: datetime | None = None) -> float | None:
    if _last_successful_snapshot_at is None:
        return None
    current = now or datetime.now(timezone.utc)
    return round(max(0.0, (current - _last_successful_snapshot_at).total_seconds()), 2)


def _gzip_file(path: Path) -> bytes:
    output = io.BytesIO()
    with gzip.GzipFile(fileobj=output, mode="wb", compresslevel=6) as target, open(path, "rb") as source:
        shutil.copyfileobj(source, target)
    return output.getvalue()


def _prune_copy(database_path: Path) -> Path:
    """Return a pruned temporary COPY (the live DB is never modified)."""
    handle = tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False)
    handle.close()
    target = Path(handle.name)
    source = sqlite3.connect(database_path)
    copy = sqlite3.connect(target)
    try:
        source.backup(copy)
        statements = [
            "DELETE FROM scan_snapshots WHERE trade_date < (SELECT MAX(trade_date) FROM scan_snapshots)",
            "DELETE FROM option_chain_snapshots WHERE substr(captured_at_ist,1,10) < (SELECT MAX(substr(captured_at_ist,1,10)) FROM option_chain_snapshots)",
            "DELETE FROM setup_skips WHERE trade_date < (SELECT MAX(trade_date) FROM setup_skips)",
            "DELETE FROM corporate_announcements WHERE ingested_at_utc < date('now','-7 day')",
            "DELETE FROM alert_deliveries WHERE trade_date < date('now','-7 day')",
            f"DELETE FROM daily_equity_bars WHERE trade_date NOT IN (SELECT DISTINCT trade_date FROM daily_equity_bars ORDER BY trade_date DESC LIMIT {_MAX_BAR_DATES})",
        ]
        for statement in statements:
            try:
                copy.execute(statement)
            except sqlite3.Error:
                logger.debug("Snapshot prune skipped: %s", statement[:60], exc_info=True)
        copy.commit()
        copy.execute("VACUUM")
    finally:
        source.close()
        copy.close()
    return target


def _compress_database(database_path: Path) -> bytes:
    global _last_snapshot_bytes
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    db_bytes = Path(database_path).stat().st_size
    payload = _gzip_file(Path(database_path))
    if len(payload) > _MAX_PAYLOAD:
        first = len(payload)
        pruned = _prune_copy(Path(database_path))
        try:
            payload = _gzip_file(pruned)
        finally:
            pruned.unlink(missing_ok=True)
        logger.warning("Snapshot exceeded cap; used pruned copy db_bytes=%d gz_before=%d gz_after=%d cap=%d", db_bytes, first, len(payload), _MAX_PAYLOAD)
        if len(payload) > _MAX_PAYLOAD:
            raise ValueError(f"compressed SQLite snapshot {len(payload)} bytes exceeds cap {_MAX_PAYLOAD} even after pruning")
    _last_snapshot_bytes = len(payload)
    logger.info("Snapshot compressed db_bytes=%d gz_bytes=%d", db_bytes, len(payload))
    return payload


def _github_url(repo: str, path: str, branch: str) -> str:
    return f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"


def _decode_github_snapshot_item(item: dict) -> bytes | None:
    encoded = item.get("content", "") if isinstance(item, dict) else ""
    if encoded:
        return base64.b64decode("".join(encoded.split()))
    download_url = item.get("download_url") if isinstance(item, dict) else None
    return _request(download_url, token=_github_config()[1]) if download_url else None


def _snapshot_history_quality(payload: bytes) -> tuple[int, int, str]:
    """Return (minimum bars per symbol, total bars, latest date) for a gzipped DB."""
    with tempfile.NamedTemporaryFile(suffix=".snapshot-check.sqlite3", delete=False) as temporary:
        temporary_path = Path(temporary.name)
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(payload)) as source, open(temporary_path, "wb") as target:
            shutil.copyfileobj(source, target)
        with sqlite3.connect(temporary_path) as connection:
            exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='daily_equity_bars'"
            ).fetchone()
            if not exists:
                return 0, 0, ""
            total, latest = connection.execute(
                "SELECT COUNT(*), COALESCE(MAX(trade_date), '') FROM daily_equity_bars"
            ).fetchone()
            counts = [int(row[0]) for row in connection.execute(
                "SELECT COUNT(*) FROM daily_equity_bars GROUP BY symbol"
            ).fetchall()]
        return (min(counts) if counts else 0, int(total or 0), str(latest or ""))
    finally:
        temporary_path.unlink(missing_ok=True)


def _github_put(repo: str, path: str, branch: str, token: str, payload: bytes, message: str) -> None:
    url = _github_url(repo, path, branch)
    for attempt in range(2):
        sha = None
        try:
            existing = json.loads(_request(url, token=token, headers={"Accept": "application/vnd.github+json"}) or b"{}")
            sha = existing.get("sha") if isinstance(existing, dict) else None
        except urllib.error.HTTPError as exc:
            if exc.code != 404:
                raise
        body = {"message": message, "content": base64.b64encode(payload).decode("ascii"), "branch": branch}
        if sha:
            body["sha"] = sha
        try:
            _request(url, method="PUT", body=json.dumps(body).encode(), token=token, headers={"Accept": "application/vnd.github+json", "Content-Type": "application/json"})
            return
        except urllib.error.HTTPError as exc:
            if exc.code == 409 and attempt == 0:
                continue
            raise
    raise RuntimeError("GitHub snapshot conflict could not be resolved")


def upload_github_snapshot(database_path: Path, now: datetime | None = None) -> bool:
    global _last_successful_snapshot_at, _last_snapshot_error
    repo, token, branch = _github_config()
    if not repo or not token or not Path(database_path).exists():
        return False
    try:
        payload = _compress_database(Path(database_path))
        current = now or datetime.now(timezone.utc)
        # Rolling Render deploys briefly overlap: an old instance can shut down
        # after the new instance has uploaded a richer database. Never let a
        # smaller/older local DB replace a remote snapshot with deeper history.
        url = _github_url(repo, "working.sqlite3.gz", branch)
        try:
            item = json.loads(_request(url, token=token, headers={"Accept": "application/vnd.github+json"}) or b"{}")
            remote_payload = _decode_github_snapshot_item(item) if isinstance(item, dict) else None
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                remote_payload = None
            else:
                raise
        if remote_payload:
            local_min, local_total, local_latest = _snapshot_history_quality(payload)
            remote_min, remote_total, remote_latest = _snapshot_history_quality(remote_payload)
            regresses = (
                local_min < remote_min
                or local_total < remote_total
                or local_latest < remote_latest
            )
            if regresses:
                _last_successful_snapshot_at = current
                _last_snapshot_bytes = len(remote_payload)
                _last_snapshot_error = None
                logger.warning(
                    "Skipping regressive GitHub snapshot; keeping remote history "
                    "local=(min_bars=%d,total=%d,latest=%s) remote=(min_bars=%d,total=%d,latest=%s)",
                    local_min, local_total, local_latest, remote_min, remote_total, remote_latest,
                )
                return True
        _github_put(repo, "working.sqlite3.gz", branch, token, payload, "chore: update durable SQLite snapshot")
        _last_successful_snapshot_at = current
        _last_snapshot_error = None
        logger.info("Durable GitHub snapshot saved backend=github bytes=%d", len(payload))
        return True
    except Exception as exc:
        _last_snapshot_error = f"{type(exc).__name__}: {str(exc)[:160]}"
        logger.warning("Durable GitHub snapshot failed; ingestion continues", exc_info=True)
        return False


def _manifest_url(base: str) -> str:
    return f"{base}/manifest.json"


def upload_url_snapshot(database_path: Path) -> bool:
    global _last_successful_snapshot_at, _last_snapshot_error
    base, token = _config()
    if not base or not Path(database_path).exists():
        return False
    try:
        payload = _compress_database(Path(database_path))
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        name = f"nse-oi-{stamp}.sqlite3.gz"
        _request(f"{base}/{name}", method="PUT", body=payload, token=token)
        try:
            manifest = json.loads(_request(_manifest_url(base), token=token) or b"[]")
            names = [x for x in manifest if isinstance(x, str) and x != name]
        except Exception:
            names = []
        _request(_manifest_url(base), method="PUT", body=json.dumps([name] + names[: _KEEP - 1]).encode(), token=token)
        _last_successful_snapshot_at = datetime.now(timezone.utc)
        _last_snapshot_error = None
        logger.info("Durable snapshot saved backend=url bytes=%d", len(payload))
        return True
    except Exception as exc:
        _last_snapshot_error = f"{type(exc).__name__}: {str(exc)[:160]}"
        logger.warning("URL snapshot failed; ingestion continues", exc_info=True)
        return False


def upload_database_snapshot(database_path: Path) -> bool:
    """Prefer the private GitHub backend and retain the existing URL backend."""
    if upload_github_snapshot(database_path):
        return True
    return upload_url_snapshot(database_path)


def _restore_payload(database_path: Path, payload: bytes) -> bool:
    # Render can mount /tmp and the project/data directory on different
    # filesystems. os.replace() is atomic only within one filesystem, so the
    # restore temp file must live beside the destination database.
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=database_path.parent, suffix=".restore.sqlite3", delete=False
    ) as temporary:
        temporary_path = Path(temporary.name)
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(payload)) as source, open(temporary_path, "wb") as target:
            shutil.copyfileobj(source, target)
        with sqlite3.connect(temporary_path) as connection:
            result = connection.execute("PRAGMA integrity_check").fetchone()
            if not result or result[0] != "ok":
                return False
        os.replace(temporary_path, database_path)
        return True
    finally:
        temporary_path.unlink(missing_ok=True)


def restore_github_snapshot(database_path: Path, *, today_ist: str | None = None) -> bool:
    repo, token, branch = _github_config()
    if not repo or not token:
        return False
    try:
        url = _github_url(repo, "working.sqlite3.gz", branch)
        item = json.loads(_request(url, token=token, headers={"Accept": "application/vnd.github+json"}) or b"{}")
        encoded = item.get("content", "") if isinstance(item, dict) else ""
        if encoded:
            payload = base64.b64decode(encoded.replace("\n", ""))
        else:
            download_url = item.get("download_url")
            if not download_url:
                return False
            payload = _request(download_url, token=token)
        # A database with bars is allowed to restore only when the caller has no bars
        # or the snapshot is known to be current. The startup caller supplies that rule.
        restored = _restore_payload(Path(database_path), payload)
        if restored:
            logger.info("Restored durable GitHub snapshot backend=github")
        return restored
    except Exception:
        logger.warning("GitHub snapshot restore skipped: no usable backup available", exc_info=True)
        return False


def restore_url_snapshot(database_path: Path) -> bool:
    base, token = _config()
    if not base:
        return False
    try:
        manifest = json.loads(_request(_manifest_url(base), token=token) or b"[]")
        names = [x for x in manifest if isinstance(x, str)]
        if not names:
            return False
        restored = _restore_payload(Path(database_path), _request(f"{base}/{names[0]}", token=token))
        if restored:
            logger.info("Restored URL snapshot backend=url")
        return restored
    except (urllib.error.URLError, OSError, ValueError, sqlite3.Error, gzip.BadGzipFile, json.JSONDecodeError):
        logger.warning("URL snapshot restore skipped: no usable backup available")
        return False


def restore_latest_backup(database_path: Path) -> bool:
    """Restore configured durable storage, preferring GitHub."""
    return restore_github_snapshot(database_path) or restore_url_snapshot(database_path)


def restore_bundled_seed(database_path: Path) -> bool:
    """Merge the latest public F&O history seed, falling back to the bundled copy."""
    project_root = Path(__file__).resolve().parents[1]
    local_seed = project_root / "data" / "seed_bhavcopy.sqlite3.gz"

    def merge_seed(seed_path: Path, label: str) -> bool:
        try:
            with gzip.open(seed_path, "rb") as source, tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False) as tmp:
                shutil.copyfileobj(source, tmp)
                tmp_path = Path(tmp.name)
            try:
                with sqlite3.connect(database_path) as dest:
                    dest.execute(f"ATTACH DATABASE '{tmp_path.as_posix()}' AS seed")
                    seed_count = dest.execute("SELECT COUNT(*) FROM seed.daily_equity_bars").fetchone()[0]
                    if int(seed_count or 0) < 1000:
                        raise ValueError(f"public seed contains too few daily bars: {seed_count}")
                    before_count = dest.execute("SELECT COUNT(*) FROM daily_equity_bars").fetchone()[0]
                    dest.execute("INSERT OR IGNORE INTO daily_equity_bars SELECT * FROM seed.daily_equity_bars")
                    after_count = dest.execute("SELECT COUNT(*) FROM daily_equity_bars").fetchone()[0]
                    try:
                        dest.execute("INSERT OR IGNORE INTO daily_index_bars SELECT * FROM seed.daily_index_bars")
                    except Exception:
                        pass
                    dest.commit()
                    dest.execute("DETACH DATABASE seed")
                logger.info("Merged %s bhavcopy seed seed_rows=%d inserted=%d", label, int(seed_count), max(0, int(after_count) - int(before_count)))
                return True
            finally:
                tmp_path.unlink(missing_ok=True)
        except Exception:
            logger.warning("Failed to merge %s bhavcopy seed", label, exc_info=True)
            return False

    database_path = Path(database_path)
    database_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        request = urllib.request.Request(
            _PUBLIC_SEED_URL,
            headers={"User-Agent": "nse-oi-tracker-history-seed/1.0", "Accept": "application/gzip"},
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = response.read(_PUBLIC_SEED_MAX_BYTES + 1)
        if len(payload) <= _PUBLIC_SEED_MAX_BYTES:
            with tempfile.NamedTemporaryFile(suffix=".sqlite3.gz", delete=False) as tmp:
                tmp.write(payload)
                public_path = Path(tmp.name)
            try:
                if merge_seed(public_path, "public NSE"):
                    return True
            finally:
                public_path.unlink(missing_ok=True)
    except Exception:
        logger.info("Public NSE history seed unavailable; trying bundled seed", exc_info=True)

    if local_seed.exists():
        return merge_seed(local_seed, "bundled")
    return False
