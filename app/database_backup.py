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
_last_successful_snapshot_at: datetime | None = None


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


def _compress_database(database_path: Path) -> bytes:
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    output = io.BytesIO()
    with gzip.GzipFile(fileobj=output, mode="wb", compresslevel=6) as target, open(database_path, "rb") as source:
        shutil.copyfileobj(source, target)
    payload = output.getvalue()
    if len(payload) > _MAX_PAYLOAD:
        raise ValueError("compressed SQLite snapshot exceeds 10 MB limit")
    return payload


def _github_url(repo: str, path: str, branch: str) -> str:
    return f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"


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
    global _last_successful_snapshot_at
    repo, token, branch = _github_config()
    if not repo or not token or not Path(database_path).exists():
        return False
    try:
        payload = _compress_database(Path(database_path))
        current = now or datetime.now(timezone.utc)
        _github_put(repo, "working.sqlite3.gz", branch, token, payload, "chore: update durable SQLite snapshot")
        _last_successful_snapshot_at = current
        logger.info("Durable GitHub snapshot saved backend=github bytes=%d", len(payload))
        return True
    except Exception:
        logger.warning("Durable GitHub snapshot failed; ingestion continues", exc_info=True)
        return False


def _manifest_url(base: str) -> str:
    return f"{base}/manifest.json"


def upload_url_snapshot(database_path: Path) -> bool:
    global _last_successful_snapshot_at
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
        logger.info("Durable snapshot saved backend=url bytes=%d", len(payload))
        return True
    except Exception:
        logger.warning("URL snapshot failed; ingestion continues", exc_info=True)
        return False


def upload_database_snapshot(database_path: Path) -> bool:
    """Prefer the private GitHub backend and retain the existing URL backend."""
    if upload_github_snapshot(database_path):
        return True
    return upload_url_snapshot(database_path)


def _restore_payload(database_path: Path, payload: bytes) -> bool:
    with tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False) as temporary:
        temporary_path = Path(temporary.name)
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(payload)) as source, open(temporary_path, "wb") as target:
            shutil.copyfileobj(source, target)
        with sqlite3.connect(temporary_path) as connection:
            result = connection.execute("PRAGMA integrity_check").fetchone()
            if not result or result[0] != "ok":
                return False
        database_path.parent.mkdir(parents=True, exist_ok=True)
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
    """Populate an empty database from the bundled repository seed."""
    project_root = Path(__file__).resolve().parents[1]
    seed_gz = project_root / "data" / "seed_bhavcopy.sqlite3.gz"
    if not seed_gz.exists():
        return False
    try:
        database_path = Path(database_path)
        database_path.parent.mkdir(parents=True, exist_ok=True)
        if database_path.exists():
            try:
                with sqlite3.connect(database_path) as conn:
                    if conn.execute("SELECT COUNT(*) FROM daily_equity_bars").fetchone()[0] > 0:
                        return False
            except Exception:
                pass
        with tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            with gzip.open(seed_gz, "rb") as source, open(tmp_path, "wb") as target:
                shutil.copyfileobj(source, target)
            with sqlite3.connect(database_path) as dest:
                dest.execute(f"ATTACH DATABASE '{tmp_path.as_posix()}' AS seed")
                dest.execute("INSERT OR IGNORE INTO daily_equity_bars SELECT * FROM seed.daily_equity_bars")
                try:
                    dest.execute("INSERT OR IGNORE INTO daily_index_bars SELECT * FROM seed.daily_index_bars")
                except Exception:
                    pass
                dest.commit()
                dest.execute("DETACH DATABASE seed")
            logger.info("Restored bundled bhavcopy seed")
            return True
        finally:
            tmp_path.unlink(missing_ok=True)
    except Exception:
        logger.warning("Failed to restore bundled bhavcopy seed", exc_info=True)
        return False
