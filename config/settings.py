"""Validated runtime settings for the NSE OI Tracker service."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


def _positive_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer; received {raw!r}") from exc
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}; received {value}")
    return value


def _nonzero_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default))
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number; received {raw!r}") from exc


def _csv_values(name: str) -> tuple[str, ...]:
    return tuple(value.strip().rstrip("/") for value in os.getenv(name, "").split(",") if value.strip())


def _optional_url(name: str) -> str | None:
    value = os.getenv(name, "").strip()
    if not value:
        return None
    if not value.startswith(("https://", "http://")):
        raise ValueError(f"{name} must be an HTTP(S) URL")
    return value


def _optional_secret(name: str) -> str | None:
    return os.getenv(name, "").strip() or None


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "1" if default else "0").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean; received {raw!r}")


@dataclass(frozen=True, slots=True)
class Settings:
    """Environment-backed operational settings with safe local defaults."""

    data_dir: Path
    database_path: Path
    poll_interval_seconds: int
    cache_ttl_seconds: int
    debug_token: str | None
    history_retention_days: int
    cors_origins: tuple[str, ...]
    alert_webhook_url: str | None
    ntfy_topic_url: str | None
    telegram_bot_token: str | None
    telegram_chat_id: str | None
    alert_min_confidence: int
    startup_backfill: bool
    angel_overlay_enabled: bool
    nse_proxy_url: str | None
    backfill_target_days: int
    backfill_max_per_min: int
    snapshot_every_min: int
    entry_start: str
    entry_end: str
    time_exit: str
    max_open: int
    max_setups_day: int
    cooldown_after_sl_min: int
    max_sl_per_symbol_dir_day: int
    daily_stop_r: float
    persistence_scans: int
    extension_atr_mult: float
    rel_volume_min: float
    stale_price_s: int
    atr_coverage_min_pct: int
    cost_pct_roundtrip: float

    @classmethod
    def from_environment(cls) -> "Settings":
        project_root = Path(__file__).resolve().parents[1]
        configured_dir = os.getenv("NSE_OI_DATA_DIR", "data")
        data_dir = Path(configured_dir)
        if not data_dir.is_absolute():
            data_dir = project_root / data_dir
        data_dir = data_dir.resolve()
        data_dir.mkdir(parents=True, exist_ok=True)
        database_name = os.getenv("NSE_OI_DATABASE", "nse_oi_tracker.sqlite3")
        database_path = Path(database_name)
        if not database_path.is_absolute():
            database_path = data_dir / database_path
        telegram_bot_token = _optional_secret("NSE_OI_TELEGRAM_BOT_TOKEN")
        telegram_chat_id = _optional_secret("NSE_OI_TELEGRAM_CHAT_ID")
        if bool(telegram_bot_token) != bool(telegram_chat_id):
            raise ValueError("NSE_OI_TELEGRAM_BOT_TOKEN and NSE_OI_TELEGRAM_CHAT_ID must be set together")
        return cls(
            data_dir=data_dir,
            database_path=database_path,
            poll_interval_seconds=_positive_int("NSE_OI_POLL_INTERVAL_SECONDS", 60),
            cache_ttl_seconds=_positive_int("NSE_OI_CACHE_TTL_SECONDS", 60),
            debug_token=_optional_secret("DEBUG_TOKEN"),
            history_retention_days=_positive_int("NSE_OI_HISTORY_RETENTION_DAYS", 30),
            cors_origins=_csv_values("NSE_OI_CORS_ORIGINS"),
            alert_webhook_url=_optional_url("NSE_OI_ALERT_WEBHOOK_URL"),
            ntfy_topic_url=_optional_url("NSE_OI_NTFY_TOPIC_URL"),
            telegram_bot_token=telegram_bot_token,
            telegram_chat_id=telegram_chat_id,
            alert_min_confidence=_positive_int("NSE_OI_ALERT_MIN_CONFIDENCE", 80),
            startup_backfill=_env_bool("NSE_OI_STARTUP_BACKFILL", True),
            angel_overlay_enabled=_env_bool("ANGEL_OVERLAY_ENABLED", False),
            nse_proxy_url=_optional_url("NSE_OI_PROXY_URL"),
            backfill_target_days=_positive_int("BACKFILL_TARGET_DAYS", 60),
            backfill_max_per_min=_positive_int("BACKFILL_MAX_PER_MIN", 10),
            snapshot_every_min=_positive_int("SNAPSHOT_EVERY_MIN", 5),
            entry_start=os.getenv("ENTRY_START", "09:30"),
            entry_end=os.getenv("ENTRY_END", "14:30"),
            time_exit=os.getenv("TIME_EXIT", "15:15"),
            max_open=_positive_int("MAX_OPEN", 8),
            max_setups_day=_positive_int("MAX_SETUPS_DAY", 25),
            cooldown_after_sl_min=_positive_int("COOLDOWN_AFTER_SL_MIN", 60),
            max_sl_per_symbol_dir_day=_positive_int("MAX_SL_PER_SYMBOL_DIR_DAY", 2),
            daily_stop_r=_nonzero_float("DAILY_STOP_R", -4),
            persistence_scans=_positive_int("PERSISTENCE_SCANS", 4),
            extension_atr_mult=_nonzero_float("EXTENSION_ATR_MULT", 1.0),
            rel_volume_min=_nonzero_float("REL_VOLUME_MIN", 1.2),
            stale_price_s=_positive_int("STALE_PRICE_S", 150),
            atr_coverage_min_pct=_positive_int("ATR_COVERAGE_MIN_PCT", 90),
            cost_pct_roundtrip=_nonzero_float("COST_PCT_ROUNDTRIP", 0.05),
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_environment()
