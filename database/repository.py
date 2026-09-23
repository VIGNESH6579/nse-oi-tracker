"""SQLite repository for immutable scan snapshots and today's signal history."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

from signal_engine.risk import build_risk_plan
from config.settings import get_settings
from utils.time import as_ist, ist_trade_date


@dataclass(frozen=True, slots=True)
class SnapshotWrite:
    snapshot_id: int
    created: bool
    signal_count: int


TG1_BOOK_FRACTION = float(os.getenv("TG1_BOOK_FRACTION", "0.5"))   # share of the position booked at TG1


def _r_multiple(direction: str, entry: float, price: float, risk: float) -> float:
    """Signed profit in R (multiples of the initial risk) for a price."""
    if risk <= 0:
        return 0.0
    move = price - entry if direction == "BUY" else entry - price
    return move / risk


def _closed_r(status: str, direction: str, entry: float, stop: float, t1: float, t2: float,
              exit_price: float, armed: bool) -> float:
    """Result in R. Half is booked at TG1 (1R); after TG1 the stop moves to entry (breakeven)."""
    risk = abs(entry - stop) or entry * 0.003
    book = TG1_BOOK_FRACTION
    r1 = abs(t1 - entry) / risk
    r2 = abs(t2 - entry) / risk
    if status == "TG2_HIT":
        return round(book * r1 + (1 - book) * r2, 3)
    r_exit = _r_multiple(direction, entry, exit_price, risk)
    if status == "BE_EXIT":
        return round(book * r1 + (1 - book) * min(0.0, r_exit), 3)
    if armed:                                       # time exit after TG1
        return round(book * r1 + (1 - book) * max(0.0, r_exit), 3)
    return round(r_exit, 3)                         # SL_HIT / EXPIRED before TG1


class SignalRepository:
    """Own the SQLite schema and all history/result mutations.

    A scan is stored as an immutable snapshot. Signal events are children of a
    snapshot, so repeated same-day occurrences are preserved instead of being
    collapsed to one browser-local record per ticker.
    """

    def __init__(self, database_path: Path):
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS scan_snapshots (
                    id INTEGER PRIMARY KEY,
                    trade_date TEXT NOT NULL,
                    captured_at_ist TEXT NOT NULL,
                    captured_minute_ist TEXT NOT NULL,
                    source TEXT NOT NULL,
                    is_stale INTEGER NOT NULL DEFAULT 0,
                    fingerprint TEXT NOT NULL,
                    signal_count INTEGER NOT NULL,
                    archived INTEGER NOT NULL DEFAULT 0,
                    created_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(trade_date, captured_minute_ist, fingerprint)
                );

                CREATE TABLE IF NOT EXISTS signal_events (
                    id INTEGER PRIMARY KEY,
                    snapshot_id INTEGER NOT NULL REFERENCES scan_snapshots(id) ON DELETE CASCADE,
                    trade_date TEXT NOT NULL,
                    captured_at_ist TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    signal TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    confidence INTEGER NOT NULL DEFAULT 0,
                    entry REAL NOT NULL,
                    stop_loss REAL NOT NULL,
                    target_1 REAL NOT NULL,
                    target_2 REAL NOT NULL,
                    risk_reward REAL NOT NULL,
                    risk_source TEXT NOT NULL,
                    current_price REAL,
                    exit_price REAL,
                    max_target_hit INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'OPEN',
                    result TEXT,
                    result_source TEXT,
                    closed_at_ist TEXT,
                    payload_json TEXT NOT NULL,
                    archived INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(snapshot_id, symbol, signal)
                );

                CREATE INDEX IF NOT EXISTS idx_snapshot_trade_date
                    ON scan_snapshots(trade_date, captured_at_ist DESC);
                CREATE INDEX IF NOT EXISTS idx_event_visible_history
                    ON signal_events(trade_date, archived, captured_at_ist DESC);
                CREATE INDEX IF NOT EXISTS idx_event_open_symbol
                    ON signal_events(trade_date, status, symbol);

                CREATE TABLE IF NOT EXISTS daily_equity_bars (
                    trade_date TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    source TEXT NOT NULL DEFAULT 'nse_bhavcopy',
                    ingested_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(trade_date, symbol)
                );

                CREATE INDEX IF NOT EXISTS idx_daily_equity_symbol_date
                    ON daily_equity_bars(symbol, trade_date DESC);

                CREATE TABLE IF NOT EXISTS daily_index_bars (
                    trade_date TEXT NOT NULL, symbol TEXT NOT NULL,
                    open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL,
                    close REAL NOT NULL, source TEXT NOT NULL DEFAULT 'nse_index_history',
                    ingested_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(trade_date, symbol)
                );
                CREATE INDEX IF NOT EXISTS idx_daily_index_symbol_date
                    ON daily_index_bars(symbol, trade_date DESC);

                CREATE TABLE IF NOT EXISTS option_chain_snapshots (
                    id INTEGER PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    expiry TEXT NOT NULL,
                    captured_at_ist TEXT NOT NULL,
                    captured_minute_ist TEXT NOT NULL,
                    spot REAL NOT NULL,
                    pcr REAL NOT NULL,
                    max_pain REAL NOT NULL,
                    total_ce_oi INTEGER NOT NULL,
                    total_pe_oi INTEGER NOT NULL,
                    oi_levels_json TEXT NOT NULL,
                    UNIQUE(symbol, expiry, captured_minute_ist)
                );

                CREATE INDEX IF NOT EXISTS idx_chain_snapshot_symbol_time
                    ON option_chain_snapshots(symbol, captured_at_ist DESC);

                CREATE TABLE IF NOT EXISTS alert_deliveries (
                    id INTEGER PRIMARY KEY,
                    alert_key TEXT NOT NULL UNIQUE,
                    trade_date TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    status TEXT NOT NULL,
                    detail TEXT,
                    created_at_ist TEXT NOT NULL,
                    delivered_at_ist TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_alert_trade_date
                    ON alert_deliveries(trade_date, created_at_ist DESC);

                CREATE TABLE IF NOT EXISTS setup_skips (
                    id INTEGER PRIMARY KEY,
                    trade_date TEXT NOT NULL,
                    captured_at_ist TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    skip_reason TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_setup_skips_date
                    ON setup_skips(trade_date, captured_at_ist DESC);

                CREATE TABLE IF NOT EXISTS corporate_announcements (
                    announcement_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    published_at TEXT NOT NULL,
                    category TEXT NOT NULL,
                    title TEXT NOT NULL,
                    attachment_url TEXT,
                    event_risk TEXT NOT NULL,
                    risk_terms_json TEXT NOT NULL,
                    ingested_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_announcement_symbol_time
                    ON corporate_announcements(symbol, published_at DESC);

                CREATE TABLE IF NOT EXISTS participant_oi_reports (
                    report_date TEXT NOT NULL,
                    participant TEXT NOT NULL,
                    net_index_futures INTEGER NOT NULL,
                    net_stock_futures INTEGER NOT NULL,
                    measures_json TEXT NOT NULL,
                    source TEXT NOT NULL,
                    ingested_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(report_date, participant)
                );

                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_participant_oi_date
                    ON participant_oi_reports(report_date DESC);
                """
            )
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(signal_events)").fetchall()
            }
            if "max_target_hit" not in columns:
                connection.execute(
                    "ALTER TABLE signal_events ADD COLUMN max_target_hit INTEGER NOT NULL DEFAULT 0"
                )
            if "result_source" not in columns:
                connection.execute(
                    "ALTER TABLE signal_events ADD COLUMN result_source TEXT"
                )
            if "tier" not in columns:
                connection.execute("ALTER TABLE signal_events ADD COLUMN tier TEXT NOT NULL DEFAULT 'TRADE'")
            if "result_r" not in columns:
                connection.execute("ALTER TABLE signal_events ADD COLUMN result_r REAL")
            if "last_seen_at_ist" not in columns:
                connection.execute(
                    "ALTER TABLE signal_events ADD COLUMN last_seen_at_ist TEXT"
                )
            version = connection.execute("SELECT version FROM schema_version LIMIT 1").fetchone()
            if version is None:
                connection.execute("INSERT INTO schema_version(version) VALUES (1)")
            elif int(version[0]) < 2:
                connection.execute("UPDATE schema_version SET version = 2")

    def upsert_daily_equity_bars(self, bars: Iterable[dict[str, Any]]) -> int:
        """Store one full daily NSE bhavcopy, replacing only matching date/symbol bars."""
        rows = [
            (
                str(bar.get("trade_date") or ""),
                str(bar.get("symbol") or "").upper(),
                float(bar.get("open") or 0),
                float(bar.get("high") or 0),
                float(bar.get("low") or 0),
                float(bar.get("close") or 0),
                float(bar.get("volume") or 0),
            )
            for bar in bars
            if bar.get("trade_date") and bar.get("symbol") and float(bar.get("close") or 0) > 0
        ]
        if not rows:
            return 0
        with self._connect() as connection:
            connection.executemany(
                """
                INSERT INTO daily_equity_bars (
                    trade_date, symbol, open, high, low, close, volume
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_date, symbol) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    source = excluded.source,
                    ingested_at_utc = CURRENT_TIMESTAMP
                """,
                rows,
            )
        return len(rows)


    def upsert_daily_index_bars(self, bars: Iterable[dict[str, Any]]) -> int:
        rows = [(str(b.get("trade_date") or ""), str(b.get("symbol") or "").upper(), float(b.get("open") or 0), float(b.get("high") or 0), float(b.get("low") or 0), float(b.get("close") or 0)) for b in bars if b.get("trade_date") and b.get("symbol") and float(b.get("close") or 0) > 0]
        with self._connect() as con:
            con.executemany("INSERT INTO daily_index_bars(trade_date,symbol,open,high,low,close) VALUES(?,?,?,?,?,?) ON CONFLICT(trade_date,symbol) DO UPDATE SET open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,ingested_at_utc=CURRENT_TIMESTAMP", rows)
        return len(rows)

    def daily_index_bars_for_symbols(self, symbols: Iterable[str], *, limit_per_symbol: int = 90) -> dict[str, list[dict[str, Any]]]:
        names = sorted({str(x).upper().strip() for x in symbols if str(x).strip()})
        if not names: return {}
        q = ",".join("?" for _ in names)
        with self._connect() as con:
            rows = con.execute(f"SELECT symbol,trade_date,open,high,low,close,NULL AS volume FROM (SELECT symbol,trade_date,open,high,low,close,ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY trade_date DESC) AS n FROM daily_index_bars WHERE symbol IN ({q})) WHERE n<=? ORDER BY symbol,trade_date", (*names, limit_per_symbol)).fetchall()
        result={x:[] for x in names}
        for row in rows: result[str(row["symbol"])].append(dict(row))
        return result

    def daily_index_bar_summary(self) -> dict[str, Any]:
        with self._connect() as con:
            return dict(con.execute("SELECT COUNT(*) AS bars, COUNT(DISTINCT symbol) AS symbols, MAX(trade_date) AS latest_trade_date FROM daily_index_bars").fetchone())

    def daily_equity_bars_for_symbol(self, symbol: str, *, limit: int = 90) -> list[dict[str, Any]]:
        """Return a chronological, bounded daily NSE-bar series for one ticker."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT trade_date, open, high, low, close, volume
                FROM daily_equity_bars
                WHERE symbol = ?
                ORDER BY trade_date DESC
                LIMIT ?
                """,
                (symbol.upper().strip(), limit),
            ).fetchall()
        return [dict(row) for row in reversed(rows)]

    def daily_equity_trade_dates(self) -> set[str]:
        """Return all dates for which at least one normalized bhavcopy bar exists."""
        with self._connect() as connection:
            rows = connection.execute("SELECT DISTINCT trade_date FROM daily_equity_bars").fetchall()
        return {str(row["trade_date"]) for row in rows}

    def daily_equity_bars_for_symbols(self, symbols: Iterable[str], *, limit_per_symbol: int = 90) -> dict[str, list[dict[str, Any]]]:
        """Return chronological daily bars for multiple symbols in one query."""
        normalized = sorted({str(symbol).upper().strip() for symbol in symbols if str(symbol).strip()})
        if not normalized:
            return {}
        placeholders = ",".join("?" for _ in normalized)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT symbol, trade_date, open, high, low, close, volume
                FROM (
                    SELECT symbol, trade_date, open, high, low, close, volume,
                           ROW_NUMBER() OVER (
                               PARTITION BY symbol ORDER BY trade_date DESC
                           ) AS position
                    FROM daily_equity_bars
                    WHERE symbol IN ({placeholders})
                )
                WHERE position <= ?
                ORDER BY symbol, trade_date ASC
                """,
                (*normalized, limit_per_symbol),
            ).fetchall()
        result: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in normalized}
        for row in rows:
            record = dict(row)
            result[str(record.pop("symbol"))].append(record)
        return result

    def purge_non_fno_bars(self, symbols: Iterable[str]) -> dict[str, int]:
        """Delete equity bars for symbols outside the F&O universe.

        No-op when the universe is empty (fail-closed: never wipe on a failed
        lookup). VACUUM only when a large share was removed.
        """
        keep = sorted({str(sym).upper() for sym in symbols if sym})
        if not keep:
            return {"before": 0, "deleted": 0, "after": 0, "vacuumed": 0}
        with self._connect() as connection:
            before = connection.execute("SELECT COUNT(*) FROM daily_equity_bars").fetchone()[0]
            connection.execute("CREATE TEMP TABLE IF NOT EXISTS keep_symbols(symbol TEXT PRIMARY KEY)")
            connection.execute("DELETE FROM keep_symbols")
            connection.executemany("INSERT OR IGNORE INTO keep_symbols(symbol) VALUES (?)", [(sym,) for sym in keep])
            connection.execute("DELETE FROM daily_equity_bars WHERE symbol NOT IN (SELECT symbol FROM keep_symbols)")
            after = connection.execute("SELECT COUNT(*) FROM daily_equity_bars").fetchone()[0]
            connection.commit()
        deleted = before - after
        vacuumed = 0
        if before and deleted / before > 0.2:
            raw = sqlite3.connect(self.database_path)
            try:
                raw.execute("VACUUM")
                vacuumed = 1
            finally:
                raw.close()
        return {"before": before, "deleted": deleted, "after": after, "vacuumed": vacuumed}

    def symbols_missing_bars(self, symbols: Iterable[str], min_bars: int = 15) -> list[str]:
        """F&O symbols that have fewer than ``min_bars`` daily bars (name mismatches, new listings)."""
        wanted = sorted({str(sym).upper() for sym in symbols if sym})
        if not wanted:
            return []
        with self._connect() as connection:
            connection.execute("CREATE TEMP TABLE IF NOT EXISTS gap_symbols(symbol TEXT PRIMARY KEY)")
            connection.execute("DELETE FROM gap_symbols")
            connection.executemany("INSERT OR IGNORE INTO gap_symbols(symbol) VALUES (?)", [(sym,) for sym in wanted])
            rows = connection.execute(
                "SELECT g.symbol FROM gap_symbols g LEFT JOIN (SELECT symbol, COUNT(*) AS n FROM daily_equity_bars GROUP BY symbol) b "
                "ON b.symbol = g.symbol WHERE COALESCE(b.n, 0) < ? ORDER BY g.symbol", (min_bars,),
            ).fetchall()
        return [row[0] for row in rows]

    def index_bar_count(self, symbol: str) -> int:
        with self._connect() as connection:
            return int(connection.execute("SELECT COUNT(*) FROM daily_index_bars WHERE symbol = ?", (symbol.upper(),)).fetchone()[0] or 0)

    def daily_equity_bar_summary(self) -> dict[str, Any]:
        """Compact technical-data freshness diagnostics for /api/health."""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS bars, COUNT(DISTINCT symbol) AS symbols, MAX(trade_date) AS latest_trade_date FROM daily_equity_bars"
            ).fetchone()
        return dict(row)

    def daily_history_coverage(self, *, atr_bars: int = 15, ready_bars: int = 60,
                               symbols: Iterable[str] | None = None) -> dict[str, float | int]:
        """Percentage of symbols (the F&O universe when given) with enough daily bars."""
        wanted = sorted({str(sym).upper() for sym in symbols or () if sym})
        if wanted:
            with self._connect() as connection:
                marks = ",".join("?" for _ in wanted)
                counts = [int(row[0]) for row in connection.execute(
                    f"SELECT COUNT(*) FROM daily_equity_bars WHERE symbol IN ({marks}) GROUP BY symbol", wanted).fetchall()]
            atr_ready = sum(1 for n in counts if n >= atr_bars)
            history_ready = sum(1 for n in counts if n >= ready_bars)
            total = len(wanted)
            return {
                "symbols": total, "atr_ready_symbols": atr_ready, "history_ready_symbols": history_ready,
                "atr_coverage_pct": round(100.0 * atr_ready / total, 2),
                "history_ready_pct": round(100.0 * history_ready / total, 2),
            }
        with self._connect() as connection:
            total = int(connection.execute("SELECT COUNT(DISTINCT symbol) FROM daily_equity_bars").fetchone()[0] or 0)
            atr_ready = int(connection.execute(
                "SELECT COUNT(*) FROM (SELECT symbol FROM daily_equity_bars GROUP BY symbol HAVING COUNT(*) >= ?)",
                (atr_bars,),
            ).fetchone()[0] or 0)
            history_ready = int(connection.execute(
                "SELECT COUNT(*) FROM (SELECT symbol FROM daily_equity_bars GROUP BY symbol HAVING COUNT(*) >= ?)",
                (ready_bars,),
            ).fetchone()[0] or 0)
        return {
            "symbols": total,
            "atr_ready_symbols": atr_ready,
            "history_ready_symbols": history_ready,
            "atr_coverage_pct": round(100.0 * atr_ready / total, 2) if total else 0.0,
            "history_ready_pct": round(100.0 * history_ready / total, 2) if total else 0.0,
        }

    def latest_daily_equity_trade_date(self) -> str | None:
        """Return the newest stored daily equity-bar date, if any."""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT MAX(trade_date) AS trade_date FROM daily_equity_bars"
            ).fetchone()
        return str(row["trade_date"]) if row and row["trade_date"] else None



    def backtest_events(self, start_date: str, end_date: str, tier: str | None = "TRADE") -> list[dict[str, Any]]:
        """Return stored candidate events, including archive, for transparent analysis."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT captured_at_ist, trade_date, symbol, signal, direction, confidence,
                       entry, stop_loss, target_1, target_2, risk_reward, risk_source,
                       current_price, exit_price, max_target_hit, status, result, result_r, tier, payload_json
                FROM signal_events
                WHERE trade_date BETWEEN ? AND ? AND (? IS NULL OR tier = ?)
                ORDER BY captured_at_ist ASC, id ASC
                """,
                (start_date, end_date, tier, tier),
            ).fetchall()
        events: list[dict[str, Any]] = []
        for row in rows:
            event = dict(row)
            payload = json.loads(str(event.pop("payload_json") or "{}"))
            # Sector is the scan-time display taxonomy. Older events stay
            # visibly unclassified instead of being rewritten by a newer map.
            event["sector"] = payload.get("sector") or "Unclassified"
            events.append(event)
        return events

    def reserve_alert(self, alert_key: str, channel: str, observed_at: datetime) -> bool:
        """Reserve an idempotent alert before network delivery."""
        observed_at = as_ist(observed_at)
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO alert_deliveries (
                    alert_key, trade_date, channel, status, created_at_ist
                ) VALUES (?, ?, ?, 'PENDING', ?)
                """,
                (alert_key, ist_trade_date(observed_at), channel, observed_at.isoformat()),
            )
        return cursor.rowcount > 0

    def complete_alert(self, alert_key: str, *, delivered: bool, detail: str, observed_at: datetime) -> None:
        """Record a delivery result without exposing endpoint secrets."""
        observed_at = as_ist(observed_at)
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE alert_deliveries
                SET status = ?, detail = ?, delivered_at_ist = ?
                WHERE alert_key = ?
                """,
                ("DELIVERED" if delivered else "FAILED", detail[:500], observed_at.isoformat(), alert_key),
            )





    @staticmethod
    def _fingerprint(signals: Iterable[dict[str, Any]]) -> str:
        canonical = json.dumps(list(signals), sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _event_payload(signal: dict[str, Any], captured_at: datetime) -> tuple[dict[str, Any], dict[str, Any]]:
        payload = dict(signal)
        direction = str(payload.get("signal_direction") or "NONE").upper()
        technical = payload.get("technical_context") or {}
        plan = build_risk_plan(
            float(payload.get("ltp") or 0),
            direction,
            str(payload.get("signal") or ""),
            atr14=technical.get("atr14"),
        )
        if plan is None:
            raise ValueError(f"Cannot persist a tradable signal without valid price/direction: {payload!r}")
        plan_data = plan.to_dict()
        entry = float(plan_data["entry"])
        target_1_pct = abs((float(plan_data["target_1"]) - entry) / entry * 100)
        target_2_pct = abs((float(plan_data["target_2"]) - entry) / entry * 100)
        stop_pct = abs((float(plan_data["stop_loss"]) - entry) / entry * 100)
        target_sign = "+" if direction == "BUY" else "-"
        stop_sign = "-" if direction == "BUY" else "+"
        payload.update(
            {
                "tradeDate": ist_trade_date(captured_at),
                "addedAt": captured_at.strftime("%H:%M:%S"),
                "currentLTP": payload.get("ltp"),
                "direction": direction,
                "entry": plan_data["entry"],
                "sl": plan_data["stop_loss"],
                "tg1": plan_data["target_1"],
                "tg2": plan_data["target_2"],
                "tg1Pct": f"{target_sign}{target_1_pct:g}",
                "tg2Pct": f"{target_sign}{target_2_pct:g}",
                "slPct": f"{stop_sign}{stop_pct:g}",
                "risk_reward": plan_data["risk_reward"],
                "risk_source": plan_data["source"],
                "reasons": payload.get("reasons")
                or ["Price/OI classification; technical/regime gates are not yet available"],
            }
        )
        return payload, plan_data

    @staticmethod
    def _clock(value: str) -> tuple[int, int]:
        hour, minute = (int(part) for part in value.split(":", 1))
        return hour, minute

    @classmethod
    def _inside_entry_window(cls, captured_at: datetime) -> bool:
        settings = get_settings()
        current = captured_at.hour * 60 + captured_at.minute
        start_h, start_m = cls._clock(settings.entry_start)
        end_h, end_m = cls._clock(settings.entry_end)
        return start_h * 60 + start_m <= current < end_h * 60 + end_m

    @staticmethod
    def _realized_r(connection: sqlite3.Connection, trade_date: str) -> float:
        rows = connection.execute(
            "SELECT status, max_target_hit, result_r FROM signal_events WHERE trade_date = ? AND archived = 0 AND tier = 'TRADE'",
            (trade_date,),
        ).fetchall()
        total = 0.0
        for row in rows:
            if row["result_r"] is not None:
                total += float(row["result_r"])
            elif str(row["status"]) == "SL_HIT":
                total -= 1.0
            elif int(row["max_target_hit"] or 0) >= 2:
                total += 2.0
            elif int(row["max_target_hit"] or 0) >= 1:
                total += 1.0
        return total

    _SKIP_PAYLOAD_KEYS = ("ltp", "confidence", "confidence_score", "signal", "price_age_s", "stale_price", "missing_confirmations", "quality_score")

    @staticmethod
    def _record_skip(connection: sqlite3.Connection, *, trade_date: str, captured_at: datetime, symbol: str, direction: str, reason: str, payload: dict[str, Any]) -> None:
        """Store one compact row per (day, symbol, direction, reason).

        The old version wrote a full payload on every scan for every blocked
        candidate, so the table (and the durable snapshot) grew without bound.
        """
        exists = connection.execute(
            "SELECT 1 FROM setup_skips WHERE trade_date = ? AND symbol = ? AND direction = ? AND skip_reason = ? LIMIT 1",
            (trade_date, symbol, direction, reason),
        ).fetchone()
        if exists:
            return
        small = {key: payload[key] for key in SignalRepository._SKIP_PAYLOAD_KEYS if key in payload}
        connection.execute(
            "INSERT INTO setup_skips(trade_date, captured_at_ist, symbol, direction, skip_reason, payload_json) VALUES (?, ?, ?, ?, ?, ?)",
            (trade_date, captured_at.isoformat(), symbol, direction, reason, json.dumps(small, sort_keys=True, separators=(",", ":"), default=str)),
        )

    def skip_reasons_for_date(self, trade_date: str) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT captured_at_ist, symbol, direction, skip_reason, payload_json FROM setup_skips WHERE trade_date = ? ORDER BY captured_at_ist, id",
                (trade_date,),
            ).fetchall()
        return [{**dict(row), "payload": json.loads(str(row["payload_json"]))} for row in rows]

    def _record_watch(self, connection: sqlite3.Connection, snapshot_id: int, trade_date: str, captured_at: datetime,
                      payload: dict[str, Any], plan: dict[str, Any], symbol: str, signal_name: str, direction: str) -> None:
        """Track a gate-failed candidate as a WATCH row: one per symbol+direction per day."""
        if direction not in {"BUY", "SELL"}:
            return
        settings = get_settings()
        minute = captured_at.hour * 60 + captured_at.minute
        start_h, start_m = self._clock(settings.entry_start)
        if minute < start_h * 60 + start_m or minute >= int(os.getenv("WATCH_END_MIN", str(15 * 60))):
            return
        if bool(payload.get("stale_price")) or float(payload.get("price_age_s") or 0) > settings.stale_price_s:
            return
        existing = connection.execute(
            "SELECT id, status FROM signal_events WHERE trade_date = ? AND archived = 0 AND tier = 'WATCH' AND symbol = ? AND direction = ? "
            "ORDER BY id DESC LIMIT 1", (trade_date, symbol, direction),
        ).fetchone()
        if existing is not None:
            if str(existing["status"]) in {"OPEN", "TG1_HIT"}:
                connection.execute(
                    "UPDATE signal_events SET current_price = ?, last_seen_at_ist = ?, payload_json = ? WHERE id = ?",
                    (float(payload.get("ltp") or 0), captured_at.isoformat(),
                     json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str), existing["id"]),
                )
            return
        if int(connection.execute("SELECT COUNT(*) FROM signal_events WHERE trade_date = ? AND archived = 0 AND tier = 'WATCH'",
                                  (trade_date,)).fetchone()[0]) >= int(os.getenv("WATCH_MAX_DAY", "60")):
            return
        connection.execute(
            """
            INSERT INTO signal_events (
                snapshot_id, trade_date, captured_at_ist, symbol, signal, direction, confidence, entry, stop_loss,
                target_1, target_2, risk_reward, risk_source, current_price, last_seen_at_ist, payload_json, tier
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'WATCH')
            """,
            (snapshot_id, trade_date, captured_at.isoformat(), symbol, signal_name, direction, int(payload.get("confidence") or 0),
             plan["entry"], plan["stop_loss"], plan["target_1"], plan["target_2"], plan["risk_reward"], plan["source"],
             float(payload.get("ltp") or 0), captured_at.isoformat(),
             json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)),
        )

    def record_scan(
        self,
        signals: list[dict[str, Any]],
        captured_at: datetime,
        *,
        source: str = "nse_oi_spurts",
        is_stale: bool = False,
        stop_loss_cooldown_minutes: int = 60,
    ) -> SnapshotWrite:
        """Persist one scan atomically, deduplicating identical minute snapshots."""
        captured_at = as_ist(captured_at)
        fingerprint = self._fingerprint(signals)
        trade_date = ist_trade_date(captured_at)
        captured_minute = captured_at.strftime("%Y-%m-%dT%H:%M")

        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO scan_snapshots (
                    trade_date, captured_at_ist, captured_minute_ist, source,
                    is_stale, fingerprint, signal_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade_date,
                    captured_at.isoformat(),
                    captured_minute,
                    source,
                    int(is_stale),
                    fingerprint,
                    len(signals),
                ),
            )
            if cursor.rowcount == 0:
                row = connection.execute(
                    """
                    SELECT id, signal_count FROM scan_snapshots
                    WHERE trade_date = ? AND captured_minute_ist = ? AND fingerprint = ?
                    """,
                    (trade_date, captured_minute, fingerprint),
                ).fetchone()
                if row is None:
                    raise RuntimeError("Snapshot deduplication did not return the existing row")
                return SnapshotWrite(snapshot_id=int(row["id"]), created=False, signal_count=int(row["signal_count"]))

            snapshot_id = int(cursor.lastrowid)
            for signal in signals:
                payload, plan = self._event_payload(signal, captured_at)
                symbol = str(payload.get("symbol") or "").upper()
                signal_name = str(payload.get("signal") or "NEUTRAL")
                direction = str(payload["direction"])
                existing = connection.execute(
                    """
                    SELECT id FROM signal_events
                    WHERE trade_date = ? AND archived = 0 AND tier = 'TRADE'
                      AND symbol = ? AND direction = ?
                      AND status IN ('OPEN', 'TG1_HIT')
                    ORDER BY id DESC LIMIT 1
                    """,
                    (trade_date, symbol, direction),
                ).fetchone()
                if existing is not None:
                    connection.execute(
                        "UPDATE signal_events SET current_price = ?, last_seen_at_ist = ?, payload_json = ? WHERE id = ?",
                        (float(payload.get("ltp") or 0), captured_at.isoformat(), json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str), existing["id"]),
                    )
                    continue
                if payload.get("confirmation_gate") == "FAILED":
                    # Not tradeable by the quality gate, but still tracked automatically (entry at first
                    # sighting, TG/SL graded by the server) so every signal gets a result and you can see
                    # whether the gate is blocking winners.
                    self._record_watch(connection, snapshot_id, trade_date, captured_at, payload, plan, symbol, signal_name, direction)
                    continue
                settings = get_settings()
                skip_reason = None
                if direction not in {"BUY", "SELL"}:
                    skip_reason = "invalid_direction"
                elif not self._inside_entry_window(captured_at):
                    skip_reason = "window"
                elif bool(payload.get("stale_price")) or float(payload.get("price_age_s") or 0) > settings.stale_price_s:
                    skip_reason = "stale_price"
                elif int(connection.execute("SELECT COUNT(*) FROM signal_events WHERE trade_date = ? AND archived = 0 AND tier = 'TRADE' AND status IN ('OPEN', 'TG1_HIT')", (trade_date,)).fetchone()[0] or 0) >= settings.max_open:
                    skip_reason = "cap"
                elif int(connection.execute("SELECT COUNT(*) FROM signal_events WHERE trade_date = ? AND archived = 0 AND tier = 'TRADE'", (trade_date,)).fetchone()[0] or 0) >= settings.max_setups_day:
                    skip_reason = "cap"
                elif self._realized_r(connection, trade_date) <= settings.daily_stop_r:
                    skip_reason = "daily_stop"
                if skip_reason:
                    self._record_skip(connection, trade_date=trade_date, captured_at=captured_at, symbol=symbol, direction=direction, reason=skip_reason, payload=payload)
                    continue
                cooldown_since = (captured_at - timedelta(minutes=stop_loss_cooldown_minutes)).isoformat()
                recent_stop = connection.execute(
                    """
                    SELECT 1 FROM signal_events
                    WHERE trade_date = ? AND archived = 0 AND tier = 'TRADE'
                      AND symbol = ? AND direction = ? AND status = 'SL_HIT'
                      AND closed_at_ist >= ?
                    LIMIT 1
                    """,
                    (trade_date, symbol, direction, cooldown_since),
                ).fetchone()
                if recent_stop is not None:
                    # Do not turn a stopped-out setup into an immediate repeat
                    # trade while the same directional pressure persists.
                    self._record_skip(connection, trade_date=trade_date, captured_at=captured_at, symbol=symbol, direction=direction, reason="cooldown", payload=payload)
                    continue
                daily_stop_count = connection.execute(
                    """
                    SELECT COUNT(*) FROM signal_events
                    WHERE trade_date = ? AND archived = 0 AND tier = 'TRADE' AND symbol = ?
                      AND direction = ? AND status = 'SL_HIT'
                    """,
                    (trade_date, symbol, direction),
                ).fetchone()[0]
                if int(daily_stop_count or 0) >= settings.max_sl_per_symbol_dir_day:
                    self._record_skip(connection, trade_date=trade_date, captured_at=captured_at, symbol=symbol, direction=direction, reason="cooldown", payload=payload)
                    continue
                opposite = "SELL" if direction == "BUY" else "BUY"
                opposite_row = connection.execute(
                    "SELECT status, closed_at_ist FROM signal_events WHERE trade_date = ? AND archived = 0 AND tier = 'TRADE' AND symbol = ? AND direction = ? ORDER BY id DESC LIMIT 1",
                    (trade_date, symbol, opposite),
                ).fetchone()
                if opposite_row is not None:
                    if str(opposite_row["status"]) in {"OPEN", "TG1_HIT"}:
                        self._record_skip(connection, trade_date=trade_date, captured_at=captured_at, symbol=symbol, direction=direction, reason="flip_gap", payload=payload)
                        continue
                    closed_at = opposite_row["closed_at_ist"]
                    if closed_at:
                        gap_minutes = (captured_at - datetime.fromisoformat(str(closed_at))).total_seconds() / 60
                        if gap_minutes < get_settings().cooldown_after_sl_min and gap_minutes < 30:
                            self._record_skip(connection, trade_date=trade_date, captured_at=captured_at, symbol=symbol, direction=direction, reason="flip_gap", payload=payload)
                            continue
                connection.execute(
                    """
                    INSERT INTO signal_events (
                        snapshot_id, trade_date, captured_at_ist, symbol, signal, direction,
                        confidence, entry, stop_loss, target_1, target_2, risk_reward,
                        risk_source, current_price, last_seen_at_ist, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot_id,
                        trade_date,
                        captured_at.isoformat(),
                        symbol,
                        signal_name,
                        direction,
                        int(payload.get("confidence") or 0),
                        plan["entry"],
                        plan["stop_loss"],
                        plan["target_1"],
                        plan["target_2"],
                        plan["risk_reward"],
                        plan["source"],
                        float(payload.get("ltp") or 0),
                        captured_at.isoformat(),
                        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
                    ),
                )
            return SnapshotWrite(snapshot_id=snapshot_id, created=True, signal_count=len(signals))

    def latest_snapshot_metadata(self) -> dict[str, Any] | None:
        """Return the newest persisted scan marker for restart-safe health data."""
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT id, captured_at_ist, is_stale, signal_count
                FROM scan_snapshots
                ORDER BY captured_at_ist DESC, id DESC
                LIMIT 1
                """
            ).fetchone()
        return dict(row) if row else None

    def update_open_events(self, signals: Iterable[dict[str, Any]], observed_at: datetime,
                           extra_prices: dict[str, float] | None = None) -> int:
        """Monitor same-day open events against live prices.

        Prices come from the published signals AND ``extra_prices`` (so a trade keeps being
        monitored after its symbol leaves the signal list). After TG1 the stop moves to
        entry (breakeven): TG1 followed by a reversal closes as BE_EXIT (+0.5R), not SL_HIT.
        Stops fill at the worse of level and observed price; targets fill at the level.
        """
        prices = {
            str(signal.get("symbol") or "").upper(): float(signal.get("ltp") or 0)
            for signal in signals
            if signal.get("symbol") and float(signal.get("ltp") or 0) > 0
        }
        for symbol, price in (extra_prices or {}).items():
            if symbol and float(price or 0) > 0:
                prices.setdefault(str(symbol).upper(), float(price))
        if not prices:
            return 0
        observed_at = as_ist(observed_at)
        trade_date = ist_trade_date(observed_at)
        updated = 0
        with self._connect() as connection:
            events = connection.execute(
                """
                SELECT id, symbol, direction, entry, stop_loss, target_1, target_2, max_target_hit
                FROM signal_events
                WHERE trade_date = ? AND archived = 0 AND status IN ('OPEN', 'TG1_HIT')
                """,
                (trade_date,),
            ).fetchall()
            for event in events:
                price = prices.get(str(event["symbol"]))
                if price is None:
                    continue
                direction = str(event["direction"])
                entry, stop = float(event["entry"]), float(event["stop_loss"])
                t1, t2 = float(event["target_1"]), float(event["target_2"])
                armed = int(event["max_target_hit"] or 0) >= 1
                if direction == "BUY":
                    hit2, hit1 = price >= t2, price >= t1
                    stop_hit = price <= (entry if armed else stop)
                else:
                    hit2, hit1 = price <= t2, price <= t1
                    stop_hit = price >= (entry if armed else stop)
                max_hit = max(int(event["max_target_hit"] or 0), 2 if hit2 else 1 if hit1 else 0)
                if hit2:
                    status, exit_price = "TG2_HIT", t2
                elif stop_hit:
                    level = entry if armed else stop
                    exit_price = min(level, price) if direction == "BUY" else max(level, price)
                    status = "BE_EXIT" if armed else "SL_HIT"
                else:
                    connection.execute(
                        "UPDATE signal_events SET current_price = ?, max_target_hit = ?, status = ? WHERE id = ?",
                        (price, max_hit, "TG1_HIT" if max_hit else "OPEN", event["id"]),
                    )
                    continue
                result_r = _closed_r(status, direction, entry, stop, t1, t2, exit_price, armed or hit2)
                connection.execute(
                    """
                    UPDATE signal_events
                    SET current_price = ?, exit_price = ?, max_target_hit = ?, status = ?, result = ?,
                        result_r = ?, result_source = 'live_monitor', closed_at_ist = ?
                    WHERE id = ?
                    """,
                    (price, exit_price, max_hit, status, status, result_r, observed_at.isoformat(), event["id"]),
                )
                updated += 1
        return updated

    @staticmethod
    def _day_end_result(direction: str, entry: Any, exit_price: Any) -> str:
        """Grade an event closed at market end from its directional P&L.

        WIN/LOSS/FLAT describe where the price went relative to entry by the
        close; a TG1 hit earlier in the session stays a WIN even if the close
        gave some of it back.  FLAT tolerates noise within 0.05% of entry.
        Returns 'EXPIRED' only when no usable exit price exists.
        """
        try:
            entry_price = float(entry or 0)
            close_price = float(exit_price or 0)
        except (TypeError, ValueError):
            return "EXPIRED"
        if entry_price <= 0 or close_price <= 0:
            return "EXPIRED"
        delta = close_price - entry_price if direction == "BUY" else entry_price - close_price
        if delta > entry_price * 0.0005:
            return "WIN"
        if delta < -entry_price * 0.0005:
            return "LOSS"
        return "FLAT"

    def expire_open_events(self, observed_at: datetime, *, result_source: str = "estimate") -> int:
        """Close unresolved current-day records after market hours.

        Unresolved events are graded WIN/LOSS/FLAT from their latest tracked
        price (callers should refresh prices just before the close).  Events
        that already hit a target keep WIN.  result_source records whether
        the exit came from a live estimate or the official bhavcopy close.
        """
        observed_at = as_ist(observed_at)
        closed_at = observed_at.isoformat()
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, direction, entry, stop_loss, target_1, target_2, max_target_hit, current_price, exit_price
                FROM signal_events
                WHERE trade_date = ? AND archived = 0 AND status IN ('OPEN', 'TG1_HIT')
                """,
                (ist_trade_date(observed_at),),
            ).fetchall()
            updated = 0
            for row in rows:
                exit_price = row["exit_price"] if row["exit_price"] is not None else row["current_price"]
                armed = int(row["max_target_hit"] or 0) >= 1
                if armed:
                    result = "WIN"
                else:
                    result = self._day_end_result(str(row["direction"]), row["entry"], exit_price)
                result_r = None
                if exit_price:
                    result_r = _closed_r("EXPIRED", str(row["direction"]), float(row["entry"]), float(row["stop_loss"]),
                                         float(row["target_1"]), float(row["target_2"]), float(exit_price), armed)
                connection.execute(
                    """
                    UPDATE signal_events
                    SET status = 'EXPIRED', result = ?, result_source = ?, exit_price = ?, result_r = ?, closed_at_ist = ?
                    WHERE id = ?
                    """,
                    (result, result_source, exit_price, result_r, closed_at, row["id"]),
                )
                updated += 1
            return updated

    def unresolved_event_symbols(self, trade_date: str) -> list[str]:
        """Distinct symbols with events still OPEN or TG1_HIT on one date."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT DISTINCT symbol FROM signal_events
                WHERE trade_date = ? AND archived = 0 AND status IN ('OPEN', 'TG1_HIT')
                ORDER BY symbol
                """,
                (trade_date,),
            ).fetchall()
        return [str(row["symbol"]) for row in rows]

    def latest_unresolved_event_trade_date(self) -> str | None:
        """Return the newest trade date with an unresolved event, if any."""
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT MAX(trade_date) AS trade_date FROM signal_events
                WHERE archived = 0 AND status IN ('OPEN', 'TG1_HIT')
                """
            ).fetchone()
        return str(row["trade_date"]) if row and row["trade_date"] else None

    def refresh_event_prices(self, prices: dict[str, float], observed_at: datetime) -> int:
        """Update tracked current_price for unresolved events from a final poll.

        Status is unchanged; only the tracked price moves, so 15:31 grading
        uses the real ~15:30 market instead of the last time the symbol
        happened to appear in the published feed.
        """
        if not prices:
            return 0
        observed_at = as_ist(observed_at)
        updated = 0
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, symbol FROM signal_events
                WHERE trade_date = ? AND archived = 0 AND status IN ('OPEN', 'TG1_HIT')
                """,
                (ist_trade_date(observed_at),),
            ).fetchall()
            for row in rows:
                price = prices.get(str(row["symbol"]))
                if price is None or float(price) <= 0:
                    continue
                connection.execute(
                    "UPDATE signal_events SET current_price = ? WHERE id = ?",
                    (float(price), row["id"]),
                )
                updated += 1
        return updated

    def regrade_with_bhavcopy_close(self, trade_date: str) -> int:
        """Re-grade estimated day-end results with the official NSE close.

        After the 18:10 bhavcopy ingestion, events whose exit price was an
        estimate get the true closing price and a recomputed verdict.  Rows
        that settled on a target/stop intraday keep their real exit price.
        """
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, symbol, direction, entry, max_target_hit
                FROM signal_events
                WHERE trade_date = ? AND archived = 0
                  AND status IN ('EXPIRED')
                  AND result_source = 'estimate'
                  AND exit_price IS NOT NULL
                  AND COALESCE(max_target_hit, 0) < 1
                """,
                (trade_date,),
            ).fetchall()
            updated = 0
            for row in rows:
                bar = connection.execute(
                    """
                    SELECT close FROM daily_equity_bars
                    WHERE trade_date = ? AND symbol = ? AND close > 0
                    """,
                    (trade_date, str(row["symbol"])),
                ).fetchone()
                if not bar:
                    continue
                close_price = float(bar[0])
                if int(row["max_target_hit"] or 0) >= 1:
                    result = "WIN"
                else:
                    result = self._day_end_result(str(row["direction"]), row["entry"], close_price)
                connection.execute(
                    """
                    UPDATE signal_events
                    SET exit_price = ?, result = ?, result_source = 'bhavcopy_close'
                    WHERE id = ?
                    """,
                    (close_price, result, row["id"]),
                )
                updated += 1
            return updated

    def history_for_date(self, trade_date: str, *, limit: int = 1000) -> tuple[list[dict[str, Any]], int]:
        """Return visible events and the total count for one IST trading date."""
        with self._connect() as connection:
            total = int(
                connection.execute(
                    "SELECT COUNT(*) FROM signal_events WHERE trade_date = ? AND archived = 0",
                    (trade_date,),
                ).fetchone()[0]
            )
            rows = connection.execute(
                """
                SELECT * FROM signal_events
                WHERE trade_date = ? AND archived = 0
                ORDER BY captured_at_ist DESC, id DESC
                LIMIT ?
                """,
                (trade_date, limit),
            ).fetchall()
        events: list[dict[str, Any]] = []
        for row in rows:
            payload = json.loads(str(row["payload_json"]))
            payload.update(
                {
                    "id": int(row["id"]),
                    "tradeDate": row["trade_date"],
                    "captured_at_ist": row["captured_at_ist"],
                    "direction": row["direction"],
                    "entry": row["entry"],
                    "sl": row["stop_loss"],
                    "tg1": row["target_1"],
                    "tg2": row["target_2"],
                    "risk_reward": row["risk_reward"],
                    "risk_source": row["risk_source"],
                    "currentLTP": row["current_price"],
                    "exitPrice": row["exit_price"],
                    "closed_at_ist": row["closed_at_ist"],
                    "max_target_hit": row["max_target_hit"],
                    "status": row["status"],
                    "result": row["result"],
                    "result_source": row["result_source"],
                    "result_r": row["result_r"],
                    "tier": row["tier"],
                }
            )
            events.append(payload)
        return events, total

    def performance_for_date(self, trade_date: str, tier: str = "TRADE") -> dict[str, int | float]:
        """Return compact, server-owned same-day outcome metrics."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT status, result, max_target_hit, risk_reward, entry, exit_price, direction, result_r
                FROM signal_events WHERE trade_date = ? AND archived = 0 AND tier = ?
                """,
                (trade_date, tier),
            ).fetchall()
        counts = {"OPEN": 0, "TG1_HIT": 0, "TG2_HIT": 0, "SL_HIT": 0, "BE_EXIT": 0, "EXPIRED": 0}
        pnl_points = 0.0
        for row in rows:
            status = str(row["status"])
            counts[status] = counts.get(status, 0) + 1
            if row["exit_price"] is not None:
                delta = float(row["exit_price"]) - float(row["entry"])
                pnl_points += delta if row["direction"] == "BUY" else -delta
        open_events = counts["OPEN"] + counts["TG1_HIT"]
        closed = len(rows) - open_events
        tp1_hits = sum(1 for row in rows if int(row["max_target_hit"]) >= 1)
        tp2_hits = sum(1 for row in rows if int(row["max_target_hit"]) >= 2)
        wins = tp1_hits
        losses = counts["SL_HIT"]
        graded = 0
        for row in rows:
            if row["status"] not in ("OPEN", "TG1_HIT") and row["exit_price"] is not None and int(row["max_target_hit"] or 0) < 1:
                if str(row["result"]) == "LOSS":
                    losses += 1
                if str(row["result"]) in ("WIN", "LOSS"):
                    graded += 1
        return {
            "signals_generated": len(rows),
            "wins": wins,
            "losses": counts["SL_HIT"],
            "tp1_hits": tp1_hits,
            "tp2_hits": tp2_hits,
            "sl_hits": counts["SL_HIT"],
            "be_exits": counts["BE_EXIT"],
            "total_r": round(sum(float(row["result_r"]) for row in rows if row["result_r"] is not None), 2),
            "open": open_events,
            "expired": counts["EXPIRED"],
            "graded": graded,
            "accuracy": round((wins / closed) * 100, 2) if closed else 0.0,
            "average_rr": round(
                sum(float(row["risk_reward"]) for row in rows) / len(rows), 2
            ) if rows else 0.0,
            "pnl_points": round(pnl_points, 2),
        }

    def archive_previous_history(self, today: str, retention_days: int) -> int:
        """Hide older history from normal endpoints and prune old archives."""
        cutoff = (datetime.fromisoformat(today).date() - timedelta(days=retention_days)).isoformat()
        with self._connect() as connection:
            archived = connection.execute(
                "UPDATE signal_events SET archived = 1 WHERE trade_date < ? AND archived = 0",
                (today,),
            ).rowcount
            connection.execute("UPDATE scan_snapshots SET archived = 1 WHERE trade_date < ?", (today,))
            connection.execute("DELETE FROM signal_events WHERE trade_date < ?", (cutoff,))
            connection.execute("DELETE FROM scan_snapshots WHERE trade_date < ?", (cutoff,))
            return archived

    def vacuum(self) -> None:
        """Compact the local SQLite file during an explicit maintenance action."""
        with self._connect() as connection:
            connection.execute("VACUUM")
