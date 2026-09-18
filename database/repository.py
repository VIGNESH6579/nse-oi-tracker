"""SQLite repository for immutable scan snapshots and today's signal history."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

from signal_engine.risk import build_risk_plan
from utils.time import as_ist, ist_trade_date


@dataclass(frozen=True, slots=True)
class SnapshotWrite:
    snapshot_id: int
    created: bool
    signal_count: int


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

    def daily_equity_bar_summary(self) -> dict[str, Any]:
        """Compact technical-data freshness diagnostics for /api/health."""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS bars, COUNT(DISTINCT symbol) AS symbols, MAX(trade_date) AS latest_trade_date FROM daily_equity_bars"
            ).fetchone()
        return dict(row)

    def latest_daily_equity_trade_date(self) -> str | None:
        """Return the newest stored daily equity-bar date, if any."""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT MAX(trade_date) AS trade_date FROM daily_equity_bars"
            ).fetchone()
        return str(row["trade_date"]) if row and row["trade_date"] else None

    def record_option_chain_snapshot(self, analysis: dict[str, Any], captured_at: datetime) -> bool:
        """Store one immutable public option-chain observation per symbol/minute."""
        captured_at = as_ist(captured_at)
        symbol = str(analysis.get("symbol") or "").upper().strip()
        expiry = str(analysis.get("expiry") or "UNKNOWN")
        if not symbol or float(analysis.get("pcr") or 0) < 0:
            return False
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO option_chain_snapshots (
                    symbol, expiry, captured_at_ist, captured_minute_ist, spot, pcr,
                    max_pain, total_ce_oi, total_pe_oi, oi_levels_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol,
                    expiry,
                    captured_at.isoformat(),
                    captured_at.strftime("%Y-%m-%dT%H:%M"),
                    float(analysis.get("atm_strike") or 0),
                    float(analysis.get("pcr") or 0),
                    float(analysis.get("max_pain") or 0),
                    int(analysis.get("total_ce_oi") or 0),
                    int(analysis.get("total_pe_oi") or 0),
                    json.dumps(analysis.get("oi_levels") or {}, sort_keys=True),
                ),
            )
        return cursor.rowcount > 0

    def option_chain_history(self, symbol: str, *, limit: int = 200) -> list[dict[str, Any]]:
        """Return chronological PCR/max-pain observations for a symbol."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT captured_at_ist, expiry, spot, pcr, max_pain, total_ce_oi, total_pe_oi, oi_levels_json
                FROM option_chain_snapshots
                WHERE symbol = ?
                ORDER BY captured_at_ist DESC
                LIMIT ?
                """,
                (symbol.upper().strip(), limit),
            ).fetchall()
        return [
            {
                **dict(row),
                "oi_levels": json.loads(str(row["oi_levels_json"])),
            }
            for row in reversed(rows)
        ]

    def backtest_events(self, start_date: str, end_date: str) -> list[dict[str, Any]]:
        """Return stored candidate events, including archive, for transparent analysis."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT captured_at_ist, trade_date, symbol, signal, direction, confidence,
                       entry, stop_loss, target_1, target_2, risk_reward, risk_source,
                       current_price, exit_price, max_target_hit, status, result, payload_json
                FROM signal_events
                WHERE trade_date BETWEEN ? AND ?
                ORDER BY captured_at_ist ASC, id ASC
                """,
                (start_date, end_date),
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

    def upsert_corporate_announcements(self, announcements: Iterable[dict[str, Any]]) -> int:
        """Persist public NSE disclosure metadata; never download attachment content."""
        rows = [
            (
                str(item["announcement_id"]), str(item["symbol"]).upper(), str(item["published_at"]),
                str(item["category"]), str(item["title"]), item.get("attachment_url"),
                str(item["event_risk"]), json.dumps(item.get("risk_terms") or []),
            )
            for item in announcements
        ]
        if not rows:
            return 0
        with self._connect() as connection:
            connection.executemany(
                """
                INSERT INTO corporate_announcements (
                    announcement_id, symbol, published_at, category, title, attachment_url,
                    event_risk, risk_terms_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(announcement_id) DO UPDATE SET
                    symbol=excluded.symbol, published_at=excluded.published_at,
                    category=excluded.category, title=excluded.title,
                    attachment_url=excluded.attachment_url, event_risk=excluded.event_risk,
                    risk_terms_json=excluded.risk_terms_json, ingested_at_utc=CURRENT_TIMESTAMP
                """,
                rows,
            )
        return len(rows)

    def recent_corporate_announcements(self, *, symbol: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        """Return stored public disclosure metadata, newest first."""
        query = "SELECT * FROM corporate_announcements"
        params: tuple[Any, ...] = ()
        if symbol:
            query += " WHERE symbol = ?"
            params = (symbol.upper().strip(),)
        query += " ORDER BY published_at DESC LIMIT ?"
        with self._connect() as connection:
            rows = connection.execute(query, (*params, limit)).fetchall()
        return [{**dict(row), "risk_terms": json.loads(str(row["risk_terms_json"]))} for row in rows]

    def upsert_participant_oi(self, rows: Iterable[dict[str, Any]]) -> int:
        """Persist one public end-of-day participant OI report by report date."""
        records = [
            (
                str(row["report_date"]), str(row["participant"]).upper(),
                int(row.get("net_index_futures") or 0), int(row.get("net_stock_futures") or 0),
                json.dumps(row.get("measures") or {}, sort_keys=True),
                str(row.get("source") or "NSE F&O participant-wise OI EOD report"),
            )
            for row in rows
        ]
        if not records:
            return 0
        with self._connect() as connection:
            connection.executemany(
                """
                INSERT INTO participant_oi_reports (
                    report_date, participant, net_index_futures, net_stock_futures, measures_json, source
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(report_date, participant) DO UPDATE SET
                    net_index_futures=excluded.net_index_futures,
                    net_stock_futures=excluded.net_stock_futures,
                    measures_json=excluded.measures_json, source=excluded.source,
                    ingested_at_utc=CURRENT_TIMESTAMP
                """,
                records,
            )
        return len(records)

    def latest_participant_oi(self) -> dict[str, Any]:
        """Return the latest complete public EOD report, preserving its date."""
        with self._connect() as connection:
            date_row = connection.execute("SELECT MAX(report_date) AS report_date FROM participant_oi_reports").fetchone()
            report_date = date_row["report_date"] if date_row else None
            if not report_date:
                return {"report_date": None, "participants": []}
            rows = connection.execute(
                """
                SELECT report_date, participant, net_index_futures, net_stock_futures, measures_json, source
                FROM participant_oi_reports WHERE report_date = ? ORDER BY participant
                """, (report_date,)
            ).fetchall()
        return {
            "report_date": str(report_date),
            "participants": [{**dict(row), "measures": json.loads(str(row["measures_json"]))} for row in rows],
        }

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

    def record_scan(
        self,
        signals: list[dict[str, Any]],
        captured_at: datetime,
        *,
        source: str = "nse_oi_spurts",
        is_stale: bool = False,
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
                connection.execute(
                    """
                    INSERT INTO signal_events (
                        snapshot_id, trade_date, captured_at_ist, symbol, signal, direction,
                        confidence, entry, stop_loss, target_1, target_2, risk_reward,
                        risk_source, current_price, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot_id,
                        trade_date,
                        captured_at.isoformat(),
                        str(payload.get("symbol") or "").upper(),
                        str(payload.get("signal") or "NEUTRAL"),
                        str(payload["direction"]),
                        int(payload.get("confidence") or 0),
                        plan["entry"],
                        plan["stop_loss"],
                        plan["target_1"],
                        plan["target_2"],
                        plan["risk_reward"],
                        plan["source"],
                        float(payload.get("ltp") or 0),
                        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
                    ),
                )
            return SnapshotWrite(snapshot_id=snapshot_id, created=True, signal_count=len(signals))

    def update_open_events(self, signals: Iterable[dict[str, Any]], observed_at: datetime) -> int:
        """Mark target/stop outcomes for visible, same-day open events."""
        prices = {
            str(signal.get("symbol") or "").upper(): float(signal.get("ltp") or 0)
            for signal in signals
            if signal.get("symbol") and float(signal.get("ltp") or 0) > 0
        }
        if not prices:
            return 0
        observed_at = as_ist(observed_at)
        trade_date = ist_trade_date(observed_at)
        updated = 0
        with self._connect() as connection:
            events = connection.execute(
                """
                SELECT id, symbol, direction, stop_loss, target_1, target_2, max_target_hit
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
                if direction == "BUY":
                    target_hit = 2 if price >= event["target_2"] else 1 if price >= event["target_1"] else 0
                    stop_hit = price <= event["stop_loss"]
                else:
                    target_hit = 2 if price <= event["target_2"] else 1 if price <= event["target_1"] else 0
                    stop_hit = price >= event["stop_loss"]
                max_target_hit = max(int(event["max_target_hit"]), target_hit)
                status = "TG2_HIT" if target_hit == 2 else "SL_HIT" if stop_hit else "TG1_HIT" if max_target_hit else "OPEN"
                if status in {"OPEN", "TG1_HIT"}:
                    connection.execute(
                        "UPDATE signal_events SET current_price = ?, max_target_hit = ?, status = ? WHERE id = ?",
                        (price, max_target_hit, status, event["id"]),
                    )
                    continue
                connection.execute(
                    """
                    UPDATE signal_events
                    SET current_price = ?, exit_price = ?, max_target_hit = ?, status = ?, result = ?, closed_at_ist = ?
                    WHERE id = ?
                    """,
                    (price, price, max_target_hit, status, status, observed_at.isoformat(), event["id"]),
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
                SELECT id, direction, entry, max_target_hit, current_price, exit_price
                FROM signal_events
                WHERE trade_date = ? AND archived = 0 AND status IN ('OPEN', 'TG1_HIT')
                """,
                (ist_trade_date(observed_at),),
            ).fetchall()
            updated = 0
            for row in rows:
                exit_price = row["exit_price"] if row["exit_price"] is not None else row["current_price"]
                if int(row["max_target_hit"] or 0) >= 1:
                    result = "WIN"
                else:
                    result = self._day_end_result(str(row["direction"]), row["entry"], exit_price)
                connection.execute(
                    """
                    UPDATE signal_events
                    SET status = 'EXPIRED', result = ?, result_source = ?, exit_price = ?, closed_at_ist = ?
                    WHERE id = ?
                    """,
                    (result, result_source, exit_price, closed_at, row["id"]),
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
                }
            )
            events.append(payload)
        return events, total

    def performance_for_date(self, trade_date: str) -> dict[str, int | float]:
        """Return compact, server-owned same-day outcome metrics."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT status, result, max_target_hit, risk_reward, entry, exit_price, direction
                FROM signal_events WHERE trade_date = ? AND archived = 0
                """,
                (trade_date,),
            ).fetchall()
        counts = {"OPEN": 0, "TG1_HIT": 0, "TG2_HIT": 0, "SL_HIT": 0, "EXPIRED": 0}
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
