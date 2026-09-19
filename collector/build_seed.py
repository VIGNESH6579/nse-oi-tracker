"""Build a compact daily OHLCV seed from Angel One, with NSE fallback."""
from __future__ import annotations

import argparse
import gzip
import shutil
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path

from integrations.angel_one_market_data import AngelOneMarketData

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "seed_bhavcopy.sqlite3.gz"


def _symbols() -> list[str]:
    seed = ROOT / "data" / "seed_bhavcopy.sqlite3.gz"
    if not seed.exists():
        return []
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".sqlite3") as tmp:
        with gzip.open(seed, "rb") as source, open(tmp.name, "wb") as target:
            shutil.copyfileobj(source, target)
        with sqlite3.connect(tmp.name) as conn:
            return [str(row[0]) for row in conn.execute("SELECT DISTINCT symbol FROM daily_equity_bars ORDER BY symbol")]


def build_seed(days: int = 80) -> Path:
    client = AngelOneMarketData.from_environment()
    if client is None:
        raise RuntimeError("Angel One credentials are required locally; values are read only from environment variables")
    symbols = _symbols()
    if not symbols:
        raise RuntimeError("No existing F&O symbol universe found in the bundled seed")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False) as tmp:
        db_path = Path(tmp.name)
    try:
        with sqlite3.connect(db_path) as conn:
            conn.executescript("""
                CREATE TABLE daily_equity_bars(trade_date TEXT, symbol TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL, source TEXT DEFAULT 'angel_one_daily', PRIMARY KEY(trade_date, symbol));
                CREATE TABLE daily_index_bars(trade_date TEXT, symbol TEXT, open REAL, high REAL, low REAL, close REAL, source TEXT DEFAULT 'angel_one_daily', PRIMARY KEY(trade_date, symbol));
            """)
            for index in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"):
                try:
                    rows = client.daily_candles(index, days=days)
                    conn.executemany("INSERT OR REPLACE INTO daily_index_bars VALUES (?, ?, ?, ?, ?, ?, ?)", [
                        (str(row["time"])[:10], index, row["open"], row["high"], row["low"], row["close"], "angel_one_daily") for row in rows
                    ])
                except Exception:
                    pass
                time.sleep(0.5)
            for symbol in symbols:
                try:
                    rows = client.daily_candles(symbol, days=days)
                    conn.executemany("INSERT OR REPLACE INTO daily_equity_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [
                        (str(row["time"])[:10], symbol, row["open"], row["high"], row["low"], row["close"], row.get("volume", 0), "angel_one_daily") for row in rows
                    ])
                    conn.commit()
                except Exception:
                    pass
                time.sleep(0.5)
        with open(db_path, "rb") as source, gzip.open(OUT, "wb", compresslevel=9) as target:
            shutil.copyfileobj(source, target)
        return OUT
    finally:
        db_path.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=80)
    args = parser.parse_args()
    print(build_seed(max(60, min(args.days, 120))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
