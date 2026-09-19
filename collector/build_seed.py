"""Build a compact daily OHLCV seed from NSE bhavcopy, with optional Angel fallback."""
from __future__ import annotations

import argparse
import gzip
import shutil
import sqlite3
import tempfile
import time
from datetime import date, timedelta
from pathlib import Path

from collector.backfill import recent_nse_trading_dates, bundled_fno_symbols
from collector.bhavcopy import collect_equity_bhavcopy
from collector.index_backfill import INDEX_TYPES
from app.nse_fetcher import fetch_index_history
from integrations.angel_one_market_data import AngelOneMarketData

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "seed_bhavcopy.sqlite3.gz"


def build_seed(days: int = 80) -> Path:
    days = max(60, min(int(days), 120))
    symbols = bundled_fno_symbols()
    if not symbols:
        raise RuntimeError("No compact F&O symbol universe found in the bundled seed")
    end = date.today() - timedelta(days=1)
    dates = recent_nse_trading_dates(end, days)
    angel = AngelOneMarketData.from_environment()
    with tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False) as tmp:
        db_path = Path(tmp.name)
    try:
        with sqlite3.connect(db_path) as conn:
            conn.executescript("""
                CREATE TABLE daily_equity_bars(trade_date TEXT, symbol TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL, source TEXT DEFAULT 'nse_bhavcopy', PRIMARY KEY(trade_date, symbol));
                CREATE TABLE daily_index_bars(trade_date TEXT, symbol TEXT, open REAL, high REAL, low REAL, close REAL, source TEXT DEFAULT 'nse_index_history', PRIMARY KEY(trade_date, symbol));
            """)
            for day in dates:
                rows = []
                try:
                    rows = [row for row in collect_equity_bhavcopy(day) if str(row.get("symbol") or "").upper() in symbols]
                except Exception:
                    if angel is not None:
                        try:
                            rows = [{**row, "trade_date": str(row["time"])[:10], "source": "angel_one_daily"} for symbol in symbols for row in angel.daily_candles(symbol, days=1)]
                        except Exception:
                            rows = []
                conn.executemany("INSERT OR REPLACE INTO daily_equity_bars(trade_date,symbol,open,high,low,close,volume,source) VALUES(?,?,?,?,?,?,?,?)", [
                    (row["trade_date"], str(row["symbol"]).upper(), row["open"], row["high"], row["low"], row["close"], row.get("volume", 0), row.get("source", "nse_bhavcopy")) for row in rows
                ])
                conn.commit()
                time.sleep(0.1)
            for symbol, index_name in INDEX_TYPES.items():
                try:
                    rows = fetch_index_history(index_name, dates[-1], dates[0])
                except Exception:
                    rows = []
                conn.executemany("INSERT OR REPLACE INTO daily_index_bars(trade_date,symbol,open,high,low,close,source) VALUES(?,?,?,?,?,?,?)", [
                    (row["trade_date"], symbol, row["open"], row["high"], row["low"], row["close"], "nse_index_history") for row in rows
                ])
        OUT.parent.mkdir(parents=True, exist_ok=True)
        with open(db_path, "rb") as source, gzip.open(OUT, "wb", compresslevel=9) as target:
            shutil.copyfileobj(source, target)
        return OUT
    finally:
        db_path.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=80)
    args = parser.parse_args()
    print(build_seed(args.days))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
