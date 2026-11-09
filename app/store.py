import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

class Store:
    def __init__(self, path: str):
        self.path = path
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with self.connect() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, company TEXT,
                category TEXT NOT NULL, impact TEXT NOT NULL, headline TEXT NOT NULL,
                details TEXT, filed_at TEXT, source_url TEXT, created_at TEXT NOT NULL,
                notified INTEGER NOT NULL DEFAULT 0
            )""")
            db.execute("CREATE INDEX IF NOT EXISTS idx_events_filed_at ON events(filed_at DESC)")
            db.execute("CREATE INDEX IF NOT EXISTS idx_events_symbol ON events(symbol)")
            db.commit()

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=15)
        db.row_factory = sqlite3.Row
        try:
            yield db
        finally:
            db.close()

    def insert(self, item: dict) -> bool:
        with self.connect() as db:
            cur = db.execute("""INSERT OR IGNORE INTO events
                (event_id,symbol,company,category,impact,headline,details,filed_at,source_url,created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (item["event_id"], item["symbol"], item.get("company",""), item["category"],
                 item["impact"], item["headline"], item.get("details",""), item.get("filed_at"),
                 item.get("source_url",""), datetime.now(timezone.utc).isoformat()))
            db.commit()
            return cur.rowcount == 1

    def mark_notified(self, event_id: str):
        with self.connect() as db:
            db.execute("UPDATE events SET notified=1 WHERE event_id=?", (event_id,))
            db.commit()

    def list_events(self, limit=100, symbol=None):
        with self.connect() as db:
            if symbol:
                rows = db.execute("SELECT * FROM events WHERE symbol=? ORDER BY COALESCE(filed_at,created_at) DESC LIMIT ?", (symbol.upper(),limit)).fetchall()
            else:
                rows = db.execute("SELECT * FROM events ORDER BY COALESCE(filed_at,created_at) DESC LIMIT ?", (limit,)).fetchall()
            return [dict(row) for row in rows]

    def count(self):
        with self.connect() as db:
            return db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
