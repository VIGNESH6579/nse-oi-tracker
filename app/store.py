import os,sqlite3
from contextlib import contextmanager
from datetime import datetime,timezone

class Store:
    def __init__(self,path):
        self.path=path
        parent=os.path.dirname(path)
        if parent: os.makedirs(parent,exist_ok=True)
        with self.connect() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS events (
              event_id TEXT PRIMARY KEY,symbol TEXT NOT NULL,company TEXT,category TEXT NOT NULL,impact TEXT NOT NULL,
              headline TEXT NOT NULL,details TEXT,filed_at TEXT,source_url TEXT,created_at TEXT NOT NULL,
              notified INTEGER NOT NULL DEFAULT 0,event_status TEXT,rationale TEXT,source_name TEXT,feed_type TEXT)""")
            cols={r[1] for r in db.execute("PRAGMA table_info(events)").fetchall()}
            for name,typ in {"event_status":"TEXT","rationale":"TEXT","source_name":"TEXT","feed_type":"TEXT"}.items():
                if name not in cols: db.execute(f"ALTER TABLE events ADD COLUMN {name} {typ}")
            db.execute("CREATE INDEX IF NOT EXISTS idx_events_filed_at ON events(filed_at DESC)")
            db.execute("CREATE INDEX IF NOT EXISTS idx_events_symbol ON events(symbol)")
            db.execute("CREATE INDEX IF NOT EXISTS idx_events_status ON events(event_status)")
            db.execute("""CREATE TABLE IF NOT EXISTS market_quotes (
              id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, ltp REAL NOT NULL,
              change_pct REAL, open REAL, high REAL, low REAL, previous_close REAL,
              volume INTEGER, source TEXT NOT NULL, fetched_at TEXT NOT NULL)""")
            db.execute("CREATE INDEX IF NOT EXISTS idx_market_quotes_symbol_time ON market_quotes(symbol,fetched_at DESC)")
            db.commit()
    @contextmanager
    def connect(self):
        db=sqlite3.connect(self.path,timeout=15); db.row_factory=sqlite3.Row
        try: yield db
        finally: db.close()
    def insert(self,x):
        with self.connect() as db:
            cur=db.execute("""INSERT OR IGNORE INTO events
            (event_id,symbol,company,category,impact,headline,details,filed_at,source_url,created_at,event_status,rationale,source_name,feed_type)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (x["event_id"],x["symbol"],x.get("company",""),x["category"],x["impact"],x["headline"],x.get("details",""),
             x.get("filed_at"),x.get("source_url",""),datetime.now(timezone.utc).isoformat(),x.get("event_status"),
             x.get("rationale"),x.get("source_name"),x.get("feed_type")))
            db.commit(); return cur.rowcount==1
    def mark_notified(self,event_id):
        with self.connect() as db: db.execute("UPDATE events SET notified=1 WHERE event_id=?",(event_id,)); db.commit()
    def pending_notifications(self,limit=50):
        with self.connect() as db: return [dict(x) for x in db.execute("SELECT * FROM events WHERE notified=0 ORDER BY COALESCE(filed_at,created_at) DESC LIMIT ?",(limit,)).fetchall()]
    def list_events(self,limit=100,symbol=None,status=None):
        sql="SELECT * FROM events WHERE 1=1"; args=[]
        if symbol: sql+=" AND symbol=?"; args.append(symbol.upper())
        if status: sql+=" AND event_status=?"; args.append(status)
        sql+=" ORDER BY COALESCE(filed_at,created_at) DESC LIMIT ?"; args.append(limit)
        with self.connect() as db: return [dict(x) for x in db.execute(sql,args).fetchall()]
    def save_market_quotes(self, quotes):
        if not quotes: return 0
        with self.connect() as db:
            db.executemany("""INSERT INTO market_quotes
              (symbol,ltp,change_pct,open,high,low,previous_close,volume,source,fetched_at)
              VALUES (?,?,?,?,?,?,?,?,?,?)""",
              [(q["symbol"],q["ltp"],q.get("change_pct"),q.get("open"),q.get("high"),q.get("low"),
                q.get("previous_close"),q.get("volume"),q.get("source","Angel One SmartAPI"),q["fetched_at"])
               for q in quotes.values()])
            db.execute("DELETE FROM market_quotes WHERE julianday(fetched_at) < julianday('now','-14 days')")
            db.commit()
        return len(quotes)
    def latest_market_quotes(self):
        with self.connect() as db:
            rows=db.execute("""SELECT q.* FROM market_quotes q
              JOIN (SELECT symbol,MAX(fetched_at) AS latest FROM market_quotes GROUP BY symbol) x
              ON q.symbol=x.symbol AND q.fetched_at=x.latest ORDER BY q.symbol""").fetchall()
            return [dict(x) for x in rows]
    def count(self):
        with self.connect() as db: return db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
