import os,sqlite3,re,hashlib
from contextlib import contextmanager
from datetime import datetime,timezone

def _event_key(symbol,category,headline,event_date=None):
    # A duplicate can arrive through a different RSS feed/category.
    normalized=re.sub(r"[^a-z0-9]+"," ",(headline or "").lower()).strip()
    normalized=re.sub(r"\s+"," ",normalized)
    return hashlib.sha256((str(symbol).upper() + "|" + normalized + "|" + str(event_date or "")).encode()).hexdigest()

class Store:
    def __init__(self,path):
        self.path=path
        parent=os.path.dirname(path)
        if parent: os.makedirs(parent,exist_ok=True)
        with self.connect() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS events (
              event_id TEXT PRIMARY KEY,symbol TEXT NOT NULL,company TEXT,category TEXT NOT NULL,impact TEXT NOT NULL,
              headline TEXT NOT NULL,details TEXT,filed_at TEXT,source_url TEXT,created_at TEXT NOT NULL,
              notified INTEGER NOT NULL DEFAULT 0,event_status TEXT,rationale TEXT,source_name TEXT,feed_type TEXT,
              event_date TEXT,dedupe_key TEXT)""")
            cols={r[1] for r in db.execute("PRAGMA table_info(events)").fetchall()}
            for name,typ in {"event_status":"TEXT","rationale":"TEXT","source_name":"TEXT","feed_type":"TEXT",
                             "event_date":"TEXT","dedupe_key":"TEXT"}.items():
                if name not in cols: db.execute(f"ALTER TABLE events ADD COLUMN {name} {typ}")
            # Rebuild keys for every stored event so legacy category-based keys
            # are migrated to the feed-independent symbol + headline fingerprint.
            rows=db.execute("SELECT event_id,symbol,category,headline,event_date FROM events").fetchall()
            for row in rows:
                db.execute("UPDATE events SET dedupe_key=? WHERE event_id=?",
                           (_event_key(row["symbol"],row["category"],row["headline"],row["event_date"]),row["event_id"]))
            db.execute("CREATE INDEX IF NOT EXISTS idx_events_filed_at ON events(filed_at DESC)")
            db.execute("CREATE INDEX IF NOT EXISTS idx_events_symbol ON events(symbol)")
            db.execute("CREATE INDEX IF NOT EXISTS idx_events_status ON events(event_status)")
            db.execute("CREATE INDEX IF NOT EXISTS idx_events_dedupe ON events(dedupe_key)")
            db.execute("""CREATE TABLE IF NOT EXISTS fno_universe (
              symbol TEXT PRIMARY KEY, updated_at TEXT NOT NULL)""")
            # Remove the legacy quote table from the previous broker-based app.
            db.execute("DROP TABLE IF EXISTS market_quotes")
            db.commit()
    @contextmanager
    def connect(self):
        db=sqlite3.connect(self.path,timeout=15); db.row_factory=sqlite3.Row
        try: yield db
        finally: db.close()
    def insert(self,x):
        key=x.get("dedupe_key") or _event_key(x["symbol"],x["category"],x["headline"],x.get("event_date"))
        with self.connect() as db:
            duplicate=db.execute("""SELECT 1 FROM events
              WHERE dedupe_key=? AND julianday(COALESCE(filed_at,created_at))>=julianday('now','-3 days')
              LIMIT 1""",(key,)).fetchone()
            if duplicate:
                return False
            cur=db.execute("""INSERT OR IGNORE INTO events
            (event_id,symbol,company,category,impact,headline,details,filed_at,source_url,created_at,event_status,
             rationale,source_name,feed_type,event_date,dedupe_key)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (x["event_id"],x["symbol"],x.get("company",x["symbol"]),x["category"],x["impact"],x["headline"],x.get("details",""),
             x.get("filed_at"),x.get("source_url",""),datetime.now(timezone.utc).isoformat(),x.get("event_status"),
             x.get("rationale"),x.get("source_name"),x.get("feed_type"),x.get("event_date"),key))
            db.commit(); return cur.rowcount==1
    def mark_notified(self,event_id):
        with self.connect() as db: db.execute("UPDATE events SET notified=1 WHERE event_id=?",(event_id,)); db.commit()
    def pending_notifications(self,limit=50):
        with self.connect() as db:
            rows=[dict(x) for x in db.execute("SELECT * FROM events WHERE notified=0 AND julianday(created_at)>=julianday('now','-1 day') ORDER BY COALESCE(filed_at,created_at) DESC LIMIT ?",(max(limit*5,limit),)).fetchall()]
        unique=[]; seen={}
        for row in rows:
            key=row.get("dedupe_key") or _event_key(row["symbol"],row["category"],row["headline"])
            stamp=row.get("filed_at") or row.get("created_at")
            try:
                current=datetime.fromisoformat(str(stamp).replace("Z","+00:00"))
                if current.tzinfo is None: current=current.replace(tzinfo=timezone.utc)
            except Exception:
                current=None
            prior=seen.get(key,[])
            if any(current is None or previous is None or abs((current-previous).total_seconds())<=72*3600 for previous in prior):
                continue
            seen.setdefault(key,[]).append(current)
            unique.append(row)
            if len(unique)>=limit: break
        return unique
    def list_events(self,limit=100,symbol=None,status=None):
        sql="SELECT * FROM events WHERE 1=1"; args=[]
        if symbol: sql+=" AND symbol=?"; args.append(symbol.upper())
        if status: sql+=" AND event_status=?"; args.append(status)
        sql+=" ORDER BY COALESCE(filed_at,created_at) DESC LIMIT ?"; args.append(max(limit*5,limit))
        with self.connect() as db: rows=[dict(x) for x in db.execute(sql,args).fetchall()]
        unique=[]; seen={}
        for row in rows:
            key=row.get("dedupe_key") or _event_key(row["symbol"],row["category"],row["headline"])
            stamp=row.get("filed_at") or row.get("created_at")
            try:
                current=datetime.fromisoformat(str(stamp).replace("Z","+00:00"))
                if current.tzinfo is None: current=current.replace(tzinfo=timezone.utc)
            except Exception:
                current=None
            prior=seen.get(key,[])
            if any(current is None or previous is None or abs((current-previous).total_seconds())<=72*3600 for previous in prior):
                continue
            seen.setdefault(key,[]).append(current)
            unique.append(row)
            if len(unique)>=limit: break
        return unique
    def save_fno_universe(self,symbols):
        clean=sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip() and str(symbol).strip().upper() not in {"SYMBOL", "UNDERLYING", "CONTRACT"}})
        if not clean:
            return 0
        stamp=datetime.now(timezone.utc).isoformat()
        with self.connect() as db:
            db.execute("DELETE FROM fno_universe")
            db.executemany("INSERT INTO fno_universe(symbol,updated_at) VALUES (?,?)",
                           [(symbol,stamp) for symbol in clean])
            db.commit()
        return len(clean)
    def load_fno_universe(self):
        with self.connect() as db:
            rows=db.execute("SELECT symbol FROM fno_universe ORDER BY symbol").fetchall()
        return [row["symbol"] for row in rows if row["symbol"] not in {"SYMBOL", "UNDERLYING", "CONTRACT"}]
    def count(self):
        with self.connect() as db: return db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
