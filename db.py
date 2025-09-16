# db.py
# SQLite helper til NIRAS greenwashing-dashboard
# - WAL-mode for bedre samtidighed (flere brugere kan være på samtidigt)
# - Korte, atomare transaktioner
# - CRUD, sync, stats, milestones + assigned_to

import sqlite3
import pandas as pd
import streamlit as st
from contextlib import contextmanager

DB_PATH = "app.db"

# --------------------------- Connection ---------------------------
@st.cache_resource
def _conn():
    con = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)  # autocommit
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    # Samtidighed og ydeevne
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA busy_timeout=5000;")  # vent op til 5s på locks
    cur.execute("PRAGMA foreign_keys=ON;")
    return con

@contextmanager
def tx():
    con = _conn()
    try:
        con.execute("BEGIN IMMEDIATE;")
        yield con
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        raise

# --------------------------- Schema & init ---------------------------
def init_db():
    with tx() as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS pages(
          url TEXT PRIMARY KEY,
          keywords TEXT,
          hits INTEGER,
          total INTEGER,
          status TEXT DEFAULT 'todo',
          assigned_to TEXT NULL,
          notes TEXT NULL,
          last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        con.execute("""
        CREATE TABLE IF NOT EXISTS achievements(
          id INTEGER PRIMARY KEY,
          name TEXT,
          unlocked_at TIMESTAMP
        )""")
        con.execute("""
        CREATE TABLE IF NOT EXISTS actions(
          id INTEGER PRIMARY KEY,
          url TEXT,
          action TEXT,
          at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

# --------------------------- Sync CSV → DB ---------------------------
def sync_pages_from_df(df: pd.DataFrame):
    if df is None or df.empty:
        return
    with tx() as con:
        for _, row in df.iterrows():
            url = str(row.get("url", "")).strip()
            if not url:
                continue
            kw = str(row.get("keywords", "")).strip()
            hits = int(row.get("hits", row.get("antal_forekomster", 0)) or 0)
            total = int(row.get("total", hits) or 0)
            con.execute("""
            INSERT INTO pages(url, keywords, hits, total)
            VALUES(?,?,?,?)
            ON CONFLICT(url) DO UPDATE SET
              keywords=excluded.keywords,
              hits=excluded.hits,
              total=excluded.total
            """, (url, kw, hits, total))

# --------------------------- CRUD ---------------------------
def update_status(url: str, new_status: str):
    with tx() as con:
        con.execute(
            "UPDATE pages SET status=?, last_updated=CURRENT_TIMESTAMP WHERE url=?",
            (new_status, url),
        )

def update_notes(url: str, notes: str):
    with tx() as con:
        con.execute(
            "UPDATE pages SET notes=?, last_updated=CURRENT_TIMESTAMP WHERE url=?",
            (notes, url),
        )

def update_assigned_to(url: str, assigned_to: str | None):
    with tx() as con:
        con.execute(
            "UPDATE pages SET assigned_to=?, last_updated=CURRENT_TIMESTAMP WHERE url=?",
            (assigned_to if assigned_to else None, url),
        )

def bulk_update_status(urls: list[str], new_status: str):
    if not urls:
        return
    with tx() as con:
        con.executemany(
            "UPDATE pages SET status=?, last_updated=CURRENT_TIMESTAMP WHERE url=?",
            [(new_status, u) for u in urls],
        )

# --------------------------- Queries ---------------------------
def get_pages(search=None, min_total=0, status=None,
              sort_by="total", sort_dir="desc", limit=100, offset=0):
    con = _conn()
    q = "SELECT * FROM pages WHERE 1=1"
    params = []
    if search:
        q += " AND (url LIKE ? OR keywords LIKE ?)"
        like = f"%{search}%"
        params.extend([like, like])
    if min_total:
        q += " AND total >= ?"
        params.append(min_total)
    if status:
        q += " AND status=?"
        params.append(status)
    q += f" ORDER BY {sort_by} {sort_dir.upper()} LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    cur = con.execute(q, params)
    rows = cur.fetchall()
    cnt = con.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
    return rows, cnt

def get_done_dataframe() -> pd.DataFrame:
    con = _conn()
    df = pd.read_sql_query(
        "SELECT url, assigned_to, notes, last_updated FROM pages WHERE status='done' ORDER BY last_updated DESC",
        con
    )
    return df

def stats():
    con = _conn()
    tot = con.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
    done = con.execute("SELECT COUNT(*) FROM pages WHERE status='done'").fetchone()[0]
    todo = tot - done
    completion = done / tot if tot else 0.0
    return {"total": tot, "done": done, "todo": todo, "completion": completion}

def done_today_count():
    con = _conn()
    row = con.execute("""
        SELECT COUNT(*) FROM pages
        WHERE status='done' AND date(last_updated) = date('now','localtime')
    """).fetchone()
    return int(row[0] or 0)

# --------------------------- Gamification ---------------------------
def check_milestones():
    """Returnér liste af nye badges, der netop blev låst op."""
    s = stats()
    unlocked = []
    if s["done"] >= 10: unlocked.append("first_10")
    if s["completion"] >= 0.5: unlocked.append("fifty_percent")
    if s["done"] >= 100: unlocked.append("hundred_done")

    con = _conn()
    have = {r[0] for r in con.execute("SELECT name FROM achievements").fetchall()}
    new = [u for u in unlocked if u not in have]
    if new:
        with tx() as con:
            for n in new:
                con.execute(
                    "INSERT INTO achievements(name, unlocked_at) VALUES(?, CURRENT_TIMESTAMP)",
                    (n,),
                )
    return new
