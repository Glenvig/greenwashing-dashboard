# db.py
# SQLite helper til NIRAS greenwashing-dashboard
# - WAL-mode for bedre samtidighed (flere brugere kan være på samtidigt)
# - Korte, atomare transaktioner
# - CRUD, sync, stats, milestones + assigned_to
# - Smart sync der håndterer sider uden matches

import sqlite3
import pandas as pd
import streamlit as st
from contextlib import contextmanager
from typing import Set, Optional

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
          last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          crawl_session TEXT NULL
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

# --------------------------- Smart Sync CSV → DB ---------------------------
def sync_pages_from_df(df: pd.DataFrame, is_crawl: bool = False, domain: Optional[str] = None):
    """
    Synkroniserer pages fra DataFrame til database.
    
    Args:
        df: DataFrame med url, keywords, hits, total kolonner
        is_crawl: True hvis dette er fra en crawl-operation
        domain: Domænet der blev crawlet (bruges til at fjerne gamle sider)
    """
    if df is None or df.empty:
        return
    
    with tx() as con:
        # Hvis det er en crawl, generer en unik session ID
        crawl_session = None
        if is_crawl:
            import time
            crawl_session = f"crawl_{int(time.time())}"
        
        # Track hvilke URLs vi ser i denne sync
        seen_urls = set()
        
        for _, row in df.iterrows():
            url = str(row.get("url", "")).strip()
            if not url:
                continue
            
            seen_urls.add(url)
            kw = str(row.get("keywords", "")).strip()
            hits = int(row.get("hits", row.get("antal_forekomster", 0)) or 0)
            total = int(row.get("total", hits) or 0)
            
            # Tjek om siden allerede eksisterer
            existing = con.execute("SELECT status, assigned_to, notes FROM pages WHERE url=?", (url,)).fetchone()
            
            if existing:
                # Opdater eksisterende side, bevar status/assigned_to/notes hvis de er sat
                con.execute("""
                UPDATE pages SET
                    keywords=?,
                    hits=?,
                    total=?,
                    last_updated=CURRENT_TIMESTAMP,
                    crawl_session=?
                WHERE url=?
                """, (kw, hits, total, crawl_session, url))
            else:
                # Ny side
                con.execute("""
                INSERT INTO pages(url, keywords, hits, total, crawl_session)
                VALUES(?,?,?,?,?)
                """, (url, kw, hits, total, crawl_session))
        
        # Hvis det er en crawl og vi har et domæne, fjern/marker sider der ikke længere har matches
        if is_crawl and domain:
            # Find alle sider fra dette domæne som IKKE er i den nye crawl
            domain_pattern = domain.rstrip('/') + '%'
            
            # Option 1: Fjern sider helt (aggressiv)
            # con.execute("""
            # DELETE FROM pages 
            # WHERE url LIKE ? 
            # AND url NOT IN ({})
            # """.format(','.join('?' * len(seen_urls))), 
            # [domain_pattern] + list(seen_urls))
            
            # Option 2: Sæt hits/total til 0 for sider uden matches (bevarer historik)
            missing_urls = con.execute("""
            SELECT url FROM pages 
            WHERE url LIKE ? 
            AND url NOT IN ({})
            """.format(','.join('?' * len(seen_urls)) if seen_urls else "''"), 
            [domain_pattern] + list(seen_urls)).fetchall()
            
            for (url,) in missing_urls:
                con.execute("""
                UPDATE pages SET
                    hits=0,
                    total=0,
                    keywords='',
                    last_updated=CURRENT_TIMESTAMP,
                    crawl_session=?
                WHERE url=?
                """, (crawl_session, url))

# --------------------------- Alternative sync for manuel import ---------------------------
def sync_pages_from_import(df: pd.DataFrame):
    """Bruges til manuel import (ikke crawl) - opdaterer kun, fjerner ikke"""
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
              total=excluded.total,
              last_updated=CURRENT_TIMESTAMP
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