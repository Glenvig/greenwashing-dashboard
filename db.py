# db.py
# SQLite helper til NIRAS greenwashing-dashboard
# - WAL-mode for bedre samtidighed (flere brugere kan være på samtidigt)
# - Korte, atomare transaktioner
# - CRUD, sync, stats, milestones + assigned_to
# - Smart sync der håndterer sider uden matches
# - Analytics integration og prioritering

import sqlite3
import pandas as pd
import streamlit as st
from contextlib import contextmanager
from typing import Set, Optional, List, Dict
from datetime import datetime, timedelta

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
          crawl_session TEXT NULL,
          -- Nye felter for prioritering
          traffic_rank INTEGER NULL,
          monthly_visits INTEGER NULL,
          priority_score REAL NULL,
          is_priority BOOLEAN DEFAULT 0,
          last_traffic_update DATE NULL
        )""")
        
        # Index for hurtigere søgning (SQLite syntax uden DESC)
        con.execute("CREATE INDEX IF NOT EXISTS idx_priority ON pages(is_priority, priority_score)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_status ON pages(status)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_traffic ON pages(traffic_rank)")
        
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
        
        # Ny tabel til at tracke ændringer
        con.execute("""
        CREATE TABLE IF NOT EXISTS change_log(
          id INTEGER PRIMARY KEY,
          url TEXT,
          field TEXT,
          old_value TEXT,
          new_value TEXT,
          changed_by TEXT,
          changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

# --------------------------- Analytics Integration ---------------------------
def update_traffic_data(traffic_df: pd.DataFrame):
    """
    Opdaterer traffic data fra Google Analytics.
    Forventer DataFrame med kolonner: url, visits (eller sessions/pageviews)
    """
    if traffic_df is None or traffic_df.empty:
        return
    
    with tx() as con:
        # Nulstil tidligere prioriteringer
        con.execute("UPDATE pages SET is_priority=0, traffic_rank=NULL, monthly_visits=NULL")
        
        # Opdater med nye data
        for rank, row in enumerate(traffic_df.itertuples(), 1):
            url = str(row.url).strip()
            visits = int(row.visits) if hasattr(row, 'visits') else 0
            
            # Match både med og uden trailing slash
            urls_to_update = [url, url.rstrip('/'), url.rstrip('/') + '/']
            
            for u in urls_to_update:
                con.execute("""
                UPDATE pages SET
                    traffic_rank=?,
                    monthly_visits=?,
                    is_priority=(CASE WHEN ? <= 100 THEN 1 ELSE 0 END),
                    priority_score=?,
                    last_traffic_update=date('now')
                WHERE url=?
                """, (rank, visits, rank, calculate_priority_score(rank, visits), u))
        
        # Log opdateringen
        con.execute("""
        INSERT INTO actions(action) VALUES('traffic_data_updated')
        """)

def calculate_priority_score(rank: int, visits: int, hits: int = 0) -> float:
    """
    Beregner en prioritetsscore baseret på trafik og greenwashing-hits.
    Højere score = højere prioritet
    """
    # Vægt: 70% trafik, 30% greenwashing
    traffic_score = (1000 - min(rank, 1000)) / 10  # 0-100 baseret på rank
    visit_score = min(visits / 1000, 100) if visits else 0  # Normaliseret til 0-100
    greenwash_score = min(hits * 10, 100) if hits else 0  # Op til 100
    
    return (traffic_score * 0.4 + visit_score * 0.3 + greenwash_score * 0.3)

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
            existing = con.execute("""
                SELECT status, assigned_to, notes, traffic_rank, monthly_visits 
                FROM pages WHERE url=?
            """, (url,)).fetchone()
            
            if existing:
                # Opdater eksisterende side, bevar status/assigned_to/notes/traffic hvis de er sat
                # Genberegn priority score hvis vi har traffic data
                if existing['traffic_rank']:
                    priority = calculate_priority_score(
                        existing['traffic_rank'], 
                        existing['monthly_visits'] or 0,
                        total
                    )
                else:
                    priority = None
                    
                con.execute("""
                UPDATE pages SET
                    keywords=?,
                    hits=?,
                    total=?,
                    last_updated=CURRENT_TIMESTAMP,
                    crawl_session=?,
                    priority_score=?
                WHERE url=?
                """, (kw, hits, total, crawl_session, priority, url))
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
def update_status(url: str, new_status: str, user: str = None):
    with tx() as con:
        # Log ændringen
        old = con.execute("SELECT status FROM pages WHERE url=?", (url,)).fetchone()
        if old:
            con.execute("""
            INSERT INTO change_log(url, field, old_value, new_value, changed_by)
            VALUES(?,?,?,?,?)
            """, (url, "status", old[0], new_status, user))
        
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
def get_pages(search=None, min_total=0, status=None, priority_only=False,
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
    if priority_only:
        q += " AND is_priority=1"
    
    # Smart sortering: prioritet først hvis vi har traffic data
    if sort_by == "smart":
        q += " ORDER BY is_priority DESC, priority_score DESC NULLS LAST, total DESC"
    else:
        q += f" ORDER BY {sort_by} {sort_dir.upper()}"
    
    q += " LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    
    cur = con.execute(q, params)
    rows = cur.fetchall()
    
    # Tæl med samme filtre
    count_q = "SELECT COUNT(*) FROM pages WHERE 1=1"
    count_params = params[:-2]  # Fjern LIMIT og OFFSET
    if search:
        count_q += " AND (url LIKE ? OR keywords LIKE ?)"
    if min_total:
        count_q += " AND total >= ?"
    if status:
        count_q += " AND status=?"
    if priority_only:
        count_q += " AND is_priority=1"
    
    cnt = con.execute(count_q, count_params).fetchone()[0]
    return rows, cnt

def get_done_dataframe() -> pd.DataFrame:
    con = _conn()
    df = pd.read_sql_query("""
        SELECT url, assigned_to, notes, last_updated, 
               traffic_rank, monthly_visits
        FROM pages 
        WHERE status='done' 
        ORDER BY last_updated DESC
    """, con)
    return df

def stats() -> Dict:
    con = _conn()
    # Generelle stats
    tot = con.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
    done = con.execute("SELECT COUNT(*) FROM pages WHERE status='done'").fetchone()[0]
    todo = tot - done
    completion = done / tot if tot else 0.0
    
    # Priority stats
    priority_total = con.execute("SELECT COUNT(*) FROM pages WHERE is_priority=1").fetchone()[0]
    priority_done = con.execute("SELECT COUNT(*) FROM pages WHERE is_priority=1 AND status='done'").fetchone()[0]
    priority_completion = priority_done / priority_total if priority_total else 0.0
    
    # Traffic stats
    has_traffic = con.execute("SELECT COUNT(*) FROM pages WHERE traffic_rank IS NOT NULL").fetchone()[0]
    
    return {
        "total": tot, 
        "done": done, 
        "todo": todo, 
        "completion": completion,
        "priority_total": priority_total,
        "priority_done": priority_done,
        "priority_completion": priority_completion,
        "has_traffic_data": has_traffic > 0
    }

def done_today_count():
    con = _conn()
    row = con.execute("""
        SELECT COUNT(*) FROM pages
        WHERE status='done' AND date(last_updated) = date('now','localtime')
    """).fetchone()
    return int(row[0] or 0)

def get_team_stats() -> pd.DataFrame:
    """Hent statistik per teammedlem"""
    con = _conn()
    return pd.read_sql_query("""
        SELECT 
            assigned_to,
            COUNT(*) as total_assigned,
            SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) as done,
            SUM(CASE WHEN is_priority=1 THEN 1 ELSE 0 END) as priority_assigned,
            AVG(total) as avg_hits
        FROM pages
        WHERE assigned_to IS NOT NULL AND assigned_to != ''
        GROUP BY assigned_to
        ORDER BY done DESC
    """, con)

def get_recent_changes(days: int = 7) -> pd.DataFrame:
    """Hent nylige ændringer"""
    con = _conn()
    return pd.read_sql_query(f"""
        SELECT url, field, old_value, new_value, changed_by, changed_at
        FROM change_log
        WHERE changed_at >= datetime('now', '-{days} days')
        ORDER BY changed_at DESC
        LIMIT 50
    """, con)

# --------------------------- Gamification ---------------------------
def check_milestones():
    """Returnér liste af nye badges, der netop blev låst op."""
    s = stats()
    unlocked = []
    if s["done"] >= 10: unlocked.append("first_10")
    if s["completion"] >= 0.5: unlocked.append("fifty_percent")
    if s["done"] >= 100: unlocked.append("hundred_done")
    
    # Nye priority badges
    if s["priority_done"] >= 10: unlocked.append("priority_10")
    if s["priority_completion"] >= 0.5: unlocked.append("priority_half")

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