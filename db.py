# db.py
# PostgreSQL helper til NIRAS greenwashing-dashboard
# - Persistent storage via Neon PostgreSQL
# - CRUD, sync, stats, milestones + assigned_to
# - Data bevares automatisk mellem sessioner

import pandas as pd
import streamlit as st
from contextlib import contextmanager

# --------------------------- Connection ---------------------------
@st.cache_resource
def get_connection():
    """Get PostgreSQL connection via Streamlit secrets"""
    return st.connection("postgresql", type="sql")

def get_conn():
    """Get raw database connection for direct SQL"""
    conn = get_connection()
    return conn.driver_connection

@contextmanager
def tx():
    """Transaction context manager"""
    conn = get_conn()
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

# --------------------------- Schema & init ---------------------------
def init_db():
    """Initialize database schema"""
    with tx() as cur:
        # Pages table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS pages(
          url TEXT PRIMARY KEY,
          keywords TEXT,
          hits INTEGER,
          total INTEGER,
          status TEXT DEFAULT 'todo',
          assigned_to TEXT,
          notes TEXT,
          last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Achievements table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS achievements(
          id SERIAL PRIMARY KEY,
          name TEXT,
          unlocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Actions table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS actions(
          id SERIAL PRIMARY KEY,
          url TEXT,
          action TEXT,
          at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

# --------------------------- Sync CSV → DB ---------------------------
def sync_pages_from_df(df: pd.DataFrame):
    """Sync pages from DataFrame to database"""
    if df is None or df.empty:
        return
    
    with tx() as cur:
        for _, row in df.iterrows():
            url = str(row.get("url", "")).strip()
            if not url:
                continue
            kw = str(row.get("keywords", "")).strip()
            hits = int(row.get("hits", row.get("antal_forekomster", 0)) or 0)
            total = int(row.get("total", hits) or 0)
            
            # Check if URL exists
            cur.execute("SELECT status, assigned_to, notes FROM pages WHERE url=%s", (url,))
            existing = cur.fetchone()
            
            if existing:
                # Update existing - preserve status, assigned_to, notes
                cur.execute("""
                UPDATE pages SET
                  keywords=%s,
                  hits=%s,
                  total=%s,
                  last_updated=CURRENT_TIMESTAMP
                WHERE url=%s
                """, (kw, hits, total, url))
            else:
                # Insert new
                cur.execute("""
                INSERT INTO pages(url, keywords, hits, total, status, assigned_to, notes)
                VALUES(%s, %s, %s, %s, 'todo', NULL, NULL)
                """, (url, kw, hits, total))

# --------------------------- CRUD ---------------------------
def update_status(url: str, new_status: str):
    """Update page status"""
    with tx() as cur:
        cur.execute(
            "UPDATE pages SET status=%s, last_updated=CURRENT_TIMESTAMP WHERE url=%s",
            (new_status, url)
        )

def update_notes(url: str, notes: str):
    """Update page notes"""
    with tx() as cur:
        cur.execute(
            "UPDATE pages SET notes=%s, last_updated=CURRENT_TIMESTAMP WHERE url=%s",
            (notes, url)
        )

def update_assigned_to(url: str, assigned_to: str | None):
    """Update page assigned_to"""
    with tx() as cur:
        cur.execute(
            "UPDATE pages SET assigned_to=%s, last_updated=CURRENT_TIMESTAMP WHERE url=%s",
            (assigned_to if assigned_to else None, url)
        )

def bulk_update_status(urls: list[str], new_status: str):
    """Bulk update status for multiple URLs"""
    if not urls:
        return
    with tx() as cur:
        for url in urls:
            cur.execute(
                "UPDATE pages SET status=%s, last_updated=CURRENT_TIMESTAMP WHERE url=%s",
                (new_status, url)
            )

# --------------------------- Queries ---------------------------
def get_pages(search=None, min_total=0, status=None,
              sort_by="total", sort_dir="desc", limit=100, offset=0):
    """Get pages with filters"""
    conn = get_connection()
    
    query = "SELECT * FROM pages WHERE 1=1"
    params = {}
    
    if search:
        query += " AND (url LIKE :search OR keywords LIKE :search)"
        params["search"] = f"%{search}%"
    
    if min_total:
        query += " AND total >= :min_total"
        params["min_total"] = min_total
    
    if status:
        query += " AND status = :status"
        params["status"] = status
    
    query += f" ORDER BY {sort_by} {sort_dir.upper()} LIMIT :limit OFFSET :offset"
    params["limit"] = limit
    params["offset"] = offset
    
    df = conn.query(query, params=params)
    
    # Get total count
    count_query = "SELECT COUNT(*) as count FROM pages"
    count_df = conn.query(count_query)
    total_count = int(count_df.iloc[0]["count"]) if not count_df.empty else 0
    
    # Convert DataFrame to list of Row-like objects
    rows = [row for _, row in df.iterrows()]
    
    return rows, total_count

def get_done_dataframe() -> pd.DataFrame:
    """Get all done pages as DataFrame"""
    conn = get_connection()
    df = conn.query(
        "SELECT url, assigned_to, notes, last_updated FROM pages WHERE status='done' ORDER BY last_updated DESC"
    )
    return df

def stats():
    """Get statistics"""
    conn = get_connection()
    
    total_df = conn.query("SELECT COUNT(*) as count FROM pages")
    tot = int(total_df.iloc[0]["count"]) if not total_df.empty else 0
    
    done_df = conn.query("SELECT COUNT(*) as count FROM pages WHERE status='done'")
    done = int(done_df.iloc[0]["count"]) if not done_df.empty else 0
    
    todo = tot - done
    completion = done / tot if tot else 0.0
    
    return {"total": tot, "done": done, "todo": todo, "completion": completion}

def done_today_count():
    """Count pages done today"""
    conn = get_connection()
    df = conn.query("""
        SELECT COUNT(*) as count FROM pages
        WHERE status='done' AND DATE(last_updated) = CURRENT_DATE
    """)
    return int(df.iloc[0]["count"]) if not df.empty else 0

# --------------------------- Gamification ---------------------------
def check_milestones():
    """Check and unlock new milestones"""
    s = stats()
    unlocked = []
    if s["done"] >= 10: unlocked.append("first_10")
    if s["completion"] >= 0.5: unlocked.append("fifty_percent")
    if s["done"] >= 100: unlocked.append("hundred_done")

    conn = get_connection()
    have_df = conn.query("SELECT name FROM achievements")
    have = set(have_df["name"].tolist()) if not have_df.empty else set()
    
    new = [u for u in unlocked if u not in have]
    
    if new:
        with tx() as cur:
            for n in new:
                cur.execute(
                    "INSERT INTO achievements(name, unlocked_at) VALUES(%s, CURRENT_TIMESTAMP)",
                    (n,)
                )
    
    return new