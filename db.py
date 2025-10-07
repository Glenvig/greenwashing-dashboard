# db.py
# PostgreSQL helper til NIRAS greenwashing-dashboard
# - Persistent storage via Neon PostgreSQL
# - CRUD, sync, stats, milestones + assigned_to
# - Data bevares automatisk mellem sessioner

import pandas as pd
import streamlit as st

# --------------------------- Connection ---------------------------
@st.cache_resource
def get_connection():
    """Get PostgreSQL connection via Streamlit secrets"""
    return st.connection("postgresql", type="sql")

# --------------------------- Schema & init ---------------------------
def init_db():
    """Initialize database schema"""
    conn = get_connection()
    
    # Pages table
    conn.query("""
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
    conn.query("""
    CREATE TABLE IF NOT EXISTS achievements(
      id SERIAL PRIMARY KEY,
      name TEXT,
      unlocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Actions table
    conn.query("""
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
    
    conn = get_connection()
    
    for _, row in df.iterrows():
        url = str(row.get("url", "")).strip()
        if not url:
            continue
        kw = str(row.get("keywords", "")).strip()
        hits = int(row.get("hits", row.get("antal_forekomster", 0)) or 0)
        total = int(row.get("total", hits) or 0)
        
        # Check if URL exists
        existing = conn.query("SELECT status, assigned_to, notes FROM pages WHERE url = :url", params={"url": url})
        
        if not existing.empty:
            # Update existing - preserve status, assigned_to, notes
            conn.query("""
            UPDATE pages SET
              keywords = :kw,
              hits = :hits,
              total = :total,
              last_updated = CURRENT_TIMESTAMP
            WHERE url = :url
            """, params={"kw": kw, "hits": hits, "total": total, "url": url})
        else:
            # Insert new
            conn.query("""
            INSERT INTO pages(url, keywords, hits, total, status, assigned_to, notes)
            VALUES(:url, :kw, :hits, :total, 'todo', NULL, NULL)
            """, params={"url": url, "kw": kw, "hits": hits, "total": total})

# --------------------------- CRUD ---------------------------
def update_status(url: str, new_status: str):
    """Update page status"""
    conn = get_connection()
    conn.query(
        "UPDATE pages SET status = :status, last_updated = CURRENT_TIMESTAMP WHERE url = :url",
        params={"status": new_status, "url": url}
    )

def update_notes(url: str, notes: str):
    """Update page notes"""
    conn = get_connection()
    conn.query(
        "UPDATE pages SET notes = :notes, last_updated = CURRENT_TIMESTAMP WHERE url = :url",
        params={"notes": notes, "url": url}
    )

def update_assigned_to(url: str, assigned_to: str | None):
    """Update page assigned_to"""
    conn = get_connection()
    conn.query(
        "UPDATE pages SET assigned_to = :assigned, last_updated = CURRENT_TIMESTAMP WHERE url = :url",
        params={"assigned": assigned_to if assigned_to else None, "url": url}
    )

def bulk_update_status(urls: list[str], new_status: str):
    """Bulk update status for multiple URLs"""
    if not urls:
        return
    conn = get_connection()
    for url in urls:
        conn.query(
            "UPDATE pages SET status = :status, last_updated = CURRENT_TIMESTAMP WHERE url = :url",
            params={"status": new_status, "url": url}
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
    count_df = conn.query("SELECT COUNT(*) as count FROM pages")
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
        for n in new:
            conn.query(
                "INSERT INTO achievements(name, unlocked_at) VALUES(:name, CURRENT_TIMESTAMP)",
                params={"name": n}
            )
    
    return new