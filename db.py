"""
Database module for NIRAS Greenwashing Dashboard
Uses PostgreSQL via Neon for persistent storage
"""

import pandas as pd
import streamlit as st


def get_db_connection():
    """Get PostgreSQL connection from Streamlit"""
    return st.connection("postgresql", type="sql")


def init_db():
    """Create database tables if they don't exist"""
    conn = get_db_connection()
    
    with conn.session as s:
        # Create pages table
        s.execute("""
            CREATE TABLE IF NOT EXISTS pages (
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
        
        # Create achievements table
        s.execute("""
            CREATE TABLE IF NOT EXISTS achievements (
                id SERIAL PRIMARY KEY,
                name TEXT,
                unlocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create actions table
        s.execute("""
            CREATE TABLE IF NOT EXISTS actions (
                id SERIAL PRIMARY KEY,
                url TEXT,
                action TEXT,
                at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        s.commit()


def sync_pages_from_df(df: pd.DataFrame):
    """Import pages from DataFrame to database"""
    if df is None or df.empty:
        return
    
    conn = get_db_connection()
    
    for _, row in df.iterrows():
        url = str(row.get("url", "")).strip()
        if not url:
            continue
            
        keywords = str(row.get("keywords", "")).strip()
        hits = int(row.get("hits", row.get("antal_forekomster", 0)) or 0)
        total = int(row.get("total", hits) or 0)
        
        # Check if page exists
        check_df = conn.query(
            "SELECT status, assigned_to, notes FROM pages WHERE url = :url",
            params={"url": url}
        )
        
        with conn.session as s:
            if not check_df.empty:
                # Update existing page - keep status/assigned_to/notes
                s.execute("""
                    UPDATE pages 
                    SET keywords = :kw, hits = :hits, total = :total,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE url = :url
                """, {"kw": keywords, "hits": hits, "total": total, "url": url})
            else:
                # Insert new page
                s.execute("""
                    INSERT INTO pages (url, keywords, hits, total)
                    VALUES (:url, :kw, :hits, :total)
                """, {"url": url, "kw": keywords, "hits": hits, "total": total})
            
            s.commit()


def update_status(url: str, new_status: str):
    """Update page status"""
    conn = get_db_connection()
    with conn.session as s:
        s.execute(
            "UPDATE pages SET status = :st, last_updated = CURRENT_TIMESTAMP WHERE url = :url",
            {"st": new_status, "url": url}
        )
        s.commit()


def update_notes(url: str, notes: str):
    """Update page notes"""
    conn = get_db_connection()
    with conn.session as s:
        s.execute(
            "UPDATE pages SET notes = :notes, last_updated = CURRENT_TIMESTAMP WHERE url = :url",
            {"notes": notes, "url": url}
        )
        s.commit()


def update_assigned_to(url: str, assigned_to: str | None):
    """Update page assigned person"""
    conn = get_db_connection()
    with conn.session as s:
        s.execute(
            "UPDATE pages SET assigned_to = :assign, last_updated = CURRENT_TIMESTAMP WHERE url = :url",
            {"assign": assigned_to if assigned_to else None, "url": url}
        )
        s.commit()


def bulk_update_status(urls: list[str], new_status: str):
    """Update status for multiple pages"""
    if not urls:
        return
        
    conn = get_db_connection()
    with conn.session as s:
        for url in urls:
            s.execute(
                "UPDATE pages SET status = :st, last_updated = CURRENT_TIMESTAMP WHERE url = :url",
                {"st": new_status, "url": url}
            )
        s.commit()


def get_pages(search=None, min_total=0, status=None, 
              sort_by="total", sort_dir="desc", limit=100, offset=0):
    """Get pages with filters"""
    conn = get_db_connection()
    
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
    
    rows = [row for _, row in df.iterrows()]
    return rows, total_count


def get_done_dataframe() -> pd.DataFrame:
    """Get all completed pages"""
    conn = get_db_connection()
    return conn.query(
        "SELECT url, assigned_to, notes, last_updated FROM pages WHERE status='done' ORDER BY last_updated DESC"
    )


def stats():
    """Get dashboard statistics"""
    conn = get_db_connection()
    
    total_df = conn.query("SELECT COUNT(*) as count FROM pages")
    total = int(total_df.iloc[0]["count"]) if not total_df.empty else 0
    
    done_df = conn.query("SELECT COUNT(*) as count FROM pages WHERE status='done'")
    done = int(done_df.iloc[0]["count"]) if not done_df.empty else 0
    
    todo = total - done
    completion = done / total if total else 0.0
    
    return {"total": total, "done": done, "todo": todo, "completion": completion}


def done_today_count():
    """Count pages completed today"""
    conn = get_db_connection()
    df = conn.query("""
        SELECT COUNT(*) as count FROM pages
        WHERE status='done' AND DATE(last_updated) = CURRENT_DATE
    """)
    return int(df.iloc[0]["count"]) if not df.empty else 0


def check_milestones():
    """Check for newly unlocked achievements"""
    s = stats()
    unlocked = []
    
    if s["done"] >= 10:
        unlocked.append("first_10")
    if s["completion"] >= 0.5:
        unlocked.append("fifty_percent")
    if s["done"] >= 100:
        unlocked.append("hundred_done")
    
    conn = get_db_connection()
    existing_df = conn.query("SELECT name FROM achievements")
    existing = set(existing_df["name"].tolist()) if not existing_df.empty else set()
    
    new_achievements = [u for u in unlocked if u not in existing]
    
    if new_achievements:
        with conn.session as session:
            for achievement in new_achievements:
                session.execute(
                    "INSERT INTO achievements (name, unlocked_at) VALUES (:name, CURRENT_TIMESTAMP)",
                    {"name": achievement}
                )
            session.commit()
    
    return new_achievements