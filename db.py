
# db.py – Streamlit + Postgres using engine.begin() to avoid Session coercion issues
import pandas as pd
import streamlit as st
from sqlalchemy import text

@st.cache_resource
def get_connection():
    return st.connection("postgresql", type="sql")

def _exec(sql: str, params: dict | None = None) -> None:
    """Execute DDL/DML safely via Engine (no ORM Session)."""
    conn = get_connection()
    # Use engine.begin() which auto-commits/rolls back
    with conn.engine.begin() as s:
        s.execute(text(sql), params or {})

def _select(sql: str, params: dict | None = None) -> pd.DataFrame:
    conn = get_connection()
    return conn.query(sql, params=params, ttl=0)

def init_db():
    _exec("""
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
    _exec("""
        CREATE TABLE IF NOT EXISTS achievements(
          id SERIAL PRIMARY KEY,
          name TEXT UNIQUE,
          unlocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    _exec("""
        CREATE TABLE IF NOT EXISTS actions(
          id SERIAL PRIMARY KEY,
          url TEXT,
          action TEXT,
          at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

def sync_pages_from_df(df: pd.DataFrame):
    if df is None or df.empty:
        return
    for _, row in df.iterrows():
        url = str(row.get("url", "")).strip()
        if not url:
            continue
        kw = str(row.get("keywords", "")).strip()
        hits = int(row.get("hits", row.get("antal_forekomster", 0)) or 0)
        total = int(row.get("total", hits) or 0)

        exists = _select("SELECT 1 FROM pages WHERE url = :url", {"url": url})
        if not exists.empty:
            _exec("""
                UPDATE pages SET
                  keywords = :kw,
                  hits = :hits,
                  total = :total,
                  last_updated = CURRENT_TIMESTAMP
                WHERE url = :url
            """, {"kw": kw, "hits": hits, "total": total, "url": url})
        else:
            _exec("""
                INSERT INTO pages(url, keywords, hits, total, status, assigned_to, notes)
                VALUES(:url, :kw, :hits, :total, 'todo', NULL, NULL)
            """, {"url": url, "kw": kw, "hits": hits, "total": total})

def update_status(url: str, new_status: str):
    _exec("UPDATE pages SET status = :status, last_updated = CURRENT_TIMESTAMP WHERE url = :url",
          {"status": new_status, "url": url})

def update_notes(url: str, notes: str):
    _exec("UPDATE pages SET notes = :notes, last_updated = CURRENT_TIMESTAMP WHERE url = :url",
          {"notes": notes, "url": url})

def update_assigned_to(url: str, assigned_to: str | None):
    _exec("UPDATE pages SET assigned_to = :assigned, last_updated = CURRENT_TIMESTAMP WHERE url = :url",
          {"assigned": assigned_to if assigned_to else None, "url": url})

def bulk_update_status(urls: list[str], new_status: str):
    if not urls:
        return
    for u in urls:
        update_status(u, new_status)

def get_pages(search=None, min_total=0, status=None,
              sort_by="total", sort_dir="desc", limit=100, offset=0):
    allowed_sort = {"url", "keywords", "hits", "total", "status", "assigned_to", "last_updated"}
    if sort_by not in allowed_sort:
        sort_by = "total"
    sort_dir = "DESC" if str(sort_dir).lower() == "desc" else "ASC"

    query = "SELECT * FROM pages WHERE 1=1"
    params: dict = {}
    if search:
        query += " AND (url ILIKE :search OR keywords ILIKE :search)"
        params["search"] = f"%{search}%"
    if min_total:
        query += " AND total >= :min_total"
        params["min_total"] = int(min_total)
    if status:
        query += " AND status = :status"
        params["status"] = status
    query += f" ORDER BY {sort_by} {sort_dir} LIMIT :limit OFFSET :offset"
    params["limit"] = int(limit)
    params["offset"] = int(offset)

    df = _select(query, params)
    count_df = _select("SELECT COUNT(*) AS count FROM pages")
    total_count = int(count_df.iloc[0]["count"]) if not count_df.empty else 0
    rows = [row for _, row in df.iterrows()]
    return rows, total_count

def get_done_dataframe() -> pd.DataFrame:
    return _select("""
        SELECT url, assigned_to, notes, last_updated
        FROM pages
        WHERE status='done'
        ORDER BY last_updated DESC
    """)

def stats():
    total_df = _select("SELECT COUNT(*) AS count FROM pages")
    tot = int(total_df.iloc[0]["count"]) if not total_df.empty else 0
    done_df = _select("SELECT COUNT(*) AS count FROM pages WHERE status='done'")
    done = int(done_df.iloc[0]["count"]) if not done_df.empty else 0
    todo = tot - done
    completion = (done / tot) if tot else 0.0
    return {"total": tot, "done": done, "todo": todo, "completion": completion}

def done_today_count():
    df = _select("""
        SELECT COUNT(*) AS count
        FROM pages
        WHERE status='done' AND DATE(last_updated) = CURRENT_DATE
    """)
    return int(df.iloc[0]["count"]) if not df.empty else 0

def check_milestones():
    s = stats()
    unlocked = []
    if s["done"] >= 10: unlocked.append("first_10")
    if s["completion"] >= 0.5: unlocked.append("fifty_percent")
    if s["done"] >= 100: unlocked.append("hundred_done")

    have_df = _select("SELECT name FROM achievements")
    have = set(have_df["name"].tolist()) if not have_df.empty else set()
    new = [u for u in unlocked if u not in have]
    for n in new:
        _exec("INSERT INTO achievements(name, unlocked_at) VALUES(:name, CURRENT_TIMESTAMP)", {"name": n})
    return new
