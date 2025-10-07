# db.py – Streamlit + Postgres (SQLAlchemy 2.x)
# Robust mod timeouts: engine.begin(), batch-upsert m. chunking + retries.
from __future__ import annotations

import time
from typing import Iterable, List, Dict

import pandas as pd
import streamlit as st
from sqlalchemy import text


# ---------- Connection ----------
@st.cache_resource
def get_connection():
    """
    Kræver i .streamlit/secrets.toml:

    [connections.postgresql]
    url = "postgresql+psycopg2://USER:PASS@HOST:5432/DBNAME?sslmode=require"
    # (valgfrit men anbefalet på hosted DB)
    # create_engine_kwargs = { pool_size=5, max_overflow=10, pool_timeout=30, pool_recycle=1800, pool_pre_ping=true }
    """
    return st.connection("postgresql", type="sql")


# ---------- Helpers ----------
def _exec(sql: str, params: dict | None = None) -> None:
    """DDL/DML i én transaktion."""
    conn = get_connection()
    with conn.engine.begin() as s:
        s.execute(text(sql), params or {})


def _exec_many(sql: str, params_list: List[Dict]) -> None:
    """Executemany i én transaktion."""
    if not params_list:
        return
    conn = get_connection()
    with conn.engine.begin() as s:
        s.execute(text(sql), params_list)


def _select(sql: str, params: dict | None = None) -> pd.DataFrame:
    """SELECT (ttl=0 for friske data i UI)."""
    conn = get_connection()
    return conn.query(sql, params=params, ttl=0)


def _chunks(seq: Iterable[dict], n: int) -> Iterable[list[dict]]:
    """Yield faste chunks af størrelse n."""
    buf: list[dict] = []
    for item in seq:
        buf.append(item)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf


def _exec_many_with_retry(sql: str, rows: list[dict], first_chunk: int = 500, micro_chunk: int = 50) -> None:
    """
    Kør executemany med backoff:
      1) forsøg hele 'first_chunk' ad gangen
      2) ved fejl: 1s sleep og ét retry
      3) stadig fejl: split i 'micro_chunk' for at komme videre
    """
    try:
        _exec_many(sql, rows)
        return
    except Exception:
        time.sleep(1.0)
        try:
            _exec_many(sql, rows)
            return
        except Exception:
            # fallback: mikro-chunks
            for micro in _chunks(rows, micro_chunk):
                _exec_many(sql, micro)


# ---------- Schema ----------
DDL_PAGES = """
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
"""

DDL_ACHIEVEMENTS = """
CREATE TABLE IF NOT EXISTS achievements(
  id SERIAL PRIMARY KEY,
  name TEXT UNIQUE,
  unlocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

DDL_ACTIONS = """
CREATE TABLE IF NOT EXISTS actions(
  id SERIAL PRIMARY KEY,
  url TEXT,
  action TEXT,
  at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def init_db():
    _exec(DDL_PAGES)
    _exec(DDL_ACHIEVEMENTS)
    _exec(DDL_ACTIONS)


# ---------- Sync CSV/DataFrame -> DB ----------
def sync_pages_from_df(df: pd.DataFrame):
    """
    Batch upsert til Postgres:
    - chunk = 500 for at undgå pool/lock timeouts under crawl
    - retries + mikro-chunk fallback
    """
    if df is None or df.empty:
        return

    rows: list[dict] = []
    for _, r in df.iterrows():
        url = str(r.get("url", "")).strip()
        if not url:
            continue
        kw = str(r.get("keywords", "")).strip()
        hits = int(r.get("hits", r.get("antal_forekomster", 0)) or 0)
        total = int(r.get("total", hits) or 0)
        rows.append({"url": url, "kw": kw, "hits": hits, "total": total})

    if not rows:
        return

    upsert_sql = """
        INSERT INTO pages(url, keywords, hits, total, status, assigned_to, notes, last_updated)
        VALUES(:url, :kw, :hits, :total, 'todo', NULL, NULL, CURRENT_TIMESTAMP)
        ON CONFLICT (url) DO UPDATE SET
          keywords     = EXCLUDED.keywords,
          hits         = EXCLUDED.hits,
          total        = EXCLUDED.total,
          last_updated = CURRENT_TIMESTAMP
    """

    for chunk in _chunks(rows, 500):
        _exec_many_with_retry(upsert_sql, chunk, first_chunk=500, micro_chunk=50)


# ---------- CRUD ----------
def update_status(url: str, new_status: str):
    _exec(
        "UPDATE pages SET status = :status, last_updated = CURRENT_TIMESTAMP WHERE url = :url",
        {"status": new_status, "url": url}
    )


def update_notes(url: str, notes: str):
    _exec(
        "UPDATE pages SET notes = :notes, last_updated = CURRENT_TIMESTAMP WHERE url = :url",
        {"notes": notes, "url": url}
    )


def update_assigned_to(url: str, assigned_to: str | None):
    _exec(
        "UPDATE pages SET assigned_to = :assigned, last_updated = CURRENT_TIMESTAMP WHERE url = :url",
        {"assigned": assigned_to if assigned_to else None, "url": url}
    )


def bulk_update_status(urls: list[str], new_status: str):
    if not urls:
        return
    params_list = [{"status": new_status, "url": u} for u in urls if u]
    _exec_many(
        "UPDATE pages SET status = :status, last_updated = CURRENT_TIMESTAMP WHERE url = :url",
        params_list
    )


# ---------- Queries til UI ----------
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
    # sikr at achievements-tabellen findes
    _exec(DDL_ACHIEVEMENTS)

    s = stats()
    unlocked: list[str] = []
    if s["done"] >= 10:
        unlocked.append("first_10")
    if s["completion"] >= 0.5:
        unlocked.append("fifty_percent")
    if s["done"] >= 100:
        unlocked.append("hundred_done")

    have_df = _select("SELECT name FROM achievements")
    have = set(have_df["name"].tolist()) if not have_df.empty else set()
    new = [u for u in unlocked if u not in have]
    if new:
        _exec_many(
            "INSERT INTO achievements(name, unlocked_at) VALUES(:name, CURRENT_TIMESTAMP) ON CONFLICT (name) DO NOTHING",
            [{"name": n} for n in new]
        )
    return new
