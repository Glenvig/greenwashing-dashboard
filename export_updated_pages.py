# export_updated_pages.py
# Eksporterer opdaterede sider fra app.db → Excel (og evt. CSV)
# Brug:
#   python export_updated_pages.py
#   python export_updated_pages.py --only-done
#   python export_updated_pages.py --db app.db --out exports/updated_pages.xlsx --csv

import argparse
import os
import sqlite3
import pandas as pd
from datetime import datetime

def read_pages(db_path: str, only_done: bool = False) -> pd.DataFrame:
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database ikke fundet: {db_path}")
    con = sqlite3.connect(db_path)
    try:
        base_q = """
            SELECT url, keywords, hits, total, status, assigned_to, notes, last_updated
            FROM pages
            {where}
            ORDER BY last_updated DESC
        """
        where = "WHERE status='done'" if only_done else ""
        df = pd.read_sql_query(base_q.format(where=where), con)
        return df
    finally:
        con.close()

def ensure_dir(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def to_excel(df: pd.DataFrame, out_path: str):
    ensure_dir(out_path)
    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="pages")
    return out_path

def to_csv(df: pd.DataFrame, out_path: str):
    ensure_dir(out_path)
    df.to_csv(out_path, index=False, encoding="utf-8")
    return out_path

def main():
    parser = argparse.ArgumentParser(description="Eksportér opdaterede sider fra app.db")
    parser.add_argument("--db", default="app.db", help="Sti til SQLite (default: app.db)")
    parser.add_argument("--out", default=None, help="Sti til Excel-fil (.xlsx). Default: exports/updated_pages-YYYYmmdd-HHMM.xlsx")
    parser.add_argument("--only-done", action="store_true", help="Kun sider med status='done'")
    parser.add_argument("--csv", action="store_true", help="Gem også CSV ved siden af Excel")
    args = parser.parse_args()

    ts = datetime.now().strftime("%Y%m%d-%H%M")
    out_xlsx = args.out or os.path.join("exports", f"updated_pages-{ts}.xlsx")
    out_csv = os.path.splitext(out_xlsx)[0] + ".csv"

    df = read_pages(args.db, only_done=args.only_done)
    if df.empty:
        print("Ingen rækker at eksportere (tjek filtre / databaseindhold).")
        return

    xlsx_path = to_excel(df, out_xlsx)
    print(f"Excel skrevet: {xlsx_path}")

    if args.csv:
        csv_path = to_csv(df, out_csv)
        print(f"CSV skrevet:   {csv_path}")

if __name__ == "__main__":
    main()
