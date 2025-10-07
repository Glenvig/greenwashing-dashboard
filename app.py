# app.py – NIRAS Greenwashing-dashboard (kompakt, stabil, no data.py/charts.py deps)
from __future__ import annotations

import os, io, re, time, json
from pathlib import Path
from urllib.parse import urlparse
from typing import Optional, List

import pandas as pd
import streamlit as st

# interne moduler (skal eksistere i projektet)
import db
from crawler import crawl_iter, scan_pages, DEFAULT_KW

st.set_page_config(page_title="NIRAS greenwashing-dashboard", layout="wide")

# =================== Utilities ===================
SETTINGS_PATH = Path("data") / "settings.json"

def _load_settings() -> dict:
    try:
        if SETTINGS_PATH.exists():
            return json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def _save_settings(obj: dict):
    try:
        SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
        SETTINGS_PATH.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

def _canon(u: str, base: str | None = None) -> str:
    u = (str(u) or "").strip()
    if not u:
        return ""
    if u.startswith("/") and base:
        u = base.rstrip("/") + u
    p = urlparse(u)
    if not p.scheme or not p.netloc:
        return ""
    clean = p._replace(fragment="").geturl()
    if not clean.endswith("/"):
        clean += "/"
    return clean

def kpi_cards(total: int, done: int, todo: int, completion: float):
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("Total sider", total)
    with c2: st.metric("Done", done)
    with c3: st.metric("Todo", todo)
    with c4: st.metric("Completion", f"{int(round((completion or 0.0)*100))}%")

def big_green_progress(completion: float, total: int, done: int):
    pct = max(0, min(int(round((completion or 0.0) * 100)), 100))
    st.markdown(
        f"""
        <div style="margin: 8px 0 18px 0; border:1px solid #e5e7eb; border-radius:12px; overflow:hidden;">
          <div style="padding:10px 14px; font-weight:600;">Fremskridt</div>
          <div style="height:26px; background:#e5e7eb; position:relative;">
            <div style="height:100%; width:{pct}%; background:#10b981; transition:width .3s;"></div>
            <div style="position:absolute; top:0; left:0; right:0; height:100%; display:flex; align-items:center; justify-content:center; font-weight:600;">
              {pct}% &nbsp; <span style="font-weight:400; color:#374151">({done} af {total} sider)</span>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# =================== Header ===================
st.markdown("### Velkommen til Greenwashing-radaren")
st.markdown("Filtrér, redigér og find forekomster hurtigt. Navigation/related tælles ikke med i forekomster.")

# =================== DB init + auto-import fra fil (valgfrit) ===================
db.init_db()

AUTO_IMPORT = os.environ.get("AUTO_IMPORT_FILE", "data/crawl.csv")  # peg på din fortrukne fil
IMPORT_ON_EMPTY = True
IMPORT_ON_CHANGE = True

def _read_any(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    if p.suffix.lower() in (".xlsx", ".xls"):
        return pd.read_excel(p)
    return pd.read_csv(p)

# Importér hvis DB er tom
try:
    s0 = db.stats()
    if IMPORT_ON_EMPTY and s0.get("total", 0) == 0 and Path(AUTO_IMPORT).exists():
        df_init = _read_any(AUTO_IMPORT)
        if not df_init.empty:
            db.sync_pages_from_df(df_init)
            s0 = db.stats()
except Exception:
    s0 = db.stats()

# Importér igen ved fil-ændring (bevarer status)
try:
    if IMPORT_ON_CHANGE and Path(AUTO_IMPORT).exists():
        sig = f"{os.path.getmtime(AUTO_IMPORT)}:{os.path.getsize(AUTO_IMPORT)}"
        if st.session_state.get("__file_sig") != sig:
            df_auto = _read_any(AUTO_IMPORT)
            if not df_auto.empty:
                db.sync_pages_from_df(df_auto)
                st.session_state["__file_sig"] = sig
except Exception:
    pass

# Progress
s0 = db.stats()
big_green_progress(s0.get("completion", 0.0), s0.get("total", 0), s0.get("done", 0))

# =================== SIDEBAR ===================
with st.sidebar:
    import csv

    st.header("Data")

    # ---- Wide → standard normalisering (én kolonne pr. keyword → url,keywords,hits,total)
    def normalize_wide(df: pd.DataFrame) -> pd.DataFrame | None:
        if df is None or df.empty:
            return None
        cols_lower = {str(c).strip().lower(): c for c in df.columns}
        url_col = cols_lower.get("url")
        if not url_col:
            return None
        def _is_num(s):
            try: return pd.api.types.is_numeric_dtype(s)
            except Exception: return False
        total_col = None
        for cand in ("total", "sum", "matches", "forekomster"):
            if cand in cols_lower:
                total_col = cols_lower[cand]
                break
        numeric_cols = [c for c in df.columns if c != url_col and _is_num(df[c])]
        kw_cols = [c for c in numeric_cols if c != (total_col or "")]
        if not kw_cols and total_col is None:
            return None
        tmp = df.copy()
        if total_col:
            total_series = pd.to_numeric(tmp[total_col], errors="coerce").fillna(0).astype(int)
        else:
            total_series = pd.to_numeric(tmp[kw_cols].fillna(0)).sum(axis=1).astype(int)
        def mk_keywords_csv(row):
            used = []
            for c in kw_cols:
                try: v = int(row.get(c, 0) or 0)
                except: v = 0
                if v > 0: used.append(str(c))
            return ",".join(used)
        keywords_csv = tmp.apply(mk_keywords_csv, axis=1) if kw_cols else pd.Series([""] * len(tmp))
        hits_series = total_series
        out = pd.DataFrame({
            "url": tmp[url_col].astype(str),
            "keywords": keywords_csv,
            "hits": hits_series,
            "total": total_series,
        })
        return out

    # Vælg/Upload datakilde
    default_path = os.path.join("data", "crawl.csv")
    path_str = st.text_input("Sti til CSV/Excel", value=default_path)
    uploaded = st.file_uploader("...eller upload fil", type=["csv", "xlsx", "xls"])

    file_source = uploaded if uploaded else (path_str if path_str.strip() else None)
    df_std = None
    label = "Ingen"
    if file_source:
        try:
            if uploaded is not None:
                label = uploaded.name
                raw = uploaded.getvalue()
            else:
                label = str(path_str)
                raw = Path(path_str).read_bytes() if Path(path_str).exists() else b""
            if raw:
                df_try = None
                # Forsøg Excel
                if label.lower().endswith((".xlsx", ".xls")):
                    for kwargs in ({"engine": None}, {"engine": "openpyxl"}):
                        try:
                            df_try = pd.read_excel(io.BytesIO(raw), **{k:v for k,v in kwargs.items() if v is not None})
                            if df_try is not None and not df_try.empty: break
                        except Exception:
                            df_try = None
                # Fald tilbage til CSV
                if df_try is None:
                    for kwargs in (
                        {"engine":"python","encoding":"utf-8","comment":"#", "on_bad_lines":"skip"},
                        {"sep":";","engine":"python","encoding":"utf-8","comment":"#", "on_bad_lines":"skip"},
                        {"sep":",","engine":"python","encoding":"utf-8","comment":"#", "on_bad_lines":"skip"},
                    ):
                        try:
                            df_try = pd.read_csv(io.BytesIO(raw), **kwargs)
                            if df_try is not None and not df_try.empty: break
                        except Exception:
                            df_try = None
                if df_try is not None and not df_try.empty:
                    cols_l = {c.lower() for c in df_try.columns}
                    if {"url","keywords","hits","total"}.issubset(cols_l):
                        df_std = df_try.rename(columns={c:c.lower() for c in df_try.columns})
                    else:
                        nw = normalize_wide(df_try)
                        df_std = nw if (nw is not None and not nw.empty) else pd.DataFrame()
        except Exception as e:
            st.error(f"Fejl ved indlæsning: {e}")
            df_std = None

    st.caption(f"Datakilde: **{label}**")

    # Importér (upsert kun keywords/hits/total – bevar status/assigned_to/notes)
    if st.button("Importér", type="primary", key="import_btn"):
        if df_std is None or df_std.empty:
            st.warning("Ingen gyldig data fundet.")
            st.stop()
        df_imp = df_std.copy()
        for c in ("url","keywords","hits","total"):
            if c not in df_imp.columns:
                df_imp[c] = "" if c in ("url","keywords") else 0
        base = st.session_state.get("__current_domain")
        df_imp["url"] = df_imp["url"].map(lambda u: _canon(u, base))
        df_imp = df_imp[df_imp["url"] != ""]
        df_imp["keywords"] = df_imp["keywords"].fillna("").astype(str)
        df_imp["hits"] = pd.to_numeric(df_imp["hits"], errors="coerce").fillna(0).astype(int)
        df_imp["total"] = pd.to_numeric(df_imp["total"], errors="coerce").fillna(0).astype(int)

        rows = df_imp.to_dict("records")
        BATCH = 500
        synced = 0
        for i in range(0, len(rows), BATCH):
            chunk = pd.DataFrame(rows[i:i+BATCH])
            tries = 0
            while True:
                try:
                    db.sync_pages_from_df(chunk)   # skal være upsert der KUN opdaterer keywords/hits/total
                    synced += len(chunk); break
                except Exception:
                    tries += 1
                    if tries >= 3:
                        st.error("DB-fejl ved import.")
                        break
                    st.warning(f"DB-fejl (forsøg {tries}). Prøver igen om 1s…")
                    time.sleep(1)
        st.success(f"Import færdig. {synced} rækker synkroniseret.")
        st.rerun()

    st.markdown("---")
    st.header("Crawler")

    domain = st.selectbox("Domæne", ["https://www.niras.dk/", "https://www.niras.com/"])
    st.session_state["__current_domain"] = domain

    # Keywords – UI
    default_kw_text = "\n".join(DEFAULT_KW)
    kw_text = st.text_area("Søgeord & udsagn (ét pr. linje)",
                           value=default_kw_text,
                           help="* som wildcard (fx 'bæredygtig*'). Regex som /co2[- ]?neutral/.")
    kw_list_manual = [k.strip() for k in re.split(r"[\n,;]", kw_text) if k.strip()]

    # Flet med keywords fra indlæst fil
    merge_with_file = st.checkbox("Flet med keywords fra datakilden", value=True)
    kw_from_file: List[str] = []
    if merge_with_file and (df_std is not None) and (not df_std.empty):
        try:
            all_kw = []
            for _, row in df_std.iterrows():
                for k in re.split(r"[;,]", str(row.get("keywords",""))):
                    k = k.strip()
                    if k: all_kw.append(k)
            seen=set(); kw_from_file=[k for k in all_kw if not (k in seen or seen.add(k))]
        except Exception:
            kw_from_file = []
    kw_seen, kw_final = set(), []
    for k in kw_list_manual + kw_from_file:
        if k and (k not in kw_seen):
            kw_seen.add(k); kw_final.append(k)

    # Eksklusioner (persist)
    st.caption("—")
    settings = _load_settings()
    exclude_text = st.text_area("Ekskludér ord/fraser (ét pr. linje)",
                                value="\n".join(settings.get("exclude", [])),
                                help="Filtreres væk fra tekst (ikke fra keyword-listen).",
                                key="exclude_kw_text")
    kw_exclude = {k.strip().lower() for k in re.split(r"[\n,;]", exclude_text) if k.strip()}
    st.session_state["kw_final"] = kw_final
    st.session_state["kw_exclude"] = sorted(list(kw_exclude)) if kw_exclude else []
    excl_sig = (exclude_text or "").strip()
    if st.session_state.get("__exclude_sig") != excl_sig:
        st.session_state["__exclude_sig"] = excl_sig
        _save_settings({"exclude":[k for k in excl_sig.split("\n") if k.strip()]})
        st.rerun()

    st.caption(f"🧩 Keywords i brug: {len(kw_final)}")

    # Crawl hele domænet (batch + retry)
    if st.button("🚀 Crawl hele domænet", type="secondary", key="crawl_all_btn"):
        if not kw_final:
            st.warning("Tilføj mindst ét ord/udsagn."); 
        else:
            db.init_db()
            stats_before = db.stats()
            prog = st.progress(0, text="Starter crawler…")
            rows_buf = []
            BATCH = 150
            db_errors = 0
            def on_progress(done:int, queued:int):
                prog.progress(min(0.99, done/8000), text=f"Crawler… {done} sider · kø: {queued}")
            for row in crawl_iter(domain, kw_final, max_pages=10000, max_depth=100, delay=0.5,
                                  progress_cb=on_progress, excludes=st.session_state.get("kw_exclude", [])):
                rows_buf.append(row)
                if len(rows_buf) >= BATCH:
                    try:
                        db.sync_pages_from_df(pd.DataFrame(rows_buf))
                        rows_buf.clear(); db_errors = 0
                    except Exception:
                        db_errors += 1
                        st.warning(f"DB-fejl (forsøg {db_errors}). Prøver igen om 1s…")
                        time.sleep(1)
                        try:
                            db.sync_pages_from_df(pd.DataFrame(rows_buf))
                            rows_buf.clear(); db_errors = 0
                        except Exception:
                            pass
            if rows_buf:
                try:
                    db.sync_pages_from_df(pd.DataFrame(rows_buf))
                except Exception:
                    st.warning("Kunne ikke skrive sidste batch; prøver igen…")
                    time.sleep(1)
                    try: db.sync_pages_from_df(pd.DataFrame(rows_buf))
                    except Exception as e: st.error(f"Kunne ikke skrive sidste batch: {e}")
            prog.progress(1.0, text="Crawler færdig")
            stats_after = db.stats()
            st.success(f"Crawl færdig. DB: {stats_before.get('total',0)} → {stats_after.get('total',0)} sider.")
            st.rerun()

    # Crawl fra fil (liste af URL’er)
    st.markdown("---")
    st.subheader("Crawl fra fil (URL-liste)")
    urls_file = st.file_uploader("Upload .txt eller .csv med URL'er", type=["txt", "csv"], key="url_list")
    if urls_file is not None:
        raw = urls_file.getvalue().decode("utf-8", errors="ignore")
        urls = []
        if urls_file.name.lower().endswith(".csv"):
            rdr = csv.DictReader(io.StringIO(raw))
            fld_l = [f.strip().lower() for f in (rdr.fieldnames or [])]
            url_field = None
            if "url" in fld_l:
                url_field = rdr.fieldnames[fld_l.index("url")]
            if url_field:
                for row in rdr:
                    u = (row.get(url_field) or "").strip()
                    if u: urls.append(u)
            else:
                urls = [row[0].strip() for row in csv.reader(io.StringIO(raw)) if row and row[0].strip()]
        else:
            urls = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        base = st.session_state.get("__current_domain")
        urls = [_canon(u, base) for u in urls]
        urls = [u for u in urls if u]
        urls = list(dict.fromkeys(urls))
        st.caption(f"Fundet {len(urls)} URL'er i filen.")
        if urls and st.button("Crawl uploadede URL'er nu", type="secondary"):
            st.info(f"Crawler {len(urls)} URL'er …")
            sub_prog = st.progress(0)
            batch = 40
            all_rows = []
            kw_final = st.session_state.get("kw_final", [])
            kw_excl = st.session_state.get("kw_exclude", [])
            from math import ceil
            total_batches = max(1, ceil(len(urls)/batch))
            for i in range(0, len(urls), batch):
                part = urls[i:i+batch]
                try:
                    part_rows = scan_pages(part, kw_final, excludes=kw_excl, delay=0.0)
                    if part_rows:
                        db.sync_pages_from_df(pd.DataFrame(part_rows))
                        all_rows.extend(part_rows)
                except Exception as e:
                    st.warning(f"Fejl ved batch {i//batch+1}: {e}")
                sub_prog.progress(min(1.0, (i//batch+1)/total_batches))
            st.success(f"Færdig. Opdateret {len(all_rows)} resultater i DB.")
            st.rerun()

    # =================== Google Analytics – Top 100 ===================
    st.markdown("---")
    st.header("Google Analytics – Top 100")
    ga_file = st.file_uploader("Upload GA CSV/Excel (kolonner: URL eller pagePath + pageviews)", type=["csv","xlsx","xls"], key="ga_csv")

    raw_ga: bytes = b""
    src_name: str = ""
    if ga_file is not None:
        src_name = (ga_file.name or "").lower()
        raw_ga = ga_file.getvalue() or b""
    else:
        default_ga_path = Path("data") / "Pageviews.csv"
        if default_ga_path.exists():
            src_name = str(default_ga_path).lower()
            try: raw_ga = default_ga_path.read_bytes()
            except Exception: raw_ga = b""

    if raw_ga:
        ga_df = None
        is_excel = src_name.endswith(".xlsx") or src_name.endswith(".xls")
        if is_excel:
            for kwargs in ({"engine": None}, {"engine":"openpyxl"}):
                try:
                    ga_df = pd.read_excel(io.BytesIO(raw_ga), **{k:v for k,v in kwargs.items() if v is not None})
                    if ga_df is not None and not ga_df.empty: break
                except Exception:
                    ga_df = None
        if ga_df is None or ga_df.empty:
            for kwargs in (
                {"engine":"python","encoding":"utf-8","comment":"#", "on_bad_lines":"skip"},
                {"sep":";","engine":"python","encoding":"utf-8","comment":"#", "on_bad_lines":"skip"},
                {"sep":",","engine":"python","encoding":"utf-8","comment":"#", "on_bad_lines":"skip"},
            ):
                try:
                    ga_df = pd.read_csv(io.BytesIO(raw_ga), **kwargs)
                    if ga_df is not None and not ga_df.empty: break
                except Exception:
                    ga_df = None

        if ga_df is None or ga_df.empty:
            st.warning("Kunne ikke læse GA-fil.")
        else:
            def _norm(s:str)->str: return re.sub(r"[^a-z]","",(str(s) or "").strip().lower())
            by_lower = {str(c).strip().lower(): c for c in ga_df.columns}
            by_norm  = {_norm(c): c for c in ga_df.columns}
            url_keys = ["url","pagepath","page","pagelocation","landingpage","landingpagepath","pathname","pagepathandscreenclass"]
            pv_keys  = ["pageviews","views","screenpageviews","screenpageview","screenviews"]

            url_col = None
            for k in url_keys:
                url_col = by_lower.get(k) or by_norm.get(k)
                if url_col: break
            if not url_col:
                for nk, orig in by_norm.items():
                    if ("pagepath" in nk) or ("pagelocation" in nk) or (nk=="url"):
                        url_col = orig; break

            pv_col = None
            for k in pv_keys:
                pv_col = by_lower.get(k) or by_norm.get(k)
                if pv_col: break
            if not pv_col:
                for nk, orig in by_norm.items():
                    if nk.endswith("views") or ("pageviews" in nk) or ("screenpageviews" in nk):
                        pv_col = orig; break

            if not url_col or not pv_col:
                st.warning("GA-fil mangler URL/pagePath og pageviews.")
            else:
                base = st.session_state.get("__current_domain") or ""
                def canon_ga(u: str) -> str: return _canon(u, base)
                ga_df = ga_df.rename(columns={url_col:"ga_url", pv_col:"pageviews"})
                ga_df["url"] = ga_df["ga_url"].map(canon_ga)
                ga_df["pageviews"] = pd.to_numeric(ga_df["pageviews"], errors="coerce").fillna(0).astype(int)
                ga_top = ga_df.sort_values("pageviews", ascending=False).head(100).copy()
                st.session_state["ga_top100"] = ga_top[["url","pageviews"]]
                st.success("Indlæst GA top 100. Se fanen 'Fokus (Top 100)'.")
# =================== Tabs ===================
tab_overview, tab_stats, tab_done, tab_review, tab_focus = st.tabs(["Oversigt", "Statistik", "Færdige sider", "Needs Review", "Fokus (Top 100)"])

# -------- Oversigt --------
with tab_overview:
    st.subheader("Oversigt")
    c1, c2, c3 = st.columns([2,1,1])
    q = c1.text_input("Søg (URL/keywords)", value="")
    min_total = c2.number_input("Min. total", min_value=0, value=0, step=1)
    try:
        status_choice = c3.segmented_control("Status", options=["Alle","Todo","Needs Review","Done"], default="Alle")
    except Exception:
        status_choice = c3.selectbox("Status", ["Alle","Todo","Needs Review","Done"], index=0)
    status_arg = {"Alle": None, "Todo":"todo", "Needs Review":"review", "Done":"done"}[status_choice]

    rows, total_count = db.get_pages(search=q.strip() or None, min_total=int(min_total), status=status_arg,
                                     sort_by="total", sort_dir="desc", limit=10000, offset=0)
    st.caption(f"Viser {len(rows)} af {total_count} sider")
    if not rows:
        st.info("Ingen sider matcher filtrene.")
    else:
        df = pd.DataFrame([dict(r) for r in rows])
        for col, default in [("url",""),("keywords",""),("hits",0),("total",0),("status","todo"),("notes",""),("assigned_to","")]:
            if col not in df.columns: df[col] = default
        df["URL"] = df["url"]; df["Keywords"] = df["keywords"].fillna("")
        df["Hits"] = pd.to_numeric(df["hits"], errors="coerce").fillna(0).astype(int)
        df["Total"] = pd.to_numeric(df["total"], errors="coerce").fillna(0).astype(int)
        df["Status"] = df["status"].map({"todo":"Todo","done":"Done","review":"Needs Review"}).fillna("Todo")
        df["Assigned to"] = df["assigned_to"].fillna("").replace({None:""})
        df["Noter"] = df["notes"].fillna("")
        view = df[["URL","Keywords","Hits","Total","Status","Assigned to","Noter"]]

        edited = st.data_editor(
            view, width="stretch", hide_index=True,
            column_config={
                "URL": st.column_config.LinkColumn(help="Klik for at åbne siden"),
                "Keywords": st.column_config.TextColumn(width="large"),
                "Hits": st.column_config.NumberColumn(format="%d"),
                "Total": st.column_config.NumberColumn(format="%d"),
                "Status": st.column_config.SelectboxColumn(options=["Todo","Needs Review","Done"]),
                "Assigned to": st.column_config.SelectboxColumn(options=["– Ingen –","RAGL","CEYD","ULRS","LBY","JAWER"]),
                "Noter": st.column_config.TextColumn(),
            },
            disabled=["URL","Keywords","Hits","Total"],
            key="overview_editor",
            on_change=lambda: st.session_state.update({"overview_changed": True}),
        )
        if st.session_state.get("overview_changed", False):
            changed = 0
            for i, row in edited.iterrows():
                orig = df.loc[i]; url = orig["URL"]
                if row["Status"] != orig["Status"]:
                    db.update_status(url, {"Todo":"todo","Done":"done","Needs Review":"review"}[row["Status"]]); changed += 1
                if row["Noter"] != orig["Noter"]:
                    db.update_notes(url, row["Noter"]); changed += 1
                new_assign = "" if row["Assigned to"] == "– Ingen –" else row["Assigned to"]
                if new_assign != orig["Assigned to"]:
                    db.update_assigned_to(url, new_assign); changed += 1
            if changed:
                st.success(f"GEMT: {# app.py – NIRAS Greenwashing-dashboard (stabil version)
from __future__ import annotations

import io, os, re, time, math, csv
from pathlib import Path
from urllib.parse import urlparse
from typing import Optional, List

import pandas as pd
import streamlit as st

import db
import charts as ch
from crawler import crawl_iter, scan_pages, DEFAULT_KW

st.set_page_config(page_title="NIRAS greenwashing-dashboard", layout="wide")

# =================== Utils ===================
SETTINGS_PATH = Path("data") / "settings.json"

def _load_settings() -> dict:
    try:
        if SETTINGS_PATH.exists():
            return pd.read_json(SETTINGS_PATH).to_dict() or {}
    except Exception:
        pass
    return {}

def _save_settings(obj: dict):
    try:
        SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
        pd.Series(obj).to_json(SETTINGS_PATH)
    except Exception:
        pass

def _canon(u: str, base: str | None = None) -> str:
    u = (str(u) or "").strip()
    if not u:
        return ""
    if u.startswith("/") and base:
        u = base.rstrip("/") + u
    p = urlparse(u)
    if not p.scheme or not p.netloc:
        return ""
    clean = p._replace(fragment="").geturl()
    if not clean.endswith("/"):
        clean += "/"
    return clean

def big_green_progress(completion: float, total: int, done: int):
    pct = max(0, min(int(round((completion or 0.0) * 100)), 100))
    st.markdown(
        f"""
        <div style="margin: 8px 0 18px 0; border:1px solid #e5e7eb; border-radius:12px; overflow:hidden;">
          <div style="padding:10px 14px; font-weight:600;">Fremskridt</div>
          <div style="height:26px; background:#e5e7eb; position:relative;">
            <div style="height:100%; width:{pct}%; background:#10b981; transition:width .3s;"></div>
            <div style="position:absolute; top:0; left:0; right:0; height:100%; display:flex; align-items:center; justify-content:center; font-weight:600;">
              {pct}% &nbsp; <span style="font-weight:400; color:#374151">({done} af {total} sider)</span>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# =================== Header + DB init ===================
st.markdown("### Velkommen til Greenwashing-radaren")
st.markdown("Filtrér, redigér og find forekomster hurtigt. Navigation/related tælles ikke med i forekomster.")

db.init_db()

# Valgfri auto-import fra fil (på tom DB eller når fil ændres)
AUTO_IMPORT = os.environ.get("AUTO_IMPORT_FILE", "data/crawl.csv")
def _read_any(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists(): return pd.DataFrame()
    if p.suffix.lower() in (".xlsx",".xls"):
        return pd.read_excel(p)
    return pd.read_csv(p)

try:
    s0 = db.stats()
    if s0.get("total",0) == 0 and Path(AUTO_IMPORT).exists():
        df_init = _read_any(AUTO_IMPORT)
        if not df_init.empty:
            db.sync_pages_from_df(df_init)
except Exception:
    pass

s0 = db.stats()
big_green_progress(s0.get("completion", 0.0), s0.get("total", 0), s0.get("done", 0))

# =================== Sidebar ===================
with st.sidebar:
    st.header("Data")

    # ---- Wide → standard (fix for 'antal_forekomster' m.fl.)
    META_PAT = re.compile(r"(total|sum|matches?|forekomst(er)?|antal(_)?forekomster?)$", re.I)
    def normalize_wide(df: pd.DataFrame) -> pd.DataFrame | None:
        if df is None or df.empty:
            return None
        cols_lower = {str(c).strip().lower(): c for c in df.columns}
        url_col = cols_lower.get("url")
        if not url_col:
            return None
        def _is_num(s):
            try: return pd.api.types.is_numeric_dtype(s)
            except: return False
        # find total/meta-kolonne (fx 'antal_forekomster')
        total_col = None
        for c in df.columns:
            name = str(c).strip().lower()
            if META_PAT.fullmatch(name) or META_PAT.search(name):
                total_col = c
                break
        numeric_cols = [c for c in df.columns if c != url_col and _is_num(df[c])]
        kw_cols = [c for c in numeric_cols if not META_PAT.search(str(c).strip().lower())]
        if not kw_cols and total_col is None:
            return None

        tmp = df.copy()
        if total_col:
            total_series = pd.to_numeric(tmp[total_col], errors="coerce").fillna(0).astype(int)
        else:
            total_series = pd.to_numeric(tmp[kw_cols].fillna(0)).sum(axis=1).astype(int)

        def mk_keywords_csv(row):
            used = []
            for c in kw_cols:
                try: v = int(row.get(c, 0) or 0)
                except: v = 0
                if v > 0: used.append(str(c))
            return ",".join(used)

        keywords_csv = tmp.apply(mk_keywords_csv, axis=1) if kw_cols else pd.Series([""]*len(tmp))
        hits_series = total_series

        return pd.DataFrame({
            "url": tmp[url_col].astype(str),
            "keywords": keywords_csv,
            "hits": hits_series,
            "total": total_series,
        })

    # Vælg/Upload fil
    default_path = os.path.join("data","crawl.csv")
    path_str = st.text_input("Sti til CSV/Excel", value=default_path)
    uploaded = st.file_uploader("...eller upload fil", type=["csv","xlsx","xls"])
    file_source = uploaded if uploaded else (path_str if path_str.strip() else None)

    df_std, label = None, "Ingen"
    if file_source:
        try:
            if uploaded is not None:
                label = uploaded.name
                raw = uploaded.getvalue()
            else:
                label = str(path_str)
                raw = Path(path_str).read_bytes() if Path(path_str).exists() else b""
            if raw:
                df_try = None
                # Excel?
                if label.lower().endswith((".xlsx",".xls")):
                    for kwargs in ({"engine": None},{"engine":"openpyxl"}):
                        try:
                            df_try = pd.read_excel(io.BytesIO(raw), **{k:v for k,v in kwargs.items() if v is not None})
                            if df_try is not None and not df_try.empty: break
                        except Exception: df_try = None
                # CSV?
                if df_try is None:
                    for kwargs in (
                        {"engine":"python","encoding":"utf-8","comment":"#","on_bad_lines":"skip"},
                        {"sep":";","engine":"python","encoding":"utf-8","comment":"#","on_bad_lines":"skip"},
                        {"sep":",","engine":"python","encoding":"utf-8","comment":"#","on_bad_lines":"skip"},
                    ):
                        try:
                            df_try = pd.read_csv(io.BytesIO(raw), **kwargs)
                            if df_try is not None and not df_try.empty: break
                        except Exception: df_try=None
                if df_try is not None and not df_try.empty:
                    cols_l = {c.lower() for c in df_try.columns}
                    if {"url","keywords","hits","total"}.issubset(cols_l):
                        df_std = df_try.rename(columns={c:c.lower() for c in df_try.columns})
                    else:
                        nw = normalize_wide(df_try)
                        df_std = nw if (nw is not None and not nw.empty) else pd.DataFrame()
                else:
                    st.warning("Kunne ikke læse filen.")
        except Exception as e:
            st.error(f"Fejl ved indlæsning: {e}")
            df_std = None

    st.caption(f"Datakilde: **{label}**")

    # Import (upsert kun kw/hits/total – bevar status)
    if st.button("Importér", type="primary"):
        if df_std is None or df_std.empty:
            st.warning("Ingen gyldig data fundet."); st.stop()
        base = st.session_state.get("__current_domain")
        df_imp = df_std.copy()
        for c in ("url","keywords","hits","total"):
            if c not in df_imp.columns:
                df_imp[c] = "" if c in ("url","keywords") else 0
        df_imp["url"] = df_imp["url"].map(lambda u: _canon(u, base))
        df_imp = df_imp[df_imp["url"] != ""]
        df_imp["keywords"] = df_imp["keywords"].fillna("").astype(str)
        df_imp["hits"] = pd.to_numeric(df_imp["hits"], errors="coerce").fillna(0).astype(int)
        df_imp["total"] = pd.to_numeric(df_imp["total"], errors="coerce").fillna(0).astype(int)

        rows = df_imp.to_dict("records")
        BATCH = 500
        synced = 0
        for i in range(0, len(rows), BATCH):
            chunk = pd.DataFrame(rows[i:i+BATCH])
            tries = 0
            while True:
                try:
                    db.sync_pages_from_df(chunk); synced += len(chunk); break
                except Exception:
                    tries += 1
                    if tries >= 3: st.error("DB-fejl ved import."); break
                    st.warning(f"DB-fejl (forsøg {tries}). Prøver igen om 1s…"); time.sleep(1)
        st.success(f"Import færdig. {synced} rækker synkroniseret.")
        st.rerun()

    st.markdown("---")
    st.header("Crawler")

    domain = st.selectbox("Domæne", ["https://www.niras.dk/","https://www.niras.com/"])
    st.session_state["__current_domain"] = domain

    default_kw_text = "\n".join(DEFAULT_KW)
    kw_text = st.text_area(
        "Søgeord & udsagn (ét pr. linje)",
        value=default_kw_text,
        help="* som wildcard (fx 'bæredygtig*'). Regex som /co2[- ]?neutral/."
    )
    kw_list_manual = [k.strip() for k in re.split(r"[\n,;]", kw_text) if k.strip()]

    # Flet med keywords fra indlæst fil
    merge_with_file = st.checkbox("Flet med keywords fra datakilden", value=True)
    kw_from_file = []
    if merge_with_file and (df_std is not None) and (not df_std.empty):
        try:
            all_kw = []
            for _, row in df_std.iterrows():
                for k in re.split(r"[;,]", str(row.get("keywords",""))):
                    k = k.strip()
                    if k: all_kw.append(k)
            seen=set()
            kw_from_file=[k for k in all_kw if not (k in seen or seen.add(k))]
        except Exception:
            kw_from_file=[]
    kw_seen, kw_final = set(), []
    for k in kw_list_manual + kw_from_file:
        if k and (k not in kw_seen):
            kw_seen.add(k); kw_final.append(k)

    st.caption("—")
    settings = _load_settings()
    exclude_text = st.text_area("Ekskludér ord/fraser (ét pr. linje)",
                                value="\n".join(settings.get("exclude", [])),
                                help="Filtreres væk fra brødtekst (ikke fra keyword-listen).",
                                key="exclude_kw_text")
    kw_exclude = {k.strip().lower() for k in re.split(r"[\n,;]", exclude_text) if k.strip()}
    st.session_state["kw_final"] = kw_final
    st.session_state["kw_exclude"] = sorted(list(kw_exclude)) if kw_exclude else []
    excl_sig = (exclude_text or "").strip()
    if st.session_state.get("__exclude_sig") != excl_sig:
        st.session_state["__exclude_sig"] = excl_sig
        _save_settings({"exclude":[k for k in excl_sig.split("\n") if k.strip()]})
        st.rerun()

    st.caption(f"🧩 Keywords i brug: {len(kw_final)}")

    if st.button("🚀 Crawl hele domænet", type="secondary"):
        if not kw_final:
            st.warning("Tilføj mindst ét ord/udsagn.")
        else:
            db.init_db()
            prog = st.progress(0, text="Starter crawler…")
            rows_buf, BATCH, db_errors = [], 150, 0
            def on_progress(done:int, queued:int):
                prog.progress(min(0.99, done/8000), text=f"Crawler… {done} sider · kø: {queued}")
            for row in crawl_iter(domain, kw_final, max_pages=12000, max_depth=100, delay=0.5,
                                  progress_cb=on_progress, excludes=st.session_state.get("kw_exclude", [])):
                rows_buf.append(row)
                if len(rows_buf) >= BATCH:
                    try:
                        db.sync_pages_from_df(pd.DataFrame(rows_buf))
                        rows_buf.clear(); db_errors = 0
                    except Exception:
                        db_errors += 1
                        st.warning(f"DB-fejl (forsøg {db_errors}). Prøver igen om 1s…")
                        time.sleep(1)
                        try:
                            db.sync_pages_from_df(pd.DataFrame(rows_buf))
                            rows_buf.clear(); db_errors = 0
                        except Exception:
                            pass
            if rows_buf:
                try:
                    db.sync_pages_from_df(pd.DataFrame(rows_buf))
                except Exception:
                    st.warning("Kunne ikke skrive sidste batch; prøver igen…")
                    time.sleep(1)
                    try: db.sync_pages_from_df(pd.DataFrame(rows_buf))
                    except Exception as e: st.error(f"Kunne ikke skrive sidste batch: {e}")
            prog.progress(1.0, text="Crawler færdig")
            st.success("Crawl færdig."); st.rerun()

    st.markdown("---")
    st.subheader("Crawl fra fil (URL-liste)")
    urls_file = st.file_uploader("Upload .txt eller .csv med URL'er", type=["txt","csv"], key="url_list")
    if urls_file is not None:
        raw = urls_file.getvalue().decode("utf-8", errors="ignore")
        urls = []
        if urls_file.name.lower().endswith(".csv"):
            rdr = csv.DictReader(io.StringIO(raw))
            fld_l = [f.strip().lower() for f in (rdr.fieldnames or [])]
            url_field = None
            if "url" in fld_l:
                url_field = rdr.fieldnames[fld_l.index("url")]
            if url_field:
                for row in rdr:
                    u = (row.get(url_field) or "").strip()
                    if u: urls.append(u)
            else:
                urls = [row[0].strip() for row in csv.reader(io.StringIO(raw)) if row and row[0].strip()]
        else:
            urls = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        base = st.session_state.get("__current_domain")
        urls = [_canon(u, base) for u in urls]
        urls = [u for u in urls if u]
        urls = list(dict.fromkeys(urls))
        st.caption(f"Fundet {len(urls)} URL'er i filen.")
        if urls and st.button("Crawl uploadede URL'er nu", type="secondary"):
            st.info(f"Crawler {len(urls)} URL'er …")
            sub_prog = st.progress(0)
            batch = 40
            all_rows = []
            kw_final = st.session_state.get("kw_final", [])
            kw_excl = st.session_state.get("kw_exclude", [])
            from math import ceil
            total_batches = max(1, ceil(len(urls)/batch))
            for i in range(0, len(urls), batch):
                part = urls[i:i+batch]
                try:
                    part_rows = scan_pages(part, kw_final, excludes=kw_excl, delay=0.0)
                    if part_rows:
                        db.sync_pages_from_df(pd.DataFrame(part_rows))
                        all_rows.extend(part_rows)
                except Exception as e:
                    st.warning(f"Fejl ved batch {i//batch+1}: {e}")
                sub_prog.progress(min(1.0, (i//batch+1)/total_batches))
            st.success(f"Færdig. Opdateret {len(all_rows)} resultater i DB.")
            st.rerun()

    # ========== GA Top 100 ==========
    st.markdown("---")
    st.header("Google Analytics – Top 100")
    ga_file = st.file_uploader("Upload GA CSV/Excel (kolonner: URL/pagePath + pageviews)", type=["csv","xlsx","xls"], key="ga_csv")
    raw: bytes = b""
    src_name: str = ""
    if ga_file is not None:
        src_name = (ga_file.name or "").lower()
        raw = ga_file.getvalue() or b""
    else:
        default_ga_path = Path("data") / "Pageviews.csv"
        if default_ga_path.exists():
            src_name = str(default_ga_path).lower()
            try: raw = default_ga_path.read_bytes()
            except Exception: raw = b""

    if raw:
        ga_df = None
        name = src_name
        is_excel = name.endswith(".xlsx") or name.endswith(".xls")
        if is_excel:
            for kwargs in ({"engine": None},{"engine":"openpyxl"}):
                try:
                    ga_df = pd.read_excel(io.BytesIO(raw), **{k:v for k,v in kwargs.items() if v is not None})
                    if ga_df is not None and not ga_df.empty: break
                except Exception: ga_df=None
        if ga_df is None or ga_df.empty:
            for kwargs in (
                {"engine":"python","encoding":"utf-8","comment":"#","on_bad_lines":"skip"},
                {"sep":";","engine":"python","encoding":"utf-8","comment":"#","on_bad_lines":"skip"},
                {"sep":",","engine":"python","encoding":"utf-8","comment":"#","on_bad_lines":"skip"},
            ):
                try:
                    ga_df = pd.read_csv(io.BytesIO(raw), **kwargs)
                    if ga_df is not None and not ga_df.empty: break
                except Exception: ga_df=None
        if ga_df is None or ga_df.empty:
            st.warning("Kunne ikke læse GA-fil.")
        else:
            def _norm(s:str)->str: return re.sub(r"[^a-z]","",(str(s) or "").strip().lower())
            by_lower = {str(c).strip().lower(): c for c in ga_df.columns}
            by_norm = {_norm(c): c for c in ga_df.columns}
            url_keys = ["url","pagepath","page","pagelocation","landingpage","landingpagepath","pathname","pagepathandscreenclass"]
            pv_keys = ["pageviews","views","screenpageviews","screenpageview","screenviews"]
            url_col = None
            for k in url_keys:
                url_col = by_lower.get(k) or by_norm.get(k)
                if url_col: break
            if not url_col:
                for nk, orig in by_norm.items():
                    if ("pagepath" in nk) or ("pagelocation" in nk) or (nk=="url"):
                        url_col = orig; break
            pv_col = None
            for k in pv_keys:
                pv_col = by_lower.get(k) or by_norm.get(k)
                if pv_col: break
            if not url_col or not pv_col:
                st.warning("GA-fil mangler URL/pagePath og pageviews.")
            else:
                def canon_ga(u: str) -> str:
                    base = st.session_state.get("__current_domain") or ""
                    return _canon(u, base)
                ga_df = ga_df.rename(columns={url_col:"ga_url", pv_col:"pageviews"})
                ga_df["url"] = ga_df["ga_url"].map(canon_ga)
                ga_df["pageviews"] = pd.to_numeric(ga_df["pageviews"], errors="coerce").fillna(0).astype(int)
                ga_top = ga_df.sort_values("pageviews", ascending=False).head(100).copy()
                st.session_state["ga_top100"] = ga_top[["url","pageviews"]]
                st.success("Indlæst GA top 100. Se fanen 'Fokus (Top 100)'.")
# =================== Tabs ===================
tab_overview, tab_stats, tab_done, tab_review, tab_focus = st.tabs(["Oversigt","Statistik","Færdige sider","Needs Review","Fokus (Top 100)"])

# -------- Oversigt --------
with tab_overview:
    st.subheader("Oversigt")
    c1,c2,c3 = st.columns([2,1,1])
    q = c1.text_input("Søg (URL/keywords)", value="")
    min_total = c2.number_input("Min. total", min_value=0, value=0, step=1)
    try:
        status_choice = c3.segmented_control("Status", options=["Alle","Todo","Needs Review","Done"], default="Alle")
    except Exception:
        status_choice = c3.selectbox("Status", ["Alle","Todo","Needs Review","Done"], index=0)
    status_arg = {"Alle":None,"Todo":"todo","Needs Review":"review","Done":"done"}[status_choice]
    rows, total_count = db.get_pages(search=q.strip() or None, min_total=int(min_total), status=status_arg,
                                     sort_by="total", sort_dir="desc", limit=10000, offset=0)
    st.caption(f"Viser {len(rows)} af {total_count} sider")
    if not rows:
        st.info("Ingen sider matcher filtrene.")
    else:
        df = pd.DataFrame([dict(r) for r in rows])
        for col, default in [("url",""),("keywords",""),("hits",0),("total",0),("status","todo"),("notes",""),("assigned_to","")]:
            if col not in df.columns: df[col] = default
        df["URL"] = df["url"]; df["Keywords"] = df["keywords"].fillna("")
        df["Hits"] = pd.to_numeric(df["hits"], errors="coerce").fillna(0).astype(int)
        df["Total"] = pd.to_numeric(df["total"], errors="coerce").fillna(0).astype(int)
        df["Status"] = df["status"].map({"todo":"Todo","done":"Done","review":"Needs Review"}).fillna("Todo")
        df["Assigned to"] = df["assigned_to"].fillna("").replace({None:""})
        df["Noter"] = df["notes"].fillna("")
        view = df[["URL","Keywords","Hits","Total","Status","Assigned to","Noter"]]
        edited = st.data_editor(
            view, width="stretch", hide_index=True,
            column_config={
                "URL": st.column_config.LinkColumn(help="Åbn"),
                "Keywords": st.column_config.TextColumn(width="large"),
                "Hits": st.column_config.NumberColumn(format="%d"),
                "Total": st.column_config.NumberColumn(format="%d"),
                "Status": st.column_config.SelectboxColumn(options=["Todo","Needs Review","Done"]),
                "Assigned to": st.column_config.SelectboxColumn(options=["– Ingen –","RAGL","CEYD","ULRS","LBY","JAWER"]),
                "Noter": st.column_config.TextColumn(),
            },
            disabled=["URL","Keywords","Hits","Total"],
            key="overview_editor",
            on_change=lambda: st.session_state.update({"overview_changed": True}),
        )
        if st.session_state.get("overview_changed", False):
            changed = 0
            for i, row in edited.iterrows():
                orig = df.loc[i]; url = orig["URL"]
                if row["Status"] != orig["Status"]:
                    db.update_status(url, {"Todo":"todo","Done":"done","Needs Review":"review"}[row["Status"]]); changed += 1
                if row["Noter"] != orig["Noter"]:
                    db.update_notes(url, row["Noter"]); changed += 1
                new_assign = "" if row["Assigned to"] == "– Ingen –" else row["Assigned to"]
                if new_assign != orig["Assigned to"]:
                    db.update_assigned_to(url, new_assign); changed += 1
            if changed:
                st.success(f"GEMT: {changed} ændring(er)")
                st.session_state["overview_changed"] = False
                time.sleep(1.0); st.rerun()

# -------- Statistik --------
with tab_stats:
    st.subheader("Statistik & Progress")
    s = db.stats()
    ch.kpi_cards(s.get("total",0), s.get("done",0), s.get("todo",0), s.get("completion",0.0))

    # Statistik direkte fra DB
    rows, _ = db.get_pages(limit=100000, offset=0)
    df_all = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame(columns=["url","keywords","total"])
    def _split_csv(s): return [k.strip() for k in str(s or "").split(",") if k.strip()]

    if not df_all.empty:
        explode = df_all.assign(_kw=df_all["keywords"].map(_split_csv)).explode("_kw")
        explode = explode[explode["_kw"].notna() & (explode["_kw"]!="")]
        pages_per_kw = (explode.groupby("_kw")["url"].nunique().sort_values(ascending=False).head(15))
        totals_per_kw = (explode.groupby("_kw")["url"].size().sort_values(ascending=False).head(15))
    else:
        pages_per_kw = pd.Series(dtype=int); totals_per_kw = pd.Series(dtype=int)

    left, right = st.columns(2)
    with left:
        st.markdown("**Sider pr. keyword**")
        if pages_per_kw.empty: st.info("Ingen data endnu.")
        else: ch.bar_keyword_pages(pages_per_kw)
    with right:
        st.markdown("**Top-keywords (forekomster, est.)**")
        if totals_per_kw.empty: st.info("Ingen data endnu.")
        else: ch.bar_keyword_totals(totals_per_kw)

# -------- Færdige --------
with tab_done:
    st.subheader("Færdige sider")
    done_df = db.get_done_dataframe()
    if done_df.empty:
        st.info("Ingen færdige sider endnu.")
    else:
        st.dataframe(done_df, use_container_width=True, hide_index=True)
        st.download_button("Eksportér CSV", data=done_df.to_csv(index=False).encode("utf-8"),
                           file_name="faerdige_sider.csv", mime="text/csv")
        undo = st.multiselect("Fortryd til Todo", options=list(done_df.get("url", [])))
        if st.button("Fortryd valgte"):
            if undo: db.bulk_update_status(undo, "todo"); st.success("Status opdateret."); st.rerun()
            else: st.info("Vælg mindst én URL.")

# -------- Needs Review --------
with tab_review:
    st.subheader("Sider der kræver ekstra opmærksomhed")
    review_rows, _ = db.get_pages(status="review", limit=100000, offset=0)
    review_df = pd.DataFrame([dict(r) for r in review_rows]) if review_rows else pd.DataFrame()
    if review_df.empty:
        st.info("Ingen sider markeret som 'Needs Review' (endnu).")
    else:
        for col, default in [("url",""),("keywords",""),("total",0),("assigned_to",""),("notes","")]:
            if col not in review_df.columns: review_df[col] = default
        review_df["URL"] = review_df["url"]
        review_df["Keywords"] = review_df["keywords"].fillna("")
        review_df["Total"] = pd.to_numeric(review_df["total"], errors="coerce").fillna(0).astype(int)
        review_df["Assigned to"] = review_df["assigned_to"].fillna("").replace({None:""})
        review_df["Noter"] = review_df["notes"].fillna("")
        st.dataframe(review_df[["URL","Keywords","Total","Assigned to","Noter"]],
                     use_container_width=True, hide_index=True)
        c1,c2 = st.columns(2)
        resolve = st.multiselect("Markér som Done", options=list(review_df["url"]))
        back_to_todo = st.multiselect("Send tilbage til Todo", options=list(review_df["url"]), key="rv_todo")
        with c1:
            if st.button("Markér valgte som Done"):
                if resolve: db.bulk_update_status(resolve, "done"); st.success(f"{len(resolve)} sider markeret som Done."); st.rerun()
        with c2:
            if st.button("Send valgte til Todo"):
                if back_to_todo: db.bulk_update_status(back_to_todo, "todo"); st.success(f"{len(back_to_todo)} sider sendt til Todo."); st.rerun()

# -------- Fokus (Top 100) --------
with tab_focus:
    st.subheader("Google Analytics Top 100 – fokusliste")
    ga_top = st.session_state.get("ga_top100")
    if ga_top is None or len(ga_top) == 0:
        st.info("Upload en GA CSV i sidebar for at se top 100.")
    else:
        rows, _ = db.get_pages(limit=100000, offset=0)
        db_df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
        for col, default in [("url",""),("total",0),("status","todo"),("assigned_to","")]:
            if col not in db_df.columns: db_df[col] = default
        focus = ga_top.merge(db_df[["url","total","status","assigned_to"]], on="url", how="left")
        focus["total"] = pd.to_numeric(focus["total"], errors="coerce")
        focus["Matches (Total)"] = focus["total"].fillna(0).astype(int)
        focus["Status"] = focus["status"].fillna("todo").map({"todo":"Todo","done":"Done","review":"Needs Review"}).fillna("Todo")
        focus["Assigned to"] = focus["assigned_to"].fillna("").replace({None:""})

        # Primær visning: ikke-Done med >0 hits – op til 100 − Done
        done_in_ga = (focus["Status"] == "Done").sum()
        target = max(0, 100 - done_in_ga)

        df_show = focus[(focus["Status"] != "Done") & (focus["Matches (Total)"] > 0)].copy()
        if df_show.empty:
            # fallback 1: ikke-Done uanset hits
            df_show = focus[focus["Status"] != "Done"].copy()
        if df_show.empty:
            # fallback 2: vis ren GA top (viser fremdrift via Status)
            df_show = focus.copy()

        # Filtre
        c1,c2,c3 = st.columns([2.5,1,1])
        q = c1.text_input("Filtrér i URL (substring eller regex /…/)", value="", key="focus_url_q")
        prefix_mode = c2.checkbox("Starter med", value=False, key="focus_prefix")
        regex_mode = c3.checkbox("Regex", value=False, key="focus_regex")
        if q:
            if regex_mode and len(q)>=2 and q.startswith("/") and q.endswith("/"):
                try:
                    pat = re.compile(q[1:-1], re.IGNORECASE)
                    df_show = df_show[df_show["url"].astype(str).apply(lambda s: bool(pat.search(s)))]
                except Exception:
                    st.warning("Ugyldig regex – bruger substring.")
                    df_show = df_show[df_show["url"].str.contains(q.strip("/"), case=False, na=False)]
            elif prefix_mode:
                def _path_starts(u: str, prefix: str) -> bool:
                    try:
                        p = urlparse(u); path = (p.path or "/")
                        return path.lower().startswith(prefix.lower())
                    except Exception: return False
                df_show = df_show[df_show["url"].astype(str).apply(lambda u: _path_starts(u, q))]
            else:
                df_show = df_show[df_show["url"].str.contains(q, case=False, na=False)]

        # Sortér: mest trafik → flest hits, cap til target hvis relevant
        df_show = df_show.sort_values(["pageviews","Matches (Total)"], ascending=[False, False]).reset_index(drop=True)
        if target > 0 and len(df_show) > target:
            df_show = df_show.head(target)

        st.info(f"GA i alt: {len(focus)} · Done i GA: {done_in_ga} · Viser: {len(df_show)}")

        df_view = df_show[["url","pageviews","Matches (Total)","Status","Assigned to"]].copy()
        df_view.insert(0,"Vælg", False)

        edited = st.data_editor(
            df_view,
            use_container_width=True, hide_index=True, height=440, key="top100_editor",
            column_config={
                "Vælg": st.column_config.CheckboxColumn(default=False),
                "url": st.column_config.LinkColumn(help="Åbn"),
                "pageviews": st.column_config.NumberColumn(format="%d"),
                "Matches (Total)": st.column_config.NumberColumn(format="%d"),
                "Status": st.column_config.SelectboxColumn(options=["Todo","Needs Review","Done"]),
                "Assigned to": st.column_config.SelectboxColumn(options=["– Ingen –","CEYD","LBY","JAWER","ULRS"]),
            },
            disabled=["url","pageviews","Matches (Total)"],
            on_change=lambda: st.session_state.update({"top100_changed": True}),
        )

        # Autosave
        if st.session_state.get("top100_changed", False):
            changed = 0
            for i, row in edited.iterrows():
                if i >= len(df_show): continue
                url = df_show.loc[i, "url"]
                if row.get("Status") != df_show.loc[i, "Status"]:
                    db.update_status(url, {"Todo":"todo","Done":"done","Needs Review":"review"}[row["Status"]]); changed += 1
                new_assign = "" if row.get("Assigned to") == "– Ingen –" else row.get("Assigned to")
                if new_assign != df_show.loc[i, "Assigned to"]:
                    db.update_assigned_to(url, new_assign); changed += 1
            if changed:
                st.success(f"GEMT: {changed} ændring(er)")
                st.session_state["top100_changed"] = False
                time.sleep(1.0); st.rerun()

        # Bulk over editor
        selected_urls = edited[edited["Vælg"] == True]["url"].tolist()
        if selected_urls:
            st.info(f"{len(selected_urls)} valgt til bulk")
            b1,b2,b3 = st.columns(3)
            with b1:
                bulk_status = st.selectbox("Sæt status", ["Ingen ændring","Todo","Needs Review","Done"], key="bulk_status_top100")
            with b2:
                bulk_assign = st.selectbox("Tildel", ["Ingen ændring","– Ingen –","CEYD","LBY","JAWER","ULRS"], key="bulk_assign_top100")
            with b3:
                st.write(""); st.write("")
                if st.button("Udfør bulk opdatering", type="primary", key="bulk_execute_top100"):
                    changed = 0
                    if bulk_status != "Ingen ændring":
                        db.bulk_update_status(selected_urls, {"Todo":"todo","Done":"done","Needs Review":"review"}[bulk_status])
                        changed += len(selected_urls)
                    if bulk_assign != "Ingen ændring":
                        assign_val = "" if bulk_assign == "– Ingen –" else bulk_assign
                        for u in selected_urls: db.update_assigned_to(u, assign_val)
                        changed += len(selected_urls)
                    if changed: st.success(f"BULK GEMT: {len(selected_urls)} opdateret"); time.sleep(1.0); st.rerun()
                    else: st.info("Vælg mindst én ændring")
changed} ændring(er)")
                st.session_state["overview_changed"] = False
                time.sleep(1.0); st.rerun()

# -------- Statistik --------
with tab_stats:
    st.subheader("Statistik & Progress")
    s = db.stats()
    kpi_cards(s.get("total",0), s.get("done",0), s.get("todo",0), s.get("completion",0.0))

    # Keyword-statistik fra DB
    rows, _
