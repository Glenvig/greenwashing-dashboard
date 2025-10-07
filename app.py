# app.py – NIRAS Greenwashing-dashboard (stabil crawl + persistens)
from __future__ import annotations

import os, re, io, math, json, time
from pathlib import Path
from urllib.parse import urlparse
from typing import List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
import streamlit as st

import db
import data as d
import charts as ch
from crawler import crawl_iter, scan_pages, DEFAULT_KW, _cache_bust, HDRS

# ──────────────────────────────────────────────────────────────────────────────
# UI config
st.set_page_config(page_title="NIRAS greenwashing-dashboard", layout="wide")

# ──────────────────────────────────────────────────────────────────────────────
# Settings (persistens af ekskluderede ord)
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

# ──────────────────────────────────────────────────────────────────────────────
# Hjælpefunktioner (snippets)
ALLOWED_TAGS = {"h1","h2","h3","h4","h5","h6","p","li","strong","em","span","a"}
EXCLUDE_CLASS_EXACT = {"menulink","anchor-link"}
EXCLUDE_SUBSTRINGS = {"related"}
EXCLUDE_TAGS = {"nav","header","footer","aside"}

def _compile_kw_patterns(keywords):
    pats = {}
    for kw in keywords:
        k = kw.strip()
        if not k: 
            continue
        if k.startswith("/") and k.endswith("/") and len(k) >= 3:
            pat = re.compile(k[1:-1], flags=re.IGNORECASE)
        elif k.endswith("*"):
            base = re.escape(k[:-1])
            pat = re.compile(rf"\b{base}\w*\b", flags=re.IGNORECASE)
        else:
            pat = re.compile(rf"\b{re.escape(k)}\b", flags=re.IGNORECASE)
        pats[kw] = pat
    return pats

def _has_excluded_ancestor(node) -> bool:
    hops, cur = 0, node
    while cur is not None and hops < 12:
        name = (getattr(cur, "name", None) or "").lower()
        if name in EXCLUDE_TAGS:
            return True
        classes = [str(c).lower() for c in (cur.get("class") or [])]
        if any(c in EXCLUDE_CLASS_EXACT for c in classes): return True
        if any(any(sub in c for sub in EXCLUDE_SUBSTRINGS) for c in classes): return True
        nid = str(cur.get("id") or "").lower()
        if nid and any(sub in nid for sub in EXCLUDE_SUBSTRINGS): return True
        cur = getattr(cur, "parent", None)
        hops += 1
    return False

def _prestrip_excluded_containers(soup: BeautifulSoup):
    for el in soup.find_all(attrs={"class": re.compile(r"related", re.I)}):
        el.decompose()
    for el in soup.find_all(id=re.compile(r"related", re.I)):
        el.decompose()
    for tag in list(EXCLUDE_TAGS):
        for el in soup.find_all(tag):
            el.decompose()

def get_snippets(url: str, keywords_csv: str, max_per_kw: int = 25):
    u_fetch = _cache_bust(url)
    r = requests.get(u_fetch, headers=HDRS, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    _prestrip_excluded_containers(soup)

    keywords = [k.strip() for k in re.split(r"[;,]", keywords_csv or "") if k.strip()]
    pats = _compile_kw_patterns(keywords)
    excludes = {k.strip().lower() for k in (st.session_state.get("kw_exclude") or []) if k.strip()}

    rows = []
    for tag in soup.find_all(ALLOWED_TAGS):
        if _has_excluded_ancestor(tag):
            continue
        text = " ".join(tag.get_text(separator=" ", strip=True).split())
        if not text:
            continue
        for kw, pat in pats.items():
            matches = list(pat.finditer(text))
            if not matches:
                continue
            if excludes and any(ex in text.lower() for ex in excludes):
                continue
            for m in matches[:max_per_kw]:
                start, end = m.start(), m.end()
                left, right = max(0, start - 80), min(len(text), end + 80)
                rows.append({"keyword": kw, "tag": tag.name, "snippet": text[left:right]})
    rows.sort(key=lambda r: (r["keyword"].lower(), r["tag"]))
    return rows

def _highlight(snippet: str, kw: str):
    pat = _compile_kw_patterns([kw])[kw]
    return pat.sub(lambda m: f"<mark>{m.group(0)}</mark>", snippet)

# ──────────────────────────────────────────────────────────────────────────────
# UI komponenter
def big_green_progress(completion: float, total: int, done: int):
    pct = max(0, min(int(round((completion or 0.0) * 100)), 100))
    st.markdown(
        f"""
        <div style="margin:8px 0 18px 0;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden;">
          <div style="padding:10px 14px;font-weight:600;">Fremskridt</div>
          <div style="height:26px;background:#e5e7eb;position:relative;">
            <div style="height:100%;width:{pct}%;background:#10b981;transition:width .3s;"></div>
            <div style="position:absolute;top:0;left:0;right:0;height:100%;display:flex;align-items:center;justify-content:center;font-weight:600;">
              {pct}% &nbsp; <span style="font-weight:400;color:#374151">({done} af {total} sider)</span>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

BADGE_COPY = {
    "first_10": ("Første 10 sider", "🚀 God start – I er i orbit!"),
    "fifty_percent": ("50% complete", "🧹 Halvvejs gennem greenwash-støvet"),
    "hundred_done": ("100 sider done", "🏆 Vaskemaskinen er tømt"),
}

def _meter_color(pct: float) -> str:
    if pct >= 0.85: return "#059669"
    if pct >= 0.60: return "#10b981"
    if pct >= 0.35: return "#f59e0b"
    return "#ef4444"

def greenwash_meter(completion_pct: float):
    c = _meter_color(completion_pct)
    nice = int(round(completion_pct * 100))
    quips = ["🧽 Der skrubbes løs…","🔍 Detektoren kalibreres…","🪣 Næsten rent vand!","🌈 Ren samvittighed i sigte!"]
    joke = quips[min(3, math.floor(completion_pct * 4))]
    st.markdown(
        f"<div style='border-radius:12px;padding:14px 16px;background:linear-gradient(90deg,{c} {nice}%,#e5e7eb {nice}%);color:#111;'><b>Greenwash-o-meter:</b> {nice}% &nbsp; {joke}</div>",
        unsafe_allow_html=True,
    )

def celebrate(unlocked: Optional[List[str]]):
    if not unlocked: return
    try:
        for key in unlocked:
            title, desc = BADGE_COPY.get(key, (key, ""))
            st.toast(f"🏅 Badge låst op: {title} – {desc}")
    except Exception:
        pass

# ──────────────────────────────────────────────────────────────────────────────
# Header + DB status
st.markdown("### Velkommen til Greenwashing-radaren")
st.markdown("Filtrér, redigér og find forekomster hurtigt. Navigation/related tælles ikke med i forekomster.")

db.init_db()
try:
    info = db._select("SELECT current_database() AS db, current_schema() AS schema, current_user AS usr")
    st.caption(f"DB: {info.iloc[0]['db']} · schema: {info.iloc[0]['schema']} · user: {info.iloc[0]['usr']}")
except Exception as e:
    st.caption(f"DB-info fejl: {e}")

s0 = db.stats()
big_green_progress(s0["completion"], s0["total"], s0["done"])

# ──────────────────────────────────────────────────────────────────────────────
# Sidebar – Data + Crawl + GA top 100
with st.sidebar:
    st.header("Data")
    default_path = os.path.join("data", "crawl.csv")
    path_str = st.text_input("Sti til CSV/Excel", value=default_path)
    uploaded = st.file_uploader("...eller upload fil", type=["csv","xlsx","xls"])
    file_source = uploaded if uploaded else (path_str if path_str.strip() else None)

    df_std, kw_long, is_demo, label = d.load_dataframe_from_file(file_source=file_source)
    st.caption(f"Datakilde: **{label}**{' (DEMO)' if is_demo else ''}")

    if st.button("Importér", type="primary", key="import_btn"):
        db.init_db()
        db.sync_pages_from_df(df_std)
        st.success("Data importeret.")
        st.rerun()

    st.markdown("---")
    st.header("Crawler")

    domain = st.selectbox("Domæne", ["https://www.niras.dk/","https://www.niras.com/"])

    default_kw_text = "\n".join(DEFAULT_KW)
    kw_text = st.text_area(
        "Søgeord & udsagn (ét pr. linje)",
        value=default_kw_text,
        help="Brug * som wildcard (fx 'bæredygtig*'). Avanceret: regex som /co2[- ]?neutral/."
    )
    kw_list_manual = [k.strip() for k in re.split(r"[\n,;]", kw_text) if k.strip()]

    merge_with_file = st.checkbox("Flet med keywords fra datakilden", value=True)
    kw_from_file = []
    if merge_with_file and (df_std is not None) and (not df_std.empty):
        try:
            all_kw = []
            for _, row in df_std.iterrows():
                all_kw.extend(d.split_keywords(row.get("keywords","")))
            seen=set(); kw_from_file=[k for k in all_kw if not (k in seen or seen.add(k))]
        except Exception:
            kw_from_file = []

    kw_seen=set(); kw_final=[]
    for k in kw_list_manual + kw_from_file:
        if k and (k not in kw_seen):
            kw_seen.add(k); kw_final.append(k)

    st.caption("—")
    settings = _load_settings()
    exclude_text = st.text_area(
        "Ekskludér ord/fraser (ét pr. linje)",
        value="\n".join(settings.get("exclude", [])),
        help="Ord/udtryk her bliver fjernet fra listen af søgeord ovenfor.",
        key="exclude_kw_text"
    )
    kw_exclude = {k.strip().lower() for k in re.split(r"[\n,;]", exclude_text) if k.strip()}
    if kw_exclude:
        kw_final = [k for k in kw_final if k.strip().lower() not in kw_exclude]

    st.caption(f"🧩 Keywords i brug: {len(kw_final)}")
    st.session_state["kw_final"] = kw_final
    st.session_state["kw_exclude"] = sorted(list(kw_exclude)) if kw_exclude else []

    excl_sig = (exclude_text or "").strip()
    if st.session_state.get("__exclude_sig") != excl_sig:
        st.session_state["__exclude_sig"] = excl_sig
        _save_settings({"exclude": [k for k in excl_sig.split("\n") if k.strip()]})
        st.rerun()

    if st.button("🚀 Crawl hele domænet", type="secondary", key="crawl_all_btn"):
        if not kw_final:
            st.warning("Tilføj mindst ét ord/udsagn (eller slå flet med datakilden til).")
        else:
            db.init_db()
            prog = st.progress(0, text="Starter crawler…")
            rows = []
            db_errors = 0

            def on_progress(done: int, queued: int):
                pct = min(0.99, done / 5000)
                prog.progress(pct, text=f"Crawler… {done} sider behandlet · kø: {queued}")

            BATCH = 300
            for row in crawl_iter(
                domain,
                kw_final,
                max_pages=5000,
                max_depth=50,
                delay=0.5,                 # ro på til net/DB
                # jitter=True,              # brug hvis crawler understøtter det
                progress_cb=on_progress,
                excludes=st.session_state.get("kw_exclude", [])
            ):
                rows.append(row)
                if len(rows) % BATCH == 0:
                    cdf_tmp = pd.DataFrame(rows[-BATCH:])
                    try:
                        db.sync_pages_from_df(cdf_tmp)
                        db_errors = 0
                    except Exception:
                        db_errors += 1
                        st.warning(f"DB-fejl (forsøg {db_errors}). Prøver igen om 1s…")
                        time.sleep(1)
                        try:
                            db.sync_pages_from_df(cdf_tmp)
                            db_errors = 0
                        except Exception:
                            st.error("DB-fejl igen. Skriver i små bidder for at fortsætte…")
                            for i in range(0, len(cdf_tmp), 50):
                                db.sync_pages_from_df(cdf_tmp.iloc[i:i+50])
                            db_errors = 0

            prog.progress(1.0, text=f"Crawler færdig – {len(rows)} sider")

            if rows:
                cdf = pd.DataFrame(rows)
                cdf = cdf[cdf["url"].astype(str).str.startswith(("http://","https://"))].copy()
                db.sync_pages_from_df(cdf)
                stats_after = db.stats()
                st.success(f"Crawl færdig: {len(cdf)} sider behandlet. DB total: {stats_after.get('total', 0)}")
                st.rerun()
            else:
                st.info("Ingen sider fundet eller ingen matches (tjek domæne/keywords).")

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
            try:
                raw = default_ga_path.read_bytes()
            except Exception:
                raw = b""
    if raw:
        ga_df = None
        name = src_name
        is_excel = name.endswith(".xlsx") or name.endswith(".xls")
        if is_excel:
            for kwargs in ({"engine": None},{"engine":"openpyxl"}):
                try:
                    ga_df = pd.read_excel(io.BytesIO(raw), **{k:v for k,v in kwargs.items() if v is not None})
                    if ga_df is not None and not ga_df.empty: break
                except Exception:
                    ga_df = None
        if ga_df is None or ga_df.empty:
            for kwargs in (
                {"engine":"python","encoding":"utf-8","comment":"#","on_bad_lines":"skip"},
                {"sep":";","engine":"python","encoding":"utf-8","comment":"#","on_bad_lines":"skip"},
                {"sep":",","engine":"python","encoding":"utf-8","comment":"#","on_bad_lines":"skip"},
            ):
                try:
                    ga_df = pd.read_csv(io.BytesIO(raw), **kwargs)
                    if ga_df is not None and not ga_df.empty: break
                except Exception:
                    ga_df = None
        if ga_df is None or ga_df.empty:
            st.warning("Kunne ikke læse filen. For Excel kræves ofte 'openpyxl'. Alternativt upload CSV.")
            st.stop()

        def _norm_name(s: str) -> str:
            s = (str(s) or "").strip().lower()
            return re.sub(r"[^a-z]", "", s)

        by_lower = {str(c).strip().lower(): c for c in ga_df.columns}
        by_norm  = {_norm_name(c): c for c in ga_df.columns}

        url_keys = ["url","pagepath","page","pagelocation","landingpage","landingpagepath","pathname","pagepathandscreenclass"]
        pv_keys  = ["pageviews","views","screenpageviews","screenpageview","screenviews"]

        url_col = None
        for k in url_keys:
            url_col = by_lower.get(k) or by_norm.get(k)
            if url_col: break
        if not url_col:
            for norm_key, orig in by_norm.items():
                if ("pagepath" in norm_key) or ("pagelocation" in norm_key) or (norm_key == "url"):
                    url_col = orig; break

        pv_col = None
        for k in pv_keys:
            pv_col = by_lower.get(k) or by_norm.get(k)
            if pv_col: break
        if not pv_col:
            for norm_key, orig in by_norm.items():
                if norm_key.endswith("views") or ("pageviews" in norm_key) or ("screenpageviews" in norm_key):
                    pv_col = orig; break

        if not url_col or not pv_col:
            st.warning(f"CSV skal have URL/pagePath og pageviews. Fandt kolonner: {list(ga_df.columns)}")
            st.stop()

        ga_df = ga_df.rename(columns={url_col:"ga_url", pv_col:"pageviews"})

        def canon(u: str) -> str:
            u = (str(u) or "").strip()
            if not u: return u
            if u.startswith("/"):
                base = domain.rstrip("/")
                u = base + u
            p = urlparse(u)
            clean = p._replace(fragment="").geturl()
            if not clean.endswith("/"):
                clean += "/"
            return clean

        ga_df["url"] = ga_df["ga_url"].map(canon)
        ga_df["pageviews"] = pd.to_numeric(ga_df["pageviews"], errors="coerce").fillna(0).astype(int)
        ga_top = ga_df.sort_values("pageviews", ascending=False).head(100).copy()
        st.session_state["ga_top100"] = ga_top[["url","pageviews"]]
        st.success(f"Indlæst {len(ga_top)} GA-rækker (top 100). Se fanen 'Fokus (Top 100)'.")

# Seed demo KUN hvis DB er tom
if s0["total"] == 0:
    try:
        db.sync_pages_from_df(df_std)
        s0 = db.stats()
    except Exception:
        pass

# ──────────────────────────────────────────────────────────────────────────────
# Tabs
tab_overview, tab_stats, tab_done, tab_review, tab_focus = st.tabs(
    ["Oversigt","Statistik","Færdige sider","Needs Review","Fokus (Top 100)"]
)

# ──────────────────────────────────────────────────────────────────────────────
# OVERSIGT
with tab_overview:
    st.subheader("Oversigt")
    st.session_state.setdefault("__snips_for_url", None)

    c1, c2, c3 = st.columns([2,1,1])
    q = c1.text_input("Søg (URL/keywords)", value="", placeholder="fx 'co2-neutral'")
    min_total = c2.number_input("Min. total", min_value=0, value=0, step=1)
    try:
        status_choice = c3.segmented_control("Status", options=["Alle","Todo","Needs Review","Done"], default="Alle")
    except Exception:
        status_choice = c3.selectbox("Status", ["Alle","Todo","Needs Review","Done"], index=0)
    status_arg = {"Alle":None, "Todo":"todo", "Needs Review":"review", "Done":"done"}[status_choice]

    rows, total_count = db.get_pages(
        search=q.strip() or None,
        min_total=int(min_total),
        status=status_arg,
        sort_by="total",
        sort_dir="desc",
        limit=10000,
        offset=0,
    )
    st.caption(f"Viser {len(rows)} af {total_count} sider")

    if not rows:
        st.info("Ingen sider matcher filtrene.")
    else:
        df = pd.DataFrame([dict(r) for r in rows])
        for col, default in [("url",""),("keywords",""),("hits",0),("total",0),("status","todo"),("notes",""),("assigned_to","")]:
            if col not in df.columns: df[col] = default

        df = df[pd.to_numeric(df.get("total",0), errors="coerce").fillna(0) > 0].copy()
        df["URL"] = df["url"]
        df["Keywords"] = df["keywords"].fillna("")
        df["Hits"] = pd.to_numeric(df["hits"], errors="coerce").fillna(0).astype(int)
        df["Total"] = pd.to_numeric(df["total"], errors="coerce").fillna(0).astype(int)
        df["Status"] = df["status"].map({"todo":"Todo","done":"Done","review":"Needs Review"}).fillna("Todo")
        df["Assigned to"] = df["assigned_to"].fillna("").replace({None:""})
        df["Noter"] = df["notes"].fillna("")
        df["Vælg"] = False

        view = df[["Vælg","URL","Keywords","Hits","Total","Status","Assigned to","Noter"]]
        bulk_placeholder = st.empty()
        edited = st.data_editor(
            view,
            width="stretch",
            hide_index=True,
            column_config={
                "Vælg": st.column_config.CheckboxColumn(help="Vælg til bulk opdatering", default=False),
                "URL": st.column_config.LinkColumn(help="Klik for at åbne siden"),
                "Keywords": st.column_config.TextColumn(width="large"),
                "Hits": st.column_config.NumberColumn(format="%d"),
                "Total": st.column_config.NumberColumn(format="%d"),
                "Status": st.column_config.SelectboxColumn(options=["Todo","Needs Review","Done"]),
                "Assigned to": st.column_config.SelectboxColumn(options=["– Ingen –","RAGL","CEYD","ULRS","LBY","JAWER"]),
                "Noter": st.column_config.TextColumn(),
            },
            disabled=["URL","Keywords","Hits","Total"],
            height=440,
            key="overview_editor",
            on_change=lambda: st.session_state.update({"overview_changed": True}),
        )

        # Bulk fra editor (øverst)
        selected_urls = edited[edited["Vælg"] == True]["URL"].tolist()
        if selected_urls:
            with bulk_placeholder.container():
                st.info(f"**{len(selected_urls)} sider valgt til bulk opdatering**")
                col1, col2, col3 = st.columns(3)
                with col1:
                    bulk_status = st.selectbox("Sæt status til", ["Ingen ændring","Todo","Needs Review","Done"], key="bulk_status_overview")
                with col2:
                    bulk_assign = st.selectbox("Tildel til", ["Ingen ændring","– Ingen –","RAGL","CEYD","ULRS","LBY","JAWER"], key="bulk_assign_overview")
                with col3:
                    st.write(""); st.write("")
                    if st.button("Udfør bulk opdatering", type="primary", key="bulk_execute_overview"):
                        changed = 0
                        if bulk_status != "Ingen ændring":
                            status_map = {"Todo":"todo","Done":"done","Needs Review":"review"}
                            db.bulk_update_status(selected_urls, status_map[bulk_status])
                            changed += len(selected_urls)
                        if bulk_assign != "Ingen ændring":
                            assign_val = "" if bulk_assign == "– Ingen –" else bulk_assign
                            for url in selected_urls:
                                db.update_assigned_to(url, assign_val)
                            changed += len(selected_urls)
                        if changed > 0:
                            st.success(f"BULK GEMT: {len(selected_urls)} sider opdateret")
                            time.sleep(1.5); st.rerun()
                        else:
                            st.info("Vælg mindst én ændring at udføre")

        # Auto-gem enkeltændringer
        if st.session_state.get("overview_changed", False):
            changed = 0
            for i, row in edited.iterrows():
                orig = df.loc[i]; url = orig["URL"]
                if row["Status"] != orig["Status"]:
                    status_map = {"Todo":"todo","Done":"done","Needs Review":"review"}
                    db.update_status(url, status_map.get(row["Status"], "todo")); changed += 1
                if row["Noter"] != orig["Noter"]:
                    db.update_notes(url, row["Noter"]); changed += 1
                new_assign = "" if row["Assigned to"] == "– Ingen –" else row["Assigned to"]
                if new_assign != orig["Assigned to"]:
                    db.update_assigned_to(url, new_assign); changed += 1
            if changed:
                newly = []
                try: newly = db.check_milestones()
                except Exception: pass
                celebrate(newly)
                st.success(f"GEMT: {changed} ændring(er)")
                st.session_state["overview_changed"] = False
                time.sleep(1.5); st.rerun()

        # Se forekomster / enkeltside opdatering
        st.divider()
        st.markdown("#### Alle sider – søg og se forekomster")
        s1, s2 = st.columns([3,1])
        url_query = s1.text_input("Søg i URL'er (live)", value="", placeholder="skriv fx '/baeredygtighed/'")
        max_show = s2.number_input("Max viste", min_value=20, max_value=2000, value=300, step=20)

        urls_tbl = df[["URL","Keywords","Total"]].copy()
        if url_query.strip():
            ql = url_query.strip().lower()
            urls_tbl = urls_tbl[urls_tbl["URL"].str.lower().str.contains(ql, na=False)]
        st.caption(f"Viser {len(urls_tbl)} URL'er i listen")

        shown = urls_tbl.head(int(max_show)).reset_index(drop=True)
        for i, row in shown.iterrows():
            u = row["URL"]; kw_csv = row["Keywords"]; total_hits = int(row["Total"] or 0)
            cA, cB, cC, cD = st.columns([7.6,1.2,1.6,1.6])
            with cA: st.markdown(f"[{u}]({u})")
            with cB: st.markdown(f"<div style='text-align:center;padding:6px 8px;border-radius:6px;background:#f2f2f2;'>Hits: <b>{total_hits}</b></div>", unsafe_allow_html=True)
            with cC:
                if st.button("🔍 Se forekomster", key=f"see_{i}_{hash(u)%10000}"):
                    st.session_state["__snips_for_url"] = (u, kw_csv); st.rerun()
            with cD:
                if st.button("♻️ Opdater", key=f"upd_{i}_{hash(u)%10000}"):
                    try:
                        rows_one = scan_pages([u], st.session_state.get("kw_final", []), excludes=st.session_state.get("kw_exclude", []), delay=0.0)
                        if rows_one:
                            db.sync_pages_from_df(pd.DataFrame(rows_one)); st.success("Opdateret."); st.rerun()
                        else:
                            db.sync_pages_from_df(pd.DataFrame([{"url":u,"keywords":"","hits":0,"total":0}]))
                            st.success("Ingen matches. Siden er opdateret til 0."); st.rerun()
                    except Exception as e:
                        st.error(f"Kunne ikke opdatere: {e}")

        if st.session_state.get("__snips_for_url"):
            url_sel, kw_sel = st.session_state["__snips_for_url"]
            st.divider(); st.markdown(f"### Forekomster for {url_sel}")
            try:
                snippets = get_snippets(url_sel, kw_sel)
            except Exception as e:
                st.error(f"Kunne ikke hente/analysere siden: {e}"); snippets = []
            if not snippets:
                st.info("Ingen forekomster fundet (efter filtrering af navigation/related).")
            else:
                from itertools import groupby
                for kw, group in groupby(snippets, key=lambda r: r["keyword"]):
                    st.markdown(f"**Keyword:** `{kw}`")
                    for item in list(group)[:25]:
                        tag = item["tag"]; snip_html = _highlight(item["snippet"], kw)
                        st.markdown(
                            f"<div style='margin:6px 0;padding:8px;border-left:4px solid #ddd;background:#fafafa'>"
                            f"<span style='font-size:12px;color:#666'>Tag: &lt;{tag}&gt;</span><br>{snip_html}</div>",
                            unsafe_allow_html=True,
                        )
            st.button("Luk forekomster", on_click=lambda: st.session_state.update({"__snips_for_url": None}))

# ──────────────────────────────────────────────────────────────────────────────
# STATISTIK
with tab_stats:
    st.subheader("Statistik & Progress")
    s = db.stats()
    ch.kpi_cards(s["total"], s["done"], s["todo"], s["completion"])
    left, right = st.columns(2)
    with left:
        st.markdown("**Sider pr. keyword**")
        counts = d.keyword_page_counts(df_std)
        ch.bar_keyword_pages(counts, top_n=15)
    with right:
        st.markdown("**Top-keywords (faktiske forekomster)**")
        kw_totals = d.keyword_totals_from_long(kw_long, top_n=15)
        ch.bar_keyword_totals(kw_totals)
    st.divider(); greenwash_meter(s.get("completion", 0.0))

# ──────────────────────────────────────────────────────────────────────────────
# FÆRDIGE SIDER
with tab_done:
    st.subheader("Færdige sider")
    done_df = db.get_done_dataframe()
    if done_df.empty:
        st.info("Ingen færdige sider endnu.")
    else:
        st.dataframe(done_df, width="stretch", hide_index=True)
        st.download_button(
            "Eksportér som CSV",
            data=done_df.to_csv(index=False).encode("utf-8"),
            file_name="faerdige_sider.csv",
            mime="text/csv",
        )
        undo = st.multiselect("Fortryd til Todo", options=list(done_df.get("url", [])))
        if st.button("Fortryd valgte"):
            if undo:
                db.bulk_update_status(undo, "todo"); st.success("Status opdateret."); st.rerun()
            else:
                st.info("Vælg mindst én URL at fortryde.")

# ──────────────────────────────────────────────────────────────────────────────
# NEEDS REVIEW
with tab_review:
    st.subheader("Sider der kræver ekstra opmærksomhed")
    review_rows, _ = db.get_pages(status="review", limit=10000, offset=0)
    review_df = pd.DataFrame([dict(r) for r in review_rows]) if review_rows else pd.DataFrame()
    if review_df.empty:
        st.info("Ingen sider markeret som 'Needs Review' endnu.")
    else:
        for col, default in [("url",""),("keywords",""),("total",0),("assigned_to",""),("notes","")]:
            if col not in review_df.columns: review_df[col] = default
        review_df["URL"] = review_df["url"]
        review_df["Keywords"] = review_df["keywords"].fillna("")
        review_df["Total"] = pd.to_numeric(review_df["total"], errors="coerce").fillna(0).astype(int)
        review_df["Assigned to"] = review_df["assigned_to"].fillna("").replace({None:""})
        review_df["Noter"] = review_df["notes"].fillna("")
        view = review_df[["URL","Keywords","Total","Assigned to","Noter"]]
        st.dataframe(view, use_container_width=True, hide_index=True)
        st.download_button("Eksportér som CSV", data=view.to_csv(index=False).encode("utf-8"), file_name="needs_review_sider.csv", mime="text/csv")
        resolve = st.multiselect("Markér som løst (skift til Done)", options=list(review_df["url"]))
        back_to_todo = st.multiselect("Send tilbage til Todo", options=list(review_df["url"]))
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Markér valgte som Done"):
                if resolve: db.bulk_update_status(resolve, "done"); st.success(f"{len(resolve)} sider markeret som Done."); st.rerun()
                else: st.info("Vælg mindst én URL.")
        with col2:
            if st.button("Send valgte til Todo"):
                if back_to_todo: db.bulk_update_status(back_to_todo, "todo"); st.success(f"{len(back_to_todo)} sider sendt til Todo."); st.rerun()
                else: st.info("Vælg mindst én URL.")

# ──────────────────────────────────────────────────────────────────────────────
# FOKUS (Top 100)
with tab_focus:
    st.subheader("Google Analytics Top 100 – fokusliste")
    ga_top = st.session_state.get("ga_top100")
    if ga_top is None or len(ga_top) == 0:
        st.info("Upload en GA CSV i sidebar for at se top 100.")
    else:
        rows, _ = db.get_pages(limit=100000, offset=0)
        db_df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
        if db_df.empty:
            st.warning("Ingen sider i databasen endnu – kør et crawl først.")
        else:
            for col, default in [("url",""),("total",0),("status","todo"),("assigned_to","")]:
                if col not in db_df.columns: db_df[col] = default
            focus = ga_top.merge(db_df[["url","total","status","assigned_to"]], on="url", how="left")
            total_ga = len(focus)
            not_crawled = focus["total"].isna().sum()
            zero_matches = (pd.to_numeric(focus["total"], errors="coerce").fillna(0) == 0).sum()
            focus = focus[pd.to_numeric(focus["total"], errors="coerce").fillna(0) > 0].copy()
            focus["status"] = focus["status"].fillna("todo").map({"todo":"Todo","done":"Done","review":"Needs Review"}).fillna("Todo")
            focus["assigned_to"] = focus["assigned_to"].fillna("").replace({None:""})
            focus = focus.rename(columns={"total":"Matches (Total)","status":"Status","assigned_to":"Assigned to"})
            st.info(f"📊 **GA Top 100 status:** {total_ga} sider i filen · {not_crawled} ikke crawlet · {zero_matches} uden matches · **{len(focus)} med matches**")

            c1, c2, c3, c4 = st.columns([2.5,1,1,1.2])
            q = c1.text_input("Filtrér i URL (substring eller regex)", value="", key="focus_url_q")
            prefix_mode = c2.checkbox("Starter med", value=False, key="focus_prefix")
            regex_mode = c3.checkbox("Regex /…/", value=False, key="focus_regex")
            show_done = c4.checkbox("Vis Done", value=False, key="show_done_top100")

            if show_done:
                df_show = focus.copy()
            else:
                df_show = focus[focus["Status"] != "Done"].copy()

            if q:
                if regex_mode and len(q) >= 2 and q.startswith("/") and q.endswith("/"):
                    try:
                        pat = re.compile(q[1:-1], re.IGNORECASE)
                        df_show = df_show[df_show["url"].astype(str).apply(lambda s: bool(pat.search(s)))]
                    except Exception:
                        st.warning("Ugyldig regex – bruger fallback (substring)")
                        df_show = df_show[df_show["url"].str.contains(q.strip("/"), case=False, na=False)]
                elif prefix_mode:
                    def _path_starts(u: str, prefix: str) -> bool:
                        try:
                            p = urlparse(u); path = (p.path or "/")
                            return path.lower().startswith(prefix.lower())
                        except Exception:
                            return False
                    df_show = df_show[df_show["url"].astype(str).apply(lambda u: _path_starts(u, q))]
                else:
                    df_show = df_show[df_show["url"].str.contains(q, case=False, na=False)]

            df_show = df_show.sort_values(["Matches (Total)","pageviews"], ascending=[False,False]).reset_index(drop=True)
            done_count = len(focus[focus["Status"] == "Done"])
            st.caption(f"Viser {len(df_show)} aktive sider · {done_count} færdige sider er skjult")

            df_show.insert(0, "Vælg", False)
            bulk_placeholder_top100 = st.empty()
            edited = st.data_editor(
                df_show,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Vælg": st.column_config.CheckboxColumn(help="Vælg til bulk opdatering", default=False),
                    "url": st.column_config.LinkColumn(help="Klik for at åbne siden"),
                    "pageviews": st.column_config.NumberColumn(format="%d"),
                    "Matches (Total)": st.column_config.NumberColumn(format="%d"),
                    "Status": st.column_config.SelectboxColumn(options=["Todo","Needs Review","Done"]),
                    "Assigned to": st.column_config.SelectboxColumn(options=["– Ingen –","CEYD","LBY","JAWER","ULRS"]),
                },
                disabled=["url","pageviews","Matches (Total)"],
                height=440,
                key="top100_editor",
                on_change=lambda: st.session_state.update({"top100_changed": True}),
            )

            selected_urls_top100 = edited[edited["Vælg"] == True]["url"].tolist()
            if selected_urls_top100:
                with bulk_placeholder_top100.container():
                    st.info(f"**{len(selected_urls_top100)} sider valgt til bulk opdatering**")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        bulk_status_top100 = st.selectbox("Sæt status til", ["Ingen ændring","Todo","Needs Review","Done"], key="bulk_status_top100")
                    with col2:
                        bulk_assign_top100 = st.selectbox("Tildel til", ["Ingen ændring","– Ingen –","CEYD","LBY","JAWER","ULRS"], key="bulk_assign_top100")
                    with col3:
                        st.write(""); st.write("")
                        if st.button("Udfør bulk opdatering", type="primary", key="bulk_execute_top100"):
                            changed = 0
                            if bulk_status_top100 != "Ingen ændring":
                                status_map = {"Todo":"todo","Done":"done","Needs Review":"review"}
                                db.bulk_update_status(selected_urls_top100, status_map[bulk_status_top100]); changed += len(selected_urls_top100)
                            if bulk_assign_top100 != "Ingen ændring":
                                assign_val = "" if bulk_assign_top100 == "– Ingen –" else bulk_assign_top100
                                for url in selected_urls_top100:
                                    db.update_assigned_to(url, assign_val)
                                changed += len(selected_urls_top100)
                            if changed > 0:
                                st.success(f"BULK GEMT: {len(selected_urls_top100)} sider opdateret")
                                time.sleep(1.5); st.rerun()
                            else:
                                st.info("Vælg mindst én ændring at udføre")

            if st.session_state.get("top100_changed", False):
                changed = 0
                for i, row in edited.iterrows():
                    orig = df_show.loc[i]; url = orig["url"]
                    if row["Status"] != orig["Status"]:
                        status_map = {"Todo":"todo","Done":"done","Needs Review":"review"}
                        db.update_status(url, status_map.get(row["Status"], "todo")); changed += 1
                    new_assign = "" if row["Assigned to"] == "– Ingen –" else row["Assigned to"]
                    if new_assign != orig["Assigned to"]:
                        db.update_assigned_to(url, new_assign); changed += 1
                if changed:
                    newly = []
                    try: newly = db.check_milestones()
                    except Exception: pass
                    celebrate(newly)
                    st.success(f"GEMT: {changed} ændring(er)")
                    st.session_state["top100_changed"] = False
                    time.sleep(1.5); st.rerun()

            st.divider()
            csv_bytes = edited.to_csv(index=False).encode("utf-8")
            st.download_button("⬇️ Eksportér filteret (CSV)", data=csv_bytes, file_name="top100_filtered.csv", mime="text/csv")

            if st.button("♻️ Recrawl viste (hurtig enkeltside-scan)"):
                urls = list(edited["url"].dropna().astype(str))
                st.info(f"Scanner {len(urls)} URL'er…")
                sub_prog = st.progress(0)
                batch = 20
                all_rows = []
                kw_final = st.session_state.get("kw_final", [])
                kw_excl = st.session_state.get("kw_exclude", [])
                for i in range(0, len(urls), batch):
                    part = urls[i:i+batch]
                    part_rows = scan_pages(part, kw_final, excludes=kw_excl)
                    all_rows.extend(part_rows)
                    sub_prog.progress(min(1.0, (i+batch)/max(1,len(urls))))
                if all_rows:
                    db.sync_pages_from_df(pd.DataFrame(all_rows))
                    st.success("Viste rækker opdateret. Opfrisker visning…")
                    st.rerun()
                else:
                    st.info("Ingen resultater at opdatere.")
