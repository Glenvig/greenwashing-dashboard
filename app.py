# app.py
# NIRAS Greenwashing-dashboard — komplet app med tydelig grøn progress bar (forside)
# - Oversigtstabel: URL klikbar, redigér Status / Assigned to / Noter
# - Forside: Stor grøn progress bar (done/total * 100%)
# - Under tabellen: live-søg i alle sider + knap "Se forekomster" pr. række
# - Snippet-visning ekskluderer navigation/related (klasser/id der indeholder 'related', + nav/header/footer/aside)
# - Let gamification: Greenwash-o-meter + badges + dags-quest (emoji-konfetti hvis streamlit-extras er installeret)

from __future__ import annotations
import os, re, math
import pandas as pd
import streamlit as st
import requests
from bs4 import BeautifulSoup

import db
import data as d
import charts as ch

# NEW: crawler
import crawler

# (valgfrit) konfetti, hvis lib findes
try:
    from streamlit_extras.let_it_rain import rain
except Exception:
    rain = None

st.set_page_config(page_title="NIRAS greenwashing-dashboard", layout="wide")

# =================== Progress bar (STOR, GRØN) ===================
def big_green_progress(completion: float, total: int, done: int):
    pct = int(round((completion or 0.0) * 100))
    pct = max(0, min(pct, 100))
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

# =================== Snippet-funktioner ===================
ALLOWED_TAGS = {"h1","h2","h3","h4","h5","h6","p","li","strong","em","span","a"}
EXCLUDE_CLASS_EXACT = {"menulink", "anchor-link"}
EXCLUDE_SUBSTRINGS = {"related"}
EXCLUDE_TAGS = {"nav", "header", "footer", "aside"}

def _compile_kw_patterns(keywords):
    pats = {}
    for kw in keywords:
        kw = kw.strip()
        if not kw:
            continue
        if kw.endswith("*"):
            base = re.escape(kw[:-1])
            pat = re.compile(rf"\b{base}\w*\b", flags=re.IGNORECASE)
        else:
            pat = re.compile(rf"\b{re.escape(kw)}\b", flags=re.IGNORECASE)
        pats[kw] = pat
    return pats

def _has_excluded_ancestor(node) -> bool:
    hops = 0
    cur = node
    while cur is not None and hops < 12:
        try:
            name = (getattr(cur, "name", None) or "").lower()
        except Exception:
            name = ""
        if name in EXCLUDE_TAGS:
            return True
        try:
            classes = [str(c).lower() for c in (cur.get("class") or [])]
        except Exception:
            classes = []
        if any(c in EXCLUDE_CLASS_EXACT for c in classes):
            return True
        if any(any(sub in c for sub in EXCLUDE_SUBSTRINGS) for c in classes):
            return True
        try:
            nid = str(cur.get("id") or "").lower()
            if nid and any(sub in nid for sub in EXCLUDE_SUBSTRINGS):
                return True
        except Exception:
            pass
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

@st.cache_data(show_spinner=False, ttl=60*60*24)
def get_snippets(url: str, keywords_csv: str, max_per_kw: int = 25):
    headers = {"User-Agent": "NIRAS-Green-Dashboard/1.0"}
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    _prestrip_excluded_containers(soup)

    keywords = [k.strip() for k in re.split(r"[;,]", keywords_csv or "") if k.strip()]
    pats = _compile_kw_patterns(keywords)

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
            for m in matches[:max_per_kw]:
                start, end = m.start(), m.end()
                left, right = max(0, start - 80), min(len(text), end + 80)
                rows.append({"keyword": kw, "tag": tag.name, "snippet": text[left:right]})
    rows.sort(key=lambda r: (r["keyword"].lower(), r["tag"]))
    return rows

def _highlight(snippet: str, kw: str):
    pat = _compile_kw_patterns([kw])[kw]
    return pat.sub(lambda m: f"<mark>{m.group(0)}</mark>", snippet)

# =================== Gamification (let) ===================
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
        f"<div style='border-radius:12px;padding:14px 16px;background:linear-gradient(90deg,{c} {nice}%,#e5e7eb {nice}%);color:#111;'>"
        f"<b>Greenwash-o-meter:</b> {nice}% &nbsp; {joke}</div>",
        unsafe_allow_html=True,
    )

def badge_strip(stats: dict, unlocked_names: list[str] | None = None):
    done = stats.get("done", 0); pct = stats.get("completion", 0.0)
    st.markdown("#### 🏅 Badges")
    cols = st.columns(3)
    items = [("first_10", f"{done}/10"), ("fifty_percent", f"{int(pct*100)}%"), ("hundred_done", f"{done}/100")]
    for i, (key, progress) in enumerate(items):
        title, desc = BADGE_COPY.get(key, (key, ""))
        active = (unlocked_names and key in unlocked_names)
        border = "2px solid #059669" if active else "1px solid #e5e7eb"
        cols[i].markdown(
            f"<div style='border:{border};border-radius:12px;padding:12px;'>"
            f"<div style='font-size:18px;'>🏅 {title}</div>"
            f"<div style='color:#6b7280;font-size:13px;'>{desc}</div>"
            f"<div style='margin-top:6px;background:#f3f4f6;border-radius:8px;padding:6px 8px;display:inline-block;'>{progress}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )

def celebrate(unlocked: list[str] | None):
    if not unlocked:
        return
    if rain:
        rain(emoji="🌱", font_size=42, falling_speed=6, animation_length="0")
    try:
        for key in unlocked:
            title, desc = BADGE_COPY.get(key, (key, ""))
            st.toast(f"🏅 Badge låst op: {title} — {desc}")
    except Exception:
        pass

# =================== Hjælp i toppen ===================
st.markdown("### Velkommen til Greenwashing-radaren")
st.markdown("Filtrér, redigér og find forekomster hurtigt. Navigation/related tælles ikke med i forekomster.")

# Progress bar på forsiden (ud fra DB-tal)
db.init_db()
s0 = db.stats()
big_green_progress(s0["completion"], s0["total"], s0["done"])

# =================== Sidebar: Data + Crawler + faste personer ===================
with st.sidebar:
    # --- Data ---
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

    TEAM = ["RAGL", "CEYD", "ULRS", "LBY", "JAWER"]
    TEAM_OPTS = ["— Ingen —"] + TEAM

    st.markdown("---")

    # --- Crawler ---
    st.header("Crawler")
    domain = st.selectbox("Domæne", ["https://www.niras.dk/", "https://www.niras.com/"])
    max_pages = st.slider("Maks sider", 20, 2000, 300, 20)
    max_depth = st.slider("Maks dybde", 1, 10, 4)

    # Keywords fra UI (med fallback til DEFAULT_KW i crawler.py)
    default_kw_text = "\n".join(
        getattr(crawler, "DEFAULT_KW", [
            "bæredygtig*", "miljøvenlig*", "miljørigtig*", "klimavenlig*",
            "grøn*", "grønnere", "klimaneutral*", "co2[- ]?neutral",
            "netto[- ]?nul", "klimakompensation*", "kompenseret for CO2",
            "100% grøn strøm", "uden udledning", "nul udledning", "zero emission*"
        ])
    )
    kw_text = st.text_area(
        "Søgeord & udsagn (ét pr. linje)",
        value=default_kw_text,
        help="Brug * som wildcard (fx 'bæredygtig*'). Avanceret: regex som /co2[- ]?neutral/."
    )
    kw_list_manual = [k.strip() for k in re.split(r"[\n,;]", kw_text) if k.strip()]

    # Valgfrit: flet med keywords fra den indlæste datakilde (robust)
    merge_with_file = st.checkbox("Flet med keywords fra datakilden", value=True)
    kw_from_file = []
    if merge_with_file and (df_std is not None) and (not df_std.empty):
        try:
            all_kw = []
            for _, row in df_std.iterrows():
                all_kw.extend(d.split_keywords(row.get("keywords", "")))
            seen = set()
            kw_from_file = [k for k in all_kw if not (k in seen or seen.add(k))]
        except Exception:
            kw_from_file = []

    # Endelig liste (unik)
    kw_seen = set()
    kw_final = []
    for k in kw_list_manual + kw_from_file:
        if k and (k not in kw_seen):
            kw_seen.add(k)
            kw_final.append(k)

    st.caption(f"🧩 Keywords i brug: {len(kw_final)}")

    if st.button("Start crawl", type="secondary", key="crawl_btn"):
        if not kw_final:
            st.warning("Tilføj mindst ét ord/udsagn (eller slå flet med datakilden til).")
        else:
            # DB-status før
            db.init_db()
            stats_before = db.stats()
            total_before = stats_before.get("total", 0)

            with st.spinner("Crawler kører – respekterer robots.txt…"):
                try:
                    rows = crawler.crawl(domain, kw_final, max_pages=max_pages, max_depth=max_depth)
                except TypeError:
                    rows = crawler.crawl(domain, kw_final, max_pages=max_pages)

            if rows:
                cdf = pd.DataFrame(rows)
                # filtrér til gyldige http(s) URL'er
                cdf = cdf[cdf["url"].astype(str).str.startswith(("http://","https://"))].copy()
                db.sync_pages_from_df(cdf)

                stats_after = db.stats()
                total_after = stats_after.get("total", 0)
                delta = total_after - total_before

                st.success(
                    f"Crawl færdig: {len(cdf)} sider behandlet. "
                    f"DB: {total_before} → {total_after} (Δ {delta})."
                )
                st.rerun()
            else:
                st.info("Ingen sider fundet eller ingen matches (tjek domæne/keywords/filtre).")

# Seed hvis tom
if s0["total"] == 0:
    try:
        db.sync_pages_from_df(df_std)
        s0 = db.stats()
    except Exception:
        pass

# =================== Tabs ===================
tab_overview, tab_stats, tab_done = st.tabs(["Oversigt", "Statistik", "Færdige sider"])

# =================== Oversigt ===================
with tab_overview:
    st.subheader("Oversigt")
    st.session_state.setdefault("__snips_for_url", None)

    # Filtre
    c1, c2, c3 = st.columns([2, 1, 1])
    q = c1.text_input("Søg (URL/keywords)", value="", placeholder="fx 'co2-neutral'")
    min_total = c2.number_input("Min. total", min_value=0, value=0, step=1)
    try:
        status_choice = c3.segmented_control("Status", options=["Alle", "Todo", "Done"], default="Alle")
    except Exception:
        status_choice = c3.selectbox("Status", ["Alle", "Todo", "Done"], index=0)
    status_arg = {"Alle": None, "Todo": "todo", "Done": "done"}[status_choice]

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

        df["URL"] = df["url"]
        df["Keywords"] = df["keywords"].fillna("")
        df["Hits"] = pd.to_numeric(df["hits"], errors="coerce").fillna(0).astype(int)
        df["Total"] = pd.to_numeric(df["total"], errors="coerce").fillna(0).astype(int)
        df["Status"] = df["status"].map({"todo":"Todo", "done":"Done"}).fillna("Todo")
        df["Assigned to"] = df["assigned_to"].fillna("").replace({None: ""})
        df["Noter"] = df["notes"].fillna("")

        view = df[["URL","Keywords","Hits","Total","Status","Assigned to","Noter"]]
        edited = st.data_editor(
            view,
            width="stretch",
            hide_index=True,
            column_config={
                "URL": st.column_config.LinkColumn(help="Klik for at åbne siden"),
                "Keywords": st.column_config.TextColumn(width="large"),
                "Hits": st.column_config.NumberColumn(format="%d"),
                "Total": st.column_config.NumberColumn(format="%d"),
                "Status": st.column_config.SelectboxColumn(options=["Todo","Done"]),
                "Assigned to": st.column_config.SelectboxColumn(options=["— Ingen —","RAGL","CEYD","ULRS","LBY","JAWER"], help="Tildel ansvarlig"),
                "Noter": st.column_config.TextColumn(),
            },
            disabled=["URL","Keywords","Hits","Total"],
            height=440,
        )

        if st.button("Gem ændringer", type="primary"):
            changed = 0
            for i, row in edited.iterrows():
                orig = df.loc[i]; url = orig["URL"]
                if row["Status"] != orig["Status"]:
                    db.update_status(url, "done" if row["Status"] == "Done" else "todo")
                    changed += 1
                if row["Noter"] != orig["Noter"]:
                    db.update_notes(url, row["Noter"])
                    changed += 1
                new_assign = row["Assigned to"]
                new_assign = "" if new_assign == "— Ingen —" else new_assign
                if new_assign != orig["Assigned to"]:
                    db.update_assigned_to(url, new_assign)
                    changed += 1

            if changed:
                try:
                    newly = db.check_milestones()
                except Exception:
                    newly = []
                st.success("Ændringer gemt.")
                celebrate(newly)
                st.rerun()
            else:
                st.info("Ingen ændringer at gemme.")

        # -------- Alle sider – live-søg + “Se forekomster” --------
        st.divider()
        st.markdown("#### Alle sider – søg og se forekomster")
        s1, s2 = st.columns([3, 1])
        url_query = s1.text_input("Søg i URL'er (live)", value="", placeholder="skriv fx '/baeredygtighed/'")
        max_show = s2.number_input("Max viste", min_value=20, max_value=2000, value=300, step=20)

        urls = df[["URL","Keywords","Total"]].copy()
        if url_query.strip():
            ql = url_query.strip().lower()
            urls = urls[urls["URL"].str.lower().str.contains(ql, na=False)]

        st.caption(f"Viser {len(urls)} URL'er i listen")
        shown = urls.head(int(max_show)).reset_index(drop=True)
        for i, row in shown.iterrows():
            u = row["URL"]; kw_csv = row["Keywords"]; total_hits = int(row["Total"] or 0)
            cA, cB, cC = st.columns([8, 1.2, 1.6])
            with cA:
                st.markdown(f"[{u}]({u})")
            with cB:
                st.markdown(
                    f"<div style='text-align:center;padding:6px 8px;border-radius:6px;background:#f2f2f2;'>Hits: <b>{total_hits}</b></div>",
                    unsafe_allow_html=True
                )
            with cC:
                if st.button("🔍 Se forekomster", key=f"see_{i}_{hash(u)%10_000}"):
                    st.session_state["__snips_for_url"] = (u, kw_csv)
                    st.rerun()

        # -------- Snippet-visning --------
        if st.session_state.get("__snips_for_url"):
            url_sel, kw_sel = st.session_state["__snips_for_url"]
            st.divider()
            st.markdown(f"### Forekomster for {url_sel}")
            try:
                snippets = get_snippets(url_sel, kw_sel)
            except Exception as e:
                st.error(f"Kunne ikke hente/analysere siden: {e}")
                snippets = []

            if not snippets:
                st.info("Ingen forekomster fundet (efter filtrering af navigation/related).")
            else:
                from itertools import groupby
                for kw, group in groupby(snippets, key=lambda r: r["keyword"]):
                    st.markdown(f"**Keyword:** `{kw}`")
                    for item in list(group)[:25]:
                        tag = item["tag"]
                        snip_html = _highlight(item["snippet"], kw)
                        st.markdown(
                            f"<div style='margin:6px 0;padding:8px;border-left:4px solid #ddd;background:#fafafa'>"
                            f"<span style='font-size:12px;color:#666'>Tag: &lt;{tag}&gt;</span><br>{snip_html}</div>",
                            unsafe_allow_html=True,
                        )
            st.button("Luk forekomster", on_click=lambda: st.session_state.update({"__snips_for_url": None}))

# =================== Statistik ===================
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

    st.divider()
    greenwash_meter(s.get("completion", 0.0))
    badge_strip(s, unlocked_names=None)
    try:
        done_today = db.done_today_count()
    except Exception:
        done_today = 0
    left_num = max(0, 5 - done_today)
    status = "✅ Klaret!" if left_num == 0 else f"⏳ {left_num} tilbage i dag"
    st.markdown("#### ⚔️ Dagens quest")
    st.info(f"Gør **5** sider færdige i dag. {status}")

# =================== Færdige sider ===================
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
                db.bulk_update_status(undo, "todo")
                try:
                    newly = db.check_milestones()
                except Exception:
                    newly = []
                st.success("Status opdateret.")
                celebrate(newly)
                st.rerun()
            else:
                st.info("Vælg mindst én URL at fortryde.")
