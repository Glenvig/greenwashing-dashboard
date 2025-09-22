# app.py
# NIRAS Greenwashing-dashboard – Optimeret version med prioritering
# - Google Analytics integration for prioritering af vigtige sider
# - Forbedret UX med hurtige handlinger og bulk operations
# - Smart filtrering og sortering baseret på trafik
# - Team performance tracking
# - Forbedret visualisering og gamification

from __future__ import annotations
import os, re, math
import pandas as pd
import streamlit as st
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

import db
import data as d
import charts as ch

# Importér crawler
from crawler import crawl, DEFAULT_KW

# (valgfrit) konfetti
try:
    from streamlit_extras.let_it_rain import rain
except Exception:
    rain = None

st.set_page_config(page_title="NIRAS Greenwashing Dashboard", layout="wide", initial_sidebar_state="collapsed")

# =================== Session State ===================
if 'user_name' not in st.session_state:
    st.session_state.user_name = None

# =================== Header med brugerinfo ===================
col_title, col_user = st.columns([4, 1])
with col_title:
    st.markdown("## 🌱 NIRAS Greenwashing Dashboard")
with col_user:
    user_name = st.text_input("Dit navn", key="username_input", placeholder="RAGL")
    if user_name:
        st.session_state.user_name = user_name

# =================== Progress bars ===================
def dual_progress_bars(stats: dict):
    """Viser både total og prioritets-progress"""
    col1, col2 = st.columns(2)
    
    with col1:
        pct = int(round(stats["completion"] * 100))
        st.markdown(f"""
        <div style="margin: 8px 0;">
          <div style="padding:8px 12px; font-weight:600;">📊 Samlet fremskridt</div>
          <div style="height:24px; background:#e5e7eb; border-radius:8px; position:relative;">
            <div style="height:100%; width:{pct}%; background:#10b981; border-radius:8px; transition:width .3s;"></div>
            <div style="position:absolute; top:0; left:0; right:0; height:100%; display:flex; align-items:center; justify-content:center; font-weight:500;">
              {pct}% ({stats["done"]} af {stats["total"]})
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        if stats["priority_total"] > 0:
            pct_p = int(round(stats["priority_completion"] * 100))
            color = "#059669" if pct_p >= 50 else "#f59e0b"
            st.markdown(f"""
            <div style="margin: 8px 0;">
              <div style="padding:8px 12px; font-weight:600;">🎯 Top 100 prioritet</div>
              <div style="height:24px; background:#e5e7eb; border-radius:8px; position:relative;">
                <div style="height:100%; width:{pct_p}%; background:{color}; border-radius:8px; transition:width .3s;"></div>
                <div style="position:absolute; top:0; left:0; right:0; height:100%; display:flex; align-items:center; justify-content:center; font-weight:500;">
                  {pct_p}% ({stats["priority_done"]} af {stats["priority_total"]})
                </div>
              </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.info("Upload Google Analytics data for at se prioritets-progress")

# =================== Quick Actions Bar ===================
def quick_action_bar():
    """Hurtige handlinger øverst"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("🎯 Vis kun Top 100", use_container_width=True):
            st.session_state.filter_priority = True
            st.rerun()
    
    with col2:
        if st.button("📋 Vis mine opgaver", use_container_width=True):
            st.session_state.filter_mine = True
            st.rerun()
    
    with col3:
        if st.button("🔄 Opdatér data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    with col4:
        if st.button("📊 Team oversigt", use_container_width=True):
            st.session_state.show_team_stats = True

# =================== Snippet-funktioner (uændret) ===================
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

# =================== Forbedret Gamification ===================
BADGE_COPY = {
    "first_10": ("Første 10 sider", "🚀 God start!"),
    "fifty_percent": ("50% complete", "🧹 Halvvejs"),
    "hundred_done": ("100 sider done", "🏆 Century!"),
    "priority_10": ("10 prioritets-sider", "🎯 Fokuseret"),
    "priority_half": ("50% prioritet done", "⭐ Prioritets-mester"),
}

def celebrate(unlocked: list[str] | None):
    if not unlocked:
        return
    if rain:
        rain(emoji="🌱", font_size=42, falling_speed=6, animation_length="0")
    try:
        for key in unlocked:
            title, desc = BADGE_COPY.get(key, (key, ""))
            st.toast(f"🏅 {title} - {desc}")
    except Exception:
        pass

# =================== Main ===================
db.init_db()
stats = db.stats()

# Progress bars
dual_progress_bars(stats)

# Quick actions
quick_action_bar()

# =================== Tabs ===================
tab_overview, tab_priority, tab_analytics, tab_team, tab_admin = st.tabs(
    ["📋 Oversigt", "🎯 Prioritet", "📊 Analytics", "👥 Team", "⚙️ Admin"]
)

# =================== Oversigt Tab ===================
with tab_overview:
    # Filtre med bedre defaults
    col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 1, 1])
    
    with col1:
        search = st.text_input("Søg", placeholder="URL eller keyword...")
    with col2:
        min_hits = st.number_input("Min hits", 0, step=5)
    with col3:
        status_filter = st.selectbox("Status", ["Alle", "Todo", "Done"])
    with col4:
        priority_filter = st.checkbox("Kun prioritet", value=st.session_state.get('filter_priority', False))
    with col5:
        sort_method = st.selectbox("Sortering", ["Smart", "Hits", "Trafik", "Seneste"])
    
    # Smart sortering mapper
    sort_map = {
        "Smart": ("smart", "desc"),
        "Hits": ("total", "desc"),
        "Trafik": ("traffic_rank", "asc"),
        "Seneste": ("last_updated", "desc")
    }
    sort_by, sort_dir = sort_map[sort_method]
    
    # Hent data med filtre
    rows, total_count = db.get_pages(
        search=search if search else None,
        min_total=min_hits,
        status={"Alle": None, "Todo": "todo", "Done": "done"}[status_filter],
        priority_only=priority_filter,
        sort_by=sort_by,
        sort_dir=sort_dir,
        limit=500,
        offset=0
    )
    
    st.caption(f"Viser {len(rows)} af {total_count} sider")
    
    if rows:
        df = pd.DataFrame([dict(r) for r in rows])
        
        # Tilføj prioritets-indikator
        df["🎯"] = df.apply(lambda r: "⭐" if r.get("is_priority") else "", axis=1)
        df["URL"] = df["url"]
        df["Keywords"] = df["keywords"].fillna("")
        df["Hits"] = pd.to_numeric(df["hits"], errors="coerce").fillna(0).astype(int)
        df["Status"] = df["status"].map({"todo":"Todo", "done":"Done"}).fillna("Todo")
        df["Ansvarlig"] = df["assigned_to"].fillna("")
        df["Noter"] = df["notes"].fillna("")
        df["Rang"] = df["traffic_rank"].fillna(9999).astype(int)
        
        # Kolonner til visning
        display_cols = ["🎯", "URL", "Keywords", "Hits", "Status", "Ansvarlig", "Noter"]
        if stats["has_traffic_data"]:
            display_cols.insert(3, "Rang")
        
        # Vis data editor
        edited = st.data_editor(
            df[display_cols],
            column_config={
                "🎯": st.column_config.TextColumn(width="small"),
                "URL": st.column_config.LinkColumn(width="large"),
                "Keywords": st.column_config.TextColumn(width="medium"),
                "Rang": st.column_config.NumberColumn(format="%d", width="small"),
                "Hits": st.column_config.NumberColumn(format="%d", width="small"),
                "Status": st.column_config.SelectboxColumn(["Todo", "Done"], width="small"),
                "Ansvarlig": st.column_config.SelectboxColumn(
                    ["", "RAGL", "CEYD", "ULRS", "LBY", "JAWER"], 
                    width="small"
                ),
                "Noter": st.column_config.TextColumn(width="medium"),
            },
            disabled=["🎯", "URL", "Keywords", "Hits", "Rang"],
            height=400,
            hide_index=True
        )
        
        # Bulk actions
        col1, col2, col3 = st.columns([1, 1, 3])
        with col1:
            if st.button("💾 Gem ændringer", type="primary", use_container_width=True):
                changed = 0
                for i, row in edited.iterrows():
                    orig = df.loc[i]
                    url = orig["URL"]
                    
                    if row["Status"] != orig["Status"]:
                        db.update_status(url, row["Status"].lower(), st.session_state.user_name)
                        changed += 1
                    if row["Noter"] != orig["Noter"]:
                        db.update_notes(url, row["Noter"])
                        changed += 1
                    if row["Ansvarlig"] != orig["Ansvarlig"]:
                        db.update_assigned_to(url, row["Ansvarlig"])
                        changed += 1
                
                if changed:
                    newly = db.check_milestones()
                    st.success(f"✅ {changed} ændringer gemt")
                    celebrate(newly)
                    st.rerun()
        
        with col2:
            selected_indices = st.multiselect(
                "Vælg rækker",
                options=list(range(len(df))),
                format_func=lambda x: f"Række {x+1}"
            )
        
        with col3:
            if selected_indices:
                bulk_col1, bulk_col2 = st.columns(2)
                with bulk_col1:
                    bulk_status = st.selectbox("Sæt status for valgte", ["", "Todo", "Done"])
                    if bulk_status and st.button("Opdatér status"):
                        urls = [df.iloc[i]["URL"] for i in selected_indices]
                        db.bulk_update_status(urls, bulk_status.lower())
                        st.success(f"Status opdateret for {len(urls)} sider")
                        st.rerun()
                
                with bulk_col2:
                    bulk_assign = st.selectbox("Tildel valgte til", ["", "RAGL", "CEYD", "ULRS", "LBY", "JAWER"])
                    if bulk_assign and st.button("Tildel"):
                        for i in selected_indices:
                            db.update_assigned_to(df.iloc[i]["URL"], bulk_assign)
                        st.success(f"{len(selected_indices)} sider tildelt {bulk_assign}")
                        st.rerun()

# =================== Prioritet Tab ===================
with tab_priority:
    st.subheader("🎯 Top 100 Prioriterede Sider")
    
    if not stats["has_traffic_data"]:
        st.warning("Upload Google Analytics data for at se prioriterede sider")
        
        with st.expander("📤 Upload Analytics Data"):
            st.info("""
            **Sådan eksporterer du data fra Google Analytics:**
            1. Gå til GA4 > Reports > Pages and screens
            2. Vælg periode: Sidste 30 dage
            3. Eksportér top 250 sider som CSV
            4. Upload filen her
            """)
            
            ga_file = st.file_uploader("Upload GA4 export", type=["csv", "xlsx"])
            if ga_file:
                try:
                    ga_df = pd.read_csv(ga_file) if ga_file.name.endswith('.csv') else pd.read_excel(ga_file)
                    
                    # Vis preview og lad bruger mappe kolonner
                    st.write("Preview af data:")
                    st.dataframe(ga_df.head())
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        url_col = st.selectbox("URL kolonne", ga_df.columns)
                    with col2:
                        visits_col = st.selectbox("Visits/Sessions kolonne", ga_df.columns)
                    
                    if st.button("Importér traffic data"):
                        # Forbered data
                        traffic_df = pd.DataFrame({
                            'url': ga_df[url_col],
                            'visits': ga_df[visits_col]
                        })
                        
                        # Rens URLs (tilføj domæne hvis mangler)
                        domain = st.selectbox("Domæne", ["https://www.niras.dk", "https://www.niras.com"])
                        traffic_df['url'] = traffic_df['url'].apply(
                            lambda x: f"{domain}{x}" if not x.startswith('http') else x
                        )
                        
                        # Opdater database
                        db.update_traffic_data(traffic_df)
                        st.success("✅ Traffic data importeret!")
                        st.rerun()
                        
                except Exception as e:
                    st.error(f"Fejl ved import: {e}")
    else:
        # Vis prioriterede sider
        priority_rows, _ = db.get_pages(
            priority_only=True,
            sort_by="traffic_rank",
            sort_dir="asc",
            limit=100
        )
        
        if priority_rows:
            # Stats cards
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Prioritet sider", stats["priority_total"])
            with col2:
                st.metric("Færdige", stats["priority_done"])
            with col3:
                st.metric("Completion", f"{int(stats['priority_completion']*100)}%")
            
            # Prioritet tabel
            priority_df = pd.DataFrame([dict(r) for r in priority_rows])
            priority_df["Rang"] = priority_df["traffic_rank"].fillna(0).astype(int)
            priority_df["Besøg/md"] = priority_df["monthly_visits"].fillna(0).astype(int)
            priority_df["URL"] = priority_df["url"]
            priority_df["Hits"] = priority_df["total"].fillna(0).astype(int)
            priority_df["Status"] = priority_df["status"].map({"todo":"🔴 Todo", "done":"✅ Done"})
            priority_df["Ansvarlig"] = priority_df["assigned_to"].fillna("-")
            
            st.dataframe(
                priority_df[["Rang", "URL", "Besøg/md", "Hits", "Status", "Ansvarlig"]],
                column_config={
                    "URL": st.column_config.LinkColumn(),
                    "Rang": st.column_config.NumberColumn(format="%d"),
                    "Besøg/md": st.column_config.NumberColumn(format="%,d"),
                    "Hits": st.column_config.NumberColumn(format="%d"),
                },
                height=400,
                hide_index=True
            )

# =================== Analytics Tab ===================
with tab_analytics:
    st.subheader("📊 Analytics & Indsigter")
    
    # KPI cards
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        avg_hits = db._conn().execute("SELECT AVG(total) FROM pages WHERE total > 0").fetchone()[0]
        st.metric("Gns. hits/side", f"{avg_hits:.1f}" if avg_hits else "0")
    with col2:
        high_risk = db._conn().execute("SELECT COUNT(*) FROM pages WHERE total >= 10 AND status='todo'").fetchone()[0]
        st.metric("Høj-risiko sider", high_risk, help="Todo sider med 10+ hits")
    with col3:
        done_week = db._conn().execute("""
            SELECT COUNT(*) FROM pages 
            WHERE status='done' 
            AND last_updated >= date('now', '-7 days')
        """).fetchone()[0]
        st.metric("Færdige denne uge", done_week)
    with col4:
        todo_priority = stats["priority_total"] - stats["priority_done"]
        st.metric("Prioritet tilbage", todo_priority)
    
    # Grafer
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Hits fordeling**")
        hist_data = pd.DataFrame(rows)
        if not hist_data.empty:
            hist_data['total_category'] = pd.cut(
                hist_data['total'], 
                bins=[0, 5, 10, 20, 50, 1000],
                labels=['0-5', '6-10', '11-20', '21-50', '50+']
            )
            category_counts = hist_data['total_category'].value_counts().reset_index()
            category_counts.columns = ['Hits', 'Antal']
            
            import altair as alt
            chart = alt.Chart(category_counts).mark_bar().encode(
                x=alt.X('Hits', sort=['0-5', '6-10', '11-20', '21-50', '50+']),
                y='Antal',
                color=alt.Color('Hits', scale=alt.Scale(scheme='greens'))
            ).properties(height=300)
            st.altair_chart(chart, use_container_width=True)
    
    with col2:
        st.markdown("**Ugentlig progress**")
        weekly_data = pd.read_sql_query("""
            SELECT 
                date(last_updated) as dato,
                COUNT(*) as antal
            FROM pages
            WHERE status='done'
            AND last_updated >= date('now', '-30 days')
            GROUP BY date(last_updated)
            ORDER BY dato
        """, db._conn())
        
        if not weekly_data.empty:
            line_chart = alt.Chart(weekly_data).mark_line(point=True).encode(
                x='dato:T',
                y='antal:Q',
                tooltip=['dato', 'antal']
            ).properties(height=300)
            st.altair_chart(line_chart, use_container_width=True)

# =================== Team Tab ===================
with tab_team:
    st.subheader("👥 Team Performance")
    
    team_stats = db.get_team_stats()
    
    if not team_stats.empty:
        # Team leaderboard
        team_stats['Completion %'] = (team_stats['done'] / team_stats['total_assigned'] * 100).round(1)
        team_stats['Effektivitet'] = team_stats['done'] / (
            (datetime.now() - datetime(2024, 1, 1)).days / 30
        )  # Done per måned
        
        st.dataframe(
            team_stats[['assigned_to', 'total_assigned', 'done', 'Completion %', 'priority_assigned']],
            column_config={
                'assigned_to': 'Team medlem',
                'total_assigned': 'Tildelt',
                'done': 'Færdige',
                'Completion %': st.column_config.ProgressColumn(),
                'priority_assigned': 'Prioritet'
            },
            hide_index=True
        )
        
        # Aktivitetslog
        st.markdown("### 📝 Seneste aktivitet")
        recent = db.get_recent_changes(days=7)
        if not recent.empty:
            st.dataframe(recent, height=200, hide_index=True)
    else:
        st.info("Ingen team data endnu. Tildel opgaver for at se statistik.")

# =================== Admin Tab ===================
with tab_admin:
    st.subheader("⚙️ Administration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🔄 Data Management")
        
        # Import section
        with st.expander("📥 Import data"):
            file = st.file_uploader("Upload CSV/Excel", type=["csv", "xlsx"])
            if file and st.button("Import"):
                df = pd.read_csv(file) if file.name.endswith('.csv') else pd.read_excel(file)
                db.sync_pages_from_import(df)
                st.success("Data importeret")
                st.rerun()
        
        # Crawler section
        with st.expander("🕷️ Crawler"):
            domain = st.selectbox("Domæne", ["https://www.niras.dk/", "https://www.niras.com/"])
            kw_text = st.text_area("Keywords (1 per linje)", "\n".join(DEFAULT_KW))
            
            if st.button("Start crawl"):
                keywords = [k.strip() for k in kw_text.split('\n') if k.strip()]
                with st.spinner("Crawler kører..."):
                    results = crawl(domain, keywords)
                    if results:
                        df = pd.DataFrame(results)
                        db.sync_pages_from_df(df, is_crawl=True, domain=domain)
                        st.success(f"Crawl færdig: {len(results)} sider")
                        st.rerun()
    
    with col2:
        st.markdown("### 📊 Database Stats")
        
        # Database info
        db_stats = {
            "Total sider": stats["total"],
            "Med traffic data": stats["has_traffic_data"],
            "Sidste crawl": db._conn().execute(
                "SELECT MAX(last_updated) FROM pages WHERE crawl_session IS NOT NULL"
            ).fetchone()[0] or "Aldrig",
            "Database størrelse": f"{os.path.getsize('app.db') / 1024 / 1024:.1f} MB"
        }
        
        for key, value in db_stats.items():
            st.metric(key, value)
        
        # Export section
        if st.button("📤 Export alle data"):
            all_data = pd.read_sql_query("SELECT * FROM pages", db._conn())
            csv = all_data.to_csv(index=False)
            st.download_button(
                "Download CSV",
                csv,
                f"niras_greenwashing_{datetime.now().strftime('%Y%m%d')}.csv",
                "text/csv"
            )