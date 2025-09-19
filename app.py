import os
import re
import pandas as pd
import streamlit as st

import db
import data as d
import charts as c
import gamification as g
import context as ctx
import crawler  # NEW: tilføj crawler-modulet

# =================== Sidebar: Data + Crawler ===================
with st.sidebar:
    # --- Data ---
    st.header("Data")
    default_path = os.path.join("data", "crawl.csv")
    path_str = st.text_input("Sti til CSV/Excel", value=default_path)
    uploaded = st.file_uploader("...eller upload fil", type=["csv", "xlsx", "xls"])
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

    # Keywords/udsagn – bruger DEFAULT_KW fra crawler.py hvis den findes
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
            # unikke
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

    if st.button("Start crawl", type="secondary", key="crawl_btn"):
        if not kw_final:
            st.warning("Tilføj mindst ét ord/udsagn (eller slå flet med datakilden til).")
        else:
            with st.spinner("Crawler kører – respekterer robots.txt…"):
                try:
                    rows = crawler.crawl(domain, kw_final, max_pages=max_pages, max_depth=max_depth)
                except TypeError:
                    rows = crawler.crawl(domain, kw_final, max_pages=max_pages)

            if rows:
                cdf = pd.DataFrame(rows)
                db.sync_pages_from_df(cdf)
                st.success(f"Crawler tilføjede/opdaterede {len(cdf)} sider fra {domain}")
                st.rerun()
            else:
                st.info("Ingen sider fundet eller ingen matches.")
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
                "Assigned to": st.column_config.SelectboxColumn(options=TEAM_OPTS, help="Tildel ansvarlig"),
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
