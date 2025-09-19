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

# =================== Resten af din app (tabs, oversigt, charts osv.) ===================
# Her indsætter du al den kode, du allerede har i app.py (uændret).
# ...
