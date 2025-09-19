# --- Crawler (auto – hele domænet) ---
st.header("Crawler")

domain = st.selectbox("Domæne", ["https://www.niras.dk/", "https://www.niras.com/"])

# Keywords/udsagn – UI kan stadig overrides; default = din standardliste
default_kw_text = "\n".join(DEFAULT_KW)
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

# Sunde, faste grænser så “crawl alt” ikke løber løbsk
MAX_PAGES  = 5000   # hård øvre grænse for antal sider
MAX_DEPTH  = 50     # dybde-rimelig “uendelig”
DELAY_SECS = 0.3    # høflig crawl

if st.button("🚀 Crawl hele domænet", type="secondary", key="crawl_all_btn"):
    if not kw_final:
        st.warning("Tilføj mindst ét ord/udsagn (eller slå flet med datakilden til).")
    else:
        # DB-status før
        db.init_db()
        stats_before = db.stats()
        total_before = stats_before.get("total", 0)

        with st.spinner(f"Crawler {domain} — kan tage lidt (respekterer serveren) …"):
            try:
                rows = crawl(domain, kw_final, max_pages=MAX_PAGES, max_depth=MAX_DEPTH, delay=DELAY_SECS)
            except TypeError:
                # fallback hvis din crawler-signatur er uden delay/max_depth
                rows = crawl(domain, kw_final, max_pages=MAX_PAGES)

        if rows:
            import pandas as pd
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
            st.info("Ingen sider fundet eller ingen matches (tjek domæne/keywords).")
