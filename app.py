# NIRAS Greenwashing-dashboard — komplet app med crawler (auto hele domænet) og live-opdatering
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