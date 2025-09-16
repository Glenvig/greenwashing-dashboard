# charts.py
# Grafer (Altair) + KPI-cards

import altair as alt
import pandas as pd
import streamlit as st


def kpi_cards(total: int, done: int, todo: int, completion: float):
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Sider i alt", f"{total}")
    c2.metric("Færdige (done)", f"{done}")
    c3.metric("Mangler (todo)", f"{todo}")
    c4.metric("Completion", f"{completion:.1f}%")
    st.progress(int(completion))


def bar_keyword_pages(df_kw_counts: pd.DataFrame, top_n: int = 20, title: str = "Sider pr. keyword"):
    if df_kw_counts.empty:
        st.info("Ingen keywords fundet.")
        return
    chart = (
        alt.Chart(df_kw_counts.head(top_n))
        .mark_bar()
        .encode(
            x=alt.X("sider:Q", title="Antal sider"),
            y=alt.Y("keyword:N", sort="-x", title="Keyword"),
            tooltip=["keyword", "sider"],
        )
        .properties(height=25 * min(top_n, len(df_kw_counts)), title=title)
    )
    st.altair_chart(chart, use_container_width=True)


def bar_keyword_totals(df_kw_totals: pd.DataFrame, title: str = "Mest forekommende keywords (faktiske counts)"):
    if df_kw_totals.empty:
        st.info("Ingen keywords fundet.")
        return
    chart = (
        alt.Chart(df_kw_totals)
        .mark_bar()
        .encode(
            x=alt.X("forekomster:Q", title="Forekomster"),
            y=alt.Y("keyword:N", sort="-x", title="Keyword"),
            tooltip=["keyword", "forekomster"],
        )
        .properties(height=25 * len(df_kw_totals), title=title)
    )
    st.altair_chart(chart, use_container_width=True)


def hist_total(df_pages: pd.DataFrame, title: str = "Fordeling af total pr. side"):
    if "total" not in df_pages.columns or df_pages.empty:
        return
    bins = max(10, min(40, int(df_pages["total"].nunique() or 10)))
    chart = (
        alt.Chart(df_pages)
        .mark_bar()
        .encode(
            x=alt.X("total:Q", bin=alt.Bin(maxbins=bins), title="Total hits"),
            y=alt.Y("count()", title="Antal sider"),
            tooltip=[alt.Tooltip("count()", title="Antal sider")],
        )
        .properties(height=240, title=title)
    )
    st.altair_chart(chart, use_container_width=True)
