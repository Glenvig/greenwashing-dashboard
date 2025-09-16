# data.py
# Indlæsning og normalisering af CSV/Excel + robust parsing + støtte for scraperens Excel (wide format)

from __future__ import annotations
import io
import os
import re
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st


# Mini-demo (fallback – simulerer scraperens struktur: én kolonne pr. keyword + total)
SAMPLE_WIDE = pd.DataFrame(
    [
        {
            "url": "https://www.niras.dk/cases/energi/biogas-opgradering/",
            "bæredygtig*": 3, "miljørigtig": 0, "klimavenlig": 1, "grøn": 0, "grønne": 1,
            "miljøvenlig": 1, "skånsom mod miljøet": 0, "co2-neutral": 2, "klimaneutral*": 1,
            "total": 9,
        },
        {
            "url": "https://www.niras.dk/yelser/lca-og-epd/",
            "bæredygtig*": 4, "miljørigtig": 0, "klimavenlig": 0, "grøn": 1, "grønne": 0,
            "miljøvenlig": 2, "skånsom mod miljøet": 0, "co2-neutral": 3, "klimaneutral*": 2,
            "total": 12,
        },
        {
            "url": "https://www.niras.dk/indsigter/klimarisici-i-byggeri/",
            "bæredygtig*": 2, "miljørigtig": 0, "klimavenlig": 1, "grøn": 0, "grønne": 0,
            "miljøvenlig": 0, "skånsom mod miljøet": 0, "co2-neutral": 1, "klimaneutral*": 1,
            "total": 5,
        },
        {
            "url": "https://www.niras.dk/sektorer/vand/vandforvaltning/",
            "bæredygtig*": 1, "miljørigtig": 0, "klimavenlig": 0, "grøn": 0, "grønne": 0,
            "miljøvenlig": 2, "skånsom mod miljøet": 0, "co2-neutral": 1, "klimaneutral*": 2,
            "total": 6,
        },
        {
            "url": "https://www.niras.dk/sektorer/fodevarer/energioptimering/",
            "bæredygtig*": 3, "miljørigtig": 0, "klimavenlig": 0, "grøn": 1, "grønne": 1,
            "miljøvenlig": 2, "skånsom mod miljøet": 0, "co2-neutral": 2, "klimaneutral*": 2,
            "total": 11,
        },
        {
            "url": "https://www.niras.dk/indsigter/csrd-rapportering/",
            "bæredygtig*": 4, "miljørigtig": 0, "klimavenlig": 1, "grøn": 0, "grønne": 0,
            "miljøvenlig": 2, "skånsom mod miljøet": 0, "co2-neutral": 5, "klimaneutral*": 2,
            "total": 14,
        },
        {
            "url": "https://www.niras.dk/indsigter/pfas-kortlaegning/",
            "bæredygtig*": 1, "miljørigtig": 0, "klimavenlig": 0, "grøn": 0, "grønne": 0,
            "miljøvenlig": 1, "skånsom mod miljøet": 0, "co2-neutral": 1, "klimaneutral*": 1,
            "total": 4,
        },
        {
            "url": "https://www.niras.dk/sektorer/industri/energieffektivitet/",
            "bæredygtig*": 2, "miljørigtig": 0, "klimavenlig": 0, "grøn": 1, "grønne": 0,
            "miljøvenlig": 1, "skånsom mod miljøet": 0, "co2-neutral": 1, "klimaneutral*": 2,
            "total": 7,
        },
    ]
)


def _read_any(handle_or_path) -> pd.DataFrame:
    if isinstance(handle_or_path, str):
        lower = handle_or_path.lower()
        if lower.endswith((".xlsx", ".xls")):
            return pd.read_excel(handle_or_path, engine="openpyxl")
        return pd.read_csv(handle_or_path, sep=None, engine="python", encoding="utf-8-sig")
    else:
        b = handle_or_path
        if hasattr(b, "seek"):
            b.seek(0)
        try:
            return pd.read_excel(b, engine="openpyxl")
        except Exception:
            pass
        if hasattr(b, "seek"):
            b.seek(0)
        return pd.read_csv(b, sep=None, engine="python", encoding_errors="ignore")


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    # Trim/ensret basisnavne (vi forventer 'url' og evt. 'total')
    rename = {c: c.strip() for c in df.columns}
    df = df.rename(columns=rename)
    # prøv at finde URL-kolonnen
    if "url" not in df.columns:
        candidates = [c for c in df.columns if c.lower() == "url" or "url" in c.lower()]
        if candidates:
            df = df.rename(columns={candidates[0]: "url"})
    if "url" not in df.columns:
        raise ValueError("Kunne ikke finde en 'url' kolonne i filen.")
    # sikr total hvis findes
    for c in df.columns:
        if c.lower().strip() == "total" and c != "total":
            df = df.rename(columns={c: "total"})
    return df


def detect_keyword_columns(df: pd.DataFrame) -> List[str]:
    # Keyword-kolonner = alle numeriske kolonner ≠ url/total
    excl = {"url", "total"}
    num_cols = [c for c in df.columns if c not in excl and pd.api.types.is_numeric_dtype(df[c])]
    # fallback: kolonner ≠ url/total hvis ikke typet endnu
    if not num_cols:
        num_cols = [c for c in df.columns if c not in excl]
    return num_cols


def wide_to_standard(df_wide: pd.DataFrame, kw_cols: List[str]) -> pd.DataFrame:
    # Byg standard-output som resten af appen forventer
    std = pd.DataFrame()
    std["url"] = df_wide["url"].astype(str).str.strip()
    # keywords-liste: kun de keywords med count > 0
    def _kw_join(row):
        present = [k for k in kw_cols if (pd.notna(row.get(k)) and int(row.get(k) or 0) > 0)]
        return ", ".join(present)
    std["keywords"] = df_wide.apply(_kw_join, axis=1)
    # antal_forekomster = sum(keyword-kolonner)
    std["antal_forekomster"] = df_wide[kw_cols].apply(pd.to_numeric, errors="coerce").fillna(0).astype(int).sum(axis=1)
    # total: brug eksisterende total hvis den findes, ellers antal_forekomster
    if "total" in df_wide.columns:
        std["total"] = pd.to_numeric(df_wide["total"], errors="coerce").fillna(0).astype(int)
        # hvis total er 0/NaN, fallback til sum
        std.loc[std["total"].isna() | (std["total"] == 0), "total"] = std["antal_forekomster"]
    else:
        std["total"] = std["antal_forekomster"]
    return std[["url", "keywords", "antal_forekomster", "total"]]


def build_kw_long_from_wide(df_wide: pd.DataFrame, kw_cols: List[str]) -> pd.DataFrame:
    # Long-format: url, keyword, count
    m = df_wide.melt(id_vars=["url"], value_vars=kw_cols, var_name="keyword", value_name="count")
    m["count"] = pd.to_numeric(m["count"], errors="coerce").fillna(0).astype(int)
    m = m[m["count"] > 0].reset_index(drop=True)
    return m


def build_kw_long_from_std(std: pd.DataFrame) -> pd.DataFrame:
    # Demo-tilfælde uden per-keyword counts: brug 1 pr. keyword
    rows = []
    for _, r in std.iterrows():
        url = r["url"]
        kws = [k.strip() for k in re.split(r"[;,]", r.get("keywords", "")) if k.strip()]
        for k in kws:
            rows.append({"url": url, "keyword": k, "count": 1})
    return pd.DataFrame(rows)


@st.cache_data(show_spinner=False)
def load_dataframe_from_file(
    file_source: str | io.BytesIO | None,
) -> Tuple[pd.DataFrame, pd.DataFrame, bool, str]:
    """
    Returner (df_standard, kw_long, is_demo, label).
    - df_standard: url, keywords (komma-separeret), antal_forekomster, total
    - kw_long: url, keyword, count (rigtige tal fra fil hvis muligt)
    """
    # Default sti
    if file_source is None:
        default_path = os.path.join("data", "crawl.csv")
        # prøv standardsti
        if os.path.exists(default_path):
            try:
                raw = _read_any(default_path)
                raw = _normalize_cols(raw)
                kw_cols = detect_keyword_columns(raw)
                std = wide_to_standard(raw, kw_cols)
                kw_long = build_kw_long_from_wide(raw, kw_cols)
                return std, kw_long, False, default_path
            except Exception:
                pass
        # Fallback demo
        raw = _normalize_cols(SAMPLE_WIDE.copy())
        kw_cols = detect_keyword_columns(raw)
        std = wide_to_standard(raw, kw_cols)
        kw_long = build_kw_long_from_wide(raw, kw_cols)
        return std, kw_long, True, "DEMO (in-memory)"

    # Fil valgt / uploadet
    try:
        if hasattr(file_source, "getvalue"):
            raw = _read_any(io.BytesIO(file_source.getvalue()))
        else:
            raw = _read_any(file_source)
        raw = _normalize_cols(raw)
        kw_cols = detect_keyword_columns(raw)
        std = wide_to_standard(raw, kw_cols)
        kw_long = build_kw_long_from_wide(raw, kw_cols)
        label = getattr(file_source, "name", str(file_source))
        return std, kw_long, False, label
    except Exception as e:
        st.warning(f"Kunne ikke indlæse filen ({e}). Viser demodata.")
        raw = _normalize_cols(SAMPLE_WIDE.copy())
        kw_cols = detect_keyword_columns(raw)
        std = wide_to_standard(raw, kw_cols)
        kw_long = build_kw_long_from_wide(raw, kw_cols)
        return std, kw_long, True, "DEMO (in-memory)"


# Hjælpere til visning
def split_keywords(raw: str, preferred_delim: Optional[str] = None) -> List[str]:
    if not isinstance(raw, str) or not raw.strip():
        return []
    text = raw.strip()
    if preferred_delim in [",", ";"]:
        parts = [p.strip() for p in text.split(preferred_delim)]
    else:
        parts = re.split(r"[;,]", text)
        parts = [p.strip() for p in parts]
    return [p for p in parts if p]


def keyword_page_counts(std_df: pd.DataFrame, preferred_kw_delim: Optional[str] = None) -> pd.DataFrame:
    # Antal unikke sider pr. keyword (fra standard 'keywords')
    rows = []
    for _, r in std_df.iterrows():
        for k in split_keywords(r["keywords"], preferred_kw_delim):
            rows.append({"url": r["url"], "keyword": k})
    ex = pd.DataFrame(rows)
    if ex.empty:
        return ex
    counts = ex.groupby("keyword")["url"].nunique().reset_index(name="sider")
    counts = counts.sort_values("sider", ascending=False, ignore_index=True)
    return counts


def keyword_totals_from_long(kw_long: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    if kw_long.empty:
        return kw_long
    agg = kw_long.groupby("keyword")["count"].sum().reset_index()
    agg = agg.sort_values("count", ascending=False, ignore_index=True).head(top_n)
    agg = agg.rename(columns={"count": "forekomster"})
    return agg
