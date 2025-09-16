# context.py
from __future__ import annotations
import re
import requests
from bs4 import BeautifulSoup
from typing import Iterable, List, Dict, Any

ALLOWED_TAGS = {"h1","h2","h3","h4","h5","h6","p","li","strong","em","span","a"}

def fetch_html(url: str, timeout: int = 15) -> str:
    headers = {"User-Agent": "NIRAS-Green-Dashboard/1.0"}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text

def compile_kw_patterns(keywords: Iterable[str]) -> Dict[str, re.Pattern]:
    pats = {}
    for kw in keywords:
        kw = kw.strip()
        if not kw:
            continue
        # understøt wildcard * i dine lister (f.eks. 'bæredygtig*')
        if kw.endswith("*"):
            base = re.escape(kw[:-1])
            regex = re.compile(rf"\b{base}\w*\b", flags=re.IGNORECASE)
        else:
            regex = re.compile(rf"\b{re.escape(kw)}\b", flags=re.IGNORECASE)
        pats[kw] = regex
    return pats

def extract_snippets(html: str, keywords: Iterable[str], max_per_kw: int = 25) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    patterns = compile_kw_patterns(keywords)
    rows: List[Dict[str, Any]] = []
    for tag in soup.find_all(ALLOWED_TAGS):
        text = " ".join(tag.get_text(separator=" ", strip=True).split())
        if not text:
            continue
        for kw, pat in patterns.items():
            matches = list(pat.finditer(text))
            if not matches:
                continue
            for m in matches[:max_per_kw]:
                start, end = m.start(), m.end()
                # lav en kort kontekst omkring match
                left = max(0, start - 80); right = min(len(text), end + 80)
                snippet = text[left:right]
                rows.append({
                    "keyword": kw,
                    "tag": tag.name,
                    "snippet": snippet,
                    "start": start,
                    "end": end,
                })
    return rows
