# crawler.py
from __future__ import annotations
import re, time
from urllib.parse import urljoin, urlparse
from typing import Iterable, Dict, Set, Tuple, List
import requests
from bs4 import BeautifulSoup

HDRS = {"User-Agent": "NIRAS-Green-Dashboard/1.0"}

def compile_kw_patterns(keywords: Iterable[str]) -> Dict[str, re.Pattern]:
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

ALLOWED_TAGS = {"h1","h2","h3","h4","h5","h6","p","li","strong","em","span","a"}

def extract_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for t in ["nav","header","footer","aside"]:
        for el in soup.find_all(t):
            el.decompose()
    texts = []
    for tag in soup.find_all(ALLOWED_TAGS):
        txt = tag.get_text(" ", strip=True)
        if txt:
            texts.append(txt)
    return "\n".join(texts)

def page_counts(text: str, patterns: Dict[str, re.Pattern]) -> Tuple[str, int]:
    present = []
    total = 0
    for kw, pat in patterns.items():
        matches = list(pat.finditer(text))
        if matches:
            present.append(kw)
            total += len(matches)
    return ", ".join(present), total

def same_site(u: str, root_netloc: str) -> bool:
    try:
        return urlparse(u).netloc.endswith(root_netloc)
    except Exception:
        return False

def crawl(seed: str, keywords: List[str], max_pages: int = 200, delay: float = 0.5) -> List[Dict[str, str]]:
    seed = seed.strip()
    root = urlparse(seed)
    root_netloc = root.netloc
    seen: Set[str] = set()
    q: List[str] = [seed]
    out: List[Dict[str, str]] = []

    pats = compile_kw_patterns(keywords)

    while q and len(seen) < max_pages:
        url = q.pop(0)
        if url in seen:
            continue
        seen.add(url)
        try:
            r = requests.get(url, headers=HDRS, timeout=15)
            if r.status_code >= 400 or not r.headers.get("content-type", "").startswith("text"):
                continue
            html = r.text
            text = extract_text(html)
            kws, total = page_counts(text, pats)
            hits = total
            out.append({"url": url, "keywords": kws, "hits": hits, "total": total})

            soup = BeautifulSoup(html, "lxml")
            for a in soup.find_all("a", href=True):
                u = urljoin(url, a["href"])
                up = urlparse(u)
                if up.scheme in ("http", "https") and same_site(u, root_netloc):
                    if ("#" not in up.path) and (u not in seen) and (u not in q):
                        q.append(u)
            time.sleep(delay)
        except Exception:
            continue
    return out
