# crawler.py
# Enkel BFS-crawler til NIRAS Greenwashing-dashboard
# - Holder sig til samme domæne
# - Respekterer max_pages og max_depth
# - Finder keywords/udsagn i tekstindhold
# - Returnerer: {url, keywords, hits, total}

from __future__ import annotations

import re
import time
from typing import Iterable, Dict, Set, Tuple, List
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

__all__ = ["crawl", "DEFAULT_KW"]

# Standardliste over greenwashing-relaterede udsagn
DEFAULT_KW = [
    "bæredygtig*", "miljøvenlig*", "miljørigtig*", "klimavenlig*",
    "grøn*", "grønnere", "klimaneutral*", "co2[- ]?neutral", "klimakompensation*"
]

HDRS = {"User-Agent": "NIRAS-Green-Dashboard/1.0"}
ALLOWED_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "strong", "em", "span", "a"}


def compile_kw_patterns(keywords: Iterable[str]) -> Dict[str, re.Pattern]:
    pats: Dict[str, re.Pattern] = {}
    for raw in keywords:
        kw = (raw or "").strip()
        if not kw:
            continue
        if kw.startswith("/") and kw.endswith("/") and len(kw) >= 3:
            pats[kw] = re.compile(kw[1:-1], re.IGNORECASE)
            continue
        if kw.endswith("*"):
            base = re.escape(kw[:-1])
            pats[kw] = re.compile(rf"\b{base}\w*\b", re.IGNORECASE)
        else:
            pats[kw] = re.compile(rf"\b{re.escape(kw)}\b", re.IGNORECASE)
    return pats


def extract_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for t in ("nav", "header", "footer", "aside"):
        for el in soup.find_all(t):
            el.decompose()
    texts: List[str] = []
    for tag in soup.find_all(ALLOWED_TAGS):
        txt = tag.get_text(" ", strip=True)
        if txt:
            texts.append(txt)
    return "\n".join(texts)


def page_counts(text: str, patterns: Dict[str, re.Pattern]) -> Tuple[str, int]:
    present: List[str] = []
    total = 0
    for kw, pat in patterns.items():
        matches = list(pat.finditer(text))
        if matches:
            present.append(kw)
            total += len(matches)
    return ", ".join(present), total


def _same_site(u: str, root_netloc: str) -> bool:
    try:
        return urlparse(u).netloc.endswith(root_netloc)
    except Exception:
        return False


def crawl(
    seed: str,
    keywords: List[str],
    max_pages: int = 5000,
    max_depth: int = 50,
    delay: float = 0.3,
) -> List[Dict[str, str]]:
    if not isinstance(seed, str) or not seed.strip():
        return []

    start = seed.strip()
    parsed = urlparse(start)
    if not parsed.scheme:
        start = f"https://{start.strip('/')}"
        parsed = urlparse(start)

    root_netloc = parsed.netloc
    seen: Set[str] = set()
    q: List[Tuple[str, int]] = [(start, 0)]
    out: List[Dict[str, str]] = []

    pats = compile_kw_patterns(keywords)

    while q and len(seen) < max_pages:
        url, depth = q.pop(0)
        if url in seen or depth > max_depth:
            continue
        seen.add(url)

        try:
            r = requests.get(url, headers=HDRS, timeout=20)
            ctype = (r.headers.get("content-type") or "")
            if r.status_code >= 400 or ("text" not in ctype and "html" not in ctype):
                continue

            html = r.text
            text = extract_text(html)
            kws, total = page_counts(text, pats)
            out.append({"url": url, "keywords": kws, "hits": total, "total": total})

            soup = BeautifulSoup(html, "lxml")
            for a in soup.find_all("a", href=True):
                u = urljoin(url, a["href"])
                up = urlparse(u)
                if up.scheme in ("http", "https") and _same_site(u, root_netloc):
                    clean = up._replace(fragment="").geturl()
                    if clean not in seen and all(clean != p for p, _ in q):
                        q.append((clean, depth + 1))

            if delay > 0:
                time.sleep(delay)

        except Exception:
            continue

    return out
