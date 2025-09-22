from __future__ import annotations

import re
import time
from typing import Iterable, Dict, Set, Tuple, List, Callable, Iterator, Optional
from urllib.parse import (
    urljoin, urlparse, urlencode, urlunparse, parse_qsl
)

import requests
from bs4 import BeautifulSoup

__all__ = [
    "crawl",
    "crawl_iter",
    "scan_pages",
    "DEFAULT_KW",
    "_cache_bust",
    "HDRS",
]

# Standardliste over greenwashing-relaterede udsagn
DEFAULT_KW = [
    "bæredygtig*", "miljøvenlig*", "miljørigtig*", "klimavenlig*",
    "grøn*", "grønnere", "klimaneutral*", "co2[- ]?neutral",
    "netto[- ]?nul", "klimakompensation*", "kompenseret for CO2",
    "100% grøn strøm", "uden udledning", "nul udledning", "zero emission*",
]

# Høflige headers + cache-bypass
HDRS = {
    "User-Agent": "NIRAS-Green-Dashboard/1.0",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}
ALLOWED_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "strong", "em", "span", "a"}


def _cache_bust(u: str) -> str:
    """Tilføj timestamp i query-string for at undgå CDN-cache."""
    p = urlparse(u)
    q = dict(parse_qsl(p.query))
    q["_gwts"] = str(int(time.time()))
    new_q = urlencode(q, doseq=True)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, new_q, p.fragment))


def compile_kw_patterns(keywords: Iterable[str]) -> Dict[str, re.Pattern]:
    """Byg regex-mønstre med støtte for '*' wildcard og evt. /regex/ input."""
    pats: Dict[str, re.Pattern] = {}
    for raw in keywords:
        kw = (raw or "").strip()
        if not kw:
            continue
        # Direkte regex som /.../
        if kw.startswith("/") and kw.endswith("/") and len(kw) >= 3:
            pats[kw] = re.compile(kw[1:-1], re.IGNORECASE)
            continue
        # '*' wildcard -> ordstamme
        if kw.endswith("*"):
            base = re.escape(kw[:-1])
            pats[kw] = re.compile(rf"\b{base}\w*\b", re.IGNORECASE)
        else:
            pats[kw] = re.compile(rf"\b{re.escape(kw)}\b", re.IGNORECASE)
    return pats


def extract_text(html: str) -> str:
    """Ekstrahér meningsfuld tekst (stripper nav/header/footer/aside)."""
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


def page_counts(
    text: str,
    patterns: Dict[str, re.Pattern],
    exclude_patterns: Optional[Dict[str, re.Pattern]] = None,
) -> Tuple[str, int]:
    """Returnér (komma-separeret liste af matchende keywords, total antal matches).
    Hvis exclude_patterns er angivet, filtreres matches fra, hvor selve match-tekst
    rammer et ekskluderet mønster (fx 'grøn*' ekskluderer 'grønningen').
    """
    present: List[str] = []
    total = 0
    ex_pats = list((exclude_patterns or {}).values())
    for kw, pat in patterns.items():
        kept = []
        for m in pat.finditer(text):
            token = m.group(0)
            if ex_pats and any(ex.search(token) for ex in ex_pats):
                continue
            kept.append(m)
        if kept:
            present.append(kw)
            total += len(kept)
    return ", ".join(present), total


def _same_site(u: str, root_netloc: str) -> bool:
    try:
        return urlparse(u).netloc.endswith(root_netloc)
    except Exception:
        return False


# -------- Generator: giver ét resultat ad gangen + valgfri progress callback --------
def crawl_iter(
    seed: str,
    keywords: List[str],
    max_pages: int = 5000,
    max_depth: int = 50,
    delay: float = 0.3,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    excludes: Optional[List[str]] = None,
) -> Iterator[Dict[str, str]]:
    if not isinstance(seed, str) or not seed.strip():
        return

    start = seed.strip()
    parsed = urlparse(start)
    if not parsed.scheme:
        start = f"https://{start.strip('/')}"
        parsed = urlparse(start)

    root_netloc = parsed.netloc
    seen: Set[str] = set()
    q: List[Tuple[str, int]] = [(start, 0)]

    pats = compile_kw_patterns(keywords)
    ex_pats = compile_kw_patterns(excludes or []) if excludes else {}
    done = 0

    while q and len(seen) < max_pages:
        url, depth = q.pop(0)
        if url in seen or depth > max_depth:
            if progress_cb:
                progress_cb(done, len(q))
            continue
        seen.add(url)

        try:
            u_fetch = _cache_bust(url)
            r = requests.get(u_fetch, headers=HDRS, timeout=20)
            ctype = (r.headers.get("content-type") or "")
            if r.status_code >= 400 or ("text" not in ctype and "html" not in ctype):
                if progress_cb:
                    progress_cb(done, len(q))
                continue

            html = r.text
            text = extract_text(html)
            kws, total = page_counts(text, pats, ex_pats)
            row = {"url": url, "keywords": kws, "hits": total, "total": total}
            done += 1
            if progress_cb:
                progress_cb(done, len(q))
            yield row

            soup = BeautifulSoup(html, "lxml")
            for a in soup.find_all("a", href=True):
                u2 = urljoin(url, a["href"])
                up = urlparse(u2)
                if up.scheme in ("http", "https") and _same_site(u2, root_netloc):
                    clean = up._replace(fragment="").geturl()
                    if clean not in seen and all(clean != p for p, _ in q):
                        q.append((clean, depth + 1))

            if delay > 0:
                time.sleep(delay)

        except Exception:
            if progress_cb:
                progress_cb(done, len(q))
            continue


# -------- Wrapper: fuldt crawl, samler til liste --------
def crawl(
    seed: str,
    keywords: List[str],
    max_pages: int = 5000,
    max_depth: int = 50,
    delay: float = 0.3,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    excludes: Optional[List[str]] = None,
) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for row in crawl_iter(seed, keywords, max_pages, max_depth, delay, progress_cb, excludes):
        out.append(row)
    return out


# -------- Targeted scan: vurder præcis disse URLs (uden BFS) --------
def scan_pages(urls: List[str], keywords: List[str], delay: float = 0.2, excludes: Optional[List[str]] = None) -> List[Dict[str, str]]:
    pats = compile_kw_patterns(keywords)
    ex_pats = compile_kw_patterns(excludes or []) if excludes else {}
    out: List[Dict[str, str]] = []
    for u in urls:
        try:
            u_fetch = _cache_bust(u)
            r = requests.get(u_fetch, headers=HDRS, timeout=20)
            ctype = (r.headers.get("content-type") or "")
            if r.status_code >= 400 or ("text" not in ctype and "html" not in ctype):
                continue
            text = extract_text(r.text)
            kws, total = page_counts(text, pats, ex_pats)
            out.append({"url": u, "keywords": kws, "hits": total, "total": total})
            if delay > 0:
                time.sleep(delay)
        except Exception:
            continue
    return out