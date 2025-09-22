# ===== FILE: crawler.py =====
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
u_fetch = _cache_bust(url)
r = requests.get(u_fetch, headers=HDRS, timeout=20)
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