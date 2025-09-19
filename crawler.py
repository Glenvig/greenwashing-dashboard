# crawler.py
for kw, pat in patterns.items():
matches = list(pat.finditer(text))
if matches:
present.append(kw)
total += len(matches)
return ", ".join(present), total


# --- robots ---
def allow_url(u: str, rp: robotparser.RobotFileParser) -> bool:
try:
return rp.can_fetch(HDRS["User-Agent"], u)
except Exception:
return True


# --- BFS crawl ---
def crawl(seed: str, keywords: List[str], max_pages: int = 300, max_depth: int = 4, delay: float = 0.4) -> List[Dict[str, str]]:
seed = seed.strip()
root = urlparse(seed)
if not root.scheme:
seed = f"https://{seed.strip('/')}"
root = urlparse(seed)
root_base = f"{root.scheme}://{root.netloc}"


# robots
rp = robotparser.RobotFileParser()
rp.set_url(urljoin(root_base, "/robots.txt"))
try:
rp.read()
except Exception:
pass


seen: Set[str] = set()
q: List[Tuple[str, int]] = [(seed, 0)]
out: List[Dict[str, str]] = []


pats = compile_kw_patterns(keywords)


while q and len(seen) < max_pages:
url, depth = q.pop(0)
if url in seen or depth > max_depth:
continue
seen.add(url)
if not allow_url(url, rp):
continue
try:
r = requests.get(url, headers=HDRS, timeout=20)
ctype = r.headers.get("content-type", "")
if r.status_code >= 400 or ("text" not in ctype and "html" not in ctype):
continue
html = r.text
text = extract_text(html)
kws, total = page_counts(text, pats)
hits = total
out.append({"url": url, "keywords": kws, "hits": hits, "total": total})


# links
soup = BeautifulSoup(html, "lxml")
for a in soup.find_all("a", href=True):
u = urljoin(url, a["href"]) # resolve relative
up = urlparse(u)
if up.scheme in ("http","https") and up.netloc == root.netloc:
# fjern fragmenter og dubletter
path = up._replace(fragment="").geturl()
if path not in seen and not any(path == p for p,_ in q):
q.append((path, depth+1))
time.sleep(delay)
except Exception:
continue
return out