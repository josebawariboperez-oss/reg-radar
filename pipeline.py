# pipeline.py
import os
import sys
import argparse
from typing import List, Dict, Any, Optional
from datetime import datetime
from urllib.parse import urljoin, urlparse

from dotenv import load_dotenv
from supabase import create_client, Client
import httpx
from selectolax.parser import HTMLParser
import feedparser
import asyncio

# =========================
# Configuración
# =========================
WRITE_TO_DB = False
SOURCE_LIMIT_DEFAULT = 10

COVERAGE_FIELDS = (
    "id,country,authority,source_url,format,has_rss,rss_url,"
    "requires_js,priority,is_active,parser_name"
)

# =========================
# Utilidades
# =========================
def log(msg: str):
    print(f"[{datetime.utcnow().isoformat()}Z] {msg}")

def is_pdf_url(u: str) -> bool:
    try:
        path = urlparse(u).path.lower()
        return path.endswith(".pdf")
    except Exception:
        return False

def normalize_country(c: str) -> str:
    c = (c or "").strip()
    if c in ("UAE", "KSA", "Qatar"):
        return c
    return c or "UAE"

def looks_like_http(u: str) -> bool:
    if not u:
        return False
    ul = u.strip().lower()
    return ul.startswith("http://") or ul.startswith("https://")

def domain_of(u: str) -> str:
    try:
        return urlparse(u).netloc.lower()
    except Exception:
        return ""

# --- Fallbacks/overrides de URLs (para dominios problemáticos) ---
def candidate_urls(base_url: str) -> list[str]:
    """
    Genera variantes robustas de URL para casos típicos:
    - añadir/quitar www
    - forzar https
    - añadir /en/ para NCSA si faltase
    - corregir rutas conocidas (Hukoomi, MCIT)
    """
    if not base_url:
        return []

    base_url = base_url.strip()
    if not looks_like_http(base_url):
        base_url = "https://" + base_url.lstrip("/")

    u = urlparse(base_url)
    host = u.netloc.lower()
    path = (u.path or "/")

    # Overrides específicos
    if "hukoomi.gov.qa" in host:
        path = "/en/policies-and-strategies"
    if "mcit.gov.qa" in host and "/policies" in path and "reports" not in path:
        path = "/en/policies-and-reports/"
    if "ncsa.gov.qa" in host and (path == "/" or path == ""):
        path = "/en/"

    base_norm = f"https://{host}{path}"
    variants = {base_norm}

    # alternar www
    if host.startswith("www."):
        variants.add(f"https://{host[4:]}{path}")
    else:
        variants.add(f"https://www.{host}{path}")

    # quitar/normalizar slash final
    variants = {v.rstrip("/") + ("/" if path.endswith("/") else "") for v in variants}
    return list(variants)

# =========================
# Supabase
# =========================
def get_supabase() -> Client:
    load_dotenv()
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        log("❌ Falta SUPABASE_URL o SUPABASE_KEY en .env")
        sys.exit(1)
    return create_client(url, key)

def fetch_active_sources(
    supabase: Client,
    limit: int,
    only: Optional[str] = None,
    country: Optional[str] = None,
) -> List[Dict[str, Any]]:
    q = supabase.table("coverage").select(COVERAGE_FIELDS).eq("is_active", True)
    if country:
        q = q.eq("country", country)
    if only:
        only = only.lower()
        if only == "rss":
            q = q.eq("has_rss", True)
        elif only == "html":
            q = q.like("format", "%HTML%")
        elif only == "pdf":
            q = q.like("format", "%PDF%")
    q = q.order("priority", desc=False).limit(limit)
    resp = q.execute()
    return resp.data or []

# =========================
# Coletores
# =========================
def collect_rss(source: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw_rss = (source.get("rss_url") or "").strip()
    if not looks_like_http(raw_rss):
        return []
    url = raw_rss
    log(f"RSS: leyendo {url}")
    fp = feedparser.parse(url)
    items = []
    for e in fp.entries[:50]:
        items.append({
            "country": normalize_country(source.get("country")),
            "authority": source.get("authority"),
            "source_url": source.get("source_url"),
            "ingest_source_type": "rss",
            "title": getattr(e, "title", None),
            "doc_url": getattr(e, "link", None),
            "published_at": getattr(e, "published", None) or getattr(e, "updated", None),
            "summary": getattr(e, "summary", None),
            "raw_meta": {"rss_url": url},
        })
    return items

async def fetch_first_ok(client: httpx.AsyncClient, urls: list[str], retries: int = 2) -> Optional[httpx.Response]:
    """
    Intenta varias URLs y hace reintentos breves. Devuelve la primera 200 OK.
    """
    for attempt in range(retries + 1):
        last_err = None
        for u in urls:
            try:
                r = await client.get(u)
                if r.status_code == 200:
                    return r
                last_err = f"HTTP {r.status_code} for {u}"
            except Exception as e:
                last_err = f"{type(e).__name__}: {e} for {u}"
        if attempt < retries:
            await asyncio.sleep(1.2 * (attempt + 1))
    if last_err:
        log(f"HTML: error {last_err}")
    return None

def want_link_for_domain(abs_url: str, page_domain: str) -> bool:
    """
    Heurística por dominio para reducir ruido.
    - data.gov.qa (CKAN): quedarse con dataset/explore/download y PDFs
    - hukoomi: quedarnos con policies, strategy y PDFs
    - por defecto: keywords suaves
    """
    u = abs_url.lower()
    if "data.gov.qa" in page_domain:
        if is_pdf_url(abs_url):
            return True
        if any(s in u for s in ["/explore/", "/dataset/", "/download", "/api/"]):
            return True
        # Evitar páginas estáticas generalistas
        if "/pages/" in u and not is_pdf_url(abs_url):
            return False
        return False
    if "hukoomi.gov.qa" in page_domain:
        if is_pdf_url(abs_url):
            return True
        if any(k in u for k in ["policy", "policies", "strategy", "strategies", "data", "ai", "cyber", "security", "privacy"]):
            return True
        return False
    # genérico
    if is_pdf_url(abs_url):
        return True
    return any(k in u for k in ["policy", "report", "document", "data", "ai", "cyber", "privacy", "security"])

async def collect_html_async(client: httpx.AsyncClient, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    src = (source.get("source_url") or "").strip()
    if not looks_like_http(src):
        src = "https://" + src.lstrip("/")

    urls = candidate_urls(src)
    log(f"HTML: leyendo {urls[0]} (con fallbacks)")

    r = await fetch_first_ok(client, urls, retries=2)
    if r is None:
        return []

    try:
        tree = HTMLParser(r.text)
    except Exception as e:
        log(f"HTML: parse error {e}")
        return []

    page_domain = domain_of(str(r.url))
    anchors = tree.css("a")
    docs: List[Dict[str, Any]] = []
    seen = set()

    for a in anchors:
        href = (a.attributes.get("href") or "").strip()
        if not href:
            continue
        # ignora anchors locales y esquemas no deseados
        if href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue

        abs_url = urljoin(str(r.url), href)
        if not looks_like_http(abs_url):
            continue
        if abs_url in seen:
            continue
        seen.add(abs_url)

        # filtro de contenido por dominio
        if not want_link_for_domain(abs_url, page_domain):
            continue

        title = a.text(strip=True) or abs_url
        item = {
            "country": normalize_country(source.get("country")),
            "authority": source.get("authority"),
            "source_url": source.get("source_url"),
            "ingest_source_type": "html:pdf-link" if is_pdf_url(abs_url) else "html",
            "title": title[:500],
            "doc_url": abs_url,
            "published_at": None,
            "summary": None,
            "raw_meta": {"from_page": str(r.url)},
        }

        docs.append(item)
        if len(docs) >= 50:
            break

    return docs

def collect_pdf_placeholder(source: Dict[str, Any]) -> List[Dict[str, Any]]:
    url = (source.get("source_url") or "").strip()
    if not looks_like_http(url):
        return []
    return [{
        "country": normalize_country(source.get("country")),
        "authority": source.get("authority"),
        "source_url": source.get("source_url"),
        "_ingest_source_type": "pdf",
        "title": f"PDF from {url}",
        "doc_url": url,
        "published_at": None,
        "summary": None,
        "raw_meta": {},
    }]

# =========================
# Persistencia (placeholder)
# =========================
def save_items(supabase: Client, items: List[Dict[str, Any]]):
    if not items:
        return
    if not WRITE_TO_DB:
        log(f"→ {len(items)} item(s) (simulación)")
        for it in items[:5]:
            log(f"  - {it['authority']} | {it['title'][:80]} | {it['doc_url']}")
        if len(items) > 5:
            log(f"  ... y {len(items)-5} más")
        return
    try:
        resp = supabase.table("ingest_items").insert(items).execute()
        log(f"Guardados: {len(resp.data or [])}")
    except Exception as e:
        log(f"❌ Error guardando en BD: {e}")

# =========================
# Main
# =========================
async def run_pipeline(limit: int, only: Optional[str], country: Optional[str]):
    supabase = get_supabase()
    sources = fetch_active_sources(supabase, limit=limit, only=only, country=country)
    if not sources:
        log("No hay fuentes activas que coincidan con el filtro.")
        return

    log(f"Fuentes a procesar: {len(sources)}")
    all_items: List[Dict[str, Any]] = []

    # 1) RSS
    if not only or only == "rss":
        for s in sources:
            try:
                if s.get("has_rss"):
                    items = collect_rss(s)
                    all_items.extend(items)
            except Exception as e:
                log(f"RSS error en {s.get('authority')}: {type(e).__name__}: {e}")

    # 2) HTML
    if not only or only == "html":
        async with httpx.AsyncClient(
            follow_redirects=True,
            headers={"User-Agent": "reg-radar-mvp/0.2 (+https://example.com)"},
            timeout=httpx.Timeout(40.0, connect=15.0)
        ) as client:
            for s in sources:
                fmt = (s.get("format") or "").upper()
                if "HTML" in fmt:
                    try:
                        items = await collect_html_async(client, s)
                        all_items.extend(items)
                    except Exception as e:
                        log(f"HTML error en {s.get('authority')}: {type(e).__name__}: {e}")

    # 3) PDF directo (placeholder)
    if not only or only == "pdf":
        for s in sources:
            fmt = (s.get("format") or "").upper()
            if "PDF" in fmt and "HTML" not in fmt and not s.get("has_rss"):
                try:
                    items = collect_pdf_placeholder(s)
                    all_items.extend(items)
                except Exception as e:
                    log(f"PDF error en {s.get('authority')}: {type(e).__name__}: {e}")

    save_items(supabase, all_items)

def parse_args():
    p = argparse.ArgumentParser(description="GCC Policy & Regulatory Radar - pipeline MVP")
    p.add_argument("--limit", type=int, default=SOURCE_LIMIT_DEFAULT, help="máximo de fuentes a leer (por prioridad)")
    p.add_argument("--only", type=str, choices=["rss","html","pdf"], default=None, help="filtrar tipo de colector")
    p.add_argument("--country", type=str, choices=["UAE","KSA","Qatar"], default=None, help="filtrar por país")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    try:
        import anyio
        anyio.run(run_pipeline, args.limit, args.only, args.country)
    except ModuleNotFoundError:
        import asyncio
        asyncio.run(run_pipeline(args.limit, args.only, args.country))
