# enrich.py — GCC Policy & Regulatory Radar
# HTML/PDF extraction, EN-only topics, Postgres-safe text, URL normalization, upsert by doc_url, run logging.

import os, re, json, argparse, unicodedata
from datetime import datetime, timezone
from typing import Optional, List
from urllib.parse import urlsplit, urlunsplit, quote

import httpx
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, ValidationError
from supabase import create_client, Client
from openai import OpenAI
from dotenv import load_dotenv

# PDF opcional (PyMuPDF)
try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

# ==========================
# Configuración
# ==========================
load_dotenv()

if not os.getenv("SUPABASE_URL") or not os.getenv("SUPABASE_KEY"):
    raise RuntimeError("Faltan SUPABASE_URL / SUPABASE_KEY en .env")
if not os.getenv("OPENAI_API_KEY"):
    raise RuntimeError("Falta OPENAI_API_KEY en .env")

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_MODEL_FALLBACK = os.getenv("OPENAI_MODEL_FALLBACK", OPENAI_MODEL)
USER_AGENT = "gcc-policy-radar/1.0 (+contact-email)"
HTTP_TIMEOUT = 45.0
MAX_TEXT_CHARS = 12000  # recorte prudente para LLM

# ==========================
# Utilidades generales
# ==========================
CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def get_supabase() -> Client:
    return create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

def clean_text(txt: str) -> str:
    if not txt:
        return ""
    return re.sub(r"\s+", " ", txt.replace("\r", " ").replace("\n", " ")).strip()

def sanitize_pg_text(s: str | None) -> str | None:
    """Elimina bytes nulos y controles no válidos para Postgres."""
    if s is None:
        return None
    s = s.replace("\r", " ")
    s = CONTROL_CHARS_RE.sub(" ", s)
    return s.strip()

def safe_url(u: str) -> str:
    """Porcentúa path/query si hay espacios u otros caracteres problemáticos."""
    if not u:
        return u
    sp = urlsplit(u)
    path = quote(sp.path, safe="/:%")
    query = quote(sp.query, safe="=&:%/?")
    return urlunsplit((sp.scheme, sp.netloc, path, query, sp.fragment))

# ==========================
# Fetchers (HTML y PDF)
# ==========================
def fetch_html_text(url: str) -> str:
    with httpx.Client(
        headers={"User-Agent": USER_AGENT},
        timeout=HTTP_TIMEOUT,
        follow_redirects=True,
    ) as client:
        r = client.get(url)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "form"]):
            tag.decompose()
        return clean_text(soup.get_text(" "))

def fetch_pdf_text(url: str) -> str:
    if fitz is None:
        return ""
    with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=HTTP_TIMEOUT, follow_redirects=True) as client:
        r = client.get(url)
        r.raise_for_status()
        data = r.content
    text = ""
    try:
        with fitz.open(stream=data, filetype="pdf") as doc:
            for page in doc:
                text += page.get_text() + "\n"
    except Exception:
        text = ""
    return clean_text(text)

# ==========================
# Esquema de salida (Pydantic v2)
# ==========================
class KeyDates(BaseModel):
    comment_deadline: Optional[str] = None  # YYYY-MM-DD
    effective: Optional[str] = None
    expiry: Optional[str] = None
    publication: Optional[str] = None

class EnrichOut(BaseModel):
    summary_es: str = Field(..., min_length=10)
    summary_en: str = Field(..., min_length=10)
    topics: List[str] = Field(..., min_length=1)  # 3–6 tags
    key_dates: KeyDates = Field(default_factory=KeyDates)
    status: str
    type: str
    impact_level: str

# ==========================
# Topics EN-only (detección/normalización)
# ==========================
SPANISH_MARKERS = [
    "politica", "política", "políticas", "regulacion", "regulación", "regulaciones",
    "datos", "gobierno", "desarrollo", "innovacion", "innovación",
    "estadistica", "estadísticas", "sostenibilidad", "administracion", "administración",
    "cuestionarios", "ods", "economica", "económica", "diversificacion", "diversificación",
    "transparencia", "abiertos", "gobernanza"
]

def _ascii_lower(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").strip().lower()

def looks_spanish_topic(topic: str) -> bool:
    t = _ascii_lower(topic)
    return any(m in t for m in SPANISH_MARKERS)

def ensure_topics_english(oai: OpenAI, topics: list[str]) -> list[str]:
    """Si parece español, pasa una traducción/normalización a EN."""
    if not topics:
        return topics
    if not any(looks_spanish_topic(t) for t in topics):
        out, seen = [], set()
        for t in (_ascii_lower(x) for x in topics):
            if t and t not in seen:
                seen.add(t); out.append(t)
        return out

    prompt = (
        "Translate and normalize the following topic tags to ENGLISH only. "
        "Return JSON with key 'topics' as an array of 3–6 concise, lowercase tags "
        "using public policy vocabulary (e.g., 'data policy','open data','digital identity','cybersecurity').\n\n"
        f"topics: {json.dumps(topics, ensure_ascii=False)}"
    )
    resp = oai.chat.completions.create(
        model=OPENAI_MODEL_FALLBACK,
        temperature=0.0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "Return ONLY JSON with an array 'topics' in ENGLISH."},
            {"role": "user", "content": prompt},
        ],
    )
    try:
        fixed = json.loads(resp.choices[0].message.content).get("topics", topics)
        out, seen = [], set()
        for t in (_ascii_lower(x) for x in fixed):
            if t and t not in seen:
                seen.add(t); out.append(t)
        return out or [_ascii_lower(x) for x in topics]
    except Exception:
        return [_ascii_lower(x) for x in topics]

# ==========================
# Prompt y llamada a OpenAI
# ==========================
def build_prompt(title: str, url: str, text: str) -> str:
    return f"""
You are a public policy analyst. Return ONLY a valid JSON object with these exact keys:
- summary_es: short paragraph in Spanish.
- summary_en: short paragraph in English.
- topics: an array (3–6) of concise THEMATIC tags in ENGLISH ONLY (e.g., ["data policy","AI","cybersecurity"]).
- key_dates: object with keys comment_deadline, effective, expiry, publication (YYYY-MM-DD or null).
- status: one of ["draft","final","consultation"].
- type: one of ["policy","regulation","strategy","guideline"].
- impact_level: one of ["low","medium","high"].

STRICT RULES:
- The "topics" array MUST be entirely in English (no Spanish, no Arabic, no mixed language).
- Use concise, lower-case multiword tags where helpful ("open data", "digital identity", "AI ethics").
- Do not add extra keys or text outside the JSON.

Context:
Title: {title}
URL: {url}

Truncated content:
{text}
"""

def call_openai(oai: OpenAI, title: str, url: str, text: str) -> EnrichOut:
    prompt = build_prompt(title, url, text)
    resp = oai.chat.completions.create(
        model=OPENAI_MODEL,
        temperature=0.2,
        response_format={"type": "json_object"},
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a precise policy analyst. "
                    "Always produce STRICT JSON only. "
                    "The 'topics' array MUST be entirely in ENGLISH."
                ),
            },
            {"role": "user", "content": prompt},
        ],
    )
    raw = resp.choices[0].message.content
    data = EnrichOut.model_validate_json(raw)

    # Normalización base + blindaje EN
    base, seen = [], set()
    for t in data.topics:
        tt = _ascii_lower(t)
        if tt and tt not in seen:
            seen.add(tt); base.append(tt)
    data.topics = ensure_topics_english(oai, base)
    return data

# ==========================
# Acceso a Supabase
# ==========================
def get_pending_items(supa: Client, limit: int):
    res = (
        supa.table("ingest_items")
        .select("*")
        .is_("enriched_at", None)
        .order("created_at", desc=False)
        .limit(limit)
        .execute()
    )
    return res.data or []

def upsert_regulation(supa: Client, row: dict):
    return (
        supa.table("regulations")
        .upsert(row, on_conflict="doc_url")
        .execute()
        .data
    )

def mark_enriched(supa: Client, item_id: str, regulation_id: Optional[str]):
    supa.table("ingest_items").update(
        {"enriched_at": utcnow_iso(), "regulation_id": regulation_id}
    ).eq("id", item_id).execute()

# ==========================
# Main
# ==========================
def main():
    parser = argparse.ArgumentParser(description="Enriquecimiento -> regulations (topics EN + HTML/PDF + sanitizers)")
    parser.add_argument("--limit", type=int, default=5, help="número de items a procesar")
    args = parser.parse_args()

    supa = get_supabase()
    oai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    # Registrar inicio de corrida (si la tabla existe)
    run_id = None
    try:
        run_row = supa.table("runs_log").insert({"run_type": "enrich"}).execute().data[0]
        run_id = run_row["id"]
    except Exception:
        run_id = None  # seguimos sin logging si no existe

    try:
        items = get_pending_items(supa, args.limit)
        if not items:
            print("[enrich] No hay items pendientes.")
            if run_id:
                supa.table("runs_log").update({
                    "finished_at": utcnow_iso(),
                    "ok_count": 0,
                    "fail_count": 0,
                    "notes": "empty-queue"
                }).eq("id", run_id).execute()
            return

        ok, fail = 0, 0
        for it in items:
            try:
                title = it.get("title") or ""
                # Tolerante con origen de URL
                url = it.get("doc_url") or it.get("url") or it.get("source_url") or ""
                if not url:
                    raise RuntimeError("Item sin URL utilizable.")
                url = safe_url(url)

                # Extracción HTML/PDF
                raw_text = ""
                try:
                    if url.lower().endswith(".pdf"):
                        raw_text = fetch_pdf_text(url)
                    else:
                        raw_text = fetch_html_text(url)
                except Exception:
                    raw_text = ""

                text_for_llm = (raw_text or "")[:MAX_TEXT_CHARS]
                data = call_openai(oai, title, url, text_for_llm)

                # Sanitizar textos para Postgres
                title_s      = sanitize_pg_text(title)
                raw_text_s   = sanitize_pg_text(raw_text)
                summary_es_s = sanitize_pg_text(data.summary_es)
                summary_en_s = sanitize_pg_text(data.summary_en)

                reg_row = {
                    "doc_url": url,
                    "url": url,  # NOT NULL
                    "source_url": safe_url(it.get("source_url") or ""),
                    "country": it.get("country"),
                    "authority": it.get("authority"),
                    "title": title_s,
                    "ingest_item_id": it.get("id"),

                    "summary_es": summary_es_s,
                    "summary_en": summary_en_s,
                    "topics": json.loads(json.dumps(data.topics)),
                    "key_dates": json.loads(data.key_dates.model_dump_json()),
                    "status": data.status,
                    "type": data.type,
                    "impact_level": data.impact_level,
                    "raw_text": raw_text_s,
                    "openai_model": OPENAI_MODEL,
                }

                up = upsert_regulation(supa, reg_row)
                reg_id = (up or [{}])[0].get("id")
                mark_enriched(supa, it["id"], reg_id)

                ok += 1
                print(f"[enrich] OK -> {it.get('authority')} | {title[:90]}")
            except ValidationError as ve:
                fail += 1
                print(f"[enrich] JSON inválido en {it.get('doc_url') or it.get('url')}: {ve}")
            except httpx.HTTPError as he:
                fail += 1
                print(f"[enrich] HTTP error en {it.get('doc_url') or it.get('url')}: {he}")
            except Exception as e:
                fail += 1
                print(f"[enrich] ERROR en {it.get('doc_url') or it.get('url')}: {e}")

        print(f"[enrich] Terminado. OK={ok} FAIL={fail}")
        if run_id:
            supa.table("runs_log").update({
                "finished_at": utcnow_iso(),
                "ok_count": ok,
                "fail_count": fail
            }).eq("id", run_id).execute()

    except Exception as e:
        if run_id:
            supa.table("runs_log").update({
                "finished_at": utcnow_iso(),
                "ok_count": 0,
                "fail_count": 0,
                "notes": f"fatal: {type(e).__name__}: {e}"
            }).eq("id", run_id).execute()
        raise

if __name__ == "__main__":
    main()


