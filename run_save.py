import re
from supabase import create_client
from postgrest.exceptions import APIError

WWW_RE = re.compile(r"^https?://(www\.)?", re.I)

def normalize_url(u: str) -> str:
    if not u:
        return u
    # Fuerza https y elimina www.
    u = WWW_RE.sub("https://", u.strip())
    return u.rstrip("/")

def save_items(supa, items: list[dict]):
    rows = []
    for it in items:
        doc = normalize_url(it.get("doc_url") or "")
        src = normalize_url(it.get("source_url") or "")
        if not doc:
            continue
        rows.append({
            "country": it.get("country"),
            "authority": it.get("authority"),
            "title": it.get("title") or "",
            "doc_url": doc,
            "source_url": src,
            "ingest_source_type": it.get("ingest_source_type") or "html",
            "created_at": it.get("created_at"),  # opcional
        })

    if not rows:
        return 0

    # ✅ Upsert por doc_url (idempotente)
    data = supa.table("ingest_items").upsert(rows, on_conflict="doc_url").execute().data
    return len(data or [])
