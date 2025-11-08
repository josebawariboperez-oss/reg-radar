# run_dump.py
import argparse
import asyncio
import csv
from datetime import datetime
from pathlib import Path

import pipeline
import httpx

# Parámetros por defecto
DEFAULT_LIMIT = 5
DEFAULT_ONLY = "html"  # opciones: rss | html | pdf
DEFAULT_COUNTRY = None  # opciones: UAE | KSA | Qatar
DEFAULT_OUT = None  # si None, genera nombre con timestamp

def ts():
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def build_out_path(out_arg: str | None) -> Path:
    if out_arg:
        return Path(out_arg)
    return Path(f"items_dump_{ts()}_excel.csv")

def clean_field(val):
    if val is None:
        return ""
    # Quita saltos de línea y comprime espacios
    s = " ".join(str(val).replace("\r", " ").replace("\n", " ").split())
    return s

async def collect_items(limit: int, only: str | None, country: str | None):
    # Aseguramos que el pipeline NO escriba en BD
    pipeline.WRITE_TO_DB = False

    supa = pipeline.get_supabase()
    sources = pipeline.fetch_active_sources(supa, limit=limit, only=only, country=country)
    if not sources:
        print("[run_dump] No hay fuentes activas que coincidan con el filtro.")
        return []

    items = []

    async with httpx.AsyncClient(
        follow_redirects=True,
        headers={"User-Agent": "reg-radar-dump/0.2 (+https://example.com)"},
        timeout=httpx.Timeout(40.0, connect=15.0)
    ) as client:
        # RSS
        if not only or only == "rss":
            for s in sources:
                if s.get("has_rss"):
                    try:
                        items.extend(pipeline.collect_rss(s))
                    except Exception as e:
                        print(f"[run_dump] RSS error en {s.get('authority')}: {e}")

        # HTML
        if not only or only == "html":
            for s in sources:
                fmt = (s.get("format") or "").upper()
                if "HTML" in fmt:
                    try:
                        items.extend(await pipeline.collect_html_async(client, s))
                    except Exception as e:
                        print(f"[run_dump] HTML error en {s.get('authority')}: {e}")

        # PDF directo (placeholder del pipeline)
        if not only or only == "pdf":
            for s in sources:
                fmt = (s.get("format") or "").upper()
                if "PDF" in fmt and "HTML" not in fmt and not s.get("has_rss"):
                    try:
                        items.extend(pipeline.collect_pdf_placeholder(s))
                    except Exception as e:
                        print(f"[run_dump] PDF error en {s.get('authority')}: {e}")

    return items

def write_csv(items, out_path: Path):
    cols = [
        "country",
        "authority",
        "ingest_source_type",
        "title",
        "doc_url",
        "source_url",
        "published_at",
    ]
    out_path = out_path.resolve()
    with out_path.open("w", encoding="utf-8", newline="") as f:
        # Delimitador ; para Excel en ES
        w = csv.DictWriter(f, fieldnames=cols, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for it in items:
            row = {k: clean_field(it.get(k, "")) for k in cols}
            w.writerow(row)
    return out_path

async def main():
    parser = argparse.ArgumentParser(description="Volcado local del pipeline a CSV (formato Excel-friendly; sin BD)")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="máximo de fuentes a leer")
    parser.add_argument("--only", type=str, choices=["rss", "html", "pdf"], default=DEFAULT_ONLY, help="filtrar colector")
    parser.add_argument("--country", type=str, choices=["UAE", "KSA", "Qatar"], default=DEFAULT_COUNTRY, help="filtrar por país")
    parser.add_argument("--out", type=str, default=DEFAULT_OUT, help="ruta del CSV de salida")
    args = parser.parse_args()

    items = await collect_items(args.limit, args.only, args.country)
    print(f"[run_dump] Ítems recogidos: {len(items)}")
    out_path = write_csv(items, build_out_path(args.out))
    print(f"[run_dump] CSV generado: {out_path}")

if __name__ == "__main__":
    asyncio.run(main())
