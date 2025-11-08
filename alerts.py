import os
import sys
import argparse
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
import httpx
from supabase import create_client, Client

# -------------------------
# Config & helpers
# -------------------------
load_dotenv()

REQUIRED_ENV = [
    "SUPABASE_URL",
    "SUPABASE_KEY",
    "MAILGUN_API_KEY",
    "MAILGUN_DOMAIN",
    "ALERT_TO_EMAIL",
]

def require_env():
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Faltan variables de entorno: {', '.join(missing)}")

def sb() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    return create_client(url, key)

def utcnow():
    return datetime.now(timezone.utc)

def fmt_dt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")

# -------------------------
# Checks
# -------------------------
def check_pending_enrich(supa: Client) -> int:
    # cuantos ingest_items quedan por enriquecer
    r = supa.table("ingest_items").select("id", count="exact").is_("enriched_at", "null").execute()
    return r.count or 0

def check_failed_runs(supa: Client, since_hours: int = 48) -> list[dict]:
    since = utcnow() - timedelta(hours=since_hours)
    # runs con fallos recientes
    r = (
        supa.table("runs_log")
        .select("run_type, started_at, finished_at, ok_count, fail_count, notes")
        .gte("started_at", since.isoformat())
        .gt("fail_count", 0)
        .order("started_at", desc=True)
        .execute()
    )
    return r.data or []

def check_silent_sources(supa: Client, min_silence_hours: int = 72) -> list[dict]:
    """
    Fuentes (authority, source_url) sin nuevos items desde hace > min_silence_hours.
    Calculamos MAX(created_at) en ingest_items por authority/source_url.
    """
    cutoff = utcnow() - timedelta(hours=min_silence_hours)

    # Aggregation con PostgREST: select=authority,source_url,max_created_at:created_at.max()
    r = (
        supa.table("ingest_items")
        .select("authority,source_url,max_created_at:created_at.max()")
        .group("authority,source_url")
        .execute()
    )
    rows = r.data or []

    silent = []
    for row in rows:
        max_created_at = row.get("max_created_at")
        # Si la fuente jamás ingresó items, no aparecerá aquí; para cobertura total,
        # podríamos LEFT JOIN con coverage, pero esto basta para alertar inactividad real.
        if not max_created_at:
            continue
        try:
            last_dt = datetime.fromisoformat(max_created_at.replace("Z", "+00:00"))
        except Exception:
            continue
        if last_dt < cutoff:
            silent.append(
                {
                    "authority": row.get("authority"),
                    "source_url": row.get("source_url"),
                    "last_item_at": last_dt.isoformat(),
                    "hours_since": round((utcnow() - last_dt).total_seconds() / 3600, 1),
                }
            )
    return silent

# -------------------------
# Mailgun
# -------------------------
def send_mail(subject: str, text: str, html: str | None = None, dry_run: bool = False):
    to_email = os.getenv("ALERT_TO_EMAIL")
    domain = os.getenv("MAILGUN_DOMAIN")
    api_key = os.getenv("MAILGUN_API_KEY")

    if dry_run:
        print("\n--- DRY RUN (no se envía correo) ---")
        print("To:", to_email)
        print("Subject:", subject)
        print(text)
        return

    url = f"https://api.mailgun.net/v3/{domain}/messages"
    data = {
        "from": f"GCC Radar Alerts <alerts@{domain}>",
        "to": [to_email],
        "subject": subject,
        "text": text,
    }
    if html:
        data["html"] = html

    with httpx.Client(timeout=20.0) as client:
        resp = client.post(url, auth=("api", api_key), data=data)
        resp.raise_for_status()

# -------------------------
# Main
# -------------------------
def main():
    parser = argparse.ArgumentParser(description="Alertas de salud del pipeline")
    parser.add_argument("--min-silence-hours", type=int, default=72, help="Umbral de inactividad por fuente")
    parser.add_argument("--since-hours", type=int, default=48, help="Ventana para buscar runs fallidos")
    parser.add_argument("--dry-run", action="store_true", help="No envía email, solo imprime")
    args = parser.parse_args()

    require_env()
    supa = sb()

    pending = check_pending_enrich(supa)
    failed = check_failed_runs(supa, since_hours=args.since_hours)
    silent = check_silent_sources(supa, min_silence_hours=args.min_silence_hours)

    # Construir mensaje
    now = fmt_dt(utcnow())
    lines = [
        f"GCC Policy & Regulatory Radar — Health Check",
        f"Timestamp (UTC): {now}",
        "",
        f"1) Pending to enrich: {pending}",
        f"2) Failed runs (last {args.since_hours}h): {len(failed)}",
    ]
    if failed:
        lines.append("   - Recent failures:")
        for f in failed[:10]:
            lines.append(
                f"     • {f['run_type']} | started {f['started_at']} | ok={f['ok_count']} fail={f['fail_count']} | notes={f.get('notes')}"
            )
    lines.append(f"3) Silent sources (> {args.min_silence_hours}h): {len(silent)}")
    if silent:
        lines.append("   - Sources:")
        for s in silent[:25]:
            lines.append(
                f"     • {s['authority']} | {s['source_url']} | last={s['last_item_at']} | ~{s['hours_since']}h"
            )

    body = "\n".join(lines)
    subject = f"[GCC Radar] Health: pending={pending} | fails={len(failed)} | silent={len(silent)}"

    send_mail(subject=subject, text=body, html=None, dry_run=args.dry_run)
    print("OK - alerta procesada.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)
