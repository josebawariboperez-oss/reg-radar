# alerts.py
import os
import sys
import argparse
from datetime import datetime, timezone, timedelta
from collections import OrderedDict

from dotenv import load_dotenv
import httpx
from supabase import create_client, Client

load_dotenv()

REQUIRED_ENV = [
    "SUPABASE_URL",
    "SUPABASE_KEY",
    "MAILGUN_API_KEY",
    "MAILGUN_DOMAIN",
    "ALERT_TO_EMAIL",
]

# -------------------------
# Utils
# -------------------------
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

def parse_overrides(s: str | None) -> dict[str, int]:
    """'PSA=48,Open Data Portal=96' -> {'PSA':48, 'Open Data Portal':96}"""
    if not s:
        return {}
    out: dict[str, int] = {}
    for p in [x.strip() for x in s.split(",") if x.strip()]:
        if "=" not in p:
            continue
        k, v = p.split("=", 1)
        try:
            out[k.strip()] = int(v.strip())
        except ValueError:
            pass
    return out

# -------------------------
# Checks
# -------------------------
def check_pending_enrich(supa: Client, country: str | None) -> int:
    q = supa.table("ingest_items").select("id", count="exact").is_("enriched_at", "null")
    if country:
        q = q.eq("country", country)
    res = q.execute()
    return res.count or 0

def check_failed_runs(supa: Client, since_hours: int = 48) -> list[dict]:
    since = utcnow() - timedelta(hours=since_hours)
    res = (
        supa.table("runs_log")
        .select("run_type, started_at, finished_at, ok_count, fail_count, notes")
        .gte("started_at", since.isoformat())
        .gt("fail_count", 0)
        .order("started_at", desc=True)
        .execute()
    )
    return res.data or []

def check_silent_sources(
    supa: Client,
    min_silence_hours_default: int,
    silence_overrides: dict[str, int],
    country: str | None,
    max_rows: int = 50000,
) -> list[dict]:
    """
    Calcula el último item por (country, authority, source_url) ordenando por created_at DESC
    y tomando la primera aparición.
    """
    q = (
        supa.table("ingest_items")
        .select("country,authority,source_url,created_at")
        .order("authority")
        .order("source_url")
        .order("created_at", desc=True)
        .limit(max_rows)
    )
    if country:
        q = q.eq("country", country)
    rows = q.execute().data or []

    latest_by_source: "OrderedDict[tuple[str,str,str], datetime]" = OrderedDict()
    for r in rows:
        key = (r.get("country"), r.get("authority"), r.get("source_url"))
        if not key[0] or not key[1] or not key[2]:
            continue
        if key in latest_by_source:
            continue
        ts = r.get("created_at")
        if not ts:
            continue
        try:
            last_dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            continue
        latest_by_source[key] = last_dt

    now = utcnow()
    silent = []
    for (ctry, authority, src_url), last_dt in latest_by_source.items():
        thr = silence_overrides.get(authority, min_silence_hours_default)
        if last_dt < (now - timedelta(hours=thr)):
            silent.append(
                {
                    "country": ctry,
                    "authority": authority,
                    "source_url": src_url,
                    "last_item_at": last_dt.isoformat(),
                    "hours_since": round((now - last_dt).total_seconds() / 3600, 1),
                    "threshold_h": thr,
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

    # Región US/EU (por defecto US). Añade MAILGUN_REGION=EU en secrets si tu dominio es europeo.
    region = (os.getenv("MAILGUN_REGION") or "US").upper()
    base = "https://api.mailgun.net/v3" if region == "US" else "https://api.eu.mailgun.net/v3"
    url = f"{base}/{domain}/messages"

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
    parser.add_argument("--country", type=str, default=None, help="Filtro por país (Qatar/KSA/UAE)")
    parser.add_argument("--min-silence-hours", type=int, default=72, help="Umbral global de inactividad por fuente")
    parser.add_argument(
        "--silence-overrides",
        type=str,
        default=None,
        help="Overrides por autoridad, ej.: 'PSA=48,Open Data Portal=96'",
    )
    parser.add_argument("--since-hours", type=int, default=48, help="Ventana para runs fallidos (global)")
    parser.add_argument("--only-if-issues", action="store_true", help="Enviar email solo si hay problemas")
    parser.add_argument("--dry-run", action="store_true", help="No envía email; imprime el mensaje")
    args = parser.parse_args()

    require_env()
    supa = sb()

    # Registrar la ejecución en runs_log
    run_row = (
        supa.table("runs_log")
        .insert({"run_type": "alert", "ok_count": 0, "fail_count": 0, "notes": "started"})
        .execute()
        .data[0]
    )
    run_id = run_row["id"]

    try:
        overrides = parse_overrides(args.silence_overrides)

        pending = check_pending_enrich(supa, args.country)
        failed = check_failed_runs(supa, since_hours=args.since_hours)
        silent = check_silent_sources(
            supa,
            min_silence_hours_default=args.min_silence_hours,
            silence_overrides=overrides,
            country=args.country,
        )

        now = fmt_dt(utcnow())
        header = [f"GCC Policy & Regulatory Radar — Health Check", f"Timestamp (UTC): {now}"]
        if args.country:
            header.append(f"Country filter: {args.country}")
        header.append("")

        lines = header + [
            f"1) Pending to enrich: {pending}",
            f"2) Failed runs (last {args.since_hours}h): {len(failed)}",
            f"3) Silent sources (> {args.min_silence_hours}h; overrides: "
            + (", ".join([f"{k}={v}" for k, v in overrides.items()]) or "none")
            + f"): {len(silent)}",
        ]
        if failed:
            lines.append("   - Recent failures:")
            for f in failed[:10]:
                lines.append(
                    f"     • {f['run_type']} | started {f['started_at']} | ok={f['ok_count']} fail={f['fail_count']} | notes={f.get('notes')}"
                )
        if silent:
            lines.append("   - Sources:")
            for s in silent[:25]:
                tag = f"[{s['country']}] " if s.get("country") else ""
                lines.append(
                    f"     • {tag}{s['authority']} | {s['source_url']} | last={s['last_item_at']} | ~{s['hours_since']}h (thr={s['threshold_h']}h)"
                )

        body = "\n".join(lines)
        subject = (
            f"[GCC Radar] Health"
            + (f" [{args.country}]" if args.country else "")
            + f": pending={pending} | fails={len(failed)} | silent={len(silent)}"
        )

        # Solo enviar si hay issues
        if args.only_if_issues and (len(silent) == 0 and len(failed) == 0):
            notes = "only-if-issues: no problems -> no email"
            print(notes)
            supa.table("runs_log").update(
                {"ok_count": 0, "fail_count": 0, "notes": notes, "finished_at": utcnow().isoformat()}
            ).eq("id", run_id).execute()
            return

        send_mail(subject=subject, text=body, html=None, dry_run=args.dry_run)
        print("OK - alerta procesada.")

        supa.table("runs_log").update(
            {"ok_count": 1, "fail_count": 0, "notes": None, "finished_at": utcnow().isoformat()}
        ).eq("id", run_id).execute()

    except Exception as e:
        print("ERROR:", e)
        supa.table("runs_log").update(
            {"ok_count": 0, "fail_count": 1, "notes": str(e), "finished_at": utcnow().isoformat()}
        ).eq("id", run_id).execute()
        sys.exit(1)

if __name__ == "__main__":
    main()
