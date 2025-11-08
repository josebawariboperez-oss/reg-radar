# -*- coding: utf-8 -*-
from dotenv import load_dotenv
from supabase import create_client
import os
from datetime import date

def main():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    assert url and key, "Faltan SUPABASE_URL o SUPABASE_KEY en .env"

    supabase = create_client(url, key)

    print("Listando últimas 5 regulations...")
    res = supabase.table("regulations").select("*").order("created_at", desc=True).limit(5).execute()
    print(f"Filas obtenidas: {len(res.data)}")

    print("Insertando fila de prueba...")
    row = {
        "country": "UAE",
        "authority": "SDAIA",
        "title": "Test Document - Connectivity Check",
        "url": "https://example.com/doc",
        "doc_type": "Guideline",
        "status": "Issued",
        "topics": ["data","ai"],
        "publish_date": date.today().isoformat(),
        "summary": "Fila de prueba para validar la conexión Supabase desde Python.",
        "raw_text": "Texto de ejemplo",
        "impact_level": 2,
        "source": "seed-script"
    }
    ins = supabase.table("regulations").insert(row).execute()
    print("Insert OK:", ins.data[0]["id"])

    back = supabase.table("regulations").select("id,country,authority,title,publish_date,text_len") \
        .eq("title", row["title"]).limit(1).execute()
    print("Leído:", back.data)

if __name__ == "__main__":
    main()
