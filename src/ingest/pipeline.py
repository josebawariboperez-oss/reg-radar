# -*- coding: utf-8 -*-
import argparse
from src.ingest.parsers.rss_generic import run

def main():
    p = argparse.ArgumentParser(description='Ingesta desde RSS')
    p.add_argument('--rss', required=True, help='URL del feed RSS/Atom')
    p.add_argument('--country', required=True, choices=['UAE','KSA','Qatar'])
    p.add_argument('--authority', required=True, help='Nombre de la autoridad (ej. SDAIA, CBUAE, MCIT Qatar)')
    p.add_argument('--doc-type', default='Guideline')
    p.add_argument('--status', default='Issued')
    p.add_argument('--topics', default='data,ai', help='Lista separada por comas')
    args = p.parse_args()

    topics = [t.strip() for t in args.topics.split(',') if t.strip()]

    inserted = run(
        rss_url=args.rss,
        country=args.country,
        authority=args.authority,
        default_doc_type=args['doc-type'] if hasattr(args,'doc-type') else args.doc_type,
        default_status=args.status,
        default_topics=topics
    )
    print(f'Upsert completado. Filas afectadas: {inserted}')

if __name__ == '__main__':
    main()
