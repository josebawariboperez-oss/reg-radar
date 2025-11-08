# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import List, Dict, Any, Optional
import feedparser
import dateparser
from src.common.supa import get_client
from src.common.types import Regulation

def parse_rss(rss_url: str,
              country: str,
              authority: str,
              default_doc_type: str = 'Guideline',
              default_status: str = 'Issued',
              default_topics: Optional[list[str]] = None) -> List[Dict[str, Any]]:
    feed = feedparser.parse(rss_url)
    items: List[Dict[str, Any]] = []
    for entry in getattr(feed, 'entries', []):
        title = (entry.get('title') or '').strip()
        link = (entry.get('link') or '').strip()
        summary = (entry.get('summary') or entry.get('description') or '').strip()

        published_str = entry.get('published') or entry.get('updated')
        publish_date = None
        if published_str:
            dt = dateparser.parse(published_str)
            if dt:
                publish_date = dt.date().isoformat()

        reg = Regulation(
            country=country,
            authority=authority,
            title=title or 'Untitled',
            url=link or 'https://example.com/empty',
            doc_type=default_doc_type,
            status=default_status,
            topics=default_topics or [],
            publish_date=publish_date,
            summary=summary[:8000] if summary else None,
            raw_text=None,
            impact_level=None,
            source=f'rss:{authority}',
        )
        items.append(reg.model_dump())
    return items

def upsert_items(items: List[Dict[str, Any]]) -> int:
    if not items:
        return 0
    supa = get_client()
    res = supa.table('regulations').upsert(items, on_conflict='url').execute()
    return len(res.data) if res and getattr(res, 'data', None) else 0

def run(rss_url: str, country: str, authority: str,
        default_doc_type: str = 'Guideline',
        default_status: str = 'Issued',
        default_topics: Optional[list[str]] = None) -> int:
    items = parse_rss(rss_url, country, authority, default_doc_type, default_status, default_topics)
    return upsert_items(items)
