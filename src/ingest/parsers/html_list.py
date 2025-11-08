# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import List, Dict, Any, Optional
import httpx
import dateparser
from selectolax.parser import HTMLParser
from src.common.supa import get_client
from src.common.types import Regulation

def fetch(url: str, timeout: float = 20.0) -> str:
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        r = client.get(url, headers={'User-Agent': 'reg-radar/0.1'})
        r.raise_for_status()
        return r.text

def parse_list(url: str,
               item_selector: str,
               title_selector: str,
               link_selector: str,
               date_selector: Optional[str],
               country: str,
               authority: str,
               default_doc_type: str = 'Guideline',
               default_status: str = 'Issued',
               default_topics: Optional[list[str]] = None) -> List[Dict[str, Any]]:

    html = fetch(url)
    doc = HTMLParser(html)
    items: List[Dict[str, Any]] = []

    for node in doc.css(item_selector):
        title_node = node.css_first(title_selector)
        link_node = node.css_first(link_selector)

        title = (title_node.text(strip=True) if title_node else '').strip()
        href = link_node.attributes.get('href', '') if link_node else ''
        link = href if href.startswith('http') else (url.rstrip('/') + '/' + href.lstrip('/'))

        publish_date = None
        if date_selector:
            date_node = node.css_first(date_selector)
            if date_node:
                dt = dateparser.parse(date_node.text(strip=True))
                if dt:
                    publish_date = dt.date().isoformat()

        reg = Regulation(
            country=country,
            authority=authority,
            title=title or 'Untitled',
            url=link,
            doc_type=default_doc_type,
            status=default_status,
            topics=default_topics or [],
            publish_date=publish_date,
            summary=None,
            raw_text=None,
            impact_level=None,
            source=f'html:{authority}',
        )
        items.append(reg.model_dump())

    return items

def upsert_items(items: List[Dict[str, Any]]) -> int:
    if not items:
        return 0
    supa = get_client()
    res = supa.table('regulations').upsert(items, on_conflict='url').execute()
    return len(res.data) if res and getattr(res, 'data', None) else 0

def run(url: str,
        item_selector: str,
        title_selector: str,
        link_selector: str,
        date_selector: Optional[str],
        country: str,
        authority: str,
        default_doc_type: str = 'Guideline',
        default_status: str = 'Issued',
        default_topics: Optional[list[str]] = None) -> int:
    items = parse_list(url, item_selector, title_selector, link_selector, date_selector,
                       country, authority, default_doc_type, default_status, default_topics)
    return upsert_items(items)
