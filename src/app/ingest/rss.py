from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List

from src.app.ingest.normalize import clean_text, parse_date

RSS_NAMESPACE = {"atom": "http://www.w3.org/2005/Atom"}


def _extract_item_fields(node: ET.Element) -> Dict[str, Any]:
    title = clean_text(
        node.findtext("title") or node.findtext("atom:title", namespaces=RSS_NAMESPACE)
    )
    link = (
        node.findtext("link") or node.findtext("atom:link", namespaces=RSS_NAMESPACE) or ""
    ).strip()
    if not link and node.find("link") is not None and "href" in node.find("link").attrib:
        link = node.find("link").attrib["href"]
    summary = clean_text(
        node.findtext("description")
        or node.findtext("summary")
        or node.findtext("atom:summary", namespaces=RSS_NAMESPACE)
        or ""
    )
    published_raw = (
        node.findtext("pubDate")
        or node.findtext("published")
        or node.findtext("updated")
        or node.findtext("dc:date")
    )
    published_at = parse_date(published_raw)
    return {
        "title": title,
        "url": link,
        "summary": summary,
        "published_at": published_at,
    }


def parse_rss(content: str, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    root = ET.fromstring(content)
    items: List[Dict[str, Any]] = []
    channel_items = root.findall(".//item")
    if channel_items:
        raw_items = channel_items
    else:
        raw_items = root.findall(".//atom:entry", namespaces=RSS_NAMESPACE)
    now = datetime.now(timezone.utc)
    for node in raw_items:
        fields = _extract_item_fields(node)
        if not fields["title"] and not fields["url"]:
            continue
        items.append(
            {
                "source_id": source["id"],
                "source_name": source["name"],
                "category": source["category"],
                "kind": source["kind"],
                "title": fields["title"],
                "summary": fields["summary"],
                "url": fields["url"],
                "published_at": fields["published_at"],
                "fetched_at": now,
            }
        )
    return items
