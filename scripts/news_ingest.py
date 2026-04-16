#!/usr/bin/env python3
"""Multi-source 7-day news ingest for forward-themes extraction.

Separate from the tight 36-hour macro/sector/FX ingest used by the morning review
narrative. This one casts a wider net to give the LLM enough signal to discover
the next 18 months of themes, while still producing a citable, verifiable corpus.
"""

from __future__ import annotations

import os
import re
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

from market_panels import (
    MASSIVE_API_KEY,
    MASSIVE_BASE_URL,
    http_get,
    http_get_json,
)

POLYGON_BASE_URL = "https://api.polygon.io"
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "").strip()

THEME_QUERIES = [
    "artificial intelligence capex OR AI infrastructure OR datacenter build-out when:7d",
    "reshoring OR onshoring OR CHIPS Act OR domestic manufacturing when:7d",
    "energy transition OR grid OR power demand OR nuclear restart when:7d",
    "defense spending OR NATO OR rearmament when:7d",
    "China economy OR yuan OR property OR stimulus when:7d",
    "Federal Reserve OR inflation OR Treasury yields OR dollar when:7d",
    "oil OR copper OR uranium OR commodities supercycle when:7d",
    "semiconductor OR TSMC OR Nvidia OR export controls when:7d",
    "fiscal deficit OR bond auction OR term premium when:7d",
    "obesity drugs OR biotech OR healthcare capex when:7d",
    "consumer spending OR credit card delinquency OR housing when:7d",
    "Japan OR BoJ OR yen carry when:7d",
]


def _normalize_title(title: str) -> str:
    return re.sub(r"\s+", " ", (title or "").strip().lower())


def _massive_news_7d(limit: int = 60) -> list[dict]:
    if not MASSIVE_API_KEY:
        return []
    try:
        since = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat().replace("+00:00", "Z")
        payload = http_get_json(
            f"{MASSIVE_BASE_URL}/v2/reference/news",
            {
                "limit": str(limit),
                "order": "desc",
                "sort": "published_utc",
                "published_utc.gte": since,
                "apiKey": MASSIVE_API_KEY,
            },
        )
        out: list[dict] = []
        for item in payload.get("results") or []:
            title = (item.get("title") or "").strip()
            if not title:
                continue
            out.append({
                "title": title,
                "summary": (item.get("description") or "").strip(),
                "publisher": ((item.get("publisher") or {}).get("name") or "Unknown").strip(),
                "published_utc": item.get("published_utc"),
                "url": item.get("article_url"),
                "source": "massive",
            })
        return out
    except Exception:
        return []


def _polygon_news_7d(limit: int = 50) -> list[dict]:
    if not POLYGON_API_KEY:
        return []
    try:
        since = (datetime.now(timezone.utc) - timedelta(days=7)).date().isoformat()
        payload = http_get_json(
            f"{POLYGON_BASE_URL}/v2/reference/news",
            {
                "limit": str(limit),
                "order": "desc",
                "sort": "published_utc",
                "published_utc.gte": since,
                "apiKey": POLYGON_API_KEY,
            },
        )
        out: list[dict] = []
        for item in payload.get("results") or []:
            title = (item.get("title") or "").strip()
            if not title:
                continue
            out.append({
                "title": title,
                "summary": (item.get("description") or "").strip(),
                "publisher": ((item.get("publisher") or {}).get("name") or "Unknown").strip(),
                "published_utc": item.get("published_utc"),
                "url": item.get("article_url"),
                "source": "polygon",
            })
        return out
    except Exception:
        return []


def _google_news_rss(query: str, limit: int = 8) -> list[dict]:
    try:
        url = (
            "https://news.google.com/rss/search?"
            + urllib.parse.urlencode({"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"})
        )
        xml_text = http_get(url)
        root = ET.fromstring(xml_text)
        out: list[dict] = []
        for item in root.findall("./channel/item")[:limit]:
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()
            publisher = item.findtext("source") or "Google News"
            if not title:
                continue
            out.append({
                "title": title,
                "summary": "",
                "publisher": publisher.strip(),
                "published_utc": pub_date,
                "url": link,
                "source": "google-news-rss",
                "theme_query": query,
            })
        return out
    except Exception:
        return []


def collect_forward_news(target_count: int = 80) -> list[dict]:
    """Return deduped 7-day news corpus suitable for theme extraction."""
    articles: list[dict] = []
    seen: set[str] = set()

    def _add(items: list[dict]) -> None:
        for item in items:
            key = _normalize_title(item.get("title") or "")
            if not key or key in seen:
                continue
            seen.add(key)
            articles.append(item)

    _add(_massive_news_7d(limit=80))
    _add(_polygon_news_7d(limit=80))

    for query in THEME_QUERIES:
        if len(articles) >= target_count * 2:
            break
        _add(_google_news_rss(query, limit=6))

    return articles


if __name__ == "__main__":
    import json
    items = collect_forward_news()
    print(json.dumps({"count": len(items), "sources": sorted({i["source"] for i in items})}, indent=2))
