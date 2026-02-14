#!/usr/bin/env python3
"""
Build a merged RSS feed from an OPML subscription list.

Usage:
  python scripts/build_merged_rss.py \
    --opml .github/rss/blog-feeds.opml \
    --output feeds/blog-radar.xml
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import time
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from email.utils import format_datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse
from xml.sax.saxutils import escape

import feedparser


UTC = dt.timezone.utc


@dataclass(frozen=True)
class FeedItem:
    title: str
    link: str
    source: str
    published: dt.datetime


def parse_opml(opml_path: Path) -> list[str]:
    tree = ET.parse(opml_path)
    root = tree.getroot()
    urls: list[str] = []
    for outline in root.findall(".//outline"):
        xml_url = outline.attrib.get("xmlUrl", "").strip()
        if xml_url:
            urls.append(xml_url)
    # Keep order while de-duplicating.
    seen: set[str] = set()
    deduped: list[str] = []
    for url in urls:
        if url not in seen:
            seen.add(url)
            deduped.append(url)
    return deduped


def fetch_bytes(url: str, timeout_sec: float) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "TsekaLuk-RSS-Radar/1.0 (+https://github.com/TsekaLuk/TsekaLuk)"
        },
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:  # nosec B310
        return resp.read()


def to_datetime(entry: feedparser.FeedParserDict) -> dt.datetime | None:
    stamp = entry.get("published_parsed") or entry.get("updated_parsed")
    if not stamp:
        return None
    return dt.datetime.fromtimestamp(time.mktime(stamp), tz=UTC)


def clean_text(value: str) -> str:
    return " ".join((value or "").split())


def parse_single_feed(url: str, timeout_sec: float) -> tuple[str, list[FeedItem]]:
    try:
        raw = fetch_bytes(url, timeout_sec)
        parsed = feedparser.parse(raw)
        source_name = clean_text(parsed.feed.get("title", "")) or urlparse(url).netloc

        items: list[FeedItem] = []
        for entry in parsed.entries:
            title = clean_text(entry.get("title", ""))
            link = (entry.get("link") or "").strip()
            published = to_datetime(entry)
            if not title or not link or not published:
                continue
            items.append(
                FeedItem(
                    title=title,
                    link=link,
                    source=source_name,
                    published=published,
                )
            )
        return url, items
    except Exception:
        return url, []


def merge_items(feed_urls: Iterable[str], timeout_sec: float) -> list[FeedItem]:
    merged: list[FeedItem] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        futures = [
            pool.submit(parse_single_feed, url=url, timeout_sec=timeout_sec)
            for url in feed_urls
        ]
        for fut in concurrent.futures.as_completed(futures):
            _, items = fut.result()
            merged.extend(items)
    # De-duplicate by canonical link; keep newest instance.
    latest_by_link: dict[str, FeedItem] = {}
    for item in merged:
        prev = latest_by_link.get(item.link)
        if prev is None or item.published > prev.published:
            latest_by_link[item.link] = item
    return sorted(latest_by_link.values(), key=lambda x: x.published, reverse=True)


def render_rss(items: list[FeedItem], title: str, link: str, description: str) -> str:
    now = dt.datetime.now(tz=UTC)
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        "<rss version=\"2.0\">",
        "  <channel>",
        f"    <title>{escape(title)}</title>",
        f"    <link>{escape(link)}</link>",
        f"    <description>{escape(description)}</description>",
        f"    <lastBuildDate>{format_datetime(now)}</lastBuildDate>",
    ]
    for item in items:
        lines.extend(
            [
                "    <item>",
                f"      <title>{escape(item.title)}</title>",
                f"      <link>{escape(item.link)}</link>",
                f"      <guid isPermaLink=\"true\">{escape(item.link)}</guid>",
                f"      <pubDate>{format_datetime(item.published)}</pubDate>",
                f"      <description>{escape(item.source)}</description>",
                "    </item>",
            ]
        )
    lines.extend(["  </channel>", "</rss>"])
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--opml", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--limit", type=int, default=120)
    parser.add_argument("--timeout", type=float, default=8.0)
    args = parser.parse_args()

    urls = parse_opml(args.opml)
    items = merge_items(urls, timeout_sec=args.timeout)[: max(0, args.limit)]

    title = "TsekaLuk Blog Radar"
    link = "https://github.com/TsekaLuk/TsekaLuk"
    description = f"Merged feed from {len(urls)} subscriptions"
    rss_xml = render_rss(items, title=title, link=link, description=description)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rss_xml, encoding="utf-8")
    print(f"feeds={len(urls)} items={len(items)} output={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
