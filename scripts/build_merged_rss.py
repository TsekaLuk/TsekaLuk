#!/usr/bin/env python3
"""
Build a merged RSS feed from an OPML subscription list.

Usage:
  python scripts/build_merged_rss.py \
    --opml .github/rss/blog-feeds.opml \
    --output feeds/blog-radar.xml \
    --readme-snippet feeds/blog-radar-snippet.md
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import logging
import re
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

log = logging.getLogger("rss-radar")

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


def parse_single_feed(url: str, timeout_sec: float) -> tuple[str, list[FeedItem], str | None]:
    """Returns (url, items, error_message_or_None)."""
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
        return url, items, None
    except Exception as exc:
        log.warning("FAILED %s — %s: %s", url, type(exc).__name__, exc)
        return url, [], f"{type(exc).__name__}: {exc}"


def merge_items(feed_urls: Iterable[str], timeout_sec: float) -> tuple[list[FeedItem], list[str], list[str]]:
    """Returns (merged_items, ok_urls, failed_urls)."""
    merged: list[FeedItem] = []
    ok_urls: list[str] = []
    failed_urls: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        futures = [
            pool.submit(parse_single_feed, url=url, timeout_sec=timeout_sec)
            for url in feed_urls
        ]
        for fut in concurrent.futures.as_completed(futures):
            url, items, err = fut.result()
            if err is not None:
                failed_urls.append(url)
            else:
                ok_urls.append(url)
            merged.extend(items)
    # De-duplicate by canonical link; keep newest instance.
    latest_by_link: dict[str, FeedItem] = {}
    for item in merged:
        prev = latest_by_link.get(item.link)
        if prev is None or item.published > prev.published:
            latest_by_link[item.link] = item
    sorted_items = sorted(latest_by_link.values(), key=lambda x: x.published, reverse=True)
    return sorted_items, ok_urls, failed_urls


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


def render_readme_snippet(items: list[FeedItem], count: int = 5) -> str:
    """Render a markdown list of the most recent items for README injection."""
    md_lines: list[str] = []
    for item in items[:count]:
        date_str = item.published.strftime("%m-%d")
        md_lines.append(f"- [{item.title}]({item.link}) — *{item.source}* `{date_str}`")
    return "\n".join(md_lines)


def inject_readme_snippet(readme_path: Path, snippet: str) -> bool:
    """Replace content between BLOG_RADAR markers in README. Returns True if changed."""
    content = readme_path.read_text(encoding="utf-8")
    pattern = r"(<!--BLOG_RADAR:start-->\n).*?(<!--BLOG_RADAR:end-->)"
    replacement = rf"\g<1>{snippet}\n\2"
    new_content, count = re.subn(pattern, replacement, content, flags=re.DOTALL)
    if count == 0:
        log.warning("BLOG_RADAR markers not found in %s", readme_path)
        return False
    if new_content == content:
        return False
    readme_path.write_text(new_content, encoding="utf-8")
    return True


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument("--opml", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--limit", type=int, default=120)
    parser.add_argument("--timeout", type=float, default=8.0)
    parser.add_argument("--readme-snippet", type=Path, default=None,
                        help="Path to README.md — inject Blog Radar snippet between markers")
    parser.add_argument("--snippet-count", type=int, default=5,
                        help="Number of items to show in README snippet")
    parser.add_argument("--strict", action="store_true",
                        help="Exit with error if >50%% of feeds fail")
    args = parser.parse_args()

    urls = parse_opml(args.opml)
    all_items, ok_urls, failed_urls = merge_items(urls, timeout_sec=args.timeout)
    items = all_items[: max(0, args.limit)]

    # Summary
    total = len(urls)
    ok = len(ok_urls)
    failed = len(failed_urls)
    failed_hosts = [urlparse(u).netloc for u in failed_urls[:5]]
    summary_parts = [f"OK: {ok}/{total}"]
    if failed:
        hosts_str = ", ".join(failed_hosts)
        if failed > 5:
            hosts_str += f", ... (+{failed - 5} more)"
        summary_parts.append(f"FAILED: {failed} ({hosts_str})")
    log.info("feeds=%d items=%d | %s", total, len(items), " | ".join(summary_parts))

    # Write RSS XML
    title = "TsekaLuk Blog Radar"
    link = "https://github.com/TsekaLuk/TsekaLuk"
    description = f"Merged feed from {total} subscriptions"
    rss_xml = render_rss(items, title=title, link=link, description=description)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rss_xml, encoding="utf-8")

    # README snippet injection
    if args.readme_snippet is not None:
        snippet = render_readme_snippet(items, count=args.snippet_count)
        changed = inject_readme_snippet(args.readme_snippet, snippet)
        log.info("README snippet: %s", "updated" if changed else "unchanged")

    # Strict mode
    if args.strict and total > 0 and failed / total > 0.5:
        log.error("Strict mode: %d/%d feeds failed (>50%%)", failed, total)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
