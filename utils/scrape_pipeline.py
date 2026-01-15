from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Iterable, List, Tuple

import requests

from sources.base import BaseSource


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}


def fetch_html(session: requests.Session, url: str, timeout_s: int = 20) -> Tuple[int, str]:
    response = session.get(url, headers=DEFAULT_HEADERS, timeout=timeout_s)
    return response.status_code, response.text


def dedupe_keep_order(values: Iterable[str]) -> List[str]:
    seen = set()
    deduped = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def collect_listing_urls(
    source: BaseSource,
    search_urls: Iterable[str],
    session: requests.Session,
    delay_s: float = 0.0,
) -> List[str]:
    listing_urls: List[str] = []
    for url in search_urls:
        status, html = fetch_html(session, url)
        if status != 200:
            continue
        listing_urls.extend(source.extract_listing_urls(html, url))
        if delay_s:
            time.sleep(delay_s)
    return dedupe_keep_order(listing_urls)


def scrape_listings(
    source: BaseSource,
    listing_urls: Iterable[str],
    session: requests.Session,
    max_workers: int = 6,
    delay_s: float = 0.0,
) -> Dict[str, Dict[str, object]]:
    results: Dict[str, Dict[str, object]] = {}

    def _scrape(url: str) -> Tuple[str, Dict[str, object]]:
        status, html = fetch_html(session, url)
        record = source.parse_listing(html, url)
        record["status_code"] = status
        if delay_s:
            time.sleep(delay_s)
        key = str(record.get("id") or url)
        return key, record

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for key, record in executor.map(_scrape, listing_urls):
            results[key] = record
    return results
