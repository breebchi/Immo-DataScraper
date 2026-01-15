from __future__ import annotations

import random
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests

from sources.base import BaseSource


DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    "DNT": "1",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}


def build_headers(cookie: Optional[str], referer: Optional[str]) -> Dict[str, str]:
    headers = dict(DEFAULT_HEADERS)
    if cookie:
        headers["Cookie"] = cookie
    if referer:
        headers["Referer"] = referer
    return headers


def fetch_html(
    session: requests.Session,
    url: str,
    timeout_s: int = 20,
    retries: int = 2,
    cookie: Optional[str] = None,
) -> Tuple[int, str]:
    last_exc: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            headers = build_headers(cookie, referer=url)
            response = session.get(url, headers=headers, timeout=timeout_s)
            if response.status_code in {403, 429, 503} and attempt < retries:
                backoff = (2 ** attempt) * 0.8 + random.uniform(0.2, 0.6)
                time.sleep(backoff)
                continue
            return response.status_code, response.text
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < retries:
                backoff = (2 ** attempt) * 0.8 + random.uniform(0.2, 0.6)
                time.sleep(backoff)
                continue
    return 0, str(last_exc) if last_exc else ""


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
    retries: int = 2,
    cookie: Optional[str] = None,
    delay_s: float = 0.0,
) -> List[str]:
    listing_urls: List[str] = []
    for url in search_urls:
        status, html = fetch_html(session, url, retries=retries, cookie=cookie)
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
    retries: int = 2,
    cookie: Optional[str] = None,
    delay_s: float = 0.0,
    debug_html_dir: Optional[Path] = None,
) -> Dict[str, Dict[str, object]]:
    results: Dict[str, Dict[str, object]] = {}
    if debug_html_dir:
        debug_html_dir.mkdir(parents=True, exist_ok=True)

    def _scrape(url: str) -> Tuple[str, Dict[str, object]]:
        status, html = fetch_html(session, url, retries=retries, cookie=cookie)
        if debug_html_dir:
            debug_path = debug_html_dir / f"{source.name}_{abs(hash(url))}.html"
            debug_path.write_text(html, encoding="utf-8")
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
