from __future__ import annotations

import time
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import requests

from sources import ImmoWeltSource
from sources.base import BaseSource
from utils.scrape_pipeline import collect_listing_urls, fetch_html, scrape_listings


_SOURCES = {
    "immowelt": ImmoWeltSource,
}


def detect_source_name(url: str) -> str:
    netloc = urlparse(url).netloc.lower()
    if "immowelt.de" in netloc:
        return "immowelt"
    raise ValueError(f"Unsupported listing URL: {url}")


def get_source(source: str | BaseSource) -> BaseSource:
    if isinstance(source, BaseSource):
        return source
    source_name = source.lower().strip()
    factory = _SOURCES.get(source_name)
    if not factory:
        raise ValueError(f"Unknown source '{source}'. Available: {', '.join(sorted(_SOURCES))}")
    return factory()


class ScrapeClient:
    def __init__(self, source: str | BaseSource = "immowelt", session: Optional[requests.Session] = None) -> None:
        self.source = get_source(source)
        self._session = session or requests.Session()
        self._owns_session = session is None

    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    def __enter__(self) -> "ScrapeClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def parse_listing_html(self, html: str, url: str) -> Dict[str, object]:
        return self.source.parse_listing(html, url)

    def fetch_listing_html(
        self,
        url: str,
        *,
        retries: int = 2,
        cookie: Optional[str] = None,
        timeout_s: int = 20,
    ) -> Tuple[int, str]:
        return fetch_html(self._session, url, timeout_s=timeout_s, retries=retries, cookie=cookie)

    def scrape_listing(
        self,
        url: str,
        *,
        retries: int = 2,
        cookie: Optional[str] = None,
        timeout_s: int = 20,
        delay_s: float = 0.0,
        include_status: bool = True,
    ) -> Dict[str, object]:
        status, html = self.fetch_listing_html(url, retries=retries, cookie=cookie, timeout_s=timeout_s)
        record = self.source.parse_listing(html, url)
        if include_status:
            record["status_code"] = status
        if delay_s:
            time.sleep(delay_s)
        return record

    def collect_listing_urls(
        self,
        search_url_template: str,
        *,
        pages: int = 1,
        retries: int = 2,
        cookie: Optional[str] = None,
        delay_s: float = 0.0,
    ) -> List[str]:
        search_urls = self.source.build_search_urls(search_url_template, pages)
        return collect_listing_urls(
            self.source,
            search_urls,
            self._session,
            retries=retries,
            cookie=cookie,
            delay_s=delay_s,
        )

    def scrape_listings(
        self,
        listing_urls: Iterable[str],
        *,
        max_workers: int = 6,
        retries: int = 2,
        cookie: Optional[str] = None,
        delay_s: float = 0.0,
    ) -> Dict[str, Dict[str, object]]:
        return scrape_listings(
            self.source,
            listing_urls,
            self._session,
            max_workers=max_workers,
            retries=retries,
            cookie=cookie,
            delay_s=delay_s,
        )

    def scrape_search(
        self,
        search_url_template: str,
        *,
        pages: int = 1,
        max_listings: int = 0,
        max_workers: int = 6,
        retries: int = 2,
        cookie: Optional[str] = None,
        delay_s: float = 0.0,
    ) -> Dict[str, Dict[str, object]]:
        listing_urls = self.collect_listing_urls(
            search_url_template,
            pages=pages,
            retries=retries,
            cookie=cookie,
            delay_s=delay_s,
        )
        if max_listings > 0:
            listing_urls = listing_urls[:max_listings]
        return self.scrape_listings(
            listing_urls,
            max_workers=max_workers,
            retries=retries,
            cookie=cookie,
            delay_s=delay_s,
        )
