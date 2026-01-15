from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List


class BaseSource(ABC):
    name: str

    def build_search_urls(self, search_url_template: str, pages: int) -> List[str]:
        if not search_url_template:
            raise ValueError("search_url_template is required for search scraping.")
        if "{page}" in search_url_template:
            return [search_url_template.format(page=page) for page in range(1, pages + 1)]
        if pages != 1:
            raise ValueError("search_url_template must include {page} when pages > 1.")
        return [search_url_template]

    @abstractmethod
    def extract_listing_urls(self, html: str, base_url: str) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def parse_listing(self, html: str, url: str) -> Dict[str, object]:
        raise NotImplementedError
