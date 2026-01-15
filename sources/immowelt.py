from __future__ import annotations

import json
import re
from typing import Dict, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from sources.base import BaseSource


class ImmoWeltSource(BaseSource):
    name = "immowelt"

    _LISTING_URL_PATTERNS = (
        re.compile(r"/expose/[^/?#]+", re.IGNORECASE),
        re.compile(r"/immobilien/[^/?#]+", re.IGNORECASE),
    )

    def extract_listing_urls(self, html: str, base_url: str) -> List[str]:
        soup = BeautifulSoup(html, "html.parser")
        urls = []
        seen = set()
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"].strip()
            if not href:
                continue
            if not self._is_listing_link(href):
                continue
            full_url = urljoin(base_url, href)
            if full_url in seen:
                continue
            seen.add(full_url)
            urls.append(full_url)
        return urls

    def parse_listing(self, html: str, url: str) -> Dict[str, object]:
        soup = BeautifulSoup(html, "html.parser")
        record: Dict[str, object] = {
            "source": self.name,
            "url": url,
            "id": self._extract_listing_id(url),
        }
        json_ld = self._extract_json_ld(soup)
        if json_ld:
            record.update(self._parse_json_ld(json_ld))
        title = soup.find("title")
        if title and title.text:
            record.setdefault("title", title.text.strip())
        return record

    def _is_listing_link(self, href: str) -> bool:
        return any(pattern.search(href) for pattern in self._LISTING_URL_PATTERNS)

    def _extract_listing_id(self, url: str) -> str:
        match = re.search(r"/expose/([^/?#]+)", url, re.IGNORECASE)
        if not match:
            match = re.search(r"/immobilien/([^/?#]+)", url, re.IGNORECASE)
        return match.group(1) if match else url

    def _extract_json_ld(self, soup: BeautifulSoup) -> Optional[dict]:
        candidates = []
        for script in soup.find_all("script", type="application/ld+json"):
            text = script.string or script.text
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, list):
                candidates.extend(payload)
            elif isinstance(payload, dict):
                candidates.append(payload)
        if not candidates:
            return None
        for candidate in candidates:
            if isinstance(candidate, dict) and ("offers" in candidate or "address" in candidate):
                return candidate
        return candidates[0] if isinstance(candidates[0], dict) else None

    def _parse_json_ld(self, data: dict) -> Dict[str, object]:
        record: Dict[str, object] = {}
        if "name" in data:
            record["title"] = data.get("name")
        if "description" in data:
            record["description"] = data.get("description")
        offers = data.get("offers")
        if isinstance(offers, dict):
            record["price"] = offers.get("price")
            record["price_currency"] = offers.get("priceCurrency")
            record["availability"] = offers.get("availability")
        address = data.get("address")
        if isinstance(address, dict):
            record["street"] = address.get("streetAddress")
            record["locality"] = address.get("addressLocality")
            record["region"] = address.get("addressRegion")
            record["postal_code"] = address.get("postalCode")
            record["country"] = address.get("addressCountry")
        geo = data.get("geo")
        if isinstance(geo, dict):
            record["latitude"] = geo.get("latitude")
            record["longitude"] = geo.get("longitude")
        if "numberOfRooms" in data:
            record["rooms"] = data.get("numberOfRooms")
        floor_size = data.get("floorSize")
        if isinstance(floor_size, dict):
            record["area"] = floor_size.get("value")
            record["area_unit"] = floor_size.get("unitText")
        return record
