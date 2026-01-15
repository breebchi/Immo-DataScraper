from __future__ import annotations

import json
import re
from typing import Dict, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

try:
    from justhtml import JustHTML
except ImportError:
    JustHTML = None

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
        record.update(self._parse_dom_bs4(soup, record))
        if JustHTML is not None:
            record.update(self._parse_dom_justhtml(html, record))
        title = self._extract_meta_content(soup, "property", "og:title")
        if title:
            record["title"] = title
        page_title = soup.find("title")
        if page_title and page_title.text:
            record.setdefault("title", page_title.text.strip())
        description = self._extract_meta_content(soup, "property", "og:description")
        if description:
            record.setdefault("description", description)
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

    def _parse_dom_bs4(self, soup: BeautifulSoup, record: Dict[str, object]) -> Dict[str, object]:
        parsed: Dict[str, object] = {}
        price_node = soup.find(attrs={"data-testid": "cdp-price"})
        if price_node and "price" not in record:
            price_text = price_node.get_text(" ", strip=True)
            raw_price, amount = self._extract_euro_amount(price_text)
            if amount is not None:
                parsed["price"] = amount
            if raw_price:
                parsed["price_raw"] = raw_price
            price_per_sqm = self._parse_price_per_sqm(price_text)
            if price_per_sqm is not None:
                parsed["price_per_sqm"] = price_per_sqm

        hardfacts = soup.find(attrs={"data-testid": "cdp-hardfacts-keyfacts"})
        if hardfacts:
            hardfacts_text = hardfacts.get_text(" ", strip=True)
            if "rooms" not in record:
                rooms = self._parse_rooms(hardfacts_text)
                if rooms is not None:
                    parsed["rooms"] = rooms
            if "area" not in record:
                area = self._parse_area(hardfacts_text)
                if area is not None:
                    parsed["area"] = area
                    parsed["area_unit"] = "m2"

        location = soup.find(attrs={"data-testid": "cdp-location-address"})
        if location:
            location_text = location.get_text(" ", strip=True)
            if "locality" not in record or "postal_code" not in record:
                locality, postal = self._parse_location(location_text)
                if locality and "locality" not in record:
                    parsed["locality"] = locality
                if postal and "postal_code" not in record:
                    parsed["postal_code"] = postal
            parsed.setdefault("location_raw", location_text)

        provider = soup.find(
            attrs={"data-testid": "aviv.CDP.Contacting.ProviderSection.IntermediaryCard.Title.Link"}
        )
        if provider and "provider" not in record:
            parsed["provider"] = provider.get_text(" ", strip=True)

        description = soup.find(attrs={"data-testid": "cdp-main-description-expandable-text"})
        if description:
            desc_text = description.get_text(" ", strip=True)
            if desc_text:
                current = str(record.get("description") or "")
                if not current or len(desc_text) > len(current):
                    parsed["description"] = desc_text

        description_title = soup.find(attrs={"data-testid": "cdp-main-description-title"})
        if description_title and "description_title" not in record:
            parsed["description_title"] = description_title.get_text(" ", strip=True)

        classified_keys = soup.find(attrs={"data-testid": "cdp-classified-keys"})
        if classified_keys:
            keys_text = classified_keys.get_text(" ", strip=True)
            online_id = self._parse_labeled_value(keys_text, "Online-ID")
            if online_id and "online_id" not in record:
                parsed["online_id"] = online_id
            reference = self._parse_labeled_value(keys_text, "Referenznummer")
            if reference and "reference_number" not in record:
                parsed["reference_number"] = reference

        return parsed

    def _parse_dom_justhtml(self, html: str, record: Dict[str, object]) -> Dict[str, object]:
        parsed: Dict[str, object] = {}
        doc = JustHTML(html)

        price_node = self._jh_first(doc, '[data-testid="cdp-price"]')
        if price_node and "price" not in record:
            price_text = self._jh_text(price_node)
            raw_price, amount = self._extract_euro_amount(price_text)
            if amount is not None:
                parsed["price"] = amount
            if raw_price:
                parsed["price_raw"] = raw_price
            price_per_sqm = self._parse_price_per_sqm(price_text)
            if price_per_sqm is not None:
                parsed["price_per_sqm"] = price_per_sqm

        hardfacts = self._jh_first(doc, '[data-testid="cdp-hardfacts-keyfacts"]')
        if hardfacts:
            hardfacts_text = self._jh_text(hardfacts)
            if "rooms" not in record:
                rooms = self._parse_rooms(hardfacts_text)
                if rooms is not None:
                    parsed["rooms"] = rooms
            if "area" not in record:
                area = self._parse_area(hardfacts_text)
                if area is not None:
                    parsed["area"] = area
                    parsed["area_unit"] = "m2"

        location = self._jh_first(doc, '[data-testid="cdp-location-address"]')
        if location:
            location_text = self._jh_text(location)
            if "locality" not in record or "postal_code" not in record:
                locality, postal = self._parse_location(location_text)
                if locality and "locality" not in record:
                    parsed["locality"] = locality
                if postal and "postal_code" not in record:
                    parsed["postal_code"] = postal
            parsed.setdefault("location_raw", location_text)

        provider = self._jh_first(
            doc,
            '[data-testid="aviv.CDP.Contacting.ProviderSection.IntermediaryCard.Title.Link"]',
        )
        if provider and "provider" not in record:
            parsed["provider"] = self._jh_text(provider)

        description = self._jh_first(doc, '[data-testid="cdp-main-description-expandable-text"]')
        if description:
            desc_text = self._jh_text(description)
            if desc_text:
                current = str(record.get("description") or "")
                if not current or len(desc_text) > len(current):
                    parsed["description"] = desc_text

        description_title = self._jh_first(doc, '[data-testid="cdp-main-description-title"]')
        if description_title and "description_title" not in record:
            parsed["description_title"] = self._jh_text(description_title)

        classified_keys = self._jh_first(doc, '[data-testid="cdp-classified-keys"]')
        if classified_keys:
            keys_text = self._jh_text(classified_keys)
            online_id = self._parse_labeled_value(keys_text, "Online-ID")
            if online_id and "online_id" not in record:
                parsed["online_id"] = online_id
            reference = self._parse_labeled_value(keys_text, "Referenznummer")
            if reference and "reference_number" not in record:
                parsed["reference_number"] = reference

        return parsed

    def _jh_first(self, doc: "JustHTML", selector: str):
        nodes = doc.query(selector)
        return nodes[0] if nodes else None

    def _jh_text(self, node) -> str:
        if hasattr(node, "to_text"):
            try:
                return node.to_text(safe=False).strip()
            except TypeError:
                return node.to_text().strip()
        if hasattr(node, "text"):
            text = node.text
            return text.strip() if isinstance(text, str) else ""
        if hasattr(node, "to_html"):
            html = node.to_html()
            return re.sub(r"<[^>]+>", " ", html).strip()
        return ""

    def _extract_meta_content(self, soup: BeautifulSoup, attr: str, value: str) -> Optional[str]:
        tag = soup.find("meta", attrs={attr: value})
        if not tag:
            return None
        content = tag.get("content")
        return content.strip() if content else None

    def _extract_euro_amount(self, text: str) -> tuple[Optional[str], Optional[int]]:
        match = re.search(r"([\d\.\s ]+)\s*€", text)
        if not match:
            return None, None
        raw = match.group(0).strip()
        return raw, self._parse_int(match.group(1))

    def _parse_price_per_sqm(self, text: str) -> Optional[float]:
        match = re.search(r"([\d\.,]+)\s*€/m²", text)
        if not match:
            return None
        return self._parse_float(match.group(1))

    def _parse_rooms(self, text: str) -> Optional[float]:
        match = re.search(r"([\d\.,]+)\s*Zimmer", text)
        if not match:
            return None
        return self._parse_float(match.group(1))

    def _parse_area(self, text: str) -> Optional[float]:
        match = re.search(r"([\d\.,]+)\s*m²", text)
        if not match:
            return None
        return self._parse_float(match.group(1))

    def _parse_location(self, text: str) -> tuple[Optional[str], Optional[str]]:
        match = re.search(r"^(.*?)\s*\((\d{4,6})\)", text)
        if not match:
            return text, None
        return match.group(1).strip(), match.group(2)

    def _parse_labeled_value(self, text: str, label: str) -> Optional[str]:
        pattern = rf"{re.escape(label)}\s*:\s*([^\s]+)"
        match = re.search(pattern, text)
        return match.group(1).strip() if match else None

    def _parse_int(self, value: str) -> Optional[int]:
        cleaned = value.replace(" ", "").replace(" ", "").replace(".", "").replace(",", "")
        digits = re.sub(r"\D", "", cleaned)
        return int(digits) if digits else None

    def _parse_float(self, value: str) -> Optional[float]:
        cleaned = value.replace(" ", "").replace(" ", "")
        cleaned = cleaned.replace(".", "").replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            return None
