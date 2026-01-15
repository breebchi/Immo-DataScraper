from __future__ import annotations

from typing import Dict


def normalize_listing(record: Dict[str, object], *, include_raw: bool = False) -> Dict[str, object]:
    normalized = {
        "source": record.get("source"),
        "url": record.get("url"),
        "id": record.get("id"),
        "title": record.get("title"),
        "purchase_price": record.get("price"),
        "price_currency": record.get("price_currency"),
        "price_per_m2": record.get("price_per_sqm"),
        "area_m2": record.get("area"),
        "rooms": record.get("rooms"),
        "locality": record.get("locality"),
        "postal_code": record.get("postal_code"),
        "region": record.get("region"),
        "street": record.get("street"),
        "latitude": record.get("latitude"),
        "longitude": record.get("longitude"),
        "description": record.get("description"),
        "provider": record.get("provider"),
        "status_code": record.get("status_code"),
    }
    if include_raw:
        normalized["raw"] = record
    return normalized
