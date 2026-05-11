import asyncio
import time
from typing import Dict, Optional

import requests

from . import scrape_base as base
from .scrape_base import *  # noqa: F401,F403 - keep the original scraper API available.


BASE_SCRAPE_BRAND = base.scrape_brand


def clean_optional(value) -> Optional[str]:
    if value in (None, ""):
        return None
    text = base.clean_text(str(value))
    return text or None


def coerce_int(value) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return None


def coerce_float(value) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def cbjeni_community(home: Dict) -> Optional[str]:
    terms = home.get("terms")
    if isinstance(terms, list) and terms:
        first = terms[0]
        if isinstance(first, dict):
            return clean_optional(first.get("name"))
    return None


def cbjeni_address(home: Dict) -> Optional[str]:
    street = clean_optional(home.get("spec_street_1"))
    city = clean_optional(home.get("spec_city"))
    state = clean_optional(home.get("spec_state"))
    zip_code = clean_optional(home.get("spec_zip"))
    if street and city and state and zip_code:
        return base.normalize_address(f"{street} {city}, {state} {zip_code}")

    title = clean_optional(home.get("title"))
    return base.normalize_address(title) if title else None


def cbjeni_status(home: Dict) -> Optional[str]:
    raw_status = clean_optional(home.get("status"))
    status_map = {
        "MoveInReady": "Move-In Ready",
        "UnderConstruction": "Under Construction",
        "Reserved": "Reserved",
    }
    parts = []
    if raw_status:
        parts.append(status_map.get(raw_status, raw_status))

    move_in = clean_optional(home.get("movein"))
    if move_in:
        parts.append(f"Ready {move_in}")

    return " | ".join(dict.fromkeys(parts)) if parts else None


def cbjeni_baths(home: Dict) -> Optional[float]:
    full = coerce_float(home.get("baths"))
    half = coerce_float(home.get("half_baths"))
    if full is None and half is None:
        return None
    return (full or 0) + ((half or 0) * 0.5)


def cbjeni_raw_text(home: Dict, community: Optional[str], address: Optional[str], status: Optional[str]) -> str:
    parts = [
        community,
        address,
        clean_optional(home.get("floor_plan_name")),
        status,
        f"Price {home.get('price')}" if home.get("price") not in (None, "") else None,
        f"Was {home.get('was')}" if home.get("was") not in (None, "") else None,
        clean_optional(home.get("spec_lot_number")),
        clean_optional(home.get("banner")),
    ]
    return " | ".join(part for part in parts if part)


def parse_cbjeni_home(home: Dict, brand_cfg: Dict, url_meta: Dict, source_url: str, snapshot_date: str) -> Dict:
    price = coerce_int(home.get("price"))
    prior_price = coerce_int(home.get("was"))
    if prior_price is not None and price is not None and prior_price <= price:
        prior_price = None

    community = cbjeni_community(home)
    address = cbjeni_address(home)
    status = cbjeni_status(home)
    raw_text = cbjeni_raw_text(home, community, address, status)

    pressure = []
    if prior_price is not None and price is not None and prior_price > price:
        pressure.append("Price Cut")
    banner = clean_optional(home.get("banner"))
    if banner:
        pressure.append(banner)

    row = {
        "snapshot_date": snapshot_date,
        "brand": brand_cfg["brand"],
        "market": url_meta.get("market") or brand_cfg.get("market"),
        "source_url": source_url,
        "url": clean_optional(home.get("link")) or source_url,
        "home_key": None,
        "community": community,
        "address": address,
        "lot": clean_optional(home.get("spec_lot_number")),
        "plan": clean_optional(home.get("floor_plan_name")),
        "status": status,
        "price": price,
        "prior_price": prior_price,
        "sqft": coerce_int(home.get("spec_sqft") or home.get("sqft")),
        "beds": coerce_float(home.get("beds")),
        "baths": cbjeni_baths(home),
        "garage": coerce_float(home.get("garage")),
        "incentive_text": " | ".join(dict.fromkeys(pressure)) if pressure else None,
        "raw_text": raw_text[:2500],
        "qa_flag": None,
    }

    row["home_key"] = base.make_home_key(row["brand"], row["address"], row["community"], row["lot"], row["url"])
    row["qa_flag"] = base.row_qa_flags(row, require_community=True)
    return row


async def scrape_cbjeni_api_page(brand_cfg: Dict, url_meta: Dict, snapshot_date: str):
    source_url = url_meta["url"]
    api_url = url_meta.get("api_url") or source_url
    response = requests.post(
        api_url,
        data={"action": "new_and_now"},
        timeout=45,
        headers={
            "User-Agent": "Mozilla/5.0 GRBK inventory research tracker",
            "Referer": source_url,
        },
    )
    response.raise_for_status()
    homes = response.json()
    if not isinstance(homes, list):
        raise ValueError("CB JENI new_and_now response was not a list")

    rows = []
    for home in homes:
        if not isinstance(home, dict):
            continue
        row = parse_cbjeni_home(home, brand_cfg, url_meta, source_url, snapshot_date)
        if base.is_valid_listing(row):
            rows.append(row)

    rows = base.dedupe_rows(rows)
    print(
        f"{brand_cfg['brand']} | {url_meta.get('market', source_url)}: "
        f"{len(homes)} API homes, {len(rows)} valid listings"
    )
    return rows


async def scrape_brand(brand_cfg: Dict, snapshot_date: str, use_playwright: bool, delay: float):
    if brand_cfg.get("parser") != "cbjeni_api":
        return await BASE_SCRAPE_BRAND(brand_cfg, snapshot_date, use_playwright, delay)

    rows = []
    for entry in brand_cfg["urls"]:
        url_meta = base.normalize_url_entry(entry)
        rows.extend(await scrape_cbjeni_api_page(brand_cfg, url_meta, snapshot_date))
        time.sleep(delay)

    return base.dedupe_rows(rows)


async def main():
    base.scrape_brand = scrape_brand
    await base.main()


if __name__ == "__main__":
    asyncio.run(main())
