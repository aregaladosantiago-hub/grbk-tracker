import argparse
import asyncio
import json
import re
import time
from datetime import date
from pathlib import Path
from typing import Dict, List, Union
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from .utils import clean_text, make_home_key

SNAPSHOT_COLUMNS = [
    "snapshot_date", "brand", "market", "source_url", "url", "home_key", "community",
    "address", "lot", "plan", "status", "price", "prior_price", "sqft", "beds", "baths",
    "garage", "incentive_text", "raw_text", "qa_flag"
]

PRICE_RE = re.compile(r"\$[\d,]+")
STATUS_RE = re.compile(
    r"\b(?:Ready\s+(?:Now|January|February|March|April|May|June|July|August|September|October|November|December)|Available Now|Quick Move-?In|Under Construction)\b",
    re.IGNORECASE,
)
LOT_RE = re.compile(r"((?:Block\s+[A-Z],?\s*)?Lot\s+\d+)", re.IGNORECASE)
STREET_RE = re.compile(
    r"\b\d{3,6}\s+[A-Za-z0-9 .'-]+?\s+"
    r"(?:Street|St\.?|Road|Rd\.?|Drive|Dr\.?|Lane|Ln\.?|Court|Ct\.?|Trail|Way|Circle|Cir\.?|Avenue|Ave\.?|Boulevard|Blvd\.?)\b",
    re.IGNORECASE,
)
CITY_STATE_RE = re.compile(r"\b(?:Allen|McKinney|Prosper)\s*,?\s*(?:TX|Texas)\s+\d{5}\b", re.IGNORECASE)

STOP_SECTION_HEADERS = {
    "Homeowner Reviews",
    "Site Map",
    "Area Attractions",
    "Meet Your Community Sales Manager",
    "Request Information",
    "Sales Information",
    "Visit Our Community Sales Office",
    "Driving Directions",
    "Footer Navigation",
}

BAD_LINE_FRAGMENTS = [
    "privacy policy",
    "terms of service",
    "all rights reserved",
    "careers",
    "realtors",
    "homeowners",
    "mortgage calculator",
]


async def fetch_html(url: str, use_playwright: bool) -> str:
    if use_playwright:
        from playwright.async_api import async_playwright

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page(user_agent="Mozilla/5.0 GRBK inventory research tracker")
            await page.goto(url, wait_until="networkidle", timeout=90000)

            for _ in range(5):
                await page.mouse.wheel(0, 2500)
                await page.wait_for_timeout(900)

            html = await page.content()
            await browser.close()
            return html

    import requests
    response = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0 GRBK inventory research tracker"})
    response.raise_for_status()
    return response.text


def extract_prices(block_lines: List[str]):
    prices = []
    for line in block_lines:
        if line.lower().startswith("from "):
            continue
        for p in PRICE_RE.findall(line):
            prices.append(int(p.replace("$", "").replace(",", "")))

    if not prices:
        return None, None

    block_text = " ".join(block_lines).lower()
    if ("new lower price" in block_text or "was" in block_text or "reduced" in block_text) and len(prices) >= 2:
        return min(prices), max(prices)

    return prices[0], None


def normalize_lines(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    lines = []
    for raw in soup.get_text("\n", strip=True).splitlines():
        line = clean_text(raw)
        if not line:
            continue
        if any(bad in line.lower() for bad in BAD_LINE_FRAGMENTS):
            continue
        lines.append(line)
    return lines


def find_qmi_window(lines: List[str]) -> List[str]:
    start = None
    for i, line in enumerate(lines):
        if line == "Quick Move-in Homes":
            nearby = " ".join(lines[i:i + 8]).lower()
            if "currently under construction" in nearby or "available now" in nearby or "quicker move-in" in nearby:
                start = i
                break

    if start is None:
        return lines

    end = len(lines)
    for j in range(start + 1, len(lines)):
        if lines[j] in STOP_SECTION_HEADERS:
            end = j
            break
    return lines[start:end]


def extract_address_from_lines(lines: List[str], i: int):
    window = " ".join(lines[i:i + 4])
    street = STREET_RE.search(window)
    city = CITY_STATE_RE.search(window)

    if street and city:
        address = f"{street.group(0)} {city.group(0)}"
        address = re.sub(r"\bTexas\b", "TX", address, flags=re.IGNORECASE)
        address = re.sub(r"\s+", " ", address).strip()
        return address

    full = re.search(
        r"(\d{3,6}\s+.+?\s+(?:Allen|McKinney|Prosper)\s*,?\s*(?:TX|Texas)\s+\d{5})",
        window,
        flags=re.IGNORECASE,
    )
    if full:
        address = full.group(1)
        address = re.sub(r"\bTexas\b", "TX", address, flags=re.IGNORECASE)
        return clean_text(address)

    return None


def find_address_indices(lines: List[str]):
    indices = []
    seen = set()
    for i in range(len(lines)):
        address = extract_address_from_lines(lines, i)
        if address and address not in seen:
            seen.add(address)
            indices.append((i, address))
    return indices


def listing_urls_from_html(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    urls = []
    for a in soup.find_all("a", href=True):
        label = clean_text(a.get_text(" ", strip=True))
        if label == "View Listing":
            urls.append(urljoin(base_url, a["href"]))
    return urls


def parse_listing_block(block_lines, address, brand_cfg, url_meta, source_url, snapshot_date, listing_url=None):
    raw_text = " | ".join(block_lines)
    price, prior_price = extract_prices(block_lines)

    status = None
    m = STATUS_RE.search(raw_text)
    if m:
        status = clean_text(m.group(0)).title()

    lot = None
    lot_m = LOT_RE.search(raw_text)
    if lot_m:
        lot = clean_text(lot_m.group(1).title())

    price_pressure = []
    if prior_price is not None and price is not None and prior_price > price:
        price_pressure.append("Price Cut")
    if "new lower price" in raw_text.lower():
        price_pressure.append("New Lower Price")

    community = url_meta.get("community")
    row = {
        "snapshot_date": snapshot_date,
        "brand": brand_cfg["brand"],
        "market": url_meta.get("market") or brand_cfg.get("market"),
        "source_url": source_url,
        "url": listing_url or source_url,
        "home_key": None,
        "community": community,
        "address": address,
        "lot": lot,
        "plan": None,
        "status": status,
        "price": price,
        "prior_price": prior_price,
        "sqft": None,
        "beds": None,
        "baths": None,
        "garage": None,
        "incentive_text": " | ".join(dict.fromkeys(price_pressure)) if price_pressure else None,
        "raw_text": raw_text[:2500],
        "qa_flag": None,
    }

    row["home_key"] = make_home_key(row["brand"], row["address"], row["community"], row["lot"], None)

    flags = []
    if not row["address"]:
        flags.append("missing_address")
    if row["price"] is None:
        flags.append("missing_price")
    row["qa_flag"] = ";".join(flags) if flags else None

    return row


def is_valid_listing(row: Dict) -> bool:
    return bool(row.get("address") and row.get("price") is not None)


async def scrape_southgate_community(brand_cfg: Dict, url_meta: Dict, snapshot_date: str, use_playwright: bool):
    source_url = url_meta["url"]
    html = await fetch_html(source_url, use_playwright=use_playwright)

    lines = find_qmi_window(normalize_lines(html))
    address_points = find_address_indices(lines)
    view_urls = listing_urls_from_html(html, source_url)

    rows = []
    for n, (idx, address) in enumerate(address_points):
        next_idx = address_points[n + 1][0] if n + 1 < len(address_points) else min(len(lines), idx + 35)
        start = max(0, idx - 8)
        end = min(len(lines), next_idx)
        block = lines[start:end]

        row = parse_listing_block(
            block_lines=block,
            address=address,
            brand_cfg=brand_cfg,
            url_meta=url_meta,
            source_url=source_url,
            snapshot_date=snapshot_date,
            listing_url=view_urls[n] if n < len(view_urls) else None,
        )
        if is_valid_listing(row):
            rows.append(row)

    deduped = {}
    for row in rows:
        deduped[(row["brand"], row["community"], row["address"])] = row

    print(
        f"{brand_cfg['brand']} | {url_meta.get('community', source_url)}: "
        f"{len(address_points)} address candidates, {len(deduped)} valid listings"
    )
    return list(deduped.values())


def normalize_url_entry(entry: Union[str, Dict]) -> Dict:
    return {"url": entry} if isinstance(entry, str) else dict(entry)


async def scrape_brand(brand_cfg: Dict, snapshot_date: str, use_playwright: bool, delay: float):
    parser = brand_cfg.get("parser")
    rows = []

    for entry in brand_cfg["urls"]:
        url_meta = normalize_url_entry(entry)

        if parser == "southgate_community":
            rows.extend(await scrape_southgate_community(brand_cfg, url_meta, snapshot_date, use_playwright))
        else:
            print(f"Skipping {brand_cfg['brand']} because parser={parser} is not implemented/tested yet.")

        time.sleep(delay)

    deduped = {}
    for row in rows:
        deduped[(row["brand"], row["community"], row["address"])] = row
    return list(deduped.values())


def write_snapshot(rows: List[Dict], snapshot_date: str, out_dir: str) -> Path:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=SNAPSHOT_COLUMNS)
    else:
        df = df.reindex(columns=SNAPSHOT_COLUMNS)

    out_path = Path(out_dir) / f"{snapshot_date}.csv"
    df.to_csv(out_path, index=False)

    latest_path = Path("reports") / "latest_active.csv"
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(latest_path, index=False)

    return out_path


async def main():
    parser = argparse.ArgumentParser(description="Scrape GRBK listing flow snapshots.")
    parser.add_argument("--config", default="config/brands.json")
    parser.add_argument("--out-dir", default="data/snapshots")
    parser.add_argument("--date", default=str(date.today()))
    args = parser.parse_args()

    with open(args.config, "r") as f:
        cfg = json.load(f)

    settings = cfg.get("settings", {})
    use_playwright = bool(settings.get("use_playwright", True))
    delay = float(settings.get("request_delay_seconds", 1))

    all_rows = []
    for brand_cfg in cfg["brands"]:
        try:
            rows = await scrape_brand(brand_cfg, args.date, use_playwright, delay)
            print(f"{brand_cfg['brand']}: {len(rows)} usable listing rows")
            all_rows.extend(rows)
        except Exception as exc:
            print(f"ERROR scraping {brand_cfg.get('brand', 'UNKNOWN')}: {exc}")

    out_path = write_snapshot(all_rows, args.date, args.out_dir)
    print(f"Saved {len(all_rows)} rows to {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
