import argparse
import asyncio
import json
import re
import time
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple, Union
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from .utils import clean_text, extract_incentive_text, make_home_key

SNAPSHOT_COLUMNS = [
    "snapshot_date", "brand", "market", "source_url", "url", "home_key", "community",
    "address", "lot", "plan", "status", "price", "prior_price", "sqft", "beds", "baths",
    "garage", "incentive_text", "raw_text", "qa_flag"
]

STOP_HEADERS = {
    "Homeowner Reviews",
    "Site Map",
    "Area Attractions",
    "Meet Your Community Sales Manager",
    "Request Information",
    "Sales Information",
    "Visit Our Community Sales Office",
    "Driving Directions",
    "Footer Navigation",
    "Search",
}

SKIP_LINES = {
    "",
    "Choose from select Southgate homes currently under construction or available now for a quicker move-in.",
    "Save",
    "View Listing",
    "View All Properties",
}

ADDRESS_RE = re.compile(
    r"(?P<address>\d{3,6}\s+.+?\s+(?:Allen|McKinney|Prosper)\s*,?\s*TX\s+\d{5})",
    re.IGNORECASE,
)
PRICE_RE = re.compile(r"\$[\d,]+")
SQFT_RE = re.compile(r"([\d,]+)\s*Sq\.?\s*Ft\.?", re.IGNORECASE)
BEDS_RE = re.compile(r"(\d+(?:\.\d+)?)\s*Beds?", re.IGNORECASE)
GARAGE_RE = re.compile(r"(\d+(?:\.\d+)?)\s*Car Garage", re.IGNORECASE)
BATHS_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(?:&\s*(\d+)\s*Half\s*)?Baths?", re.IGNORECASE)
STORIES_RE = re.compile(r"(\d+(?:\.\d+)?)\s*Stories?", re.IGNORECASE)
LOT_RE = re.compile(r"((?:Block\s+[A-Z],?\s*)?Lot\s+\d+)", re.IGNORECASE)
LOT_WIDTH_RE = re.compile(r"(\d+)'\s*Wide Lot", re.IGNORECASE)
STATUS_RE = re.compile(r"\bReady\s+(?:Now|January|February|March|April|May|June|July|August|September|October|November|December)\b", re.IGNORECASE)


async def fetch_html(url: str, use_playwright: bool) -> str:
    """Fetch rendered HTML.

    Playwright is used because builder sites often render/modify listing sections after initial load.
    """
    if use_playwright:
        from playwright.async_api import async_playwright

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page(user_agent="Mozilla/5.0 GRBK inventory research tracker")
            await page.goto(url, wait_until="networkidle", timeout=90000)

            # Scroll to force lazy-loaded listing sections to render.
            for _ in range(5):
                await page.mouse.wheel(0, 2500)
                await page.wait_for_timeout(900)

            html = await page.content()
            await browser.close()
            return html

    import requests
    response = requests.get(
        url,
        timeout=30,
        headers={"User-Agent": "Mozilla/5.0 GRBK inventory research tracker"},
    )
    response.raise_for_status()
    return response.text


def parse_money(value: str):
    if not value:
        return None
    match = PRICE_RE.search(value)
    if not match:
        return None
    return int(match.group(0).replace("$", "").replace(",", ""))


def parse_int_from_re(regex: re.Pattern, text: str):
    match = regex.search(text or "")
    if not match:
        return None
    return int(match.group(1).replace(",", ""))


def parse_float_from_re(regex: re.Pattern, text: str):
    match = regex.search(text or "")
    if not match:
        return None
    return float(match.group(1))


def parse_baths(text: str):
    match = BATHS_RE.search(text or "")
    if not match:
        return None
    full = float(match.group(1))
    half = 0.5 if match.group(2) else 0.0
    return full + half


def normalize_address(text: str):
    if not text:
        return None
    text = clean_text(text)
    match = ADDRESS_RE.search(text)
    if not match:
        return None

    address = match.group("address")
    address = re.sub(r"\s+,", ",", address)
    address = re.sub(r"\s+", " ", address).strip()
    address = address.replace(" Texas ", " TX ")
    return address


def normalized_lines_from_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    raw_lines = soup.get_text("\n", strip=True).splitlines()
    lines = []
    for line in raw_lines:
        line = clean_text(line)
        # Remove common decorative bullets / repeated separators.
        if line in {"*", "* * *", "•"}:
            continue
        if line:
            lines.append(line)
    return lines


def get_view_listing_urls(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    urls = []
    for a in soup.find_all("a", href=True):
        text = clean_text(a.get_text(" ", strip=True))
        if text == "View Listing":
            urls.append(urljoin(base_url, a["href"]))
    return urls


def find_qmi_section(lines: List[str]) -> Tuple[int, int]:
    """Return start/end line indexes for the actual Quick Move-in Homes section.

    The phrase appears in top navigation too, so we require the explanatory sentence
    that appears under the real section.
    """
    start = -1
    for i, line in enumerate(lines):
        if line == "Quick Move-in Homes":
            lookahead = " ".join(lines[i:i + 4])
            if "currently under construction or available now" in lookahead:
                start = i
                break

    if start == -1:
        return -1, -1

    end = len(lines)
    for j in range(start + 1, len(lines)):
        if lines[j] in STOP_HEADERS or lines[j].startswith("## "):
            # Do not stop on the start header itself; stop on later major sections.
            end = j
            break
    return start, end


def is_plan_line(line: str) -> bool:
    if not line:
        return False
    if line in SKIP_LINES:
        return False
    if STATUS_RE.search(line):
        return False
    if "New Lower Price" in line:
        return False
    if normalize_address(line):
        return False
    if PRICE_RE.search(line):
        return False
    if any(regex.search(line) for regex in [SQFT_RE, BEDS_RE, GARAGE_RE, BATHS_RE, LOT_RE, LOT_WIDTH_RE, STORIES_RE]):
        return False
    if line.startswith("From "):
        return False
    if len(line) > 60:
        return False
    return True


def split_qmi_blocks(section_lines: List[str]) -> List[List[str]]:
    """Split Southgate QMI section into individual home blocks.

    Expected pattern:
    plan -> optional New Lower Price -> status -> address -> specs -> lot -> price(s) -> Save -> View Listing.
    """
    blocks = []
    current = []

    for idx, line in enumerate(section_lines):
        if line in SKIP_LINES:
            if line == "View Listing" and current:
                current.append(line)
                blocks.append(current)
                current = []
            continue

        # Start a new block when a plausible plan line is followed by status/address soon after.
        next_few = " | ".join(section_lines[idx:idx + 6])
        starts_new = is_plan_line(line) and (STATUS_RE.search(next_few) or ADDRESS_RE.search(next_few))

        if starts_new:
            if current:
                blocks.append(current)
            current = [line]
        elif current:
            current.append(line)

    if current:
        blocks.append(current)

    return blocks


def parse_southgate_block(
    block: List[str],
    brand_cfg: Dict,
    url_meta: Dict,
    source_url: str,
    snapshot_date: str,
    listing_url: str = None,
) -> Dict:
    raw_text = " | ".join(block)

    plan = block[0] if block and is_plan_line(block[0]) else None
    status_match = STATUS_RE.search(raw_text)
    status = status_match.group(0).title() if status_match else None

    address = None
    for line in block:
        address = normalize_address(line)
        if address:
            break

    lot = None
    for line in block:
        match = LOT_RE.search(line)
        if match:
            lot = clean_text(match.group(1).title().replace("Block ", "Block "))
            break

    prices = []
    for line in block:
        if line.startswith("From "):
            continue
        prices.extend([int(p.replace("$", "").replace(",", "")) for p in PRICE_RE.findall(line)])

    price = prices[0] if prices else None
    prior_price = prices[1] if len(prices) > 1 else None

    sqft = None
    beds = None
    baths = None
    garage = None

    for line in block:
        if sqft is None:
            sqft = parse_int_from_re(SQFT_RE, line)
        if beds is None:
            beds = parse_float_from_re(BEDS_RE, line)
        if baths is None:
            baths = parse_baths(line)
        if garage is None:
            garage = parse_float_from_re(GARAGE_RE, line)

    incentive_bits = []
    if "New Lower Price" in raw_text:
        incentive_bits.append("New Lower Price")
    line_level_incentive = extract_incentive_text(raw_text)
    if line_level_incentive:
        incentive_bits.append(line_level_incentive)

    community = url_meta.get("community") or brand_cfg.get("community")
    market = url_meta.get("market") or brand_cfg.get("market")
    city = url_meta.get("city")
    state = url_meta.get("state", "TX")

    row = {
        "snapshot_date": snapshot_date,
        "brand": brand_cfg["brand"],
        "market": market,
        "source_url": source_url,
        "url": listing_url or source_url,
        "community": community,
        "address": address,
        "lot": lot,
        "plan": plan,
        "status": status,
        "price": price,
        "prior_price": prior_price,
        "sqft": sqft,
        "beds": beds,
        "baths": baths,
        "garage": garage,
        "incentive_text": " | ".join(dict.fromkeys(incentive_bits)) if incentive_bits else None,
        "raw_text": raw_text[:2500],
        "qa_flag": None,
    }

    # Stable ID: address is the strongest unique key. Lot/community backstop if address changes formatting.
    row["home_key"] = make_home_key(
        row["brand"],
        row["address"],
        row["community"],
        row["lot"],
        None if row["address"] else row["url"],
    )

    flags = []
    if not row["address"]:
        flags.append("missing_address")
    if row["price"] is None:
        flags.append("missing_price")
    if row["sqft"] is None:
        flags.append("missing_sqft")
    if row["beds"] is None:
        flags.append("missing_beds")
    if row["baths"] is None:
        flags.append("missing_baths")
    if row["garage"] is None:
        flags.append("missing_garage")
    if not row["status"]:
        flags.append("missing_status")
    row["qa_flag"] = ";".join(flags) if flags else None

    return row


def is_valid_southgate_row(row: Dict) -> bool:
    """Reject community/floorplan/nav rows.

    For research use, we require the minimum data that proves this is an individual home.
    """
    return bool(
        row.get("address")
        and row.get("price") is not None
        and row.get("sqft") is not None
        and row.get("beds") is not None
        and row.get("baths") is not None
    )


async def scrape_southgate_community(
    brand_cfg: Dict,
    url_meta: Dict,
    snapshot_date: str,
    use_playwright: bool,
) -> List[Dict]:
    source_url = url_meta["url"]
    html = await fetch_html(source_url, use_playwright=use_playwright)
    lines = normalized_lines_from_html(html)
    start, end = find_qmi_section(lines)

    if start == -1:
        print(f"{brand_cfg['brand']} | {source_url}: no QMI section found")
        return []

    section_lines = lines[start + 1:end]
    blocks = split_qmi_blocks(section_lines)
    listing_urls = get_view_listing_urls(html, source_url)

    rows = []
    for idx, block in enumerate(blocks):
        listing_url = listing_urls[idx] if idx < len(listing_urls) else None
        row = parse_southgate_block(block, brand_cfg, url_meta, source_url, snapshot_date, listing_url)
        if is_valid_southgate_row(row):
            rows.append(row)

    print(
        f"{brand_cfg['brand']} | {url_meta.get('community', source_url)}: "
        f"{len(blocks)} parsed blocks, {len(rows)} valid home listings"
    )
    return rows


def normalize_url_entry(entry: Union[str, Dict]) -> Dict:
    if isinstance(entry, str):
        return {"url": entry}
    return dict(entry)


async def scrape_brand(brand_cfg: Dict, snapshot_date: str, use_playwright: bool, delay: float) -> List[Dict]:
    parser = brand_cfg.get("parser", "generic")
    rows = []

    for entry in brand_cfg["urls"]:
        url_meta = normalize_url_entry(entry)

        if parser == "southgate_community":
            rows.extend(await scrape_southgate_community(brand_cfg, url_meta, snapshot_date, use_playwright))
        else:
            # Safety: for now, do not run broad generic scraping in research mode.
            # Each GRBK brand should get its own tested parser to avoid false signals.
            print(f"Skipping {brand_cfg['brand']} because parser={parser} is not implemented/tested yet.")

        time.sleep(delay)

    # Deduplicate at individual-home level.
    deduped = {}
    for row in rows:
        key = (row["brand"], row["home_key"])
        deduped[key] = row

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
    parser = argparse.ArgumentParser(description="Scrape tested GRBK public brand listings into a daily CSV snapshot.")
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
