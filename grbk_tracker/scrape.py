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
CURRENT_PRICE_LINE_RE = re.compile(r"^\$[\d,]+$")
SHOWING_TOTAL_RE = re.compile(r"Showing\s+\d+\s+of\s+(\d+)\s+Quick\s+Move-?In\s+Homes", re.IGNORECASE)
STATUS_RE = re.compile(
    r"\b(?:Ready\s+(?:Now|January|February|March|April|May|June|July|August|September|October|November|December)|Available Date:\s*Now|Est Completion Date:\s*[A-Za-z]+\s+\d{4}|Available Now|Quick Move-?In|Under Construction)\b",
    re.IGNORECASE,
)
LOT_RE = re.compile(r"((?:Block\s+[A-Z],?\s*)?Lot\s+\d+)", re.IGNORECASE)
SQFT_RE = re.compile(r"([\d,]+)\s*SQ\s*FT", re.IGNORECASE)
BEDS_RE = re.compile(r"(\d+(?:\.\d+)?)\s*Beds?", re.IGNORECASE)
BATHS_RE = re.compile(r"(\d+(?:\.\d+)?)\s*Baths?", re.IGNORECASE)
STREET_SUFFIX_PATTERN = (
    r"Street|St\.?|Road|Rd\.?|Drive|Dr\.?|Lane|Ln\.?|Court|Ct\.?|Trail|Way|"
    r"Circle|Cir\.?|Avenue|Ave\.?|Boulevard|Blvd\.?|Loop|Run|Bend|Parkway|Pkwy\.?"
)
STREET_RE = re.compile(
    rf"\b\d{{3,6}}\s+[A-Za-z0-9 .'-]+?\s+(?:{STREET_SUFFIX_PATTERN})\b",
    re.IGNORECASE,
)
FULL_ADDRESS_RE = re.compile(
    rf"\b\d{{3,6}}\s+[A-Za-z0-9 .'-]+?\s+(?:{STREET_SUFFIX_PATTERN})\s+"
    r"[A-Za-z .'-]+,?\s+(?:TX|Texas)\s+\d{5}\b",
    re.IGNORECASE,
)

# Cities visible in current Trophy market pages + Southgate pages.
CITY_STATE_RE = re.compile(
    r"\b(?:Aledo|Alvarado|Aubrey|Austin|Celina|Crowley|Elgin|Farmersville|Forney|Fort Worth|Greenville|Gunter|Haslet|Huffman|Hutto|Lago Vista|Lavon|McKinney|Pilot Point|Ponder|Princeton|Prosper|Seagoville|Waxahachie|Allen)\s*,?\s*(?:TX|Texas)\s+\d{5}\b",
    re.IGNORECASE,
)

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
    "The  Spring  Sales Collection",
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


async def fetch_html(url: str, use_playwright: bool, click_load_more: bool = False) -> str:
    if use_playwright:
        from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page(user_agent="Mozilla/5.0 GRBK inventory research tracker")
            await page.goto(url, wait_until="networkidle", timeout=90000)

            # Force lazy-loaded content to render.
            for _ in range(5):
                await page.mouse.wheel(0, 2500)
                await page.wait_for_timeout(700)

            if click_load_more:
                # Trophy pages show "Showing 12 of X Quick Move-In Homes Load 12 More".
                # Click until the button disappears or stops adding content.
                last_showing_text = ""
                for _ in range(60):  # enough for more than 700 homes at 12 per click
                    text = await page.locator("body").inner_text()
                    showing = SHOWING_TOTAL_RE.search(text)
                    current_showing_text = showing.group(0) if showing else ""
                    if current_showing_text == last_showing_text and "Load 12 More" not in text:
                        break
                    last_showing_text = current_showing_text

                    try:
                        button = page.get_by_text("Load 12 More", exact=True)
                        if await button.count() == 0:
                            break
                        await button.first.click(timeout=5000)
                        await page.wait_for_timeout(1200)
                        await page.mouse.wheel(0, 2500)
                    except PlaywrightTimeoutError:
                        break
                    except Exception:
                        break

            html = await page.content()
            await browser.close()
            return html

    import requests
    response = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0 GRBK inventory research tracker"})
    response.raise_for_status()
    return response.text


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


def parse_price_value(value: str):
    match = PRICE_RE.search(value or "")
    if not match:
        return None
    return int(match.group(0).replace("$", "").replace(",", ""))


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
    if ("new lower price" in block_text or "save:" in block_text or "was" in block_text or "reduced" in block_text) and len(prices) >= 2:
        return min(prices), max(prices)

    return prices[0], None


def parse_int(regex, text):
    m = regex.search(text or "")
    if not m:
        return None
    return int(m.group(1).replace(",", ""))


def parse_float(regex, text):
    m = regex.search(text or "")
    if not m:
        return None
    return float(m.group(1))


def normalize_address(address: str):
    address = re.sub(r"\bTexas\b", "TX", address or "", flags=re.IGNORECASE)
    address = re.sub(r"\s+", " ", address)
    address = re.sub(r"\s+,", ",", address)
    return clean_text(address)


def extract_full_address(text: str):
    match = FULL_ADDRESS_RE.search(text or "")
    if not match:
        return None
    return normalize_address(match.group(0))


def address_key(address: str):
    return clean_text(address).lower()


def extract_address_from_lines(lines: List[str], i: int):
    window = " ".join(lines[i:i + 4])
    full_address = extract_full_address(window)
    if full_address:
        return full_address

    street = STREET_RE.search(window)
    city = CITY_STATE_RE.search(window)

    if street and city:
        return normalize_address(f"{street.group(0)} {city.group(0)}")

    full = re.search(
        r"(\d{3,6}\s+.+?\s+(?:Aledo|Alvarado|Aubrey|Austin|Celina|Crowley|Elgin|Farmersville|Forney|Fort Worth|Greenville|Gunter|Haslet|Huffman|Hutto|Lago Vista|Lavon|McKinney|Pilot Point|Ponder|Princeton|Prosper|Seagoville|Waxahachie|Allen)\s*,?\s*(?:TX|Texas)\s+\d{5})",
        window,
        flags=re.IGNORECASE,
    )
    if full:
        return normalize_address(full.group(1))

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
        if label in {"View Detail", "View Listing"}:
            urls.append(urljoin(base_url, a["href"]))
    return urls


def trophy_address_url_map(html: str, base_url: str):
    soup = BeautifulSoup(html, "lxml")
    urls_by_address = {}
    for a in soup.find_all("a", href=True):
        address = extract_full_address(clean_text(a.get_text(" ", strip=True)))
        if not address:
            continue
        urls_by_address[address_key(address)] = urljoin(base_url, a["href"])
    return urls_by_address


def extract_expected_trophy_total(lines: List[str]):
    text = " ".join(lines)
    match = SHOWING_TOTAL_RE.search(text)
    if not match:
        return None
    return int(match.group(1).replace(",", ""))


def extract_community(raw_text: str):
    m = re.search(r"Community\s+([A-Za-z0-9 &'./-]+?)\s+Floor Plan", raw_text)
    if m:
        return clean_text(m.group(1))
    return None


def extract_plan(raw_text: str):
    m = re.search(r"Floor Plan\s+([A-Za-z0-9 &'./|-]+?)(?:\s+View Detail|\s+View Listing|$)", raw_text)
    if m:
        plan = clean_text(m.group(1))
        # Remove common artifacts that can leak into the plan capture.
        plan = re.sub(r"\s+Image:.*$", "", plan).strip()
        return plan
    return None


def extract_status(raw_text: str):
    m = STATUS_RE.search(raw_text or "")
    if not m:
        return None
    status = clean_text(m.group(0))
    status = status.replace("Available Date:", "Available")
    return status


def parse_listing_block(block_lines, address, brand_cfg, url_meta, source_url, snapshot_date, listing_url=None):
    raw_text = " | ".join(block_lines)
    price, prior_price = extract_prices(block_lines)

    community = extract_community(raw_text) or url_meta.get("community")
    plan = extract_plan(raw_text)
    status = extract_status(raw_text)

    lot = None
    lot_m = LOT_RE.search(raw_text)
    if lot_m:
        lot = clean_text(lot_m.group(1).title())

    price_pressure = []
    if prior_price is not None and price is not None and prior_price > price:
        price_pressure.append("Price Cut")
    if "new lower price" in raw_text.lower():
        price_pressure.append("New Lower Price")
    if "save:" in raw_text.lower():
        price_pressure.append("Savings Shown")

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
        "plan": plan,
        "status": status,
        "price": price,
        "prior_price": prior_price,
        "sqft": parse_int(SQFT_RE, raw_text),
        "beds": parse_float(BEDS_RE, raw_text),
        "baths": parse_float(BATHS_RE, raw_text),
        "garage": None,
        "incentive_text": " | ".join(dict.fromkeys(price_pressure)) if price_pressure else None,
        "raw_text": raw_text[:2500],
        "qa_flag": None,
    }

    row["home_key"] = make_home_key(row["brand"], row["address"], row["community"], row["lot"], row["url"])

    flags = []
    if not row["address"]:
        flags.append("missing_address")
    if row["price"] is None:
        flags.append("missing_price")
    if not row["community"]:
        flags.append("missing_community")
    row["qa_flag"] = ";".join(flags) if flags else None

    return row


def is_valid_listing(row: Dict) -> bool:
    return bool(row.get("address") and row.get("price") is not None)


def is_trophy_current_price_line(line: str):
    if not CURRENT_PRICE_LINE_RE.match(line or ""):
        return False
    price = parse_price_value(line)
    return price is not None and price >= 100000


def trophy_price_indices(lines: List[str]):
    return [i for i, line in enumerate(lines) if is_trophy_current_price_line(line)]


def trophy_prices_from_block(block_lines: List[str]):
    current = parse_price_value(block_lines[0]) if block_lines else None
    if current is None:
        return None, None

    prior_candidates = []
    for line in block_lines[1:]:
        for raw_price in PRICE_RE.findall(line):
            price = int(raw_price.replace("$", "").replace(",", ""))
            # Ignore mortgage payments and incentive amounts like Save: $7,000.
            if price >= 100000 and price > current:
                prior_candidates.append(price)

    prior = max(prior_candidates) if prior_candidates else None
    return current, prior


def parse_trophy_listing_block(block_lines, brand_cfg, url_meta, source_url, snapshot_date, address_urls):
    raw_text = " | ".join(block_lines)
    address = extract_full_address(raw_text)
    price, prior_price = trophy_prices_from_block(block_lines)
    listing_url = address_urls.get(address_key(address)) if address else None

    community = extract_community(raw_text)
    plan = extract_plan(raw_text)
    status = extract_status(raw_text)

    price_pressure = []
    if prior_price is not None and price is not None and prior_price > price:
        price_pressure.append("Price Cut")
    if "new lower price" in raw_text.lower():
        price_pressure.append("New Lower Price")
    if "save:" in raw_text.lower():
        price_pressure.append("Savings Shown")

    row = {
        "snapshot_date": snapshot_date,
        "brand": brand_cfg["brand"],
        "market": url_meta.get("market") or brand_cfg.get("market"),
        "source_url": source_url,
        "url": listing_url or source_url,
        "home_key": None,
        "community": community,
        "address": address,
        "lot": None,
        "plan": plan,
        "status": status,
        "price": price,
        "prior_price": prior_price,
        "sqft": parse_int(SQFT_RE, raw_text),
        "beds": parse_float(BEDS_RE, raw_text),
        "baths": parse_float(BATHS_RE, raw_text),
        "garage": None,
        "incentive_text": " | ".join(dict.fromkeys(price_pressure)) if price_pressure else None,
        "raw_text": raw_text[:2500],
        "qa_flag": None,
    }

    row["home_key"] = make_home_key(row["brand"], row["address"], row["community"], None, row["url"])

    flags = []
    if not row["address"]:
        flags.append("missing_address")
    if row["price"] is None:
        flags.append("missing_price")
    if not row["community"]:
        flags.append("missing_community")
    if not listing_url:
        flags.append("missing_detail_url")
    row["qa_flag"] = ";".join(flags) if flags else None

    return row


def dedupe_rows(rows: List[Dict]):
    deduped = {}
    for row in rows:
        key = (
            row.get("brand"),
            row.get("market"),
            address_key(row.get("address") or ""),
            row.get("url") or "",
        )
        deduped[key] = row
    return list(deduped.values())


def append_qa_flag(row: Dict, flag: str):
    existing = row.get("qa_flag")
    row["qa_flag"] = f"{existing};{flag}" if existing else flag


def qmi_window_or_all(lines: List[str]) -> List[str]:
    """Southgate-specific: prefer the Quick Move-in Homes section if there is one."""
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


async def scrape_address_based_page(
    brand_cfg: Dict,
    url_meta: Dict,
    snapshot_date: str,
    use_playwright: bool,
    click_load_more: bool,
    southgate_qmi_only: bool = False,
):
    source_url = url_meta["url"]
    html = await fetch_html(source_url, use_playwright=use_playwright, click_load_more=click_load_more)

    lines = normalize_lines(html)
    if southgate_qmi_only:
        lines = qmi_window_or_all(lines)

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

    rows = dedupe_rows(rows)
    print(
        f"{brand_cfg['brand']} | {url_meta.get('market', url_meta.get('community', source_url))}: "
        f"{len(address_points)} address candidates, {len(rows)} valid listings"
    )
    return rows


async def scrape_trophy_market_page(
    brand_cfg: Dict,
    url_meta: Dict,
    snapshot_date: str,
    use_playwright: bool,
):
    source_url = url_meta["url"]
    html = await fetch_html(source_url, use_playwright=use_playwright, click_load_more=True)
    lines = normalize_lines(html)
    expected_total = extract_expected_trophy_total(lines)
    address_urls = trophy_address_url_map(html, source_url)

    price_points = trophy_price_indices(lines)
    rows = []
    for n, idx in enumerate(price_points):
        next_idx = price_points[n + 1] if n + 1 < len(price_points) else len(lines)
        block = lines[idx:next_idx]
        row = parse_trophy_listing_block(
            block_lines=block,
            brand_cfg=brand_cfg,
            url_meta=url_meta,
            source_url=source_url,
            snapshot_date=snapshot_date,
            address_urls=address_urls,
        )
        if is_valid_listing(row):
            rows.append(row)

    rows = dedupe_rows(rows)
    if expected_total is not None and len(rows) != expected_total:
        mismatch = f"trophy_count_mismatch_expected_{expected_total}_got_{len(rows)}"
        for row in rows:
            append_qa_flag(row, mismatch)
        print(f"WARNING {brand_cfg['brand']} | {url_meta.get('market')}: {mismatch}")

    print(
        f"{brand_cfg['brand']} | {url_meta.get('market', source_url)}: "
        f"{len(price_points)} price cards, {len(rows)} valid listings"
        + (f", expected {expected_total}" if expected_total is not None else "")
    )
    return rows


def normalize_url_entry(entry: Union[str, Dict]) -> Dict:
    return {"url": entry} if isinstance(entry, str) else dict(entry)


async def scrape_brand(brand_cfg: Dict, snapshot_date: str, use_playwright: bool, delay: float):
    parser = brand_cfg.get("parser")
    rows = []

    for entry in brand_cfg["urls"]:
        url_meta = normalize_url_entry(entry)

        if parser == "southgate_community":
            rows.extend(await scrape_address_based_page(
                brand_cfg, url_meta, snapshot_date, use_playwright,
                click_load_more=False, southgate_qmi_only=True
            ))
        elif parser == "trophy_market":
            rows.extend(await scrape_trophy_market_page(
                brand_cfg, url_meta, snapshot_date, use_playwright
            ))
        else:
            print(f"Skipping {brand_cfg['brand']} because parser={parser} is not implemented/tested yet.")

        time.sleep(delay)

    return dedupe_rows(rows)


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
    errors = []
    for brand_cfg in cfg["brands"]:
        try:
            rows = await scrape_brand(brand_cfg, args.date, use_playwright, delay)
            print(f"{brand_cfg['brand']}: {len(rows)} usable listing rows")
            all_rows.extend(rows)
        except Exception as exc:
            error = f"ERROR scraping {brand_cfg.get('brand', 'UNKNOWN')}: {exc}"
            print(error)
            errors.append(error)

    if errors:
        raise SystemExit("\n".join(errors))

    out_path = write_snapshot(all_rows, args.date, args.out_dir)
    print(f"Saved {len(all_rows)} rows to {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
