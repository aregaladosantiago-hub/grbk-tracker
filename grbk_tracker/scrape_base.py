import argparse
import asyncio
import json
import re
import time
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
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
LOAD_MORE_RE = re.compile(r"Load(?:\s+\d+)?\s+More", re.IGNORECASE)
SHOWING_TOTAL_RE = re.compile(
    r"Showing\s+\d+\s+of\s+(\d+)\s+Quick\s+Move-?In\s+Homes",
    re.IGNORECASE,
)
STATUS_RE = re.compile(
    r"\b(?:Ready\s+(?:Now|January|February|March|April|May|June|July|August|September|October|November|December)|"
    r"Available Date:?\s*(?:\|\s*)?Now|"
    r"Est Completion Date:?\s*(?:\|\s*)?[A-Za-z]+\s+\d{4}|"
    r"Move-?In Ready|Move-?In:?\s*\d{2}/\d{4}|Available Now|Quick Move-?In|Under Construction)\b",
    re.IGNORECASE,
)
LOT_RE = re.compile(r"((?:Block\s+[A-Z],?\s*)?Lot\s+\d+)", re.IGNORECASE)
SQFT_RE = re.compile(r"([\d,]+)\s*SQ\s*FT", re.IGNORECASE)
BEDS_RE = re.compile(r"(\d+(?:\.\d+)?)\s*Beds?", re.IGNORECASE)
BATHS_RE = re.compile(r"(\d+(?:\.\d+)?)\s*Baths?", re.IGNORECASE)
STREET_SUFFIX_PATTERN = (
    r"Street|St\.?|Road|Rd\.?|Drive|Dr\.?|Lane|Ln\.?|Court|Ct\.?|Trail|Trl\.?|Way|"
    r"Circle|Cir\.?|Avenue|Ave\.?|Boulevard|Blvd\.?|Loop|Run|Bend|Parkway|Pkwy\.?|"
    r"Place|Pl\.?|Terrace|Trace|Pass|Crossing|Cove|Row|Path|Square|Ridge|Hollow|Landing|Glen|Springs"
)
STREET_RE = re.compile(
    rf"(?<![$,\d])\b(?!\d{{3,6}}\s+\d{{3,6}}\b)\d{{3,6}}\s+[A-Za-z .'-]+?\s+(?:{STREET_SUFFIX_PATTERN})\b",
    re.IGNORECASE,
)
PREFIX_STREET_RE = re.compile(
    r"(?<![$,\d])\b(?!\d{3,6}\s+\d{3,6}\b)\d{3,6}\s+(?:N|S|E|W|NE|NW|SE|SW)?\.?\s*(?:Via|Rio)\s+[A-Za-z .'-]+\b",
    re.IGNORECASE,
)
FULL_ADDRESS_RE = re.compile(
    rf"(?<![$,\d])\b(?!\d{{3,6}}\s+\d{{3,6}}\b)\d{{3,6}}\s+[A-Za-z .'-]+?\s+(?:{STREET_SUFFIX_PATTERN})\s+"
    r"[A-Za-z .'-]+,?\s+(?:TX|Texas|GA|Georgia|FL|Florida)\s+\d{5}\b",
    re.IGNORECASE,
)

CITY_STATE_RE = re.compile(
    r"\b[A-Za-z .'-]+,?\s+(?:TX|Texas|GA|Georgia|FL|Florida)\s+\d{5}\b",
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
        from playwright.async_api import TimeoutError as PlaywrightTimeoutError
        from playwright.async_api import async_playwright

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page(user_agent="Mozilla/5.0 GRBK inventory research tracker")
            # Some builder pages keep analytics/background requests open, so a hard
            # networkidle wait can fail even after the listing HTML has loaded.
            await page.goto(url, wait_until="domcontentloaded", timeout=90000)
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except PlaywrightTimeoutError:
                pass

            for _ in range(5):
                await page.mouse.wheel(0, 2500)
                await page.wait_for_timeout(700)

            if click_load_more:
                # Trophy's last button is often "Load 1 More", "Load 9 More", etc.
                # The old exact "Load 12 More" selector stopped early and triggered QA failures.
                last_showing_text = ""
                for _ in range(80):
                    text = await page.locator("body").inner_text()
                    showing = SHOWING_TOTAL_RE.search(text)
                    current_showing_text = showing.group(0) if showing else ""
                    if current_showing_text == last_showing_text and not LOAD_MORE_RE.search(text):
                        break
                    last_showing_text = current_showing_text

                    try:
                        button = page.get_by_role("button", name=LOAD_MORE_RE)
                        if await button.count() == 0:
                            button = page.get_by_text(LOAD_MORE_RE)
                        if await button.count() == 0:
                            break
                        await button.first.scroll_into_view_if_needed(timeout=5000)
                        await button.first.click(timeout=5000)
                        await page.wait_for_timeout(1300)
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


def parse_price_value(value: str) -> Optional[int]:
    match = PRICE_RE.search(value or "")
    if not match:
        return None
    return int(match.group(0).replace("$", "").replace(",", ""))


def extract_prices(block_lines: List[str]) -> Tuple[Optional[int], Optional[int]]:
    prices = []
    for line in block_lines:
        if line.lower().startswith("from "):
            continue
        for raw_price in PRICE_RE.findall(line):
            price = int(raw_price.replace("$", "").replace(",", ""))
            if price >= 100000:
                prices.append(price)

    if not prices:
        return None, None

    block_text = " ".join(block_lines).lower()
    has_cut_language = any(term in block_text for term in ("new lower price", "save", "was", "reduced"))
    has_descending_prices = len(prices) >= 2 and prices[0] > prices[1]
    if (has_cut_language or has_descending_prices) and len(prices) >= 2:
        current = min(prices)
        prior_candidates = [price for price in prices if price > current]
        return current, max(prior_candidates) if prior_candidates else None

    return prices[0], None


def parse_int(regex, text):
    match = regex.search(text or "")
    if not match:
        return None
    return int(match.group(1).replace(",", ""))


def parse_float(regex, text):
    match = regex.search(text or "")
    if not match:
        return None
    return float(match.group(1))


def normalize_address(address: str) -> str:
    address = re.sub(r"^\d{3,6}\s+(?=\d{3,6}\s+[A-Za-z])", "", address or "")
    address = re.sub(r"\bTexas\b", "TX", address, flags=re.IGNORECASE)
    address = re.sub(r"\bGeorgia\b", "GA", address, flags=re.IGNORECASE)
    address = re.sub(r"\bFlorida\b", "FL", address, flags=re.IGNORECASE)
    address = re.sub(r"\s+", " ", address)
    address = re.sub(r"\s+,", ",", address)
    return clean_text(address)


def extract_full_address(text: str) -> Optional[str]:
    match = FULL_ADDRESS_RE.search(text or "")
    if not match:
        return None
    return normalize_address(match.group(0))


def extract_street_address(text: str) -> Optional[str]:
    street = STREET_RE.search(text or "") or PREFIX_STREET_RE.search(text or "")
    return clean_text(street.group(0)) if street else None


def fallback_city_state(url_meta: Dict) -> Optional[str]:
    city = clean_text(url_meta.get("city"))
    state = clean_text(url_meta.get("state"))
    if not city or not state:
        return None

    zip_code = clean_text(str(url_meta.get("zip", "")))
    suffix = f"{city}, {state}"
    if zip_code:
        suffix = f"{suffix} {zip_code}"
    return suffix


def address_key(address: str) -> str:
    return clean_text(address).lower()


def extract_address_from_lines(lines: List[str], i: int, url_meta: Optional[Dict] = None) -> Optional[str]:
    current_line = lines[i]
    full_address = extract_full_address(current_line)
    if full_address:
        return full_address

    street = extract_street_address(current_line)
    if not street:
        return None

    window = " ".join(lines[i:i + 4])
    full_address = extract_full_address(window)
    if full_address:
        return full_address

    city = CITY_STATE_RE.search(window)
    if street and city:
        return normalize_address(f"{street} {city.group(0)}")

    fallback = fallback_city_state(url_meta or {})
    if street and fallback:
        return normalize_address(f"{street} {fallback}")

    return None


def find_address_indices(lines: List[str], url_meta: Optional[Dict] = None) -> List[Tuple[int, str]]:
    indices = []
    seen = set()
    for i in range(len(lines)):
        address = extract_address_from_lines(lines, i, url_meta=url_meta)
        key = address_key(address or "")
        if address and key not in seen:
            seen.add(key)
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


def trophy_address_url_map(html: str, base_url: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "lxml")
    urls_by_address = {}
    for a in soup.find_all("a", href=True):
        address = extract_full_address(clean_text(a.get_text(" ", strip=True)))
        if address:
            urls_by_address[address_key(address)] = urljoin(base_url, a["href"])
    return urls_by_address


def extract_expected_trophy_total(lines: List[str]) -> Optional[int]:
    text = " ".join(lines)
    match = SHOWING_TOTAL_RE.search(text)
    if not match:
        return None
    return int(match.group(1).replace(",", ""))


def split_segments(raw_text: str) -> List[str]:
    return [clean_text(part) for part in (raw_text or "").split("|") if clean_text(part)]


def labeled_value(raw_text: str, label: str) -> Optional[str]:
    label_key = label.lower().rstrip(":")
    segments = split_segments(raw_text)
    for i, segment in enumerate(segments[:-1]):
        if segment.lower().rstrip(":") == label_key:
            value = segments[i + 1]
            if value.lower().rstrip(":") not in {"community", "floor plan", "view detail", "view listing"}:
                return value
    return None


def extract_community(raw_text: str) -> Optional[str]:
    value = labeled_value(raw_text, "Community")
    if value:
        return value

    match = re.search(r"Community\s+([A-Za-z0-9 &'./-]+?)\s+Floor Plan", raw_text or "", re.IGNORECASE)
    if match:
        return clean_text(match.group(1))
    return None


def extract_plan(raw_text: str) -> Optional[str]:
    value = labeled_value(raw_text, "Floor Plan")
    if value:
        return re.sub(r"\s+Image:.*$", "", value).strip()

    match = re.search(
        r"Floor Plan\s+([A-Za-z0-9 &'./|-]+?)(?:\s+View Detail|\s+View Listing|$)",
        raw_text or "",
        re.IGNORECASE,
    )
    if match:
        plan = clean_text(match.group(1))
        return re.sub(r"\s+Image:.*$", "", plan).strip()
    return None


def extract_status(raw_text: str) -> Optional[str]:
    match = STATUS_RE.search(raw_text or "")
    if match:
        status = clean_text(match.group(0))
        status = status.replace("Available Date:", "Available")
        status = status.replace("Available Date", "Available")
        status = status.replace("Est Completion Date:", "Est Completion")
        status = status.replace("Est Completion Date", "Est Completion")
        return status

    available = labeled_value(raw_text, "Available Date")
    if available:
        return f"Available {available}"

    completion = labeled_value(raw_text, "Est Completion Date")
    if completion:
        return f"Est Completion {completion}"

    return None


def price_pressure_flags(raw_text: str, price: Optional[int], prior_price: Optional[int]) -> List[str]:
    flags = []
    lower = (raw_text or "").lower()
    if prior_price is not None and price is not None and prior_price > price:
        flags.append("Price Cut")
    if "new lower price" in lower:
        flags.append("New Lower Price")
    if "save:" in lower:
        flags.append("Savings Shown")
    return list(dict.fromkeys(flags))


def row_qa_flags(row: Dict, require_community: bool = False) -> Optional[str]:
    flags = []
    if not row.get("address"):
        flags.append("missing_address")
    if row.get("price") is None:
        flags.append("missing_price")
    if require_community and not row.get("community"):
        flags.append("missing_community")
    return ";".join(flags) if flags else None


def parse_listing_block(block_lines, address, brand_cfg, url_meta, source_url, snapshot_date, listing_url=None):
    raw_text = " | ".join(block_lines)
    price, prior_price = extract_prices(block_lines)
    community = extract_community(raw_text) or url_meta.get("community")
    pressure = price_pressure_flags(raw_text, price, prior_price)

    lot = None
    lot_match = LOT_RE.search(raw_text)
    if lot_match:
        lot = clean_text(lot_match.group(1).title())

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
        "plan": extract_plan(raw_text),
        "status": extract_status(raw_text),
        "price": price,
        "prior_price": prior_price,
        "sqft": parse_int(SQFT_RE, raw_text),
        "beds": parse_float(BEDS_RE, raw_text),
        "baths": parse_float(BATHS_RE, raw_text),
        "garage": None,
        "incentive_text": " | ".join(pressure) if pressure else None,
        "raw_text": raw_text[:2500],
        "qa_flag": None,
    }

    row["home_key"] = make_home_key(row["brand"], row["address"], row["community"], row["lot"], None)
    row["qa_flag"] = row_qa_flags(row)
    return row


def parse_trophy_listing_block(
    block_lines,
    address,
    current_price,
    prior_price,
    brand_cfg,
    url_meta,
    source_url,
    snapshot_date,
    address_urls,
):
    raw_text = " | ".join(block_lines)
    listing_url = address_urls.get(address_key(address)) if address else None
    community = extract_community(raw_text)
    pressure = price_pressure_flags(raw_text, current_price, prior_price)

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
        "plan": extract_plan(raw_text),
        "status": extract_status(raw_text),
        "price": current_price,
        "prior_price": prior_price,
        "sqft": parse_int(SQFT_RE, raw_text),
        "beds": parse_float(BEDS_RE, raw_text),
        "baths": parse_float(BATHS_RE, raw_text),
        "garage": None,
        "incentive_text": " | ".join(pressure) if pressure else None,
        "raw_text": raw_text[:2500],
        "qa_flag": None,
    }

    row["home_key"] = make_home_key(row["brand"], row["address"], row["community"], None, None)
    row["qa_flag"] = row_qa_flags(row)
    return row


def is_valid_listing(row: Dict) -> bool:
    return bool(row.get("address") and row.get("price") is not None)


def dedupe_rows(rows: List[Dict]) -> List[Dict]:
    deduped = {}
    for row in rows:
        identity = address_key(row.get("address") or row.get("url") or "")
        deduped[(row.get("brand"), row.get("market"), identity)] = row
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


def card_start_before_address(lines: List[str], address_idx: int, lower_bound: int) -> int:
    start = max(lower_bound, address_idx - 8)
    for j in range(address_idx - 1, start - 1, -1):
        if lines[j] in {"View Listing", "View Detail"}:
            return j + 1
    return start


def compact_card_start_before_address(lines: List[str], address_idx: int, lower_bound: int) -> int:
    start = max(lower_bound, address_idx - 3)
    for j in range(address_idx - 1, start - 1, -1):
        if STATUS_RE.search(lines[j] or "") and not PRICE_RE.search(lines[j] or ""):
            return j
    return address_idx


def trophy_address_points(lines: List[str]) -> List[Tuple[int, str]]:
    points = []
    seen = set()
    for i in range(len(lines)):
        address = extract_address_from_lines(lines, i)
        key = address_key(address or "")
        if address and key not in seen:
            seen.add(key)
            points.append((i, address))
    return points


def is_trophy_price_line(line: str) -> bool:
    if not CURRENT_PRICE_LINE_RE.match(line or ""):
        return False
    price = parse_price_value(line)
    return price is not None and price >= 100000


def real_home_price_lines(lines: List[str], start: int, end: int) -> List[Tuple[int, int]]:
    prices = []
    for i in range(max(0, start), min(len(lines), end)):
        if is_trophy_price_line(lines[i]):
            prices.append((i, parse_price_value(lines[i])))
    return prices


def choose_current_and_prior(candidates: List[Tuple[int, int]]) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    if not candidates:
        return None, None, None
    current_idx, current_price = min(candidates, key=lambda item: item[1])
    prior_prices = [price for _, price in candidates if price > current_price]
    return current_idx, current_price, max(prior_prices) if prior_prices else None


def trophy_price_window(
    lines: List[str],
    address_points: List[Tuple[int, str]],
    n: int,
) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    address_idx, _ = address_points[n]
    floor = address_points[n - 1][0] + 1 if n > 0 else 0
    candidates = real_home_price_lines(lines, floor, address_idx)
    if candidates:
        return choose_current_and_prior(candidates)

    ceiling = address_points[n + 1][0] if n + 1 < len(address_points) else min(len(lines), address_idx + 35)
    return choose_current_and_prior(real_home_price_lines(lines, address_idx, ceiling))


def configured_section_window(lines: List[str], brand_cfg: Dict, url_meta: Dict) -> List[str]:
    start_label = url_meta.get("section_start") or brand_cfg.get("section_start")
    if not start_label:
        return lines

    start = None
    needle = clean_text(start_label).lower()
    for i, line in enumerate(lines):
        if needle in line.lower():
            start = i
            break

    if start is None:
        return lines

    end = len(lines)
    end_labels = url_meta.get("section_end") or brand_cfg.get("section_end") or []
    end_needles = [clean_text(label).lower() for label in end_labels if clean_text(label)]
    for j in range(start + 1, len(lines)):
        if any(needle in lines[j].lower() for needle in end_needles):
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
    wide_card_lookback: bool = False,
):
    source_url = url_meta["url"]
    html = await fetch_html(source_url, use_playwright=use_playwright, click_load_more=click_load_more)

    lines = normalize_lines(html)
    if southgate_qmi_only:
        lines = qmi_window_or_all(lines)
    lines = configured_section_window(lines, brand_cfg, url_meta)

    address_points = find_address_indices(lines, url_meta=url_meta)
    view_urls = listing_urls_from_html(html, source_url)

    rows = []
    for n, (idx, address) in enumerate(address_points):
        lower_bound = address_points[n - 1][0] + 1 if n > 0 else 0
        next_idx = address_points[n + 1][0] if n + 1 < len(address_points) else min(len(lines), idx + 35)
        if wide_card_lookback:
            start = card_start_before_address(lines, idx, lower_bound)
        else:
            start = compact_card_start_before_address(lines, idx, lower_bound)
        block = lines[start:next_idx]

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
    address_points = trophy_address_points(lines)
    price_windows = [trophy_price_window(lines, address_points, n) for n in range(len(address_points))]

    rows = []
    for n, (address_idx, address) in enumerate(address_points):
        current_idx, current_price, prior_price = price_windows[n]
        if current_idx is None or current_price is None:
            continue

        next_price_idx = next((window[0] for window in price_windows[n + 1:] if window[0] is not None), None)
        next_address_idx = address_points[n + 1][0] if n + 1 < len(address_points) else None
        block_end_candidates = [
            idx for idx in (next_price_idx, next_address_idx)
            if idx is not None and idx > current_idx
        ]
        block_start = current_idx
        block_end = min(block_end_candidates) if block_end_candidates else min(len(lines), address_idx + 40)
        block = lines[block_start:block_end]

        row = parse_trophy_listing_block(
            block_lines=block,
            address=address,
            current_price=current_price,
            prior_price=prior_price,
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
        f"{len(address_points)} address cards, {len(rows)} valid listings"
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
                click_load_more=False, southgate_qmi_only=True, wide_card_lookback=True
            ))
        elif parser == "address_based":
            rows.extend(await scrape_address_based_page(
                brand_cfg,
                url_meta,
                snapshot_date,
                use_playwright,
                click_load_more=bool(url_meta.get("click_load_more", brand_cfg.get("click_load_more", False))),
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
