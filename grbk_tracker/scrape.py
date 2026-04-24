import argparse
import asyncio
import json
import time
from datetime import date
from pathlib import Path
from typing import Dict, List
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from .utils import (
    BATH_RE, BED_RE, GARAGE_RE, SQFT_RE, clean_text, extract_address,
    extract_incentive_text, extract_lot, extract_status, make_home_key,
    parse_float, parse_int, parse_price, parse_prior_price,
)

LISTING_KEYWORDS = [
    "ready now", "available now", "quick move", "move-in", "move in", "under construction",
    "homesite", "home site", "lot", "sq. ft", "sq ft", "beds", "baths", "garage", "new lower price"
]

COMMON_CARD_SELECTORS = [
    "article", "li", ".card", ".home-card", ".listing", ".available-home", ".qmi",
    ".home", ".property", "[class*='home']", "[class*='listing']", "[class*='card']"
]

SNAPSHOT_COLUMNS = [
    "snapshot_date", "brand", "market", "source_url", "url", "home_key", "community",
    "address", "lot", "plan", "status", "price", "prior_price", "sqft", "beds", "baths",
    "garage", "incentive_text", "raw_text", "qa_flag"
]


async def fetch_html(url: str, use_playwright: bool) -> str:
    if use_playwright:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page(user_agent="Mozilla/5.0 GRBK inventory research tracker")
            await page.goto(url, wait_until="networkidle", timeout=60000)
            html = await page.content()
            await browser.close()
            return html

    import requests
    response = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0 GRBK inventory research tracker"})
    response.raise_for_status()
    return response.text


def looks_like_listing(text: str) -> bool:
    lower = text.lower()
    keyword_score = sum(1 for word in LISTING_KEYWORDS if word in lower)
    has_price = "$" in text
    useful_length = 80 <= len(text) <= 2800
    return useful_length and ((has_price and keyword_score >= 2) or keyword_score >= 4)


def extract_candidate_cards(html: str, base_url: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    candidates = []
    seen = set()

    for selector in COMMON_CARD_SELECTORS:
        for node in soup.select(selector):
            text = clean_text(node.get_text(" ", strip=True))
            if not looks_like_listing(text) or text in seen:
                continue
            seen.add(text)
            link = node.find("a", href=True)
            candidates.append({
                "raw_text": text,
                "url": urljoin(base_url, link["href"]) if link else base_url,
            })
    return candidates


def normalize_candidate(card: Dict[str, str], brand_cfg: Dict, snapshot_date: str, source_url: str) -> Dict:
    raw_text = card["raw_text"]
    brand = brand_cfg["brand"]
    address = extract_address(raw_text)
    lot = extract_lot(raw_text)
    community = None  # Brand-specific parsers can improve this later.

    row = {
        "snapshot_date": snapshot_date,
        "brand": brand,
        "market": brand_cfg.get("market"),
        "source_url": source_url,
        "url": card.get("url"),
        "community": community,
        "address": address,
        "lot": lot,
        "plan": None,
        "status": extract_status(raw_text),
        "price": parse_price(raw_text),
        "prior_price": parse_prior_price(raw_text),
        "sqft": parse_int(SQFT_RE, raw_text),
        "beds": parse_float(BED_RE, raw_text),
        "baths": parse_float(BATH_RE, raw_text),
        "garage": parse_float(GARAGE_RE, raw_text),
        "incentive_text": extract_incentive_text(raw_text),
        "raw_text": raw_text[:2500],
        "qa_flag": None,
    }
    row["home_key"] = make_home_key(brand, address, community, lot, row["url"])

    flags = []
    if not row["address"]:
        flags.append("missing_address")
    if row["price"] is None:
        flags.append("missing_price")
    if not row["status"]:
        flags.append("missing_status")
    row["qa_flag"] = ";".join(flags) if flags else None
    return row


async def scrape_brand(brand_cfg: Dict, snapshot_date: str, use_playwright: bool, delay: float) -> List[Dict]:
    rows = []
    for source_url in brand_cfg["urls"]:
        html = await fetch_html(source_url, use_playwright=use_playwright)
        cards = extract_candidate_cards(html, source_url)
        for card in cards:
            rows.append(normalize_candidate(card, brand_cfg, snapshot_date, source_url))
        time.sleep(delay)

    deduped = {}
    for row in rows:
        deduped[(row["brand"], row["home_key"])] = row
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
    parser = argparse.ArgumentParser(description="Scrape GRBK public brand listings into a daily CSV snapshot.")
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
            print(f"{brand_cfg['brand']}: {len(rows)} candidate listings")
            all_rows.extend(rows)
        except Exception as exc:
            print(f"ERROR scraping {brand_cfg['brand']}: {exc}")

    out_path = write_snapshot(all_rows, args.date, args.out_dir)
    print(f"Saved {len(all_rows)} rows to {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
