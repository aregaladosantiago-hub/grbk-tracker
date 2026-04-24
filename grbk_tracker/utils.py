import hashlib
import re
from typing import Optional

PRICE_RE = re.compile(r"\$\s*([0-9][0-9,]*)")
SQFT_RE = re.compile(r"([0-9][0-9,]*)\s*(?:sq\.?\s*ft\.?|square\s*feet)", re.I)
BED_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*(?:beds?|bedrooms?)", re.I)
BATH_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*(?:baths?|bathrooms?)", re.I)
GARAGE_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*(?:car\s*)?garage", re.I)
LOT_RE = re.compile(r"(?:lot|homesite|home\s*site)\s*#?\s*([A-Za-z0-9\-]+)", re.I)

STATUS_TERMS = [
    "Ready Now", "Available Now", "Quick Move-In", "Move-In Ready",
    "Under Construction", "Coming Soon", "New Lower Price", "Ready Soon",
]

INCENTIVE_KEYWORDS = [
    "incentive", "special", "save", "savings", "new lower price", "lower price",
    "closing cost", "rate", "buydown", "limited time", "promotion", "discount"
]

STREET_SUFFIXES = {
    "street", "st", "st.", "road", "rd", "rd.", "lane", "ln", "ln.",
    "court", "ct", "ct.", "drive", "dr", "dr.", "avenue", "ave", "ave.",
    "trail", "way", "circle", "cir", "place", "pl", "boulevard", "blvd",
    "parkway", "pkwy", "terrace", "trace"
}


def clean_text(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def parse_price(text: str) -> Optional[int]:
    matches = PRICE_RE.findall(text or "")
    if not matches:
        return None
    return int(matches[0].replace(",", ""))


def parse_prior_price(text: str) -> Optional[int]:
    matches = PRICE_RE.findall(text or "")
    if len(matches) < 2:
        return None
    first = int(matches[0].replace(",", ""))
    second = int(matches[1].replace(",", ""))
    return second if second > first else None


def parse_int(regex, text: str) -> Optional[int]:
    match = regex.search(text or "")
    if not match:
        return None
    return int(match.group(1).replace(",", ""))


def parse_float(regex, text: str) -> Optional[float]:
    match = regex.search(text or "")
    if not match:
        return None
    return float(match.group(1))


def extract_lot(text: str) -> Optional[str]:
    match = LOT_RE.search(text or "")
    return match.group(1) if match else None


def extract_status(text: str) -> Optional[str]:
    lower = (text or "").lower()
    for term in STATUS_TERMS:
        if term.lower() in lower:
            return term
    ready_month = re.search(r"ready\s+(january|february|march|april|may|june|july|august|september|october|november|december)", lower, re.I)
    if ready_month:
        return ready_month.group(0).title()
    return None


def extract_address(text: str) -> Optional[str]:
    tokens = clean_text(text).replace(",", " ").split()
    for i, token in enumerate(tokens):
        if token.lower().strip(".") in {s.strip('.') for s in STREET_SUFFIXES}:
            start = max(0, i - 5)
            end = min(len(tokens), i + 3)
            candidate = clean_text(" ".join(tokens[start:end]))
            if re.search(r"\d", candidate):
                return candidate
    return None


def extract_incentive_text(text: str) -> Optional[str]:
    lower = (text or "").lower()
    if not any(k in lower for k in INCENTIVE_KEYWORDS):
        return None
    chunks = re.split(r"(?<=[.!?])\s+|\s{2,}", clean_text(text))
    hits = [c for c in chunks if any(k in c.lower() for k in INCENTIVE_KEYWORDS)]
    return " | ".join(hits[:3]) if hits else None


def make_home_key(brand: str, address: Optional[str], community: Optional[str], lot: Optional[str], url: Optional[str]) -> str:
    # Prefer address + community + lot. Fall back to URL when address is unavailable.
    base = "|".join([brand or "", address or "", community or "", lot or ""])
    if not clean_text(base.replace("|", "")):
        base = "|".join([brand or "", url or ""])
    return hashlib.sha1(base.lower().encode("utf-8")).hexdigest()[:18]
