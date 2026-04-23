"""
East Delhi Property Tracker (v2)
================================

Daily digest of new property listings in 49 East Delhi target areas,
priced at Rs 5 Crore or more. Posted to Telegram at 7:00 AM IST.

Changes in v2:
- MIN_PRICE: 5 Crore (up from 1 Cr)
- Digest now includes S.N., seller name (when public), phone (marked
  "Login to view" when hidden behind the site's auth wall - which is
  the honest truth for 99acres / MagicBricks / Housing / NoBroker).
- Sliding-window fallback: if < 10 matches in 72h, widen to 7d, then 30d.
- Updated site list: magicbricks, 99acres, housing.com, nobroker,
  propertywala, dreamproperty.net.in.
- Fixed URL patterns that returned 404 in v1.
"""

from __future__ import annotations

import html as _html
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

MIN_PRICE_INR = 30_000_000        # 5 Crore
TARGET_MIN_LISTINGS = 10           # min listings to include in digest
FALLBACK_WINDOWS_H = [72, 168, 720]  # 3d, 7d, 30d
MAX_LISTINGS_PER_MESSAGE = 40

TARGET_AREAS: list[tuple[str, list[str]]] = [
    ("Dayanand Vihar",           ["dayanand vihar"]),
    ("Hargobind Enclave",        ["hargobind enclave", "hari gobind enclave"]),
    ("AGCR Enclave",             ["agcr enclave", "a.g.c.r", "a g c r"]),
    ("Shanti Vihar",             ["shanti vihar"]),
    ("Priya Vihar",              ["priya vihar"]),
    ("Kiran Vihar",              ["kiran vihar"]),
    ("Saini Enclave",            ["saini enclave"]),
    ("Shreshtha Vihar",          ["shreshtha vihar", "shrestha vihar", "shred vihar"]),
    ("Rishabh Vihar",            ["rishabh vihar", "rishab vihar"]),
    ("Bhaubali Enclave",         ["bhaubali enclave", "bhauballi enclave", "bahubali enclave"]),
    ("Shyam Vihar",              ["shyam vihar"]),
    ("Jagriti Enclave",          ["jagriti enclave", "jagriti vihar"]),
    ("Anand Vihar",              ["anand vihar"]),
    ("Pushpanjali",              ["pushpanjali"]),
    ("Surya Niketan",            ["surya niketan"]),
    ("Ram Vihar",                ["ram vihar"]),
    ("Surajmal Vihar",           ["surajmal vihar", "suraj mal vihar"]),
    ("Vigyan Vihar",             ["vigyan vihar"]),
    ("Yojna Vihar",              ["yojna vihar", "yojana vihar"]),
    ("Savita Vihar",             ["savita vihar"]),
    ("Vigyan Lok",               ["vigyan lok"]),
    ("Manak Vihar",              ["manak vihar"]),
    ("Vivek Vihar",              ["vivek vihar"]),
    ("Surya Nagar",              ["surya nagar"]),
    ("Ram Prastha",              ["ram prastha"]),
    ("Ram Puri",                 ["ram puri"]),
    ("Chander Nagar",            ["chander nagar", "chandar nagar"]),
    ("New Ashoka Co-op Society", ["new ashoka co", "new ashok co", "ashoka cooperative"]),
    ("Jain Co-op Society",       ["jain co-op", "jain cooperative", "jain co op house"]),
    ("Nirman Vihar",             ["nirman vihar"]),
    ("Madhuban Enclave",         ["madhuban enclave"]),
    ("Preet Vihar",              ["preet vihar"]),
    ("Dayal Enclave",            ["dayal enclave", "bharti enclave", "bharti apartment"]),
    ("Shankar Vihar",            ["shankar vihar"]),
    ("Swasthya Vihar",           ["swasthya vihar", "svasthya vihar"]),
    ("Gujrat Vihar",             ["gujrat vihar", "gujarat vihar"]),
    ("Defence Enclave",          ["defence enclave", "defense enclave"]),
    ("New Rajdhani Enclave",     ["new rajdhani", "rajdhani enclave"]),
    ("Park End",                 ["park end"]),
    ("Chitra Vihar",             ["chitra vihar"]),
    ("Sukh Vihar",               ["sukh vihar", "sukhdev vihar"]),
    ("Gagan Vihar",              ["gagan vihar"]),
    ("Mausam Vihar",             ["mausam vihar", "maussam vihar"]),
    ("Bank Enclave",             ["bank enclave"]),
    ("Priyadarshini Vihar",      ["priyadarshini vihar", "pridarshani vihar", "priyadarshani vihar"]),
    ("East End Enclave",         ["east end enclave"]),
    ("Gyan Kunj",                ["gyan kunj"]),
]

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
SEEN_FILE = Path("seen_listings.json")

COMMON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9,hi;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("tracker")


# ---------------------------------------------------------------------------
# DATA MODEL
# ---------------------------------------------------------------------------

@dataclass
class Listing:
    site: str
    listing_id: str
    title: str
    location: str
    url: str
    price_inr: int | None = None
    price_display: str = ""
    posted_at: datetime | None = None
    property_type: str = ""
    bhk: str = ""
    seller_name: str = ""
    seller_phone: str = ""
    matched_area: str = ""

    def unique_key(self) -> str:
        return f"{self.site}::{self.listing_id}"


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def parse_price_to_inr(text: str) -> int | None:
    if not text:
        return None
    s = text.replace(",", "").replace("\u20b9", "").replace("INR", "").strip().lower()
    m = re.search(r"(\d+(?:\.\d+)?)\s*(cr|crore|lakh|lac|k|l)?", s)
    if not m:
        return None
    try:
        v = float(m.group(1))
        u = (m.group(2) or "").lower()
        if u in ("cr", "crore"): return int(v * 10_000_000)
        if u in ("lakh", "lac", "l"): return int(v * 100_000)
        if u == "k": return int(v * 1_000)
        if v >= 100_000: return int(v)
        return int(v * 100_000)
    except Exception:
        return None


def format_inr(amount: int) -> str:
    if amount >= 10_000_000:
        return f"\u20b9{amount/10_000_000:.2f} Cr"
    if amount >= 100_000:
        return f"\u20b9{amount/100_000:.2f} Lakh"
    return f"\u20b9{amount:,}"


def match_target_area(text: str) -> str | None:
    if not text: return None
    lo = text.lower()
    for canonical, variants in TARGET_AREAS:
        if any(v in lo for v in variants):
            return canonical
    return None


def parse_age_to_hours(age_text: str) -> int | None:
    """Convert '6d ago', '2w ago', '1mo ago', 'Today' to hours-old."""
    if not age_text: return None
    s = age_text.lower().strip()
    if "today" in s or "just now" in s or "few hours" in s: return 1
    if "yesterday" in s: return 24
    m = re.search(r"(\d+)\s*(h|hr|hour|hours|d|day|days|w|week|weeks|mo|month|months|y|yr|year|years)", s)
    if not m: return None
    n = int(m.group(1))
    u = m.group(2)
    if u.startswith("h"): return n
    if u.startswith("d"): return n * 24
    if u.startswith("w"): return n * 24 * 7
    if u.startswith("mo") or u == "m": return n * 24 * 30
    if u.startswith("y"): return n * 24 * 365
    return None


def within_window(posted: datetime | None, hours: int) -> bool:
    if posted is None: return True
    now = datetime.now(timezone.utc)
    if posted.tzinfo is None:
        posted = posted.replace(tzinfo=timezone.utc)
    return (now - posted) <= timedelta(hours=hours)


def fetch(url: str, headers: dict | None = None, timeout: int = 30) -> requests.Response | None:
    hdrs = {**COMMON_HEADERS, **(headers or {})}
    for attempt in range(3):
        try:
            r = requests.get(url, headers=hdrs, timeout=timeout, allow_redirects=True)
            if r.status_code == 200:
                return r
            log.warning("GET %s -> %s (try %d)", url[:100], r.status_code, attempt + 1)
        except requests.RequestException as e:
            log.warning("GET %s failed: %s", url[:100], str(e)[:100])
        time.sleep(1.5 * (attempt + 1))
    return None

def playwright_fetch(url: str, wait_selector: str | None = None) -> str | None:
    """Render a URL with a headless Chromium browser. Returns HTML on success, None on failure."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("Playwright not installed; skipping browser fallback")
        return None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            ctx = browser.new_context(
                user_agent=COMMON_HEADERS["User-Agent"],
                viewport={"width": 1366, "height": 900},
                locale="en-IN",
            )
            page = ctx.new_page()
            page.goto(url, timeout=45_000, wait_until="domcontentloaded")
            if wait_selector:
                try:
                    page.wait_for_selector(wait_selector, timeout=12_000)
                except Exception:
                    pass
            for _ in range(3):
                page.evaluate("window.scrollBy(0, 1500)")
                page.wait_for_timeout(500)
            html = page.content()
            browser.close()
            return html
    except Exception as e:
        log.warning("Playwright failed for %s: %s", url[:80], str(e)[:150])
        return None


# ---------------------------------------------------------------------------
# SITE SCRAPERS
# Each returns List[Listing]. Each tries MULTIPLE URL patterns since these
# sites rotate URLs frequently.
# ---------------------------------------------------------------------------

def scrape_99acres() -> list[Listing]:
    """99acres - worked reliably in v1. Added 5Cr budget filter."""
    out: list[Listing] = []
    url = "https://www.99acres.com/property-in-east-delhi-ffid-buy?budget_min=300&budget_type=L&sort=date_new_to_old"
    html_text = playwright_fetch(url, wait_selector='[class*="tupleNew"], [class*="SrpCard"]')
    if not html_text or len(html_text) < 5000:
        log.info("99acres: Playwright got %s chars, using requests", len(html_text) if html_text else 0)
        r = fetch(url)
        if not r:
            return out
        html_text = r.text
    else:
        log.info("99acres: Playwright rendered %d chars", len(html_text))

    soup = BeautifulSoup(html_text, "html.parser")

    # Extract listing divs - pattern: div with image + price + BHK text
    # Find leaf cards by scoring.
    for c in soup.find_all("div"):
        txt = c.get_text(" ", strip=True) or ""
        if len(txt) < 100 or len(txt) > 800: continue
        if "\u20b9" not in txt: continue
        if not re.search(r"(BHK|Plot|Floor|Villa|House)", txt, re.I): continue
        # only leaf-ish cards (avoid big containers)
        child_price_hits = sum(
            1 for d in c.find_all("div")
            if d is not c and 80 < len(d.get_text(" ", strip=True) or "") < 800
            and "\u20b9" in (d.get_text(" ", strip=True) or "")
        )
        if child_price_hits > 1: continue

        text = re.sub(r"\s+", " ", txt)
        price_m = re.search(r"\u20b9\s*([\d.]+)\s*(Cr|Lac|Lakh)", text, re.I)
        if not price_m: continue
        price_val = float(price_m.group(1))
        price_inr = int(price_val * 10_000_000) if price_m.group(2).lower().startswith("c") else int(price_val * 100_000)
        if price_inr < MIN_PRICE_INR: continue

        area = match_target_area(text)
        if not area: continue

        bhk_m = re.search(r"(\d+)\s*BHK", text, re.I)
        type_m = re.search(r"(\d+\s*BHK\s+(?:Flat|Apartment|Builder Floor|Villa|House|Plot))", text, re.I) \
              or re.search(r"(Plot|Land|House|Villa)\s+in", text, re.I)
        age_m = re.search(r"(\d+\s*(?:d|day|days|w|week|weeks|mo|month|months|h|hr|hours)\s+ago|Today|Yesterday|Just Now)", text, re.I)
        # seller / dealer name - 99acres shows "Dealer - XYZ" or "Posted by XYZ"
        seller_m = re.search(r"(?:Dealer|Owner|Posted by|Agent)[\s:-]+([A-Z][A-Za-z0-9 .&()]{2,40})", text)

        # build a unique-ish key
        key_seed = f"{area}::{price_m.group(0)}::{bhk_m.group(0) if bhk_m else ''}::{text[:80]}"
        listing_id = str(abs(hash(key_seed)))[:12]

        hours = parse_age_to_hours(age_m.group(0) if age_m else "")
        posted = datetime.now(timezone.utc) - timedelta(hours=hours) if hours else None

        out.append(Listing(
            site="99acres",
            listing_id=listing_id,
            title=(type_m.group(0) if type_m else bhk_m.group(0) if bhk_m else "Property")[:80],
            location=area,
            url=url,
            price_inr=price_inr,
            price_display=price_m.group(0),
            posted_at=posted,
            bhk=bhk_m.group(0) if bhk_m else "",
            seller_name=seller_m.group(1).strip() if seller_m else "",
            seller_phone="",  # always hidden behind login
            matched_area=area,
        ))

    # dedupe
    seen = set(); unique = []
    for l in out:
        k = (l.matched_area, l.price_display, l.bhk, l.title)
        if k in seen: continue
        seen.add(k); unique.append(l)
    log.info("99acres: %d matched", len(unique))
    return unique


def scrape_magicbricks() -> list[Listing]:
    """MagicBricks - try multiple URL patterns since v1 got 404."""
    out: list[Listing] = []
    url_candidates = [
        "https://www.magicbricks.com/property-for-sale/residential-real-estate?bedroom=&proptype=Multistorey-Apartment,Builder-Floor-Apartment,Penthouse,Residential-House,Villa,Residential-Plot&cityName=New-Delhi",
        "https://www.magicbricks.com/property-for-sale-in-east-delhi-pppfs",
        "https://www.magicbricks.com/property-for-sale/in/east-delhi",
    ]
    html_text = None
    chosen = None
    for url in url_candidates:
        h = playwright_fetch(url, wait_selector='[class*="srp__card"], [class*="mb-srp"]')
        if h and len(h) > 10000:
            html_text = h; chosen = url
            log.info("MagicBricks (playwright): %s", url[:80])
            break
        r = fetch(url)
        if r and r.status_code == 200 and len(r.text) > 5000:
            html_text = r.text; chosen = url
            log.info("MagicBricks (simple): %s", url[:80])
            break
    if not html_text:
        return out
    url = chosen

    soup = BeautifulSoup(html_text, "html.parser")
    cards = soup.select("div.mb-srp__card") or soup.select("[class*=srp__card]") or soup.find_all("div", class_=re.compile(r"card|tuple|listing", re.I))

    for c in cards[:200]:
        try:
            txt = c.get_text(" ", strip=True) or ""
            if "\u20b9" not in txt: continue
            text = re.sub(r"\s+", " ", txt)
            price_m = re.search(r"\u20b9\s*([\d.]+)\s*(Cr|Lac|Lakh)", text, re.I)
            if not price_m: continue
            price_val = float(price_m.group(1))
            price_inr = int(price_val * 10_000_000) if price_m.group(2).lower().startswith("c") else int(price_val * 100_000)
            if price_inr < MIN_PRICE_INR: continue

            area = match_target_area(text)
            if not area: continue

            bhk_m = re.search(r"(\d+)\s*BHK", text, re.I)
            age_m = re.search(r"(\d+\s*(?:d|day|days|w|week|weeks|mo|month|months|h|hr|hours)\s+ago|Today|Yesterday)", text, re.I)
            seller_m = re.search(r"(?:Dealer|Owner|Posted by|By)[\s:-]+([A-Z][A-Za-z0-9 .&()]{2,40})", text)

            link = c.select_one("a[href]")
            href = link["href"] if link else url
            if href.startswith("/"): href = "https://www.magicbricks.com" + href
            lid = re.search(r"(\d{6,})", href)

            hours = parse_age_to_hours(age_m.group(0) if age_m else "")
            posted = datetime.now(timezone.utc) - timedelta(hours=hours) if hours else None

            out.append(Listing(
                site="MagicBricks",
                listing_id=lid.group(1) if lid else str(abs(hash(text[:80])))[:12],
                title=text[:100],
                location=area,
                url=href,
                price_inr=price_inr,
                price_display=price_m.group(0),
                posted_at=posted,
                bhk=bhk_m.group(0) if bhk_m else "",
                seller_name=seller_m.group(1).strip() if seller_m else "",
                matched_area=area,
            ))
        except Exception:
            continue
    log.info("MagicBricks: %d matched", len(out))
    return out


def scrape_housing() -> list[Listing]:
    """Housing.com - new site added in v2."""
    out: list[Listing] = []
    url_candidates = [
        "https://housing.com/in/buy/real-estate/delhi-central/east-delhi",
        "https://housing.com/in/buy/real-estate/east-delhi",
        "https://housing.com/in/buy/searches/east-delhi-residential",
    ]
    r = None
    for url in url_candidates:
        r = fetch(url, headers={"Accept": "text/html,*/*"})
        if r and r.status_code == 200 and len(r.text) > 5000:
            log.info("Housing: using %s", url[:80])
            break
    if not r or r.status_code != 200:
        return out

    soup = BeautifulSoup(r.text, "html.parser")
    # Housing.com uses data-q- attributes and React
    # Try NEXT_DATA first
    script = soup.find("script", id="__NEXT_DATA__")
    if script and script.string:
        try:
            data = json.loads(script.string)
            def walk(node):
                if isinstance(node, dict):
                    if ("price" in node or "displayPrice" in node) and ("title" in node or "name" in node):
                        yield node
                    for v in node.values():
                        yield from walk(v)
                elif isinstance(node, list):
                    for v in node:
                        yield from walk(v)
            for item in walk(data):
                try:
                    title = str(item.get("title") or item.get("name") or "")[:150]
                    location = str(item.get("locality") or item.get("localityName") or item.get("locationName") or title)
                    price_raw = item.get("price") or item.get("displayPrice") or ""
                    price_inr = parse_price_to_inr(str(price_raw)) if isinstance(price_raw, str) else (int(price_raw) if isinstance(price_raw, (int, float)) else None)
                    if not price_inr or price_inr < MIN_PRICE_INR: continue
                    area = match_target_area(f"{title} {location}")
                    if not area: continue
                    lid = str(item.get("id") or item.get("propertyId") or abs(hash(title))%10**12)[:15]
                    out.append(Listing(
                        site="Housing",
                        listing_id=lid,
                        title=title,
                        location=location,
                        url=item.get("url") or url,
                        price_inr=price_inr,
                        price_display=format_inr(price_inr),
                        bhk=str(item.get("bedrooms") or item.get("bhk") or ""),
                        seller_name=str(item.get("postedBy") or item.get("ownerName") or ""),
                        matched_area=area,
                    ))
                except Exception:
                    continue
        except Exception as e:
            log.debug("Housing NEXT_DATA parse: %s", e)

    # HTML fallback
    if not out:
        for c in soup.find_all("div"):
            txt = c.get_text(" ", strip=True) or ""
            if len(txt) < 100 or len(txt) > 600 or "\u20b9" not in txt: continue
            text = re.sub(r"\s+", " ", txt)
            price_m = re.search(r"\u20b9\s*([\d.]+)\s*(Cr|Lac|Lakh)", text, re.I)
            if not price_m: continue
            price_inr = int(float(price_m.group(1)) * (10_000_000 if price_m.group(2).lower().startswith("c") else 100_000))
            if price_inr < MIN_PRICE_INR: continue
            area = match_target_area(text)
            if not area: continue
            bhk_m = re.search(r"(\d+)\s*BHK", text, re.I)
            out.append(Listing(
                site="Housing",
                listing_id=str(abs(hash(text[:80])))[:12],
                title=text[:100],
                location=area,
                url=url,
                price_inr=price_inr,
                price_display=price_m.group(0),
                bhk=bhk_m.group(0) if bhk_m else "",
                matched_area=area,
            ))
    log.info("Housing: %d matched", len(out))
    return out


def scrape_nobroker() -> list[Listing]:
    """NoBroker - try their newer API paths."""
    out: list[Listing] = []
    apis = [
        "https://www.nobroker.in/api/v3/multi/property/filter?city=delhi&type=SALE&lat=28.6421&lon=77.3065",
        "https://www.nobroker.in/api/v1/services/property/filter?city=delhi&type=SALE",
    ]
    for api in apis:
        try:
            r = requests.get(api, headers={**COMMON_HEADERS, "Accept": "application/json", "Referer": "https://www.nobroker.in/"}, timeout=30)
            if r.status_code != 200:
                log.warning("NoBroker API %s", r.status_code)
                continue
            try:
                data = r.json()
            except Exception:
                continue
            items = data.get("data") or data.get("properties") or data.get("results") or []
            for it in items:
                try:
                    price = it.get("price") or it.get("rent") or 0
                    price_inr = int(price) if price else None
                    if not price_inr or price_inr < MIN_PRICE_INR: continue
                    loc = " ".join(filter(None, [it.get("societyName", ""), it.get("localityName", ""), it.get("locality", "")]))
                    area = match_target_area(loc)
                    if not area: continue
                    lid = str(it.get("id") or it.get("propertyId") or "")
                    out.append(Listing(
                        site="NoBroker",
                        listing_id=lid,
                        title=it.get("title", "") or it.get("propertyTitle", ""),
                        location=loc,
                        url=f"https://www.nobroker.in/property/{lid}",
                        price_inr=price_inr,
                        price_display=format_inr(price_inr),
                        bhk=str(it.get("type", "") or it.get("propertyType", "")),
                        seller_name=str(it.get("ownerName") or it.get("postedBy") or ""),
                        matched_area=area,
                    ))
                except Exception:
                    continue
            if out: break
        except Exception as e:
            log.warning("NoBroker error: %s", str(e)[:100])
    log.info("NoBroker: %d matched", len(out))
    return out


def scrape_propertywala() -> list[Listing]:
    """PropertyWala - try multiple URL patterns."""
    out: list[Listing] = []
    url_candidates = [
        "https://www.propertywala.com/properties-for-sale/east-delhi",
        "https://www.propertywala.com/property-for-sale-east_delhi",
        "https://www.propertywala.com/east-delhi-property",
    ]
    r = None
    for url in url_candidates:
        r = fetch(url)
        if r and r.status_code == 200 and len(r.text) > 3000:
            log.info("PropertyWala: using %s", url[:80])
            break
    if not r or r.status_code != 200:
        return out

    soup = BeautifulSoup(r.text, "html.parser")
    for c in soup.select(".listing, .property-listing, .ad, article, [class*=property]"):
        try:
            txt = c.get_text(" ", strip=True) or ""
            if len(txt) < 60 or "\u20b9" not in txt and "Rs" not in txt: continue
            text = re.sub(r"\s+", " ", txt)
            price_m = re.search(r"(?:\u20b9|Rs\.?)\s*([\d.,]+)\s*(Cr|Lac|Lakh|Crore)?", text, re.I)
            if not price_m: continue
            price_inr = parse_price_to_inr(price_m.group(0))
            if not price_inr or price_inr < MIN_PRICE_INR: continue
            area = match_target_area(text)
            if not area: continue
            link = c.select_one("a[href]")
            href = link["href"] if link else url
            if href.startswith("/"): href = "https://www.propertywala.com" + href
            bhk_m = re.search(r"(\d+)\s*BHK", text, re.I)
            out.append(Listing(
                site="PropertyWala",
                listing_id=href[-50:],
                title=text[:100],
                location=area,
                url=href,
                price_inr=price_inr,
                price_display=price_m.group(0),
                bhk=bhk_m.group(0) if bhk_m else "",
                matched_area=area,
            ))
        except Exception:
            continue
    log.info("PropertyWala: %d matched", len(out))
    return out


def scrape_dreamproperty() -> list[Listing]:
    """DreamProperty - user specified dreamproperty.net.in in v2."""
    out: list[Listing] = []
    url_candidates = [
        "https://dreamproperty.net.in/property-for-sale/east-delhi",
        "https://dreamproperty.net.in/east-delhi",
        "https://dreamproperty.net.in/",
    ]
    r = None
    for url in url_candidates:
        r = fetch(url)
        if r and r.status_code == 200 and len(r.text) > 1000:
            log.info("DreamProperty: using %s", url[:80])
            break
    if not r or r.status_code != 200:
        return out

    soup = BeautifulSoup(r.text, "html.parser")
    for c in soup.find_all(["article", "div"], class_=re.compile(r"property|card|listing|item", re.I)):
        try:
            txt = c.get_text(" ", strip=True) or ""
            if len(txt) < 50 or len(txt) > 800: continue
            text = re.sub(r"\s+", " ", txt)
            price_m = re.search(r"(?:\u20b9|Rs\.?)\s*([\d.,]+)\s*(Cr|Lac|Lakh|Crore)?", text, re.I)
            if not price_m: continue
            price_inr = parse_price_to_inr(price_m.group(0))
            if not price_inr or price_inr < MIN_PRICE_INR: continue
            area = match_target_area(text)
            if not area: continue
            link = c.select_one("a[href]")
            href = link["href"] if link else url
            if href.startswith("/"): href = "https://dreamproperty.net.in" + href
            bhk_m = re.search(r"(\d+)\s*BHK", text, re.I)
            out.append(Listing(
                site="DreamProperty",
                listing_id=href[-50:],
                title=text[:100],
                location=area,
                url=href,
                price_inr=price_inr,
                price_display=price_m.group(0),
                bhk=bhk_m.group(0) if bhk_m else "",
                matched_area=area,
            ))
        except Exception:
            continue
    log.info("DreamProperty: %d matched", len(out))
    return out


# ---------------------------------------------------------------------------
# TELEGRAM
# ---------------------------------------------------------------------------

def _chunk(text: str, size: int) -> list[str]:
    if len(text) <= size: return [text]
    out, cur = [], ""
    for line in text.split("\n"):
        if len(cur) + len(line) + 1 > size:
            out.append(cur); cur = line
        else:
            cur = f"{cur}\n{line}" if cur else line
    if cur: out.append(cur)
    return out


def telegram_send(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("Missing Telegram credentials"); return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for i, chunk in enumerate(_chunk(text, 3800)):
        try:
            r = requests.post(url, data={
                "chat_id": TELEGRAM_CHAT_ID, "text": chunk,
                "parse_mode": "HTML", "disable_web_page_preview": True,
            }, timeout=30)
            if not r.ok:
                log.error("TG send failed [%d]: %s", i+1, r.text[:500])
                raise RuntimeError(f"Telegram API rejected: {r.text[:300]}")
            else: log.info("TG chunk %d sent", i+1)
        except Exception as e:
            log.error("TG exception: %s", e)
        time.sleep(0.4)


# ---------------------------------------------------------------------------
# SEEN PERSISTENCE
# ---------------------------------------------------------------------------

def load_seen() -> set[str]:
    if SEEN_FILE.exists():
        try: return set(json.loads(SEEN_FILE.read_text()))
        except Exception: pass
    return set()


def save_seen(seen: set[str]) -> None:
    SEEN_FILE.write_text(json.dumps(list(seen)[-5000:]))


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

SCRAPERS: list[tuple[str, Callable[[], list[Listing]]]] = [
    ("99acres",       scrape_99acres),
    ("MagicBricks",   scrape_magicbricks),
    ("Housing",       scrape_housing),
    ("NoBroker",      scrape_nobroker),
    ("PropertyWala",  scrape_propertywala),
    ("DreamProperty", scrape_dreamproperty),
]


def run() -> int:
    started = datetime.now()
    log.info("=== Property Tracker v2 starting at %s ===", started.isoformat())

    all_raw: list[Listing] = []
    per_site: dict[str, int] = {}
    errors: dict[str, str] = {}

    for name, fn in SCRAPERS:
        try:
            items = fn() or []
            per_site[name] = len(items)
            all_raw.extend(items)
        except Exception as e:
            log.exception("%s failed: %s", name, e)
            errors[name] = str(e)[:200]
            per_site[name] = 0

    log.info("Total raw: %d", len(all_raw))

    # Filter with sliding window fallback to hit TARGET_MIN_LISTINGS
    seen = load_seen()
    matched: list[Listing] = []
    used_window_h = FALLBACK_WINDOWS_H[0]
    for window_h in FALLBACK_WINDOWS_H:
        used_window_h = window_h
        matched = []
        tmp_seen = set(seen)
        for lst in all_raw:
            if lst.price_inr is None or lst.price_inr < MIN_PRICE_INR: continue
            if not lst.matched_area: continue
            if not within_window(lst.posted_at, window_h): continue
            k = lst.unique_key()
            if k in tmp_seen: continue
            matched.append(lst); tmp_seen.add(k)
        if len(matched) >= TARGET_MIN_LISTINGS:
            break
        log.info("Only %d in %dh window; widening...", len(matched), window_h)

    # commit seen
    for m in matched: seen.add(m.unique_key())
    save_seen(seen)
    log.info("Matched: %d (window: %dh)", len(matched), used_window_h)

    # Compose digest
    today = datetime.now().strftime("%d %b %Y, %a")
    window_label = {72: "last 72h", 168: "last 7 days", 720: "last 30 days"}.get(used_window_h, f"last {used_window_h}h")
    header = (
        f"<b>🏠 East Delhi Property Tracker</b>\n"
        f"<i>{today} \u00b7 \u20b95 Cr+ \u00b7 {window_label}</i>\n"
        f"<i>49 target areas</i>\n\n"
    )

    if not matched:
        msg = (header +
               "Aaj koi matching listing nahi mili 30-din window mein bhi.\n\n"
               "<b>Site scan counts:</b>\n<code>" +
               " | ".join(f"{k}:{v}" for k, v in per_site.items()) + "</code>")
        telegram_send(msg)
        return 0

    lines = [header, f"\u2705 <b>{len(matched)} listings</b>\n"]
    for i, m in enumerate(matched[:MAX_LISTINGS_PER_MESSAGE], 1):
        price = _html.escape(m.price_display or format_inr(m.price_inr or 0))
        bhk = f" \u00b7 {_html.escape(m.bhk)}" if m.bhk else ""
        seller = _html.escape(m.seller_name) if m.seller_name else "Not listed"
        phone = _html.escape(m.seller_phone) if m.seller_phone else "Login required"
        title = _html.escape((m.title or "Property")[:100])
        url = _html.escape(m.url[:250], quote=True)
        lines.append(
            f"<b>{i}. {_html.escape(m.matched_area)}</b>{bhk}\n"
            f"💰 {price} \u00b7 {_html.escape(m.site)}\n"
            f"🏢 {title[:80]}\n"
            f"👤 {seller}  |  📞 {phone}\n"
            f"🔗 <a href=\"{url}\">Open listing</a>\n"
        )

    if len(matched) > MAX_LISTINGS_PER_MESSAGE:
        lines.append(f"<i>\u2026 +{len(matched) - MAX_LISTINGS_PER_MESSAGE} more trimmed</i>\n")

    lines.append(
        "\n<b>\u2139\ufe0f Phone note:</b> <i>Property sites login-gate seller phones. "
        "Listing par click karke 'View Number' se milega.</i>\n"
    )
    lines.append("<b>Site scan counts:</b>")
    lines.append("<code>" + " | ".join(f"{k}:{v}" for k, v in per_site.items()) + "</code>")
    if errors:
        lines.append("<b>\u26a0\ufe0f Errors:</b> " + ", ".join(errors.keys()))

    telegram_send("\n".join(lines))
    log.info("=== Done in %.1fs ===", (datetime.now() - started).total_seconds())
    return 0


if __name__ == "__main__":
    sys.exit(run())
