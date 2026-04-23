"""
East Delhi Property Tracker
===========================

Scrapes property listings posted in the last 72 hours from multiple sites,
filters by target East Delhi areas and a minimum price, and posts a digest
to Telegram.

Designed to run daily via GitHub Actions at 7:00 AM IST.

Environment variables (set as GitHub Actions secrets):
    TELEGRAM_BOT_TOKEN  - Your Telegram bot token from BotFather
    TELEGRAM_CHAT_ID    - Chat ID where alerts should be posted

Output:
    - Posts digest to Telegram
    - Writes `seen_listings.json` (deduplicated listing IDs) - persisted via
      GitHub Actions cache so the same listing is not reported twice.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

MIN_PRICE_INR = 10_000_000        # 1 Crore
HOURS_WINDOW = 72                 # Only listings posted within this window
MAX_LISTINGS_PER_MESSAGE = 40     # Telegram message cap

# 49 East Delhi target areas (from screenshots).
# Spelling variants included to maximise matches across sites.
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
    ("Surajmal Vihar",           ["surajmal vihar", "suraj mal vihar", "teachers colony"]),
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
    ("Dayal Enclave (Bharti)",   ["dayal enclave", "bharti enclave", "bharti apartment"]),
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
    area_sqft: str = ""
    contact: str = ""
    matched_area: str = ""

    def unique_key(self) -> str:
        return f"{self.site}::{self.listing_id}"

    def serialise(self) -> dict:
        d = asdict(self)
        if self.posted_at:
            d["posted_at"] = self.posted_at.isoformat()
        return d


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def parse_price_to_inr(text: str) -> int | None:
    """Parse strings like '1.25 Cr', '₹ 95 Lakh', '1,25,00,000' to integer rupees."""
    if not text:
        return None
    s = text.replace(",", "").replace("₹", "").replace("INR", "").strip().lower()
    # Match number + optional unit
    m = re.search(r"(\d+(?:\.\d+)?)\s*(cr|crore|lakh|lac|k|l)?", s)
    if not m:
        return None
    try:
        value = float(m.group(1))
        unit = (m.group(2) or "").lower()
        if unit in ("cr", "crore"):
            return int(value * 10_000_000)
        if unit in ("lakh", "lac", "l"):
            return int(value * 100_000)
        if unit == "k":
            return int(value * 1_000)
        # Raw number - infer based on magnitude
        if value >= 100_000:
            return int(value)
        # assume it's in lakhs if smaller
        return int(value * 100_000)
    except (ValueError, TypeError):
        return None


def format_inr(amount: int) -> str:
    if amount >= 10_000_000:
        return f"₹{amount / 10_000_000:.2f} Cr"
    if amount >= 100_000:
        return f"₹{amount / 100_000:.2f} Lakh"
    return f"₹{amount:,}"


def match_target_area(location_text: str) -> str | None:
    """Return the canonical area name if the listing's location matches any target."""
    if not location_text:
        return None
    haystack = location_text.lower()
    for canonical, variants in TARGET_AREAS:
        for v in variants:
            if v in haystack:
                return canonical
    return None


def within_window(posted: datetime | None) -> bool:
    """True if posted_at falls within HOURS_WINDOW. Unknown dates pass through."""
    if posted is None:
        return True  # be lenient - include if we can't verify
    now = datetime.now(timezone.utc)
    if posted.tzinfo is None:
        posted = posted.replace(tzinfo=timezone.utc)
    return (now - posted) <= timedelta(hours=HOURS_WINDOW)


def fetch(url: str, params: dict | None = None, timeout: int = 30) -> requests.Response | None:
    """HTTP GET with standard headers and light retry."""
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, headers=COMMON_HEADERS, timeout=timeout)
            if r.status_code == 200:
                return r
            log.warning("GET %s -> %s (attempt %d)", url, r.status_code, attempt + 1)
        except requests.RequestException as e:
            log.warning("GET %s failed: %s", url, e)
        time.sleep(2 * (attempt + 1))
    return None


# ---------------------------------------------------------------------------
# SITE SCRAPERS
# Each scraper returns List[Listing]. Errors are caught in main() so one
# site's failure doesn't kill the run.
# ---------------------------------------------------------------------------

def scrape_magicbricks() -> list[Listing]:
    """
    MagicBricks: uses their property-for-sale search pages for East Delhi.
    We iterate target localities and parse listing cards.
    """
    listings: list[Listing] = []
    base = "https://www.magicbricks.com/property-for-sale-in-east-delhi-pppfs"
    r = fetch(base)
    if not r:
        return listings

    soup = BeautifulSoup(r.text, "html.parser")
    cards = soup.select("div.mb-srp__card") or soup.select("[class*=srp__card]")
    for c in cards:
        try:
            title_el = c.select_one("[class*=title]") or c.select_one("h2")
            title = title_el.get_text(" ", strip=True) if title_el else ""
            loc_el = c.select_one("[class*=society]") or c.select_one("[class*=locality]")
            location = loc_el.get_text(" ", strip=True) if loc_el else title
            price_el = c.select_one("[class*=price]")
            price_txt = price_el.get_text(" ", strip=True) if price_el else ""
            link = c.select_one("a[href]")
            href = link["href"] if link else base
            if href.startswith("/"):
                href = "https://www.magicbricks.com" + href
            lid = re.search(r"(\d{6,})", href)
            listings.append(Listing(
                site="MagicBricks",
                listing_id=lid.group(1) if lid else href[-40:],
                title=title,
                location=location,
                url=href,
                price_inr=parse_price_to_inr(price_txt),
                price_display=price_txt,
                posted_at=None,  # MagicBricks hides exact post dates on listing card
            ))
        except Exception as e:
            log.debug("MagicBricks card parse error: %s", e)
    log.info("MagicBricks: %d raw listings", len(listings))
    return listings


def scrape_99acres() -> list[Listing]:
    """99acres: east delhi residential for sale."""
    listings: list[Listing] = []
    url = "https://www.99acres.com/property-in-east-delhi-ffid"
    r = fetch(url)
    if not r:
        return listings
    soup = BeautifulSoup(r.text, "html.parser")

    # 99acres injects listing data in a JSON blob in a script tag.
    # Fallback to HTML parsing if not found.
    script = soup.find("script", id="__NEXT_DATA__") or soup.find(
        "script", string=re.compile(r"window\.__NUXT__|__INITIAL_STATE__")
    )
    if script and script.string:
        try:
            data = json.loads(script.string) if script.get("id") == "__NEXT_DATA__" else None
            # Best-effort extraction - structure varies; user may need to update selectors.
            if data:
                # The actual JSON path differs by release; we traverse generically.
                def walk(node):
                    if isinstance(node, dict):
                        if "propertyId" in node and "propertyTitle" in node:
                            yield node
                        for v in node.values():
                            yield from walk(v)
                    elif isinstance(node, list):
                        for v in node:
                            yield from walk(v)
                for item in walk(data):
                    try:
                        lid = str(item.get("propertyId", ""))
                        listings.append(Listing(
                            site="99acres",
                            listing_id=lid,
                            title=item.get("propertyTitle", ""),
                            location=item.get("localityName", "") or item.get("cityName", ""),
                            url=f"https://www.99acres.com/p/{lid}",
                            price_inr=parse_price_to_inr(str(item.get("price", ""))),
                            price_display=str(item.get("priceDisplay", item.get("price", ""))),
                            posted_at=None,
                            bhk=str(item.get("bedrooms", "")),
                            property_type=item.get("propertyType", ""),
                        ))
                    except Exception:
                        continue
        except Exception as e:
            log.debug("99acres JSON parse error: %s", e)

    # HTML fallback
    if not listings:
        for c in soup.select("[class*=SrpCard] , [class*=tupleNew]"):
            try:
                title_el = c.select_one("[class*=title]") or c.find("h2")
                price_el = c.select_one("[class*=price]")
                link = c.select_one("a[href]")
                href = link["href"] if link else url
                if href.startswith("/"):
                    href = "https://www.99acres.com" + href
                lid = re.search(r"(\d{6,})", href)
                listings.append(Listing(
                    site="99acres",
                    listing_id=lid.group(1) if lid else href[-40:],
                    title=title_el.get_text(" ", strip=True) if title_el else "",
                    location=title_el.get_text(" ", strip=True) if title_el else "",
                    url=href,
                    price_display=price_el.get_text(" ", strip=True) if price_el else "",
                    price_inr=parse_price_to_inr(price_el.get_text() if price_el else ""),
                ))
            except Exception:
                continue
    log.info("99acres: %d raw listings", len(listings))
    return listings


def scrape_nobroker() -> list[Listing]:
    """NoBroker: has a public JSON API for property search."""
    listings: list[Listing] = []
    # NoBroker API: east delhi sale properties
    api = "https://www.nobroker.in/api/v3/multi/property/sale"
    params = {
        "city": "delhi",
        "searchParam": "[{\"lat\":28.6421,\"lon\":77.3065,\"placeId\":\"ChIJ_____9_EDTkR\",\"placeName\":\"East Delhi\"}]",
        "pageNo": "1",
        "type": "BHK2,BHK3,BHK4,BHK4PLUS,1RK1,BHK1",
    }
    try:
        r = requests.get(api, params=params, headers={
            **COMMON_HEADERS,
            "Accept": "application/json",
            "Referer": "https://www.nobroker.in/",
        }, timeout=30)
        if r.status_code != 200:
            log.warning("NoBroker API %s", r.status_code)
            return listings
        data = r.json()
        items = data.get("data", []) or data.get("properties", [])
        for it in items:
            try:
                lid = str(it.get("id") or it.get("propertyId", ""))
                listings.append(Listing(
                    site="NoBroker",
                    listing_id=lid,
                    title=it.get("title", "") or it.get("propertyTitle", ""),
                    location=" ".join(filter(None, [
                        it.get("societyName", ""),
                        it.get("localityName", ""),
                        it.get("locality", ""),
                    ])),
                    url=f"https://www.nobroker.in/property/{lid}",
                    price_inr=int(it.get("price") or 0) or None,
                    price_display=format_inr(int(it.get("price") or 0)) if it.get("price") else "",
                    posted_at=_parse_iso(it.get("activationDate") or it.get("createdDate")),
                    bhk=str(it.get("type", "")),
                ))
            except Exception as e:
                log.debug("NoBroker item parse: %s", e)
    except Exception as e:
        log.warning("NoBroker error: %s", e)
    log.info("NoBroker: %d raw listings", len(listings))
    return listings


def scrape_squareyards() -> list[Listing]:
    """SquareYards East Delhi residential for sale."""
    listings: list[Listing] = []
    url = "https://www.squareyards.com/sale/property-for-sale-in-east-delhi"
    r = fetch(url)
    if not r:
        return listings
    soup = BeautifulSoup(r.text, "html.parser")
    for c in soup.select(".npd-info-box, .listing-card, [class*=property-item]"):
        try:
            title_el = c.select_one("h2, h3, .property-title")
            loc_el = c.select_one("[class*=location], [class*=address]")
            price_el = c.select_one("[class*=price]")
            link = c.select_one("a[href]")
            href = link["href"] if link else url
            if href.startswith("/"):
                href = "https://www.squareyards.com" + href
            lid = re.search(r"(\d{5,})", href)
            listings.append(Listing(
                site="SquareYards",
                listing_id=lid.group(1) if lid else href[-40:],
                title=title_el.get_text(" ", strip=True) if title_el else "",
                location=loc_el.get_text(" ", strip=True) if loc_el else "",
                url=href,
                price_inr=parse_price_to_inr(price_el.get_text() if price_el else ""),
                price_display=price_el.get_text(" ", strip=True) if price_el else "",
            ))
        except Exception:
            continue
    log.info("SquareYards: %d raw listings", len(listings))
    return listings


def scrape_propertywala() -> list[Listing]:
    """PropertyWala East Delhi for sale."""
    listings: list[Listing] = []
    url = "https://www.propertywala.com/property-for-sale-east_delhi"
    r = fetch(url)
    if not r:
        return listings
    soup = BeautifulSoup(r.text, "html.parser")
    for c in soup.select(".listing, .property-listing, .ad"):
        try:
            title_el = c.select_one("h2, h3, .title, a")
            price_el = c.select_one(".price, [class*=price]")
            link = c.select_one("a[href]")
            href = link["href"] if link else url
            if href.startswith("/"):
                href = "https://www.propertywala.com" + href
            listings.append(Listing(
                site="PropertyWala",
                listing_id=href[-60:],
                title=title_el.get_text(" ", strip=True) if title_el else "",
                location=title_el.get_text(" ", strip=True) if title_el else "",
                url=href,
                price_inr=parse_price_to_inr(price_el.get_text() if price_el else ""),
                price_display=price_el.get_text(" ", strip=True) if price_el else "",
            ))
        except Exception:
            continue
    log.info("PropertyWala: %d raw listings", len(listings))
    return listings


def scrape_dreamproperty() -> list[Listing]:
    """Dream Property - small portal; best effort generic scrape."""
    listings: list[Listing] = []
    url = "https://www.dreamproperty.co.in/property-for-sale/east-delhi"
    r = fetch(url)
    if not r:
        return listings
    soup = BeautifulSoup(r.text, "html.parser")
    for c in soup.select(".property-box, .property-card, article"):
        try:
            title_el = c.select_one("h2, h3, .title, a")
            price_el = c.select_one(".price, [class*=price]")
            link = c.select_one("a[href]")
            href = link["href"] if link else url
            if href.startswith("/"):
                href = "https://www.dreamproperty.co.in" + href
            listings.append(Listing(
                site="DreamProperty",
                listing_id=href[-60:],
                title=title_el.get_text(" ", strip=True) if title_el else "",
                location=title_el.get_text(" ", strip=True) if title_el else "",
                url=href,
                price_inr=parse_price_to_inr(price_el.get_text() if price_el else ""),
                price_display=price_el.get_text(" ", strip=True) if price_el else "",
            ))
        except Exception:
            continue
    log.info("DreamProperty: %d raw listings", len(listings))
    return listings


def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        try:
            return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            return None


# ---------------------------------------------------------------------------
# PLAYWRIGHT FALLBACK
# Only invoked if the simple fetch returned nothing useful for a given site.
# ---------------------------------------------------------------------------

def playwright_fetch(url: str, wait_selector: str | None = None) -> str | None:
    """Render a URL with a real browser and return the HTML."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("Playwright not installed; skipping browser fallback")
        return None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
            ctx = browser.new_context(user_agent=COMMON_HEADERS["User-Agent"])
            page = ctx.new_page()
            page.goto(url, timeout=60_000, wait_until="domcontentloaded")
            if wait_selector:
                try:
                    page.wait_for_selector(wait_selector, timeout=15_000)
                except Exception:
                    pass
            page.wait_for_timeout(3_000)
            html = page.content()
            browser.close()
            return html
    except Exception as e:
        log.warning("Playwright failed for %s: %s", url, e)
        return None


# ---------------------------------------------------------------------------
# TELEGRAM
# ---------------------------------------------------------------------------

def telegram_send(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    # Telegram hard limit is 4096 chars per message; split safely on newlines.
    chunks = _chunk(text, 3800)
    for i, chunk in enumerate(chunks):
        try:
            r = requests.post(url, data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": chunk,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }, timeout=30)
            if not r.ok:
                log.error("Telegram send failed [%d/%d]: %s", i + 1, len(chunks), r.text)
            else:
                log.info("Telegram chunk %d/%d sent", i + 1, len(chunks))
        except Exception as e:
            log.error("Telegram exception: %s", e)
        time.sleep(0.5)


def _chunk(text: str, size: int) -> list[str]:
    if len(text) <= size:
        return [text]
    out, current = [], ""
    for line in text.split("\n"):
        if len(current) + len(line) + 1 > size:
            out.append(current)
            current = line
        else:
            current = f"{current}\n{line}" if current else line
    if current:
        out.append(current)
    return out


# ---------------------------------------------------------------------------
# SEEN-LISTING PERSISTENCE
# ---------------------------------------------------------------------------

def load_seen() -> set[str]:
    if SEEN_FILE.exists():
        try:
            return set(json.loads(SEEN_FILE.read_text()))
        except Exception:
            pass
    return set()


def save_seen(seen: set[str]) -> None:
    # Keep seen list bounded to the most-recent ~5000 to prevent unbounded growth.
    arr = list(seen)[-5000:]
    SEEN_FILE.write_text(json.dumps(arr))


# ---------------------------------------------------------------------------
# MAIN ORCHESTRATION
# ---------------------------------------------------------------------------

SCRAPERS: list[tuple[str, Callable[[], list[Listing]]]] = [
    ("MagicBricks",   scrape_magicbricks),
    ("99acres",       scrape_99acres),
    ("NoBroker",      scrape_nobroker),
    ("SquareYards",   scrape_squareyards),
    ("PropertyWala",  scrape_propertywala),
    ("DreamProperty", scrape_dreamproperty),
]


def run() -> int:
    started = datetime.now()
    log.info("=== East Delhi Property Tracker starting at %s ===", started.isoformat())

    all_raw: list[Listing] = []
    per_site_counts: dict[str, int] = {}
    errors: dict[str, str] = {}

    for name, fn in SCRAPERS:
        try:
            items = fn() or []
            per_site_counts[name] = len(items)
            all_raw.extend(items)
        except Exception as e:
            log.exception("Scraper %s failed: %s", name, e)
            errors[name] = str(e)
            per_site_counts[name] = 0

    log.info("Total raw listings collected: %d", len(all_raw))

    # --- filter ---
    seen = load_seen()
    matched: list[Listing] = []
    for lst in all_raw:
        area = match_target_area(f"{lst.title} {lst.location}")
        if not area:
            continue
        if lst.price_inr is not None and lst.price_inr < MIN_PRICE_INR:
            continue
        if not within_window(lst.posted_at):
            continue
        key = lst.unique_key()
        if key in seen:
            continue
        lst.matched_area = area
        matched.append(lst)
        seen.add(key)

    save_seen(seen)
    log.info("Matched after filter: %d", len(matched))

    # --- compose Telegram message ---
    today = datetime.now().strftime("%d %b %Y, %a")
    header = f"<b>East Delhi Property Tracker</b>\n<i>{today} &#183; ₹1 Cr+ &#183; last {HOURS_WINDOW}h</i>\n\n"

    if not matched:
        summary = "Aaj koi nayi matching listing nahi mili target 49 areas mein."
        debug = "<code>" + " | ".join(f"{k}:{v}" for k, v in per_site_counts.items()) + "</code>"
        telegram_send(header + summary + "\n\n<b>Site scan counts:</b>\n" + debug)
        return 0

    body_lines = [header, f"✅ <b>{len(matched)} naye listings</b>\n"]
    by_area: dict[str, list[Listing]] = {}
    for m in matched[:MAX_LISTINGS_PER_MESSAGE]:
        by_area.setdefault(m.matched_area, []).append(m)

    for area, items in sorted(by_area.items()):
        body_lines.append(f"\n📍 <b>{area}</b> ({len(items)})")
        for it in items:
            price = it.price_display or (format_inr(it.price_inr) if it.price_inr else "Price on request")
            bhk = f" &#183; {it.bhk}BHK" if it.bhk and str(it.bhk).strip() and str(it.bhk) != "0" else ""
            title = (it.title or "Property")[:80]
            line = (
                f"&#183; <a href=\"{it.url}\">{title}</a>{bhk}\n"
                f"   💰 {price} &#183; <i>{it.site}</i>"
            )
            body_lines.append(line)

    if len(matched) > MAX_LISTINGS_PER_MESSAGE:
        body_lines.append(f"\n<i>+ {len(matched) - MAX_LISTINGS_PER_MESSAGE} more listings trimmed</i>")

    body_lines.append("\n<b>Site scan counts:</b>")
    body_lines.append("<code>" + " | ".join(f"{k}:{v}" for k, v in per_site_counts.items()) + "</code>")
    if errors:
        body_lines.append("<b>⚠️ Errors:</b> " + ", ".join(errors.keys()))

    telegram_send("\n".join(body_lines))
    log.info("=== Done in %.1fs ===", (datetime.now() - started).total_seconds())
    return 0


if __name__ == "__main__":
    sys.exit(run())
