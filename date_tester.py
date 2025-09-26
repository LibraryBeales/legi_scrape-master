#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Iowa Legislature — Playwright date scraper (use the working 'test script' approach)

- Enumerate bills per GA via directory pages (HF/SF/HSB/SSB)
- Pull bill FULL TEXT (LGE/LGI attachments preferred; iframe fallback) to keyword-filter
- For matches, open BillBook with Playwright and parse Bill History table like the test script
- Output a CSV with date fields + keyword hits

Install:
    pip install playwright requests beautifulsoup4 pdfminer.six tenacity tqdm
    python -m playwright install chromium

Run:
    python iowa_dates_from_history.py
"""

import csv
import io
import random
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from tenacity import retry, wait_exponential, stop_after_attempt
from tqdm import tqdm
from pdfminer.high_level import extract_text as pdf_extract_text

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ===================== USER SETTINGS =====================
KEYWORDS = [
    "Immigration",
    "Citizenship",
    "Alien",
    "Migrant",
    "Undocumented",
    "Visa",
    "Border",
    "Foreign",
]

GA_START = 84   # change as needed
GA_END   = 91   # change as needed

LEG_TYPES = ["HF", "SF", "HSB", "SSB"]  # directory bill types

# Optional: cap bills per GA during testing (None = all)
MAX_BILLS_PER_GA = None

OUT_CSV = "iowa_bill_dates_playwright.csv"
# ========================================================

BASE = "https://www.legis.iowa.gov"
DIRECTORY_URL_TMPL = (
    BASE + "/legislation/billTracking/directory/index/listing"
           "?ga={ga}&legType={leg}&min={min_n}&max={max_n}"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

CSV_COLUMNS = [
    "State",
    "GA",
    "Policy (bill) identifier",
    "Introduced date",
    "Effective date",
    "Passed introduced chamber date",
    "Passed second chamber date",
    "Dead date",
    "Enacted (Y/N)",
    "Enacted Date",
    "Matched keywords",
]

# ---- polite requests throttling ----
REQUESTS_PER_MINUTE = 30
JITTER_RANGE_SECONDS = (0.6, 1.8)
PAUSE_EVERY_N_REQUESTS = 40
PAUSE_DURATION_SECONDS = 20

BASE_SLEEP = 60.0 / max(1, REQUESTS_PER_MINUTE)
_req_count = 0
_session = requests.Session()
_session.headers.update(HEADERS)

def polite_sleep():
    global _req_count
    _req_count += 1
    time.sleep(BASE_SLEEP + random.uniform(*JITTER_RANGE_SECONDS))
    if PAUSE_EVERY_N_REQUESTS and _req_count % PAUSE_EVERY_N_REQUESTS == 0:
        time.sleep(PAUSE_DURATION_SECONDS)

@retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(4))
def fetch(url: str) -> requests.Response:
    polite_sleep()
    r = _session.get(url, timeout=45)
    r.raise_for_status()
    return r

# ---- soup / helpers ----
def soupify(html: str) -> BeautifulSoup:
    # use built-in parser to avoid lxml dependency
    return BeautifulSoup(html, "html.parser")

def clean(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def infer_chamber_from_billno(billno: str) -> str:
    bn = billno.upper()
    if bn.startswith(("HF", "HSB", "HR", "HCR", "HCJ")):
        return "House"
    if bn.startswith(("SF", "SSB", "SR", "SCR", "SCJ")):
        return "Senate"
    return ""

def keywords_found(text: str) -> List[str]:
    t = text.lower()
    hits = [kw for kw in KEYWORDS if kw.lower() in t]
    seen, ordered = set(), []
    for kw in hits:
        if kw not in seen:
            seen.add(kw)
            ordered.append(kw)
    return ordered

# parse YYYYMMDD from hrefs like ?hdate=YYYYMMDD or /HJNL/20250304_...
YMD_IN_HREF = re.compile(r"(?:[?&](?:hdate|date|actionDate)=)(\d{8})", re.I)
YMD_IN_PATH = re.compile(r"/(?:HJNL|SJNL|HJRNL|SJRNL)/(\d{8})", re.I)

def _ymd_to_mdy(ymd: str) -> str:
    y, m, d = ymd[:4], ymd[4:6], ymd[6:8]
    return f"{int(m)}/{int(d)}/{y}"

def _date_from_href_str(href: str) -> Optional[str]:
    m = YMD_IN_HREF.search(href or "")
    if m:
        return _ymd_to_mdy(m.group(1))
    m2 = YMD_IN_PATH.search(href or "")
    if m2:
        return _ymd_to_mdy(m2.group(1))
    return None

DATE_PATS = [
    re.compile(r"\b([A-Za-z]+ \d{1,2}, \d{4})\b"),  # Month DD, YYYY
    re.compile(r"\b(\d{1,2}/\d{1,2}/\d{4})\b"),     # MM/DD/YYYY
]

# ===================== Directory enumerator =====================
def enumerate_bill_links_via_directory(ga: int, max_range: int = 9999) -> List[Tuple[str, str]]:
    """
    Walk Directory listings by type and 100-number ranges.
    Returns [(bill_number, BillBook_url), ...].
    """
    all_links: List[Tuple[str, str]] = []
    for leg in LEG_TYPES:
        empty_ranges = 0
        for start in range(1, max_range + 1, 100):
            end = min(start + 99, max_range)
            url = DIRECTORY_URL_TMPL.format(ga=ga, leg=leg, min_n=start, max_n=end)
            try:
                r = fetch(url)
            except Exception:
                empty_ranges += 1
                if empty_ranges >= 3:
                    break
                continue

            soup = soupify(r.text)
            rows = soup.select("table tr")
            if not rows:
                anchors = soup.select("a[href*='/legislation/BillBook?ba=']")
                if not anchors:
                    empty_ranges += 1
                    if empty_ranges >= 2:
                        break
                    continue
                empty_ranges = 0
                for a in anchors:
                    href = a.get("href", "")
                    if not href:
                        continue
                    bill_url = BASE + href if href.startswith("/") else href
                    m = re.search(r"ba=([^&]+)", bill_url, flags=re.I)
                    if not m:
                        continue
                    billno = clean(m.group(1)).replace("+", " ").upper()
                    if f"&ga={ga}" not in bill_url:
                        bill_url = f"{bill_url}&ga={ga}"
                    all_links.append((billno, bill_url))
                continue

            empty_ranges = 0
            for tr in rows:
                a = tr.select_one("a[href*='/legislation/BillBook?ba=']")
                if not a:
                    continue
                href = a.get("href", "")
                bill_url = BASE + href if href.startswith("/") else href
                m = re.search(r"ba=([^&]+)", bill_url, flags=re.I)
                if not m:
                    continue
                billno = clean(m.group(1)).replace("+", " ").upper()
                if f"&ga={ga}" not in bill_url:
                    bill_url = f"{bill_url}&ga={ga}"
                all_links.append((billno, bill_url))

    # de-dup
    uniq, seen = [], set()
    for bn, url in all_links:
        key = (bn, url)
        if key not in seen:
            seen.add(key)
            uniq.append((bn, url))
    return uniq

# ===================== Bill text (attachments first, iframe fallback) =====================
ATTACHMENT_PAT = re.compile(r"/docs/publications/(LGE|LGI)/[^\"'>]+?\.(?:html?|pdf)\b", flags=re.I)

def _extract_text_from_url(url: str) -> str:
    try:
        r = fetch(url)
    except Exception:
        return ""
    low = url.lower()
    if low.endswith((".htm", ".html")):
        try:
            return clean(soupify(r.text).get_text(" "))
        except Exception:
            return ""
    if low.endswith(".pdf") or "pdf" in r.headers.get("Content-Type", "").lower():
        try:
            return clean(pdf_extract_text(io.BytesIO(r.content)) or "")
        except Exception:
            return ""
    try:
        return clean(soupify(r.text).get_text(" "))
    except Exception:
        return ""

def fetch_attachment_texts_prioritized(billbook_html: str, base_url: str) -> Tuple[str, str]:
    """
    Return (combined_text, primary_text) from attachments.
    Priority: LGE first, then LGI.
    """
    soup = soupify(billbook_html)
    urls: List[Tuple[str, str]] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        abs_url = urljoin(base_url, href)
        m = ATTACHMENT_PAT.search(abs_url)
        if m:
            urls.append((m.group(1).upper(), abs_url))

    urls.sort(key=lambda t: (0 if t[0] == "LGE" else 1, t[1]))

    texts: List[str] = []
    for _, u in urls:
        txt = _extract_text_from_url(u)
        if txt:
            texts.append(txt)

    primary_text = texts[0] if texts else ""
    combined_text = " ".join(texts).strip()
    return combined_text, primary_text

def extract_bill_text_from_iframe(billbook_html: str, billbook_url: str) -> str:
    soup = soupify(billbook_html)
    iframe = soup.find("iframe", id="bbContextDoc")
    if not iframe or not iframe.get("src"):
        return ""
    src = urljoin(billbook_url, iframe["src"])
    try:
        r = fetch(src)
    except Exception:
        return ""
    ct = r.headers.get("Content-Type", "").lower()
    if "pdf" in ct or src.lower().endswith(".pdf"):
        try:
            return clean(pdf_extract_text(io.BytesIO(r.content)) or "")
        except Exception:
            return ""
    try:
        sub = soupify(r.text)
        inner_if = sub.find("iframe")
        if inner_if and inner_if.get("src"):
            inner_src = urljoin(src, inner_if["src"])
            try:
                r2 = fetch(inner_src)
                ct2 = r2.headers.get("Content-Type", "").lower()
                if "pdf" in ct2 or inner_src.lower().endswith(".pdf"):
                    return clean(pdf_extract_text(io.BytesIO(r2.content)) or "")
                else:
                    return clean(BeautifulSoup(r2.text, "html.parser").get_text(" "))
            except Exception:
                pass
        return clean(sub.get_text(" "))
    except Exception:
        return ""

# ===================== Playwright: parse Bill History like the test =====================
INTRO_RE   = re.compile(r"\bIntroduced\b", re.I)
PASSED_H   = re.compile(r"\bPassed\s+House\b", re.I)
PASSED_S   = re.compile(r"\bPassed\s+Senate\b", re.I)
SIGNED_GOV = re.compile(r"\b(Signed|Approved)\s+by\s+Governor\b", re.I)
WITHDRAWN  = re.compile(r"\bWithdrawn\b|\bDied in (House|Senate)\b|\bFailed\b", re.I)
EFFECTIVE  = re.compile(r"\bEffective (?:date|on)\b|\bEffective\b", re.I)

def parse_dates_with_playwright(page, url: str, billno: str) -> Dict[str, str]:
    # Load with retries (ERR_EMPTY_RESPONSE sometimes occurs)
    for attempt in range(3):
        try:
            page.goto(url, timeout=60000)
            break
        except Exception:
            if attempt == 2:
                raise
            time.sleep(2)

    # expand history if collapsed — just like the test script
    try:
        page.locator("a.actionWidgetExpand").first.click(timeout=2000)
    except Exception:
        pass

    # rows under the canonical billAction table
    try:
        rows = page.locator("div.billAction table.billActionTable tbody tr")
        n = rows.count()
    except Exception:
        return {
            "Introduced date": "",
            "Effective date": "",
            "Passed introduced chamber date": "",
            "Passed second chamber date": "",
            "Dead date": "",
            "Enacted (Y/N)": "",
            "Enacted Date": "",
        }

    introduced_chamber = infer_chamber_from_billno(billno)

    intro_dates: List[str] = []
    passed_intro_date = ""
    passed_second_date = ""
    effective_date = ""
    dead_date = ""
    enacted_flag = ""
    enacted_date = ""

    for i in range(n):
        tds = rows.nth(i).locator("td")
        # date from first cell
        try:
            date = tds.nth(0).inner_text().strip()
        except Exception:
            date = ""

        # action text
        try:
            action = tds.nth(1).inner_text().strip()
        except Exception:
            try:
                action = rows.nth(i).inner_text().strip()
            except Exception:
                action = ""

        # if date missing, try from links then literal patterns
        if not date:
            try:
                links = rows.nth(i).locator("a[href]")
                for j in range(links.count()):
                    href = links.nth(j).get_attribute("href") or ""
                    d = _date_from_href_str(href)
                    if d:
                        date = d
                        break
            except Exception:
                pass
            if not date:
                for pat in DATE_PATS:
                    m = pat.search(action)
                    if m:
                        date = m.group(1)
                        break

        low = action.lower()

        # EXACTLY like the test for Introduced — collect all and pick earliest
        if "introduced" in low and date:
            intro_dates.append(date)

        # Other dates using the same simple pattern-matching
        if PASSED_H.search(action):
            if introduced_chamber == "House" and not passed_intro_date:
                passed_intro_date = date
            elif not passed_second_date:
                passed_second_date = date

        if PASSED_S.search(action):
            if introduced_chamber == "Senate" and not passed_intro_date:
                passed_intro_date = date
            elif not passed_second_date:
                passed_second_date = date

        if EFFECTIVE.search(action) and not effective_date:
            effective_date = date

        if SIGNED_GOV.search(action) and not enacted_date:
            enacted_flag = "Y"
            enacted_date = date

        if WITHDRAWN.search(action) and not dead_date:
            dead_date = date

    # earliest Introduced date by M/D/YYYY
    def mdy_key(d: str):
        try:
            m, dd, y = [int(x) for x in d.split("/")]
            return (y, m, dd)
        except Exception:
            return (9999, 12, 31)

    introduced_date = ""
    if intro_dates:
        intro_dates = [d for d in intro_dates if d]
        intro_dates.sort(key=mdy_key)
        if intro_dates:
            introduced_date = intro_dates[0]

    return {
        "Introduced date": introduced_date,
        "Effective date": effective_date,
        "Passed introduced chamber date": passed_intro_date,
        "Passed second chamber date": passed_second_date,
        "Dead date": dead_date,
        "Enacted (Y/N)": enacted_flag,
        "Enacted Date": enacted_date,
    }

# ===================== Process one bill (filter + dates) =====================
def process_bill(page, ga: int, billno: str, url: str) -> Optional[Dict[str, str]]:
    # pull shell page to find attachments/iframe for text (keyword filter)
    try:
        r = fetch(url)
    except Exception:
        return None

    billbook_html = r.text

    # Prefer attachments (LGE > LGI); fallback to iframe
    combined_text, _primary = fetch_attachment_texts_prioritized(billbook_html, url)
    if not combined_text:
        combined_text = extract_bill_text_from_iframe(billbook_html, url)

    text = (combined_text or "").strip()
    if not text:
        return None

    hits = keywords_found(text)
    if not hits:
        return None

    # Use the working test-script approach to parse dates
    dates = parse_dates_with_playwright(page, url, billno)

    return {
        "State": "Iowa",
        "GA": str(ga),
        "Policy (bill) identifier": billno,
        "Introduced date": dates.get("Introduced date", ""),
        "Effective date": dates.get("Effective date", ""),
        "Passed introduced chamber date": dates.get("Passed introduced chamber date", ""),
        "Passed second chamber date": dates.get("Passed second chamber date", ""),
        "Dead date": dates.get("Dead date", ""),
        "Enacted (Y/N)": dates.get("Enacted (Y/N)", ""),
        "Enacted Date": dates.get("Enacted Date", ""),
        "Matched keywords": ", ".join(hits),
    }

# ===================== Main =====================
def main():
    rows: List[Dict[str, str]] = []
    seen: Set[Tuple[int, str]] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=HEADERS["User-Agent"])
        page = context.new_page()
        page.set_default_timeout(60000)

        for ga in range(GA_START, GA_END + 1):
            links = enumerate_bill_links_via_directory(ga)
            if not links:
                print(f"GA {ga}: no directory listings found (site may not host directories for this GA).")
                continue
            if MAX_BILLS_PER_GA:
                links = links[:MAX_BILLS_PER_GA]
            print(f"GA {ga}: discovered {len(links)} bill links (cap={MAX_BILLS_PER_GA})")

            processed = 0
            matched = 0
            for billno, url in tqdm(links, desc=f"GA {ga}", leave=False):
                key = (ga, billno.upper())
                if key in seen:
                    continue
                processed += 1
                try:
                    row = process_bill(page, ga, billno, url)
                except PWTimeout:
                    row = None
                except Exception:
                    row = None
                if row:
                    seen.add(key)
                    rows.append(row)
                    matched += 1

            print(f"GA {ga}: processed {processed}, matched {matched}")

        context.close()
        browser.close()

    Path(OUT_CSV).parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote {len(rows)} rows to {OUT_CSV}")

if __name__ == "__main__":
    main()
