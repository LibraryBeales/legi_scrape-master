#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Iowa Legislature bill scraper — supports old GAs (<=79) without directory pages.

What this does
- For modern GAs: enumerates bills via the directory pages.
- For older GAs (or when directory returns nothing): brute-forces BillBook pages
  by building HF/SF/HSB/SSB numbers and stopping after a safe number of misses.
- Extracts full text (LGE/LGI first; iframe fallback).
- Filters by KEYWORDS appearing in the full text.
- Saves the bill text to: bill_texts/GA{ga}/{billno}.txt
- Writes CSV with the path to the saved text (no date/status fields).
- Includes GA column in the CSV as the first column.

Install:
    pip install requests beautifulsoup4 lxml tenacity tqdm pdfminer.six

Run:
    python iowa_legis_oldga_supported.py
"""

import csv
import io
import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests
from tenacity import retry, wait_exponential, stop_after_attempt
from tqdm import tqdm

# BeautifulSoup with graceful parser fallback
from bs4 import BeautifulSoup, FeatureNotFound
def soupify(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(html, "html.parser")

# PDF text
from pdfminer.high_level import extract_text as pdf_extract_text

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

# GA range — include older GAs if you want
GA_START = 78
GA_END   = 81     # example: older GAs; set to 91 for current, etc.

# Bill types to crawl
LEG_TYPES = ["HF", "SF", "HSB", "SSB"]

# Brute-force search bounds & guards (used when directory is missing/empty)
OLD_GA_MAX_BILLNO = 2000          # upper bound per type (tune as needed)
CONSEC_MISS_BREAK = 250            # stop after this many consecutive misses per type

# Polite rate limits
REQUESTS_PER_MINUTE = 30           # ~2s average between requests + jitter
JITTER_RANGE_SECONDS = (0.6, 1.8)  # small random jitter
PAUSE_EVERY_N_REQUESTS = 40
PAUSE_DURATION_SECONDS = 20

# Optional: cap bills per GA during testing (None = all discovered)
MAX_BILLS_PER_GA = None

# Output CSV (reduced schema, with GA column)
OUT_CSV = "iowa_bills_keywords_with_GA.csv"

# Where to store bill text files
BILL_TEXT_ROOT = Path("bill_texts")   # files -> bill_texts/GA{ga}/{billno}.txt
# ===============================================================

BASE = "https://www.legis.iowa.gov"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

DIRECTORY_URL_TMPL = (
    BASE + "/legislation/billTracking/directory/index/listing"
           "?ga={ga}&legType={leg}&min={min_n}&max={max_n}"
)

CSV_COLUMNS = [
    "GA",
    "State",
    "Policy (bill) identifier",
    "Policy sponsor",
    "Policy sponsor party",
    "Link to bill",
    "bill text",
    "Cosponsor",
    "Enacted (Y/N)",
    "Act identifier",
    "Enacted Date",
    "Matched keywords",
]

# ---- Requests throttling ----
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

# ---- Utils ----
def clean(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def keywords_found(text: str) -> List[str]:
    t = text.lower()
    hits = [kw for kw in KEYWORDS if kw.lower() in t]
    seen, ordered = set(), []
    for kw in hits:
        if kw not in seen:
            seen.add(kw)
            ordered.append(kw)
    return ordered

# ===================== ATTACHMENTS (PRIORITIZE LGE, THEN LGI) =====================
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

def fetch_attachment_texts_prioritized(billbook_html: str, base_url: str) -> Tuple[str, str, List[str]]:
    """
    Return (combined_text, primary_text, all_texts) from attachments.
    Priority: LGE* first, then LGI*.
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
    return combined_text, primary_text, texts

# ---- Bill text from iframe (fallback only if no attachments) ----
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
                    return clean(BeautifulSoup(r2.text, "lxml").get_text(" "))
            except Exception:
                pass
        return clean(sub.get_text(" "))
    except Exception:
        return ""

# ===================== SPONSOR / CHAPTER / ENACTED =====================
def extract_primary_sponsor_from_text(bill_text: str) -> Tuple[str, str]:
    """
    Parse the 'BY ...' sponsor line from the bill text.
    Returns (sponsor_line, party_letter).
    """
    m = re.search(r"\bBY\s+(.{0,200}?)\s{2,}", bill_text, flags=re.IGNORECASE)
    if not m:
        m = re.search(r"\bBY\s+(.{0,200}?)(?:\s+A\s+BILL\s+FOR\b|\n)", bill_text, flags=re.IGNORECASE)
    sponsor_line = clean(m.group(1)) if m else ""
    party = ""
    pm = re.search(r"\(([RDI])\)", sponsor_line)
    if pm:
        party = pm.group(1)
    return sponsor_line, party

CHAPTER_RE = re.compile(r"\bCHAPTER\s+(\d+)\b", re.I)
EFFECTIVE_TEXT_RE = re.compile(r"\bEffective\s+(?:date|on)\s*[:,-]?\s*([A-Za-z]+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{2,4})", re.I)
SIGNED_GOV_TEXT_RE = re.compile(r"\b(Signed|Approved)\s+by\s+Governor\b", re.I)

def enrich_from_bill_text(raw_text: str) -> Dict[str, str]:
    out = {"Enacted (Y/N)": "", "Act identifier": "", "Enacted Date": ""}
    m = CHAPTER_RE.search(raw_text)
    if m:
        out["Act identifier"] = f"Chapter {m.group(1)}"
    # We're not guaranteed enacted dates in the bill text, but keep a light pass:
    if SIGNED_GOV_TEXT_RE.search(raw_text):
        out["Enacted (Y/N)"] = "Y"
    m2 = EFFECTIVE_TEXT_RE.search(raw_text)
    if m2:
        # The CSV schema still has "Enacted Date"; effective != enacted, so leave Enacted Date blank,
        # but we could stash effective into Act identifier if needed. Keeping conservative.
        pass
    return out

# ---- Detect a valid BillBook page quickly ----
def is_valid_billbook(html: str) -> bool:
    """
    Heuristics: page has known BillBook markers, or has LGE/LGI links or the bbContextDoc iframe.
    """
    if not html or len(html) < 500:
        return False
    s = html.lower()
    if "billbook" in s and ("bbcontextdoc" in s or "/docs/publications/lgi/" in s or "/docs/publications/lge/" in s):
        return True
    # Some older pages: look for "A BILL FOR" text and "BY" pattern in body when iframe is used.
    if "a bill for" in s and " bill " in s:
        return True
    # As a fallback, check for the title strip:
    if "Bill Information" in html:
        return True
    return False

# ---- Directory enumerator (for modern GAs) ----
def enumerate_bill_links_via_directory(ga: int, max_range: int = 9999) -> List[Tuple[str, str, str]]:
    all_links: List[Tuple[str, str, str]] = []
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
                    if not bill_url:
                        continue
                    m = re.search(r"ba=([^&]+)", bill_url, flags=re.I)
                    if not m:
                        continue
                    billno = clean(m.group(1)).replace("+", " ").upper()
                    if f"&ga={ga}" not in bill_url:
                        bill_url = f"{bill_url}&ga={ga}"
                    row_text = clean(a.get_text(" "))
                    all_links.append((billno, bill_url, row_text))
                continue

            empty_ranges = 0
            for tr in rows:
                a = tr.select_one("a[href*='/legislation/BillBook?ba=']")
                if not a:
                    continue
                href = a.get("href", "")
                if not href:
                    continue
                bill_url = BASE + href if href.startswith("/") else href
                if not bill_url:
                    continue
                m = re.search(r"ba=([^&]+)", bill_url, flags=re.I)
                if not m:
                    continue
                billno = clean(m.group(1)).replace("+", " ").upper()
                if f"&ga={ga}" not in bill_url:
                    bill_url = f"{bill_url}&ga={ga}"
                row_text = clean(tr.get_text(" "))
                all_links.append((billno, bill_url, row_text))

    # de-dup
    uniq, seen = [], set()
    for bn, url, txt in all_links:
        key = (bn, url)
        if key not in seen:
            seen.add(key)
            uniq.append((bn, url, txt))
    return uniq

# ---- Brute-force enumerator (for old GAs with no directory) ----
def enumerate_bill_links_bruteforce(ga: int) -> List[Tuple[str, str, str]]:
    """
    For older GAs (<=79) or when directory is empty:
    Try BillBook for each LEG_TYPES + [1..OLD_GA_MAX_BILLNO], stop per-type after CONSEC_MISS_BREAK misses.
    Returns [(bill_number, billbook_url, "")]
    """
    results: List[Tuple[str, str, str]] = []
    for leg in LEG_TYPES:
        misses = 0
        hits = 0
        for n in range(1, OLD_GA_MAX_BILLNO + 1):
            billno = f"{leg}{n}"
            url = f"{BASE}/legislation/BillBook?ba={billno}&ga={ga}"
            try:
                r = fetch(url)
                html = r.text
            except Exception:
                misses += 1
                if hits > 0 and misses >= CONSEC_MISS_BREAK:
                    break
                continue

            if is_valid_billbook(html):
                results.append((billno, url, ""))
                hits += 1
                misses = 0
            else:
                misses += 1
                # once we've seen at least one hit, allow a streak of misses then stop for this leg type
                if hits > 0 and misses >= CONSEC_MISS_BREAK:
                    break
        # proceed to next leg type
    # de-dup (shouldn’t be needed here, but keep consistent)
    uniq, seen = [], set()
    for bn, url, txt in results:
        key = (bn, url)
        if key not in seen:
            seen.add(key)
            uniq.append((bn, url, txt))
    return uniq

# ---- Save bill text to file and return path for CSV ----
def save_bill_text(ga: int, billno: str, text: str) -> str:
    folder = BILL_TEXT_ROOT / f"GA{ga}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{billno}.txt"
    try:
        path.write_text(text, encoding="utf-8")
        return str(path).replace("\\", "/")
    except Exception:
        return ""

# ---- Row model (reduced schema, with GA) ----
@dataclass
class Row:
    GA: int
    State: str = "Iowa"
    Policy_bill_identifier: str = ""
    Policy_sponsor: str = ""
    Policy_sponsor_party: str = ""
    Link_to_bill: str = ""
    bill_text: str = ""  # path to txt file
    Cosponsor: str = ""
    Enacted_YN: str = ""
    Act_identifier: str = ""
    Enacted_Date: str = ""
    Matched_keywords: str = ""

    def to_csv_row(self) -> Dict[str, str]:
        return {
            "GA": self.GA,
            "State": self.State,
            "Policy (bill) identifier": self.Policy_bill_identifier,
            "Policy sponsor": self.Policy_sponsor,
            "Policy sponsor party": self.Policy_sponsor_party,
            "Link to bill": self.Link_to_bill,
            "bill text": self.bill_text,
            "Cosponsor": self.Cosponsor,
            "Enacted (Y/N)": self.Enacted_YN,
            "Act identifier": self.Act_identifier,
            "Enacted Date": self.Enacted_Date,
            "Matched keywords": self.Matched_keywords,
        }

# ---- Bill processing (no date/status scraping; same as your reduced CSV) ----
def process_bill(ga: int, billno: str, url: str) -> Optional[Row]:
    try:
        r = fetch(url)
    except Exception:
        return None

    billbook_html = r.text

    # Prefer attachments (LGE first, then LGI)
    combined_attach_text, primary_attach_text, _ = fetch_attachment_texts_prioritized(billbook_html, url)

    # Fallback to iframe viewer text if no attachments present
    iframe_text = ""
    if not combined_attach_text:
        iframe_text = extract_bill_text_from_iframe(billbook_html, url)

    raw_text = (combined_attach_text or iframe_text or "").strip()
    if not raw_text:
        return None

    # Sponsors
    sponsor_text_source = primary_attach_text or raw_text
    sponsor_line, sponsor_party = extract_primary_sponsor_from_text(sponsor_text_source)

    # Very light enrichment from text (chapter, maybe enacted flag)
    enacted_bits = enrich_from_bill_text(raw_text)

    # keywords
    hits = keywords_found(raw_text)
    if not hits:
        return None

    # save bill text
    text_path = save_bill_text(ga, billno, raw_text)

    row = Row(GA=ga)
    row.Link_to_bill = url
    row.Policy_bill_identifier = billno
    row.Policy_sponsor = sponsor_line
    row.Policy_sponsor_party = sponsor_party
    row.Cosponsor = ""                    # left empty in this reduced schema
    row.bill_text = text_path or ""       # path to file
    row.Enacted_YN = enacted_bits.get("Enacted (Y/N)", "")
    row.Act_identifier = enacted_bits.get("Act identifier", "")
    row.Enacted_Date = enacted_bits.get("Enacted Date", "")
    row.Matched_keywords = ", ".join(hits)

    return row

# ---- Main ----
def main():
    rows: List[Dict[str, str]] = []
    seen_bills: Set[str] = set()

    for ga in range(GA_START, GA_END + 1):
        # Try directory first
        links = enumerate_bill_links_via_directory(ga)
        if not links:
            # Fall back to brute force for old GA or when directory is empty
            print(f"GA {ga}: directory empty or unsupported; switching to brute-force discovery…")
            links = enumerate_bill_links_bruteforce(ga)

        if MAX_BILLS_PER_GA:
            links = links[:MAX_BILLS_PER_GA]

        print(f"GA {ga}: discovered {len(links)} bill links (cap={MAX_BILLS_PER_GA})")

        processed = 0
        matched = 0
        for billno, url, _ in tqdm(links, desc=f"GA {ga}", leave=False):
            key = f"{ga}:{billno.upper()}"
            if key in seen_bills:
                continue
            processed += 1
            row = process_bill(ga, billno, url)
            if row:
                seen_bills.add(key)
                rows.append(row.to_csv_row())
                matched += 1

        print(f"GA {ga}: processed {processed}, matched {matched}")

    Path(OUT_CSV).parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote {len(rows)} rows to {OUT_CSV}")

if __name__ == "__main__":
    main()
