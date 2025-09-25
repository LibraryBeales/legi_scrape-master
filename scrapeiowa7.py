#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Iowa Legislature bill scraper — no Playwright, saves full text to files,
and writes CSV with 'bill text' = path to saved file.

What it does:
- Enumerates bills for GA(s)
- Filters by keywords in bill full text (LGE/LGI attachments preferred; iframe fallback)
- Saves bill text to bill_texts/GA{ga}/{billno}.txt
- Writes CSV with columns: State, GA, Policy (bill) identifier, Policy sponsor, Policy sponsor party,
  Link to bill, bill text (path), Cosponsor (blank here), Enacted (Y/N), Act identifier, Enacted Date,
  Matched keywords.

Notes:
- No Playwright. No Bill History status/date parsing.
- We still try to extract "Chapter ####" from bill text to fill "Act identifier".
- BeautifulSoup falls back to built-in 'html.parser' if 'lxml' isn't installed.
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
from bs4 import BeautifulSoup, FeatureNotFound
from tenacity import retry, wait_exponential, stop_after_attempt
from tqdm import tqdm
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

GA_START = 1
GA_END   = 91

# Bill types to crawl
LEG_TYPES = ["HF", "SF", "HSB", "SSB"]

# Polite rate limits for requests
REQUESTS_PER_MINUTE = 30            # ~1 request every 2 seconds
JITTER_RANGE_SECONDS = (0.6, 1.8)   # small random jitter
PAUSE_EVERY_N_REQUESTS = 40         # periodic pause
PAUSE_DURATION_SECONDS = 20

# Optional: cap bills per GA during testing (None = all)
MAX_BILLS_PER_GA = None

# Output CSV (with GA column, no Policy name, no status/date fields)
OUT_CSV = "iowa_bills_keywords.csv"

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
    "State",
    "GA",                         # <— Added
    "Policy (bill) identifier",
    "Policy sponsor",
    "Policy sponsor party",
    "Link to bill",
    "bill text",                  # path to saved file
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
def soupify(html: str) -> BeautifulSoup:
    """Use lxml if available; otherwise fall back to the stdlib parser."""
    try:
        return BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(html, "html.parser")

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

# ---- Directory enumerator ----
def enumerate_bill_links_via_directory(ga: int, max_range: int = 9999) -> List[Tuple[str, str, str]]:
    """
    Walk Directory listings by type and 100-number ranges.
    Returns [(bill_number, BillBook_url, row_text), ...].
    """
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

# ===================== SPONSOR PARSING & ENRICHMENT =====================
def extract_primary_sponsor_from_text(bill_text: str) -> Tuple[str, str]:
    """
    Parse the 'BY ...' sponsor line from the bill text (LGI/LGE/iframe).
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

# From text only (no history): Chapter and optional “Effective …” phrase.
CHAPTER_RE = re.compile(r"\bCHAPTER\s+(\d+)\b", re.I)
EFFECTIVE_TEXT_RE = re.compile(r"\bEffective\s+(?:date|on)\s*[:,-]?\s*([A-Za-z]+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{2,4})", re.I)

def enrich_from_bill_text(bill_text: str) -> Dict[str, str]:
    out = {"Enacted (Y/N)": "", "Act identifier": "", "Enacted Date": ""}
    m = CHAPTER_RE.search(bill_text)
    if m:
        out["Act identifier"] = f"Chapter {m.group(1)}"
        # We don't actually know enactment; leave Enacted fields blank unless you want to infer.
    # If the bill text explicitly states an effective date, you may want to put it in Enacted Date or leave it off.
    # Here we leave Enacted Date blank unless you want to map that phrase directly.
    return out

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

# ---- Bill processing ----
def process_bill(ga: int, billno: str, url: str) -> Optional[Dict[str, str]]:
    """
    Fetch BillBook page, prefer LGI/LGE attachments for full text,
    fallback to iframe viewer if needed; match keywords; save text to file; build CSV row dict.
    """
    try:
        r = fetch(url)
    except Exception:
        return None

    billbook_html = r.text

    # Prefer attachments: LGE first, then LGI
    combined_attach_text, primary_attach_text, _ = fetch_attachment_texts_prioritized(billbook_html, url)

    # Fallback to iframe viewer text if no attachments present
    iframe_text = ""
    if not combined_attach_text:
        iframe_text = extract_bill_text_from_iframe(billbook_html, url)

    bill_text = (combined_attach_text or iframe_text or "").strip()
    if not bill_text:
        bill_text = ""

    # Sponsors
    sponsor_text_source = primary_attach_text or bill_text
    sponsor_line, sponsor_party = extract_primary_sponsor_from_text(sponsor_text_source)

    # Keyword match on bill text (attachments/viewer)
    hits = keywords_found(bill_text)
    if not hits:
        return None

    # Save bill text to file and store path in CSV
    text_path = save_bill_text(ga, billno, bill_text)

    # Minimal enrichment from bill text (chapter)
    enrich = enrich_from_bill_text(bill_text)

    row = {
        "State": "Iowa",
        "GA": str(ga),
        "Policy (bill) identifier": billno,
        "Policy sponsor": sponsor_line,
        "Policy sponsor party": sponsor_party,
        "Link to bill": url,
        "bill text": text_path or "",
        "Cosponsor": "",                 # left blank without Bill History parsing
        "Enacted (Y/N)": enrich.get("Enacted (Y/N)", ""),
        "Act identifier": enrich.get("Act identifier", ""),
        "Enacted Date": enrich.get("Enacted Date", ""),
        "Matched keywords": ", ".join(hits),
    }
    return row

# ---- Main ----
def main():
    rows: List[Dict[str, str]] = []
    seen_bills: Set[str] = set()

    for ga in range(GA_START, GA_END + 1):
        links = enumerate_bill_links_via_directory(ga)
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
            try:
                row = process_bill(ga, billno, url)
            except Exception:
                row = None
            if row:
                seen_bills.add(key)
                rows.append(row)
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
