#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ILGA Phase 1 — enumerate by deterministic Full-Text URLs (no SessionIDs),
filter by KEYWORDS, save text, and write a minimal CSV for Phase 2 enrichment.

Outputs CSV columns:
  State, GA, Bill Identifier, URL, Path to full text, Keywords

Examples:
  # Quick sanity test on GA 103 only, first 8000 HB & SB numbers, stop after 400 misses:
  py -3.11 ilga_phase1_fulltext.py --ga-start 103 --ga-end 103 --max-docnum 8000 --stop-misses 400

  # Wider run:
  py -3.11 ilga_phase1_fulltext.py --ga-start 90 --ga-end 103 --max-docnum 12000 --stop-misses 600
"""

import argparse
import csv
import random
import re
import time
from pathlib import Path
from typing import List, Optional

import requests
from bs4 import BeautifulSoup

# ===================== USER SETTINGS (override with CLI) =====================
KEYWORDS = [
    "Immigration", "Citizenship", "Alien", "Migrant",
    "Undocumented", "Visa", "Border", "Foreign",
]

GA_START_DEFAULT = 100
GA_END_DEFAULT   = 103
CHAMBERS_DEFAULT = ["HB", "SB"]

# Probing behavior
MAX_DOCNUM_DEFAULT   = 10000      # absolute ceiling to try
STOP_MISSES_DEFAULT  = 1000        # after first hit, stop after this many consecutive 404s
SLEEP_BASE_SECONDS   = 0.35       # base politeness delay between requests
SLEEP_JITTER_RANGE   = (0.05, 0.20)

# Output
OUT_CSV_DEFAULT = "illinois_phase1_fulltext_hits.csv"
BILL_TEXT_ROOT  = Path("billtext")
# =============================================================================

BASE = "https://www.ilga.gov"
UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
]
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": random.choice(UA_POOL),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": BASE + "/Legislation/",
})

# ====================== helpers ======================
def sleep_politely():
    time.sleep(SLEEP_BASE_SECONDS + random.uniform(*SLEEP_JITTER_RANGE))

def clean_text(s: Optional[str]) -> str:
    import re as _re
    return _re.sub(r"\s+", " ", (s or "")).strip()

def fulltext_url(ga: int, chamber: str, num: int) -> str:
    # Pattern like: https://www.ilga.gov/Documents/legislation/103/HB/10300HB5909.htm
    return f"{BASE}/Documents/legislation/{ga}/{chamber}/{ga:03d}00{chamber}{num:04d}.htm"

def get_fulltext(url: str) -> Optional[str]:
    """Fetch the page and return text (None on 4xx/5xx). Handles minor server quirks."""
    try:
        sleep_politely()
        r = SESSION.get(url, timeout=(10, 45))
        if r.status_code == 404:
            return None
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        return clean_text(soup.get_text(" "))
    except requests.RequestException:
        return None

def keywords_found(text: str, keywords: List[str]) -> List[str]:
    t = text.lower()
    hits = []
    seen = set()
    for kw in keywords:
        if kw.lower() in t and kw not in seen:
            seen.add(kw)
            hits.append(kw)
    return hits

def save_text(ga: int, chamber: str, num: int, text: str) -> str:
    folder = BILL_TEXT_ROOT / f"GA{ga}"
    folder.mkdir(parents=True, exist_ok=True)
    filename = f"{chamber}{num:04d}.txt"
    path = folder / filename
    path.write_text(text, encoding="utf-8", errors="ignore")
    return str(path).replace("\\", "/")

# ====================== main logic ======================
def enumerate_and_collect(
    ga: int,
    chamber: str,
    max_docnum: int,
    stop_misses: int,
    keywords: List[str],
) -> List[dict]:
    """
    Probe Full-Text URLs by counting up doc numbers for a GA+chamber,
    stop after 'stop_misses' consecutive 404s *after* seeing at least one hit.
    Return list of CSV rows for hits (keyword matches only).
    """
    rows = []
    saw_any_existing = False
    consecutive_misses = 0

    for n in range(1, max_docnum + 1):
        url = fulltext_url(ga, chamber, n)
        text = get_fulltext(url)
        if text is None:
            # URL doesn't exist
            if saw_any_existing:
                consecutive_misses += 1
                if consecutive_misses >= stop_misses:
                    print(f"  [{chamber}] GA {ga}: stopping at {n} (consecutive misses: {consecutive_misses})")
                    break
            continue

        # Reset miss tracking
        saw_any_existing = True
        consecutive_misses = 0

        # Keyword filter
        hits = keywords_found(text, keywords)
        if not hits:
            continue

        # Save text
        path = save_text(ga, chamber, n, text)
        bill_id = f"{chamber}{n:04d}"

        rows.append({
            "State": "Illinois",
            "GA": str(ga),
            "Bill Identifier": bill_id,
            "URL": url,
            "Path to full text": path,
            "Keywords": ", ".join(hits),
        })

        # Progress logging
        if len(rows) % 25 == 0:
            print(f"  [{chamber}] GA {ga}: {len(rows)} keyword matches so far…")

    return rows

def main():
    parser = argparse.ArgumentParser(description="ILGA Phase 1 — probe Full-Text URLs, save matches, write minimal CSV.")
    parser.add_argument("--ga-start", type=int, default=GA_START_DEFAULT, help="Starting GA (inclusive)")
    parser.add_argument("--ga-end", type=int, default=GA_END_DEFAULT, help="Ending GA (inclusive)")
    parser.add_argument("--chambers", type=str, default="HB,SB", help="Comma list: HB, SB, or both (e.g., HB,SB)")
    parser.add_argument("--max-docnum", type=int, default=MAX_DOCNUM_DEFAULT, help="Upper bound to probe per GA+chamber")
    parser.add_argument("--stop-misses", type=int, default=STOP_MISSES_DEFAULT, help="Stop after this many 404s once we've seen any hits")
    parser.add_argument("--out", type=str, default=OUT_CSV_DEFAULT, help="Output CSV path")
    args = parser.parse_args()

    ga_start = max(77, int(args.ga_start))
    ga_end   = min(103, int(args.ga_end))
    chambers = [c.strip().upper() for c in args.chambers.split(",") if c.strip()]
    max_docnum   = int(args.max_docnum or MAX_DOCNUM_DEFAULT)
    stop_misses  = int(args.stop_misses or STOP_MISSES_DEFAULT)
    out_csv_path = Path(args.out)

    print(f"Phase 1: GA {ga_start}..{ga_end}, chambers={chambers}, max_docnum={max_docnum}, stop_misses={stop_misses}")

    # Warm-up (helps set cookies/cloudfront)
    try:
        sleep_politely()
        SESSION.get(BASE + "/", timeout=(5, 15))
        sleep_politely()
        SESSION.get(BASE + "/Legislation/", timeout=(5, 15))
    except requests.RequestException:
        pass

    all_rows: List[dict] = []
    for ga in range(ga_start, ga_end + 1):
        print(f"== GA {ga} ==")
        for chamber in chambers:
            print(f" Probing {chamber} full-text pages…")
            rows = enumerate_and_collect(
                ga=ga,
                chamber=chamber,
                max_docnum=max_docnum,
                stop_misses=stop_misses,
                keywords=KEYWORDS,
            )
            print(f"  [{chamber}] GA {ga}: matched {len(rows)} bills with keywords.")
            all_rows.extend(rows)

    # Write CSV
    out_csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["State", "GA", "Bill Identifier", "URL", "Path to full text", "Keywords"]
    with out_csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(all_rows)

    print(f"Done. Wrote {len(all_rows)} rows to {out_csv_path}")

if __name__ == "__main__":
    main()
