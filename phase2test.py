#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ILGA Phase 2 — Enrich Illinois bill rows by FOLLOWING the Bill Status link from the Full-Text page.

Input CSV columns (Phase 1):
  State, GA, Bill Identifier, URL, Path to full text, Keywords

Output CSV columns (requested):
  State,GA,Policy (bill) identifier,Policy sponsor,Policy sponsor party,Link to bill,
  bill text,Cosponsor,Act identifier,Matched keywords,Introduced date,Effective date,
  Passed introduced chamber date,Passed second chamber date,Dead date,Enacted (Y/N),Enacted Date
"""

import argparse
import csv
import random
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, FeatureNotFound

BASE = "https://www.ilga.gov"

# ---------- HTTP session / politeness ----------
UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
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
REQUEST_DELAY = (0.25, 0.55)  # jittered per request

def sleep_politely():
    time.sleep(random.uniform(*REQUEST_DELAY))

def soupify(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(html, "html.parser")

def clean(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

# ---------- GA → GAID (fallbacks for older ASP pages when needed) ----------
GA_TO_GAID: Dict[int, int] = {
    103: 17, 102: 16, 101: 15, 100: 14,  99: 13,  98: 12,  97: 11,  96: 10,
     95:  9,  94:  8,  93:  3,  92:  2,  91:  1,  90:  7,  89:  6,  88:  5,
     87:  4,  86: -1,  85: -2,  84: -3,  83: -4,  82: -5,  81: -6,  80: -7,
     79: -8,  78: -9,  77: -10,
}

# ---------- Bill Status URL candidates (fallbacks) ----------
def static_billstatus_candidates(ga: int, doctype: str, docnum: int) -> List[str]:
    # e.g., https://www.ilga.gov/legislation/93/BillStatus/09300HB0001.html|.htm
    base = f"{BASE}/legislation/{ga}/BillStatus/{ga:03d}00{doctype}{docnum:04d}"
    return [base + ".html", base + ".htm"]

def asp_billstatus_candidates(ga: int, doctype: str, docnum: int) -> List[str]:
    gaid = GA_TO_GAID.get(ga, 0)
    d = str(int(docnum))
    return [
        f"{BASE}/legislation/BillStatus.asp?DocNum={d}&DocTypeID={doctype}&GAID={gaid}&GA={ga}",
        f"{BASE}/Legislation/BillStatus.asp?DocNum={d}&DocTypeID={doctype}&GAID={gaid}&GA={ga}",
        f"{BASE}/Legislation/BillStatus?DocNum={d}&DocTypeID={doctype}&GAID={gaid}&GA={ga}",
        # GAID-less variants (sometimes OK in newer eras)
        f"{BASE}/Legislation/BillStatus.asp?DocNum={d}&DocTypeID={doctype}&GA={ga}",
        f"{BASE}/legislation/BillStatus.asp?DocNum={d}&DocTypeID={doctype}&GA={ga}",
    ]

def candidate_status_urls(ga: int, doctype: str, docnum: int) -> List[str]:
    # Fallbacks only; primary path is to follow the link from the Full-Text page
    return static_billstatus_candidates(ga, doctype, docnum) + asp_billstatus_candidates(ga, doctype, docnum)

# ---------- Parsing helpers ----------
ACTION_ROW_RE = re.compile(r"^\s*(\d{1,2}/\d{1,2}/\d{4})\s+(\w+)\s+(.*)$")
PA_RE = re.compile(r"Public Act\s+(\d{2,3}-\d{4})")
EFF_DATE_RE = re.compile(r"Effective Date\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})")

def parse_actions_table_any(soup: BeautifulSoup) -> List[str]:
    # Prefer a section titled "Actions" or similar
    for hdr in soup.find_all(["h2", "h3", "h4", "h5"]):
        if "action" in hdr.get_text(" ", strip=True).lower():
            tbl = hdr.find_next("table")
            if tbl:
                rows = [clean(tr.get_text(" ")) for tr in tbl.select("tr")]
                rows = [r for r in rows if r]
                if rows:
                    return rows
    # Fallback: any table with multiple date-like rows
    best: List[str] = []
    for tbl in soup.select("table"):
        rows = [clean(tr.get_text(" ")) for tr in tbl.select("tr")]
        rows = [r for r in rows if r]
        score = sum(1 for r in rows if re.search(r"\d{1,2}/\d{1,2}/\d{4}", r))
        if score >= 3 and len(rows) > len(best):
            best = rows
    return best

def parse_actions_for_dates(actions_text: List[str], origin_chamber: str) -> Dict[str, str]:
    introduced_date = ""
    passed_first_chamber = ""
    passed_second_chamber = ""
    effective_date = ""
    enacted_date = ""
    dead_date = ""
    other_chamber = "Senate" if origin_chamber == "House" else "House"
    for line in actions_text:
        m = ACTION_ROW_RE.match(line)
        if not m:
            continue
        date, chamber, action = m.groups()
        chamber = chamber.capitalize()  # normalize
        if not introduced_date and (("Filed with Secretary" in action) or ("First Reading" in action and chamber == origin_chamber)):
            introduced_date = date
        if (not passed_first_chamber) and ("Third Reading" in action and "Passed" in action and chamber == origin_chamber):
            passed_first_chamber = date
        if (not passed_second_chamber) and ("Third Reading" in action and "Passed" in action and chamber == other_chamber):
            passed_second_chamber = date
        if ("Governor Approved" in action) and not enacted_date:
            enacted_date = date
        if ("Effective Date" in action) and not effective_date:
            effective_date = date
        if any(tag in action for tag in [
            "Session Sine Die", "Re-referred to Rules Committee", "Rule 19(a)", "Rule 3-9(a)",
            "Vetoed", "Amendatory Veto Overridden - Fail"
        ]):
            dead_date = date
    return {
        "Introduced date": introduced_date,
        "Passed introduced chamber date": passed_first_chamber,
        "Passed second chamber date": passed_second_chamber,
        "Effective date": effective_date,
        "Enacted Date": enacted_date,
        "Dead date": dead_date,
    }

def extract_public_act_and_effective_text(soup: BeautifulSoup) -> Tuple[str, str]:
    txt = soup.get_text(" ")
    act_identifier = ""
    effective_literal = ""
    m_pa = PA_RE.search(txt)
    if m_pa:
        act_identifier = m_pa.group(1)
    m_eff = EFF_DATE_RE.search(txt)
    if m_eff:
        effective_literal = m_eff.group(1)
    return act_identifier, effective_literal

def fetch_member_party(member_url: str) -> str:
    """Best-effort party extraction from a member profile."""
    try:
        sleep_politely()
        r = SESSION.get(member_url, timeout=(5, 15))
        r.raise_for_status()
        txt = soupify(r.text).get_text(" ")
        m = re.search(r"\(([DRI])\)\s*-\s*\d{2,3}th General Assembly", txt)
        if m:
            return m.group(1)
    except Exception:
        pass
    return ""

def extract_sponsors_and_party(soup: BeautifulSoup) -> Tuple[str, str]:
    # Pull sponsor blocks (works on both static and ASP pages)
    sponsor_blocks: List[str] = []
    for hdr in soup.find_all(["h2", "h3", "h4", "h5"]):
        title = hdr.get_text(" ", strip=True).lower()
        if "sponsor" in title:
            ul = hdr.find_next("ul")
            if ul:
                sponsor_blocks.append(clean(ul.get_text(" ", strip=True)))
    sponsor_line = " | ".join([b for b in sponsor_blocks if b])

    # Try to get party of primary sponsor via first link under a sponsor header
    sponsor_party = ""
    primary_link = None
    for hdr in soup.find_all(["h2", "h3", "h4", "h5"]):
        if "sponsor" in hdr.get_text(" ", strip=True).lower():
            a = hdr.find_next("a", href=True)
            if a:
                primary_link = urljoin(BASE, a["href"])
                break
    if primary_link:
        sponsor_party = fetch_member_party(primary_link)

    return sponsor_line, sponsor_party

def extract_cosponsors_from_text(soup: BeautifulSoup) -> str:
    """Static/ASP pages often have 'Co-Sponsors:' inline; get whatever follows the label."""
    txt = soup.get_text(" ")
    m = re.search(r"(Co-?Sponsors?:\s*)(.+?)(?:\s{2,}|\n|$)", txt, flags=re.IGNORECASE)
    if m:
        return clean(m.group(2))
    return ""

# ---------- Resolve Bill Status URL from the Full-Text page ----------
def resolve_status_url_from_fulltext(fulltext_url: str) -> Optional[str]:
    """
    Fetch the Full-Text page and extract the Bill Status URL (href contains 'BillStatus').
    Returns absolute URL or None.
    """
    try:
        sleep_politely()
        r = SESSION.get(fulltext_url, timeout=(10, 45))
        if r.status_code == 404:
            return None
        r.raise_for_status()
    except requests.RequestException:
        return None

    s = soupify(r.text)

    # 1) any <a> whose href contains 'BillStatus'
    for a in s.select("a[href]"):
        href = a.get("href", "")
        if "BillStatus" in href:
            return urljoin(fulltext_url, href)

    # 2) any link whose text contains 'Bill Status'
    for a in s.find_all("a"):
        txt = a.get_text(" ", strip=True).lower()
        if "bill status" in txt and a.get("href"):
            return urljoin(fulltext_url, a["href"])

    return None

# ---------- Per-bill enrichment ----------
def enrich_from_bill_status(ga: int, bill_identifier: str, fulltext_url_str: str) -> Dict[str, str]:
    """
    bill_identifier like 'HB1234' or 'SB0099'.
    First try: resolve Bill Status URL by reading the Full-Text page.
    Fallbacks: static/ASP candidate URLs.
    """
    doctype = bill_identifier[:2].upper()
    docnum = int(bill_identifier[2:])
    origin = "House" if doctype == "HB" else "Senate"

    # 1) Try to resolve via Full-Text page link (preferred; includes GAID/SessionID/LegId)
    status_url = resolve_status_url_from_fulltext(fulltext_url_str)
    status_urls: List[str] = []
    if status_url:
        status_urls.append(status_url)

    # 2) Add fallbacks (static + ASP variants)
    status_urls.extend(candidate_status_urls(ga, doctype, docnum))

    # Fetch first working page and parse
    for url in status_urls:
        try:
            sleep_politely()
            r = SESSION.get(url, timeout=(10, 45))
            if r.status_code == 404:
                continue
            r.raise_for_status()
        except requests.RequestException:
            continue

        s = soupify(r.text)

        sponsor_line, sponsor_party = extract_sponsors_and_party(s)
        actions_lines = parse_actions_table_any(s)
        dates = parse_actions_for_dates(actions_lines, origin)
        act_identifier, effective_literal = extract_public_act_and_effective_text(s)
        if effective_literal and not dates["Effective date"]:
            dates["Effective date"] = effective_literal
        cosponsor = extract_cosponsors_from_text(s)

        if sponsor_line or cosponsor or act_identifier or any(dates.values()):
            return {
                "Policy sponsor": sponsor_line,
                "Policy sponsor party": sponsor_party,
                "Cosponsor": cosponsor,
                "Act identifier": act_identifier,
                "Introduced date": dates["Introduced date"],
                "Effective date": dates["Effective date"],
                "Passed introduced chamber date": dates["Passed introduced chamber date"],
                "Passed second chamber date": dates["Passed second chamber date"],
                "Dead date": dates["Dead date"],
                "Enacted (Y/N)": "Y" if act_identifier else "",
                "Enacted Date": dates["Enacted Date"],
            }

    # Nothing found
    return {
        "Policy sponsor": "",
        "Policy sponsor party": "",
        "Cosponsor": "",
        "Act identifier": "",
        "Introduced date": "",
        "Effective date": "",
        "Passed introduced chamber date": "",
        "Passed second chamber date": "",
        "Dead date": "",
        "Enacted (Y/N)": "",
        "Enacted Date": "",
    }

# ---------- Main ----------
OUT_COLUMNS = [
    "State","GA","Policy (bill) identifier","Policy sponsor","Policy sponsor party","Link to bill","bill text",
    "Cosponsor","Act identifier","Matched keywords","Introduced date","Effective date","Passed introduced chamber date",
    "Passed second chamber date","Dead date","Enacted (Y/N)","Enacted Date",
]

def main():
    ap = argparse.ArgumentParser(description="Enrich Illinois bill rows by following Bill Status link from Full-Text.")
    ap.add_argument("--in", dest="inp", required=True, help="Input CSV from Phase 1")
    ap.add_argument("--out", dest="outp", required=True, help="Output enriched CSV path")
    ap.add_argument("--only-ga", nargs="*", type=int, default=None,
                    help="Limit to these GA numbers (e.g., --only-ga 93 or --only-ga 92 93 94). If omitted, process all rows.")
    args = ap.parse_args()

    inp = Path(args.inp)
    outp = Path(args.outp)
    outp.parent.mkdir(parents=True, exist_ok=True)

    only_gas = set(args.only_ga) if args.only_ga else None

    with inp.open("r", encoding="utf-8", newline="") as f_in, outp.open("w", encoding="utf-8", newline="") as f_out:
        r = csv.DictReader(f_in)
        w = csv.DictWriter(f_out, fieldnames=OUT_COLUMNS)
        w.writeheader()

        processed = 0
        written = 0

        for row in r:
            state = (row.get("State") or "Illinois").strip()
            bill_id = (row.get("Bill Identifier") or "").strip()
            link = (row.get("URL") or "").strip()               # Full-Text link
            bill_text_path = (row.get("Path to full text") or "").strip()
            matched_keywords = (row.get("Keywords") or "").strip()

            # Determine GA (from CSV or infer from Full-Text URL)
            ga_str = (row.get("GA") or "").strip()
            if ga_str.isdigit():
                ga = int(ga_str)
            else:
                m = re.search(r"/Documents/legislation/(\d{2,3})/", link)
                ga = int(m.group(1)) if m else 0

            if only_gas and ga not in only_gas:
                continue

            enriched = enrich_from_bill_status(ga, bill_id, link)

            out_row = {
                "State": state,
                "GA": str(ga) if ga else "",
                "Policy (bill) identifier": bill_id,
                "Policy sponsor": enriched["Policy sponsor"],
                "Policy sponsor party": enriched["Policy sponsor party"],
                "Link to bill": link,
                "bill text": bill_text_path,
                "Cosponsor": enriched["Cosponsor"],
                "Act identifier": enriched["Act identifier"],
                "Matched keywords": matched_keywords,
                "Introduced date": enriched["Introduced date"],
                "Effective date": enriched["Effective date"],
                "Passed introduced chamber date": enriched["Passed introduced chamber date"],
                "Passed second chamber date": enriched["Passed second chamber date"],
                "Dead date": enriched["Dead date"],
                "Enacted (Y/N)": enriched["Enacted (Y/N)"],
                "Enacted Date": enriched["Enacted Date"],
            }
            w.writerow(out_row)

            processed += 1
            written += 1
            if written % 100 == 0:
                print(f"Wrote {written} rows…")

    print(f"Done. Processed {processed} input rows. Wrote {written} rows to {outp}")

if __name__ == "__main__":
    main()
