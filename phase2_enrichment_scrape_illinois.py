#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ILGA Phase 2 — Enrich Illinois bill rows from an existing CSV (Phase 1 output).

Input CSV columns (already scraped):
  State, GA, Bill Identifier, URL, Path to full text, Keywords

Output CSV columns (requested):
  State,GA,Policy (bill) identifier,Policy sponsor,Policy sponsor party,Link to bill,
  bill text,Cosponsor,Act identifier,Matched keywords,Introduced date,Effective date,
  Passed introduced chamber date,Passed second chamber date,Dead date,Enacted (Y/N),Enacted Date

Usage:
  py -3.11 ilga_phase2_enrich.py --in il/phase1.csv --out il/phase2_enriched.csv
"""

import argparse
import csv
import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, FeatureNotFound

# -------------- Politeness / requests --------------
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

REQUEST_DELAY = (0.25, 0.55)   # seconds; jittered

def sleep_politely():
    time.sleep(random.uniform(*REQUEST_DELAY))

def soupify(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(html, "html.parser")

def clean(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

# -------------- GA → GAID map (from ILGA dropdown; GA 77–103) --------------
GA_TO_GAID: Dict[int, int] = {
    103: 17, 102: 16, 101: 15, 100: 14,  99: 13,  98: 12,  97: 11,  96: 10,
     95:  9,  94:  8,  93:  3,  92:  2,  91:  1,  90:  7,  89:  6,  88:  5,
     87:  4,  86: -1,  85: -2,  84: -3,  83: -4,  82: -5,  81: -6,  80: -7,
     79: -8,  78: -9,  77: -10,
}

# -------------- Bill Status URL candidates --------------
def billstatus_url_candidates(ga: int, doctype: str, docnum: int) -> List[str]:
    gaid = GA_TO_GAID.get(ga, 0)
    d = str(int(docnum))
    return [
        f"{BASE}/legislation/BillStatus.asp?DocNum={d}&DocTypeID={doctype}&GAID={gaid}&GA={ga}",
        f"{BASE}/Legislation/BillStatus.asp?DocNum={d}&DocTypeID={doctype}&GAID={gaid}&GA={ga}",
        f"{BASE}/Legislation/BillStatus?DocNum={d}&DocTypeID={doctype}&GAID={gaid}&GA={ga}",
        # GAID-less variants (sometimes work in newer eras)
        f"{BASE}/Legislation/BillStatus.asp?DocNum={d}&DocTypeID={doctype}&GA={ga}",
        f"{BASE}/legislation/BillStatus.asp?DocNum={d}&DocTypeID={doctype}&GA={ga}",
    ]

# -------------- Parsing helpers --------------
ACTION_ROW_RE = re.compile(r"^\s*(\d{1,2}/\d{1,2}/\d{4})\s+(\w+)\s+(.*)$")
PA_RE = re.compile(r"Public Act\s+(\d{2,3}-\d{4})")
EFF_DATE_RE = re.compile(r"Effective Date\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})")

def parse_actions_table(soup: BeautifulSoup) -> List[str]:
    # Prefer explicit "Actions" header
    for hdr in soup.find_all(["h3","h4","h5"]):
        if hdr.get_text(strip=True).lower() == "actions":
            tbl = hdr.find_next("table")
            if tbl:
                out = [clean(tr.get_text(" ")) for tr in tbl.select("tr")]
                return [t for t in out if t]
    # Fallback: any table with date-like rows
    for tbl in soup.select("table"):
        txt = clean(tbl.get_text(" "))
        if re.search(r"\d{1,2}/\d{1,2}/\d{4}", txt) and ("Reading" in txt or "Governor" in txt or "Veto" in txt):
            out = [clean(tr.get_text(" ")) for tr in tbl.select("tr")]
            out = [t for t in out if t]
            if out:
                return out
    return []

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
    header = soup.get_text(" ")
    act_identifier = ""
    effective_literal = ""
    m_pa = PA_RE.search(header)
    if m_pa:
        act_identifier = m_pa.group(1)
    m_eff = EFF_DATE_RE.search(header)
    if m_eff:
        effective_literal = m_eff.group(1)
    return act_identifier, effective_literal

def collect_cosponsors_from_actions(actions_text: List[str]) -> str:
    names: List[str] = []
    for line in actions_text:
        if "Added as Co-Sponsor" in line or "Added as Chief Co-Sponsor" in line:
            nm = re.sub(r".*Added as (Chief )?Co-Sponsor\s+", "", line)
            nm = re.sub(r"\s*;.*$", "", nm)
            nm = re.sub(r"\s*\[[^\]]+\]\s*$", "", nm)
            nm = nm.strip()
            if nm:
                names.append(nm)
    # de-dup preserving order
    seen, out = set(), []
    for n in names:
        if n not in seen:
            seen.add(n); out.append(n)
    return "; ".join(out)

def fetch_member_party(member_url: str) -> str:
    """Best-effort party extraction from a member profile; short timeout to avoid stalls."""
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
    sponsor_line = ""
    blocks = []
    for hdr in soup.find_all(["h3","h4","h5"]):
        title = hdr.get_text(strip=True)
        if title in ("Senate Sponsors", "House Sponsors"):
            ul = hdr.find_next("ul")
            if ul:
                blocks.append(clean(ul.get_text(" ", strip=True)))
    if blocks:
        sponsor_line = " | ".join([b for b in blocks if b])

    # Try to get party of the primary sponsor via first link under a sponsor header
    primary_link = None
    for hdr in soup.find_all(["h3","h4","h5"]):
        if hdr.get_text(strip=True) in ("Senate Sponsors", "House Sponsors"):
            a = hdr.find_next("a", href=True)
            if a:
                primary_link = urljoin(BASE, a["href"])
                break
    sponsor_party = fetch_member_party(primary_link) if primary_link else ""
    return sponsor_line, sponsor_party

# -------------- Enrichment per bill --------------
def enrich_from_bill_status(ga: int, bill_identifier: str) -> Dict[str, str]:
    """
    bill_identifier like 'HB1234' or 'SB0099'
    """
    doctype = bill_identifier[:2].upper()
    docnum = int(bill_identifier[2:])
    origin = "House" if doctype == "HB" else "Senate"

    # Try multiple Bill Status URL variants until one yields data
    for burl in billstatus_url_candidates(ga, doctype, docnum):
        try:
            sleep_politely()
            r = SESSION.get(burl, timeout=(10, 45))
            if r.status_code == 404:
                continue
            r.raise_for_status()
        except requests.RequestException:
            continue

        s = soupify(r.text)
        sponsor_line, sponsor_party = extract_sponsors_and_party(s)
        actions_lines = parse_actions_table(s)
        dates = parse_actions_for_dates(actions_lines, origin)
        cosponsor = collect_cosponsors_from_actions(actions_lines)
        act_identifier, effective_literal = extract_public_act_and_effective_text(s)
        if effective_literal and not dates["Effective date"]:
            dates["Effective date"] = effective_literal

        # If we got anything, return this result set
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

    # Nothing found; return empty fields
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

# -------------- Main --------------
OUT_COLUMNS = [
    "State","GA","Policy (bill) identifier","Policy sponsor","Policy sponsor party","Link to bill","bill text",
    "Cosponsor","Act identifier","Matched keywords","Introduced date","Effective date","Passed introduced chamber date",
    "Passed second chamber date","Dead date","Enacted (Y/N)","Enacted Date",
]

def main():
    ap = argparse.ArgumentParser(description="Enrich Illinois bill rows (Phase 2).")
    ap.add_argument("--in", dest="inp", required=True, help="Input CSV from Phase 1")
    ap.add_argument("--out", dest="outp", required=True, help="Output enriched CSV path")
    args = ap.parse_args()

    inp = Path(args.inp)
    outp = Path(args.outp)
    outp.parent.mkdir(parents=True, exist_ok=True)

    with inp.open("r", encoding="utf-8", newline="") as f_in, outp.open("w", encoding="utf-8", newline="") as f_out:
        r = csv.DictReader(f_in)
        w = csv.DictWriter(f_out, fieldnames=OUT_COLUMNS)
        w.writeheader()

        count = 0
        for row in r:
            # Expect: State, GA, Bill Identifier, URL, Path to full text, Keywords
            state = row.get("State", "").strip() or "Illinois"
            ga_str = row.get("GA", "").strip()
            bill_id = row.get("Bill Identifier", "").strip()
            link = row.get("URL", "").strip()
            bill_text_path = row.get("Path to full text", "").strip()
            matched_keywords = row.get("Keywords", "").strip()

            try:
                ga = int(ga_str)
            except Exception:
                # If GA missing/invalid, try to infer from the full-text URL
                m = re.search(r"/Documents/legislation/(\d{2,3})/", link)
                ga = int(m.group(1)) if m else 0

            enriched = enrich_from_bill_status(ga, bill_id)

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

            count += 1
            if count % 100 == 0:
                print(f"Processed {count} rows…")

    print(f"Done. Wrote {count} rows to {outp}")

if __name__ == "__main__":
    main()
