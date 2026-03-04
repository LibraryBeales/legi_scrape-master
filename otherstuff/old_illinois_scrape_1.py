#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ILGA scraper — multi-session with robust list fetching.
- Handles sporadic 500s on RegularSession list pages.
- Falls back across SessionId/SessionID.
- Prewarms cookies, sets Referer, rotates UA, and uses HTTPAdapter retries.
"""

import argparse
import csv
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

# ===================== USER SETTINGS =====================
KEYWORDS = [
    "Immigration", "Citizenship", "Alien", "Migrant",
    "Undocumented", "Visa", "Border", "Foreign",
]

SESSION_ID_START = 114   # inclusive; override with --session-start
SESSION_ID_END   = 114   # inclusive; override with --session-end

DOC_TYPES = ["HB", "SB"]  # bills only

REQUESTS_PER_MINUTE = 30
JITTER_RANGE_SECONDS = (0.6, 1.8)
PAUSE_EVERY_N_REQUESTS = 40
PAUSE_DURATION_SECONDS = 20

MAX_BILLS_PER_TYPE = None  # None = no cap

OUT_CSV_TMPL = "illinois_bills_keywords_sessions_{start}_{end}.csv"
BILL_TEXT_ROOT = Path("bill_texts")
# =========================================================

BASE = "https://www.ilga.gov"
UA_POOL = [
    # rotate between a couple of common desktop UAs
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0",
]
HEADERS = {
    "User-Agent": random.choice(UA_POOL),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": "https://www.ilga.gov/Legislation/",
}

LIST_URL_TMPLS = [
    BASE + "/Legislation/RegularSession/{doctype}?SessionId={sid}",
    BASE + "/Legislation/RegularSession/{doctype}?SessionID={sid}",
]

CSV_COLUMNS = [
    "State","GA","Policy (bill) identifier","Policy sponsor","Policy sponsor party",
    "Link to bill","bill text","Cosponsor","Act identifier","Matched keywords",
    "Introduced date","Effective date","Passed introduced chamber date",
    "Passed second chamber date","Dead date","Enacted (Y/N)","Enacted Date",
]

# ---- session + polite throttling ----
BASE_SLEEP = 60.0 / max(1, REQUESTS_PER_MINUTE)
_req_count = 0
_session = None

def _build_session() -> requests.Session:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    s = requests.Session()
    s.headers.update(HEADERS)
    # Robust retry for server errors in addition to tenacity
    retry_strategy = Retry(
        total=5, backoff_factor=1.0,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def _ensure_session():
    global _session
    if _session is None:
        _session = _build_session()

def _rotate_ua_and_reset():
    global _session
    HEADERS["User-Agent"] = random.choice(UA_POOL)
    _session = _build_session()

def polite_sleep():
    global _req_count
    _req_count += 1
    time.sleep(BASE_SLEEP + random.uniform(*JITTER_RANGE_SECONDS))
    if PAUSE_EVERY_N_REQUESTS and _req_count % PAUSE_EVERY_N_REQUESTS == 0:
        time.sleep(PAUSE_DURATION_SECONDS)

@retry(wait=wait_exponential(multiplier=1, min=2, max=45), stop=stop_after_attempt(6))
def fetch(url: str) -> requests.Response:
    """GET with tenacity; on hard 5xx we rotate UA and rebuild session once per attempt."""
    _ensure_session()
    polite_sleep()
    r = _session.get(url, timeout=(10, 60))
    # If ILGA throws a 500/503 sporadically, force a retry and rebuild session/UA
    if r.status_code >= 500:
        _rotate_ua_and_reset()
        polite_sleep()
        r = _session.get(url, timeout=(10, 60))
    r.raise_for_status()
    return r

def soupify(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(html, "html.parser")

def clean(s: Optional[str]) -> str:
    import re as _re
    return _re.sub(r"\s+", " ", (s or "")).strip()

def keywords_found(text: str) -> List[str]:
    t = text.lower()
    hits = [kw for kw in KEYWORDS if kw.lower() in t]
    seen, out = set(), []
    for kw in hits:
        if kw not in seen:
            seen.add(kw)
            out.append(kw)
    return out

def save_bill_text(ga: int, doctype: str, docnum: str, text: str) -> str:
    folder = BILL_TEXT_ROOT / f"GA{ga if ga else 'unknown'}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{doctype}{docnum}.txt"
    try:
        path.write_text(text, encoding="utf-8")
        return str(path).replace("\\", "/")
    except Exception:
        return ""

# ===================== Enumerate bills =====================
@dataclass
class BillRef:
    session_id: int
    doctype: str
    docnum: str
    bill_status_url: str

def prewarm():
    """Touch a couple pages to set cookies and context; ignore errors."""
    try:
        fetch(BASE + "/")
    except Exception:
        pass
    try:
        fetch(BASE + "/Legislation/")
    except Exception:
        pass

def enumerate_bill_list(doctype: str, session_id: int, limit: Optional[int] = None) -> List[BillRef]:
    """
    Try both SessionId/SessionID list URLs. If both fail (5xx), return [] (don't crash).
    """
    refs: List[BillRef] = []
    last_exc: Optional[Exception] = None

    for tmpl in LIST_URL_TMPLS:
        url = tmpl.format(doctype=doctype, sid=session_id)
        try:
            r = fetch(url)
            soup = soupify(r.text)
            for a in soup.select("a[href*='/Legislation/BillStatus']"):
                href = a.get("href", "")
                if "DocTypeID=" not in href or "DocNum=" not in href:
                    continue
                abs_url = urljoin(BASE, href)
                m_num = re.search(r"DocNum=(\d+)", abs_url)
                m_type = re.search(r"DocTypeID=([A-Z]+)", abs_url)
                if not (m_num and m_type):
                    continue
                docnum = m_num.group(1)
                doctype_in_url = m_type.group(1).upper()
                if doctype_in_url != doctype:
                    continue
                refs.append(BillRef(session_id=session_id, doctype=doctype, docnum=docnum, bill_status_url=abs_url))
                if limit and len(refs) >= limit:
                    break
            if refs:
                break  # success on this template
        except Exception as e:
            last_exc = e
            _rotate_ua_and_reset()
            continue

    # De-dup
    seen, out = set(), []
    for ref in refs:
        key = (ref.session_id, ref.doctype, ref.docnum)
        if key not in seen:
            seen.add(key)
            out.append(ref)

    if not out and last_exc:
        # Log a concise note; continue gracefully
        print(f"[warn] Could not fetch list for {doctype} Session {session_id}: {last_exc}")
    return out

# ===================== Bill Status scraping =====================
ACTION_ROW_RE = re.compile(r"^\s*(\d{1,2}/\d{1,2}/\d{4})\s+(\w+)\s+(.*)$")

def parse_actions_for_dates(actions_text: List[str], origin_chamber: str) -> Dict[str, str]:
    introduced_date = ""
    passed_first_chamber = ""
    passed_second_chamber = ""
    effective_date = ""
    enacted_date = ""
    dead_date = ""
    other_chamber = "Senate" if origin_chamber == "House" else "House"

    for line in actions_text:
        line = clean(line)
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
        if any(tag in action for tag in ["Session Sine Die", "Re-referred to Rules Committee", "Rule 19(a)", "Rule 3-9(a)", "Vetoed", "Amendatory Veto Overridden - Fail"]):
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
    act_identifier = ""
    effective_literal = ""
    header = soup.get_text(" ")
    m_pa = re.search(r"Public Act\s+(\d{2,3}-\d{4})", header)
    if m_pa:
        act_identifier = m_pa.group(1)
    m_eff = re.search(r"Effective Date\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", header)
    if m_eff:
        effective_literal = m_eff.group(1)
    return act_identifier, effective_literal

def collect_cosponsors_from_actions(actions_text: List[str]) -> str:
    names: List[str] = []
    for line in actions_text:
        line = clean(line)
        if "Added as Co-Sponsor" in line or "Added as Chief Co-Sponsor" in line:
            nm = re.sub(r".*Added as (Chief )?Co-Sponsor\s+", "", line)
            nm = re.sub(r"\s*;.*$", "", nm)
            nm = re.sub(r"\s*\[[^\]]+\]\s*$", "", nm)
            if nm:
                names.append(nm)
    seen, out = set(), []
    for n in names:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return "; ".join(out)

def fetch_member_party(member_url: str) -> str:
    try:
        r = fetch(member_url)
        s = soupify(r.text)
        txt = s.get_text(" ")
        m = re.search(r"\(([DRI])\)\s*-\s*\d{2,3}th General Assembly", txt)
        if m:
            return m.group(1)
    except Exception:
        pass
    return ""

def extract_sponsors_and_party(soup: BeautifulSoup) -> Tuple[str, str]:
    sponsor_blocks = []
    for hdr in soup.find_all(["h4", "h5"]):
        if hdr.get_text(strip=True) in ("Senate Sponsors", "House Sponsors"):
            ul = hdr.find_next("ul")
            if ul:
                sponsor_blocks.append(ul.get_text(" ", strip=True))
    sponsor_line = " | ".join([clean(b) for b in sponsor_blocks if b])

    primary_link = None
    for hdr in soup.find_all(["h4", "h5"]):
        if hdr.get_text(strip=True) in ("Senate Sponsors", "House Sponsors"):
            a = hdr.find_next("a", href=True)
            if a:
                primary_link = urljoin(BASE, a["href"])
                break
    party = fetch_member_party(primary_link) if primary_link else ""
    return sponsor_line, party

def collect_full_text_versions(bill_status_soup: BeautifulSoup) -> List[str]:
    ft_link = bill_status_soup.find("a", href=re.compile(r"/Legislation/BillStatus/FullText", re.I))
    if not ft_link:
        return []
    ft_url = urljoin(BASE, ft_link["href"])
    texts: List[str] = []

    try:
        r = fetch(ft_url)
    except Exception:
        return texts
    s = soupify(r.text)

    version_links = []
    for a in s.select("a[href]"):
        href = a.get("href", "")
        if re.search(r"/Legislation/BillStatus/FullText", href, re.I):
            version_links.append(urljoin(ft_url, href))
    version_links.append(ft_url)

    seen = set()
    uniq = []
    for u in version_links:
        if u not in seen:
            seen.add(u)
            uniq.append(u)

    for vurl in uniq:
        try:
            rr = fetch(vurl)
            ss = soupify(rr.text)
            texts.append(clean(ss.get_text(" ")))
        except Exception:
            continue
    return texts

def parse_ga_from_url(url: str) -> str:
    m = re.search(r"[?&]GA=(\d+)\b", url)
    return m.group(1) if m else ""

# ===================== Processing =====================
@dataclass
class BillRef:
    session_id: int
    doctype: str
    docnum: str
    bill_status_url: str

def process_bill(ref: BillRef) -> Optional[Dict[str, str]]:
    try:
        r = fetch(ref.bill_status_url)
    except Exception:
        return None
    bs = soupify(r.text)

    sponsor_line, sponsor_party = extract_sponsors_and_party(bs)

    actions_lines: List[str] = []
    for hdr in bs.find_all(["h4", "h5"]):
        if hdr.get_text(strip=True) == "Actions":
            tbl = hdr.find_next("table")
            if tbl:
                for tr in tbl.select("tr"):
                    t = clean(tr.get_text(" "))
                    if t:
                        actions_lines.append(t)
            break

    origin = "House" if ref.doctype == "HB" else "Senate"
    dates = parse_actions_for_dates(actions_lines, origin)

    act_identifier, effective_literal = extract_public_act_and_effective_text(bs)
    enacted_yn = "Y" if act_identifier else ""
    if effective_literal and not dates["Effective date"]:
        dates["Effective date"] = effective_literal

    cosponsor = collect_cosponsors_from_actions(actions_lines)

    texts = collect_full_text_versions(bs)
    full_text = " ".join([t for t in texts if t]).strip()

    hits = keywords_found(full_text)
    if not hits:
        return None

    ga_str = parse_ga_from_url(ref.bill_status_url)
    ga_int = int(ga_str) if ga_str.isdigit() else 0

    text_path = save_bill_text(ga_int, ref.doctype, ref.docnum, full_text or "")

    billno = f"{ref.doctype}{int(ref.docnum):04d}"
    return {
        "State": "Illinois",
        "GA": ga_str,
        "Policy (bill) identifier": billno,
        "Policy sponsor": sponsor_line,
        "Policy sponsor party": sponsor_party,
        "Link to bill": ref.bill_status_url,
        "bill text": text_path or "",
        "Cosponsor": cosponsor,
        "Act identifier": act_identifier,
        "Matched keywords": ", ".join(hits),
        "Introduced date": dates["Introduced date"],
        "Effective date": dates["Effective date"],
        "Passed introduced chamber date": dates["Passed introduced chamber date"],
        "Passed second chamber date": dates["Passed second chamber date"],
        "Dead date": dates["Dead date"],
        "Enacted (Y/N)": enacted_yn,
        "Enacted Date": dates["Enacted Date"],
    }

# ===================== Main =====================
def main():
    parser = argparse.ArgumentParser(description="Scrape ILGA bills over a range of SessionIDs.")
    parser.add_argument("--session-start", type=int, default=SESSION_ID_START)
    parser.add_argument("--session-end", type=int, default=SESSION_ID_END)
    parser.add_argument("--max-per-type", type=int, default=(MAX_BILLS_PER_TYPE or 0),
                        help="Max bills per type per session (0 = no cap)")
    args = parser.parse_args()

    session_start = int(args.session_start)
    session_end = int(args.session_end)
    cap = None if int(args.max_per_type) <= 0 else int(args.max_per_type)

    out_csv = OUT_CSV_TMPL.format(start=session_start, end=session_end)

    # Prewarm cookies / context so list pages are less likely to 500
    prewarm()

    rows: List[Dict[str, str]] = []
    seen: Set[Tuple[int, str, str]] = set()  # (session_id, doctype, docnum)

    for sid in range(session_start, session_end + 1):
        for doctype in DOC_TYPES:
            refs = enumerate_bill_list(doctype, sid, limit=cap)
            print(f"Session {sid} {doctype}: discovered {len(refs)} Bill Status links (cap={cap})")
            if not refs:
                # we skip gracefully if the list endpoint kept failing
                continue
            for ref in refs:
                key = (ref.session_id, ref.doctype, ref.docnum)
                if key in seen:
                    continue
                seen.add(key)
                try:
                    row = process_bill(ref)
                except Exception:
                    row = None
                if row:
                    rows.append(row)

    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote {len(rows)} rows to {out_csv}")

if __name__ == "__main__":
    main()
