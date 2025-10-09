#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Iowa Legislature bill scraper — GA range, keyword filter.
- Saves bill texts to bill_texts/GA{ga}/{billno}.txt
- Writes the path to the text file in the CSV 'bill text' field
- Parses Bill History dates from div.billAction table (and date-in-URL fallback)
- No Playwright required

Install:
    pip install requests beautifulsoup4 lxml tenacity tqdm pdfminer.six
Run:
    python iowa_legis_scrape.py
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
from bs4 import BeautifulSoup
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

GA_START = 91
GA_END   = 91

# Bill types to crawl
LEG_TYPES = ["HF", "SF", "HSB", "SSB"]

# Polite rate limits
REQUESTS_PER_MINUTE = 30            # ~1 request every 2 seconds
JITTER_RANGE_SECONDS = (0.6, 1.8)   # small random jitter
PAUSE_EVERY_N_REQUESTS = 40         # periodic pause
PAUSE_DURATION_SECONDS = 20

# Optional: cap bills per GA during testing (None = all)
MAX_BILLS_PER_GA = None

# Output CSV (no "Policy name" column)
OUT_CSV = "iowa_bills_keywords.csv"

# Where to store bill text files
BILL_TEXT_ROOT = Path("bill_texts")   # files -> bill_texts/GA{ga}/{billno}.txt
# ===============================================================

BASE = "https://www.legis.iowa.gov"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; academic-research; +https://example.org)",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

DIRECTORY_URL_TMPL = (
    BASE + "/legislation/billTracking/directory/index/listing"
           "?ga={ga}&legType={leg}&min={min_n}&max={max_n}"
)

CSV_COLUMNS = [
    "State",
    "Policy (bill) identifier",
    "Policy sponsor",
    "Policy sponsor party",
    "Link to bill",
    "bill text",
    "Cosponsor",
    "Introduced (Y/N)",
    "Introduced date",
    "Chamber where introduced",
    "Effective (Y/N)",
    "Effective date",
    "Passed introduced chamber (Y/N)",
    "Passed introduced chamber date",
    "Passed second chamber (Y/N)",
    "Passed second chamber date",
    "Pending (Y/N)",
    "Dead (Y/N)",
    "Chamber where died",
    "Dead date",
    "Enacted (Y/N)",
    "Act identifier",
    "Enacted Date",
    "Matched keywords",
]

# ---- Throttling ----
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
    return BeautifulSoup(html, "lxml")

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

# --- Date helpers ---
DATE_PATS = [
    re.compile(r"\b([A-Za-z]+ \d{1,2}, \d{4})\b"),   # Month DD, YYYY
    re.compile(r"\b(\d{1,2}/\d{1,2}/\d{4})\b"),      # MM/DD/YYYY
]
YMD_IN_HREF = re.compile(r"(?:[?&](?:hdate|date|actionDate)=)(\d{8})", re.I)
YMD_IN_PATH = re.compile(r"/(?:HJNL|SJNL|HJRNL|SJRNL)/(\d{8})", re.I)

def _ymd_to_mdy(ymd: str) -> str:
    # yyyymmdd -> M/D/YYYY
    y, m, d = ymd[:4], ymd[4:6], ymd[6:8]
    return f"{int(m)}/{int(d)}/{y}"

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

# ===================== SPONSOR PARSING =====================
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

# ===================== BILL HISTORY PARSER (HTML) =====================
INTRO_RE   = re.compile(r"\bIntroduced\b", re.I)
PASSED_H   = re.compile(r"\bPassed\s+House\b", re.I)
PASSED_S   = re.compile(r"\bPassed\s+Senate\b", re.I)
SIGNED_GOV = re.compile(r"\b(Signed|Approved)\s+by\s+Governor\b", re.I)
WITHDRAWN  = re.compile(r"\bWithdrawn\b|\bDied in (House|Senate)\b|\bFailed\b", re.I)
EFFECTIVE  = re.compile(r"\bEffective (?:date|on)\b|\bEffective\b", re.I)
SPONSORS   = re.compile(r"\bSponsors?\s+added,?\s*(.+)", re.I)

def _rows_from_bill_action(soup: BeautifulSoup) -> List[Tuple[str, str]]:
    """
    Extract (date, action_text) rows from the canonical history table:
      <div class="billAction"><table class="billActionTable">
        <tbody><tr><td>MM/DD/YYYY</td><td>Action ... <a href="/.../20250304_...">H.J. ...</a></td></tr> ...
    Returns list of tuples (date_str_or_empty, full_action_text_with_links_text).
    """
    out: List[Tuple[str, str]] = []
    for div in soup.select("div.billAction"):
        for tr in div.select("table.billActionTable tbody tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            date_cell = clean(tds[0].get_text(" ")) if len(tds) >= 1 else ""
            # Include link texts in action
            action_cell_text = ""
            if len(tds) >= 2:
                action_cell_text = clean(tds[1].get_text(" "))
            else:
                action_cell_text = clean(tr.get_text(" "))

            out.append((date_cell, action_cell_text))
    return out

def _fallback_history_rows(soup: BeautifulSoup) -> List[Tuple[str, str]]:
    """
    Fallback: find any table that looks like 'history' (older markup).
    Return rows as (date, action) best-effort.
    """
    out: List[Tuple[str, str]] = []
    # Try a header "Bill History"
    hdr = soup.find(lambda tag: tag.name in ("h1", "h2", "h3", "h4") and "bill history" in tag.get_text(" ").strip().lower())
    if hdr:
        t = hdr.find_next("table")
        if t:
            for tr in t.select("tr"):
                tds = tr.find_all("td")
                if not tds:
                    continue
                date_cell = clean(tds[0].get_text(" ")) if len(tds) >= 1 else ""
                action = clean(" ".join(td.get_text(" ") for td in tds[1:])) if len(tds) >= 2 else clean(tr.get_text(" "))
                out.append((date_cell, action))
    # Or any table that contains likely actions
    if not out:
        for t in soup.find_all("table"):
            txt = clean(t.get_text(" "))
            if any(k in txt for k in ["Introduced", "Passed House", "Passed Senate", "Signed by Governor", "Approved by Governor"]):
                for tr in t.select("tr"):
                    tds = tr.find_all("td")
                    if not tds:
                        continue
                    date_cell = clean(tds[0].get_text(" ")) if len(tds) >= 1 else ""
                    action = clean(" ".join(td.get_text(" ") for td in tds[1:])) if len(tds) >= 2 else clean(tr.get_text(" "))
                    out.append((date_cell, action))
                break
    return out

def _date_from_links(tr: BeautifulSoup) -> Optional[str]:
    # If no date in first cell, try to pull from any href (?hdate=YYYYMMDD or path /HJNL/20250304_...)
    for a in tr.find_all("a", href=True):
        href = a["href"]
        m = YMD_IN_HREF.search(href)
        if m:
            return _ymd_to_mdy(m.group(1))
        m2 = YMD_IN_PATH.search(href)
        if m2:
            return _ymd_to_mdy(m2.group(1))
    return None

def parse_bill_history_fields(billbook_html: str, billno: str) -> Dict[str, str]:
    """
    Parse all status fields from Bill History (dates + flags).
    Pulls dates from <td> date cell in billAction table, with fallback to link URLs containing YYYYMMDD.
    """
    out = {k: "" for k in [
        "Introduced (Y/N)","Introduced date","Chamber where introduced",
        "Effective (Y/N)","Effective date",
        "Passed introduced chamber (Y/N)","Passed introduced chamber date",
        "Passed second chamber (Y/N)","Passed second chamber date",
        "Pending (Y/N)","Dead (Y/N)","Chamber where died","Dead date",
        "Enacted (Y/N)","Act identifier","Enacted Date",
        "Cosponsor"
    ]}
    out["Chamber where introduced"] = infer_chamber_from_billno(billno)

    soup = soupify(billbook_html)

    # Primary: billAction table
    rows = _rows_from_bill_action(soup)
    # Fallback: legacy tables
    if not rows:
        rows = _fallback_history_rows(soup)

    if not rows:
        out["Pending (Y/N)"] = "Y"
        return out

    cosponsors: List[str] = []
    introduced_chamber = out["Chamber where introduced"]

    # Iterate in document order
    for div in soup.select("div.billAction"):
        for tr in div.select("table.billActionTable tbody tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            raw_date = clean(tds[0].get_text(" ")) if len(tds) >= 1 else ""
            action_text = clean(tds[1].get_text(" ")) if len(tds) >= 2 else clean(tr.get_text(" "))
            date = raw_date

            # If the date cell is empty, try to extract from any link in this row
            if not date:
                date_from_href = _date_from_links(tr)
                if date_from_href:
                    date = date_from_href
                else:
                    # last chance: scan whole row text for a date literal
                    rt = clean(tr.get_text(" "))
                    for pat in DATE_PATS:
                        m = pat.search(rt)
                        if m:
                            date = m.group(1)
                            break

            # Introduced
            if INTRO_RE.search(action_text) and not out["Introduced date"]:
                out["Introduced (Y/N)"] = "Y"
                out["Introduced date"] = date

            # Passed House
            if PASSED_H.search(action_text):
                if introduced_chamber == "House" and not out["Passed introduced chamber date"]:
                    out["Passed introduced chamber (Y/N)"] = "Y"
                    out["Passed introduced chamber date"] = date
                elif not out["Passed second chamber date"]:
                    out["Passed second chamber (Y/N)"] = "Y"
                    out["Passed second chamber date"] = date

            # Passed Senate
            if PASSED_S.search(action_text):
                if introduced_chamber == "Senate" and not out["Passed introduced chamber date"]:
                    out["Passed introduced chamber (Y/N)"] = "Y"
                    out["Passed introduced chamber date"] = date
                elif not out["Passed second chamber date"]:
                    out["Passed second chamber (Y/N)"] = "Y"
                    out["Passed second chamber date"] = date

            # Enacted (Signed/Approved)
            if SIGNED_GOV.search(action_text) and not out["Enacted Date"]:
                out["Enacted (Y/N)"] = "Y"
                out["Enacted Date"] = date

            # Effective
            if EFFECTIVE.search(action_text) and not out["Effective date"]:
                out["Effective (Y/N)"] = "Y"
                out["Effective date"] = date

            # Cosponsors
            m = SPONSORS.search(action_text)
            if m:
                names = [clean(x) for x in re.split(r"[;,]", m.group(1)) if clean(x)]
                cosponsors.extend(names)

            # Dead/Withdrawn/Failed
            m2 = WITHDRAWN.search(action_text)
            if m2 and not out["Dead (Y/N)"]:
                out["Dead (Y/N)"] = "Y"
                out["Dead date"] = date
                if m2.group(1):
                    out["Chamber where died"] = m2.group(1).title()
                else:
                    out["Chamber where died"] = introduced_chamber

    # If we didn't iterate via billAction (edge case), handle the simple rows list
    if not out["Introduced date"] or (
        not any(out[k] for k in ["Passed introduced chamber date","Passed second chamber date","Enacted Date","Effective date","Dead date"])
    ):
        for date_cell, action in rows:
            date = date_cell
            if not date:
                # Try to rebuild date from any links in the action cell (we don't have the <tr> here)
                # We can't parse links without the node, so try literal date pattern:
                for pat in DATE_PATS:
                    m = pat.search(action)
                    if m:
                        date = m.group(1)
                        break
            # Fill missing bits
            if INTRO_RE.search(action) and not out["Introduced date"]:
                out["Introduced (Y/N)"] = "Y"; out["Introduced date"] = date
            if PASSED_H.search(action):
                if out["Chamber where introduced"] == "House" and not out["Passed introduced chamber date"]:
                    out["Passed introduced chamber (Y/N)"] = "Y"; out["Passed introduced chamber date"] = date
                elif not out["Passed second chamber date"]:
                    out["Passed second chamber (Y/N)"] = "Y"; out["Passed second chamber date"] = date
            if PASSED_S.search(action):
                if out["Chamber where introduced"] == "Senate" and not out["Passed introduced chamber date"]:
                    out["Passed introduced chamber (Y/N)"] = "Y"; out["Passed introduced chamber date"] = date
                elif not out["Passed second chamber date"]:
                    out["Passed second chamber (Y/N)"] = "Y"; out["Passed second chamber date"] = date
            if SIGNED_GOV.search(action) and not out["Enacted Date"]:
                out["Enacted (Y/N)"] = "Y"; out["Enacted Date"] = date
            if EFFECTIVE.search(action) and not out["Effective date"]:
                out["Effective (Y/N)"] = "Y"; out["Effective date"] = date
            m = SPONSORS.search(action)
            if m:
                names = [clean(x) for x in re.split(r"[;,]", m.group(1)) if clean(x)]
                cosponsors.extend(names)
            m2 = WITHDRAWN.search(action)
            if m2 and not out["Dead (Y/N)"]:
                out["Dead (Y/N)"] = "Y"; out["Dead date"] = date
                out["Chamber where died"] = (m2.group(1).title() if m2.group(1) else out["Chamber where introduced"])

    # Finalize cosponsors
    if cosponsors:
        seen, uniq = set(), []
        for n in cosponsors:
            if n not in seen:
                seen.add(n)
                uniq.append(n)
        out["Cosponsor"] = ", ".join(uniq)

    # Pending if neither enacted nor dead
    if out["Enacted (Y/N)"] != "Y" and out["Dead (Y/N)"] != "Y":
        out["Pending (Y/N)"] = "Y"
    else:
        out["Pending (Y/N)"] = "N"

    return out

# ===================== ACT/CHAPTER & EFFECTIVE FROM TEXT =====================
CHAPTER_RE = re.compile(r"\bCHAPTER\s+(\d+)\b", re.I)
EFFECTIVE_TEXT_RE = re.compile(r"\bEffective\s+(?:date|on)\s*[:,-]?\s*([A-Za-z]+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{2,4})", re.I)

def enrich_from_bill_text(status: Dict[str,str], bill_text: str) -> None:
    if not status.get("Act identifier"):
        m = CHAPTER_RE.search(bill_text)
        if m:
            status["Act identifier"] = f"Chapter {m.group(1)}"
    if status.get("Effective (Y/N)") != "Y":
        m2 = EFFECTIVE_TEXT_RE.search(bill_text)
        if m2:
            status["Effective (Y/N)"] = "Y"
            status["Effective date"] = m2.group(1)

# ---- Row model ----
@dataclass
class Row:
    State: str = "Iowa"
    Policy_bill_identifier: str = ""
    Policy_sponsor: str = ""
    Policy_sponsor_party: str = ""
    Link_to_bill: str = ""
    bill_text: str = ""  # path to txt file
    Cosponsor: str = ""
    Introduced_YN: str = ""
    Introduced_date: str = ""
    Chamber_where_introduced: str = ""
    Effective_YN: str = ""
    Effective_date: str = ""
    Passed_introduced_chamber_YN: str = ""
    Passed_introduced_chamber_date: str = ""
    Passed_second_chamber_YN: str = ""
    Passed_second_chamber_date: str = ""
    Pending_YN: str = ""
    Dead_YN: str = ""
    Chamber_where_died: str = ""
    Dead_date: str = ""
    Enacted_YN: str = ""
    Act_identifier: str = ""
    Enacted_Date: str = ""
    Matched_keywords: str = ""

    def to_csv_row(self) -> Dict[str, str]:
        return {
            "State": self.State,
            "Policy (bill) identifier": self.Policy_bill_identifier,
            "Policy sponsor": self.Policy_sponsor,
            "Policy sponsor party": self.Policy_sponsor_party,
            "Link to bill": self.Link_to_bill,
            "bill text": self.bill_text,
            "Cosponsor": self.Cosponsor,
            "Introduced (Y/N)": self.Introduced_YN,
            "Introduced date": self.Introduced_date,
            "Chamber where introduced": self.Chamber_where_introduced,
            "Effective (Y/N)": self.Effective_YN,
            "Effective date": self.Effective_date,
            "Passed introduced chamber (Y/N)": self.Passed_introduced_chamber_YN,
            "Passed introduced chamber date": self.Passed_introduced_chamber_date,
            "Passed second chamber (Y/N)": self.Passed_second_chamber_YN,
            "Passed second chamber date": self.Passed_second_chamber_date,
            "Pending (Y/N)": self.Pending_YN,
            "Dead (Y/N)": self.Dead_YN,
            "Chamber where died": self.Chamber_where_died,
            "Dead date": self.Dead_date,
            "Enacted (Y/N)": self.Enacted_YN,
            "Act identifier": self.Act_identifier,
            "Enacted Date": self.Enacted_Date,
            "Matched keywords": self.Matched_keywords,
        }

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
def process_bill(ga: int, billno: str, url: str) -> Optional[Row]:
    """
    Fetch BillBook page, prefer LGI/LGE attachments for full text,
    fallback to iframe viewer if needed; match keywords; parse history & sponsors.
    """
    try:
        r = fetch(url)
    except Exception:
        return None

    soup = soupify(r.text)
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

    # Bill History fields (from HTML, not Playwright)
    status = parse_bill_history_fields(billbook_html, billno)

    # Enrich from bill text (chapter/effective textual mentions)
    enrich_from_bill_text(status, bill_text)

    # Keyword match on bill text (attachments/viewer)
    hits = keywords_found(bill_text)
    if not hits:
        return None

    # Save bill text to file and store path in CSV
    text_path = save_bill_text(ga, billno, bill_text)

    row = Row()
    row.Link_to_bill = url
    row.Policy_bill_identifier = billno
    row.Policy_sponsor = sponsor_line
    row.Policy_sponsor_party = sponsor_party
    row.Cosponsor = status.get("Cosponsor", "")
    row.bill_text = text_path or ""  # file path

    # Status fields
    row.Introduced_YN = status["Introduced (Y/N)"]
    row.Introduced_date = status["Introduced date"]
    row.Chamber_where_introduced = status["Chamber where introduced"] or infer_chamber_from_billno(billno)
    row.Effective_YN = status["Effective (Y/N)"]
    row.Effective_date = status["Effective date"]
    row.Passed_introduced_chamber_YN = status["Passed introduced chamber (Y/N)"]
    row.Passed_introduced_chamber_date = status["Passed introduced chamber date"]
    row.Passed_second_chamber_YN = status["Passed second chamber (Y/N)"]
    row.Passed_second_chamber_date = status["Passed second chamber date"]
    row.Pending_YN = status["Pending (Y/N)"]
    row.Dead_YN = status["Dead (Y/N)"]
    row.Chamber_where_died = status["Chamber where died"]
    row.Dead_date = status["Dead date"]
    row.Enacted_YN = status["Enacted (Y/N)"]
    row.Act_identifier = status["Act identifier"]
    row.Enacted_Date = status["Enacted Date"]
    row.Matched_keywords = ", ".join(hits)

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
