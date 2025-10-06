#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Iowa Legislature bill scraper — GA 91, single-term test ("Immigration"),
with bill text pulled from the BillBook iframe (id="bbContextDoc").

Install:
    pip install requests beautifulsoup4 lxml tenacity tqdm pdfminer.six

Run:
    python iowa_legis_scrape_iframe_text.py
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
KEYWORDS = ["Immigration"]  # single-term test
GA_START = 91
GA_END   = 91

# Limit to common bill types while testing (add more later if you want)
LEG_TYPES = ["HF", "SF", "HSB", "SSB"]

# Polite rate limits
REQUESTS_PER_MINUTE = 10            # ~1 request every 6 seconds
JITTER_RANGE_SECONDS = (0.6, 1.8)   # small random jitter
PAUSE_EVERY_N_REQUESTS = 40         # periodic pause
PAUSE_DURATION_SECONDS = 20

# No directory prefiltering (we want to hit BillBook pages)
PREFILTER_FROM_DIRECTORY = False

# Optional: cap bills per GA during testing (None = all)
MAX_BILLS_PER_GA = None

OUT_CSV = "iowa_bills_keywords.csv"
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
    "Policy name",
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

# ---- Attachments (HTML + PDF) ----
ATTACHMENT_PAT = re.compile(r"/docs/publications/[^\"'>]+?\.(?:html?|pdf)\b", flags=re.I)

def fetch_attachment_texts(billbook_html: str) -> List[str]:
    """Fetch text from LGI/LGE HTML/PDF attachments linked on the BillBook page."""
    soup = soupify(billbook_html)
    texts: List[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        url = urljoin(BASE, href)
        if not ATTACHMENT_PAT.search(url):
            continue
        try:
            r = fetch(url)
        except Exception:
            continue

        if url.lower().endswith((".htm", ".html")):
            try:
                sub_soup = soupify(r.text)
                txt = clean(sub_soup.get_text(" "))
                if txt:
                    texts.append(txt)
            except Exception:
                pass
        elif url.lower().endswith(".pdf"):
            try:
                pdf_bytes = io.BytesIO(r.content)
                txt = pdf_extract_text(pdf_bytes) or ""
                txt = clean(txt)
                if txt:
                    texts.append(txt)
            except Exception:
                pass
    return texts

# ---- Bill text from iframe (bbContextDoc) ----
def extract_bill_text_from_iframe(billbook_html: str, billbook_url: str) -> str:
    """
    Find <iframe id="bbContextDoc">, fetch its src, and return text.
    Handles nested viewer pages and PDF/HTML.
    """
    soup = soupify(billbook_html)
    iframe = soup.find("iframe", id="bbContextDoc")
    if not iframe or not iframe.get("src"):
        return ""  # fallback will be attachments

    src = urljoin(billbook_url, iframe["src"])
    try:
        r = fetch(src)
    except Exception:
        return ""

    # If the iframe directly points to PDF/HTML, parse it.
    ct = r.headers.get("Content-Type", "").lower()
    if "pdf" in ct or src.lower().endswith(".pdf"):
        try:
            pdf_bytes = io.BytesIO(r.content)
            txt = pdf_extract_text(pdf_bytes) or ""
            return clean(txt)
        except Exception:
            return ""

    # It's likely an HTML viewer; look for another iframe or document content.
    try:
        sub = soupify(r.text)
        # If it itself nests another iframe, follow once more.
        inner_if = sub.find("iframe")
        if inner_if and inner_if.get("src"):
            inner_src = urljoin(src, inner_if["src"])
            try:
                r2 = fetch(inner_src)
                ct2 = r2.headers.get("Content-Type", "").lower()
                if "pdf" in ct2 or inner_src.lower().endswith(".pdf"):
                    pdf_bytes = io.BytesIO(r2.content)
                    txt = pdf_extract_text(pdf_bytes) or ""
                    return clean(txt)
                else:
                    return clean(BeautifulSoup(r2.text, "lxml").get_text(" "))
            except Exception:
                pass
        # Otherwise, return the viewer page's own visible text.
        return clean(sub.get_text(" "))
    except Exception:
        return ""

# ---- Status parsing (regex over BillBook page text) ----
def parse_status_fields(page_text: str) -> Dict[str, str]:
    out = {k: "" for k in [
        "Policy sponsor","Policy sponsor party","Cosponsor",
        "Introduced (Y/N)","Introduced date","Chamber where introduced",
        "Effective (Y/N)","Effective date",
        "Passed introduced chamber (Y/N)","Passed introduced chamber date",
        "Passed second chamber (Y/N)","Passed second chamber date",
        "Pending (Y/N)","Dead (Y/N)","Chamber where died","Dead date",
        "Enacted (Y/N)","Act identifier","Enacted Date"
    ]}

    m = re.search(r"Introduced(?: in)?(?: the)?\s*(House|Senate)?\s*on\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})", page_text, flags=re.I)
    if m:
        out["Introduced (Y/N)"] = "Y"
        out["Chamber where introduced"] = (m.group(1) or "").title()
        out["Introduced date"] = m.group(2)

    h = re.search(r"Passed\s+House\s+on\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", page_text, flags=re.I)
    s = re.search(r"Passed\s+Senate\s+on\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", page_text, flags=re.I)
    if h:
        out["Passed introduced chamber (Y/N)"] = "Y"
        out["Passed introduced chamber date"] = h.group(1)
    if s:
        if out["Passed introduced chamber (Y/N)"] == "Y":
            out["Passed second chamber (Y/N)"] = "Y"
            out["Passed second chamber date"] = s.group(1)
        else:
            out["Passed introduced chamber (Y/N)"] = "Y"
            out["Passed introduced chamber date"] = s.group(1)

    g = re.search(r"Signed by Governor\s+on\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", page_text, flags=re.I)
    if g:
        out["Enacted (Y/N)"] = "Y"
        out["Enacted Date"] = g.group(1)
    act = re.search(r"(?:Chapter|Act)\s+(\d+)", page_text, flags=re.I)
    if act:
        out["Act identifier"] = f"Chapter {act.group(1)}"

    eff = re.search(r"Effective\s+(?:Date|on)\s*:?[\s]*([A-Za-z]+\s+\d{1,2},\s+\d{4})", page_text, flags=re.I)
    if eff:
        out["Effective (Y/N)"] = "Y"
        out["Effective date"] = eff.group(1)

    dd = re.search(r"(died in (House|Senate).+?on\s+([A-Za-z]+\s+\d{1,2},\s+\d{4}))|(withdrawn\s+on\s+([A-Za-z]+\s+\d{1,2},\s+\d{4}))", page_text, flags=re.I)
    if dd:
        out["Dead (Y/N)"] = "Y"
        c = re.search(r"died in (House|Senate)", dd.group(0), flags=re.I)
        if c:
            out["Chamber where died"] = c.group(1).title()
        d = re.search(r"on\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", dd.group(0), flags=re.I)
        if d:
            out["Dead date"] = d.group(1)

    sp = re.search(r"(?:Sponsor|By|Sponsors?)\s*:\s*([^\n]+)", page_text, flags=re.I)
    if sp:
        sponsor_line = clean(sp.group(1))
        out["Policy sponsor"] = sponsor_line
        pm = re.search(r"\(([RDI])\)", sponsor_line)
        if pm:
            out["Policy sponsor party"] = pm.group(1)

    cos = re.search(r"(?:Co-?sponsors?)\s*:\s*([^\n]+)", page_text, flags=re.I)
    if cos:
        out["Cosponsor"] = clean(cos.group(1))

    if out["Enacted (Y/N)"] != "Y" and out["Dead (Y/N)"] != "Y":
        out["Pending (Y/N)"] = "Y"

    return out

def first_heading_text(soup: BeautifulSoup) -> str:
    for tag in ("h1", "h2", "h3"):
        el = soup.find(tag)
        if el:
            return clean(el.get_text(" "))
    return clean(soup.title.get_text()) if soup.title else ""

# ---- Row model ----
@dataclass
class Row:
    State: str = "Iowa"
    Policy_name: str = ""
    Policy_bill_identifier: str = ""
    Policy_sponsor: str = ""
    Policy_sponsor_party: str = ""
    Link_to_bill: str = ""
    bill_text: str = ""
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
            "Policy name": self.Policy_name,
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

# ---- Bill processing ----
def process_bill(billno: str, url: str) -> Optional[Row]:
    """Fetch BillBook page, pull iframe text (bbContextDoc), plus attachments; match keywords; return row if hit."""
    try:
        r = fetch(url)
    except Exception:
        return None

    soup = soupify(r.text)
    billbook_page_text = clean(soup.get_text(" "))  # used for status parsing only

    # 1) Pull the MAIN bill text from the iframe
    iframe_text = extract_bill_text_from_iframe(r.text, url)

    # 2) Add any LGI/LGE attachments (HTML/PDF) as supplementary text
    attach_texts = fetch_attachment_texts(r.text)

    # 3) Use ONLY iframe+attachments for keyword matching and bill_text field
    combined_bill_text = (iframe_text + " " + " ".join(attach_texts)).strip()

    hits = keywords_found(combined_bill_text)
    if not hits:
        return None

    # Parse status from the BillBook page (not from iframe)
    status = parse_status_fields(billbook_page_text)

    row = Row()
    row.Link_to_bill = url
    row.Policy_bill_identifier = billno
    row.Policy_name = first_heading_text(soup) or billno
    row.Policy_sponsor = status["Policy sponsor"]
    row.Policy_sponsor_party = status["Policy sponsor party"]
    row.Cosponsor = status["Cosponsor"]
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

    # Put the actual bill text (iframe + attachments) into the CSV
    row.bill_text = combined_bill_text[:15000]  # keep CSV manageable
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

        if PREFILTER_FROM_DIRECTORY:
            kept = []
            for billno, url, row_text in links:
                if keywords_found(row_text):
                    kept.append((billno, url, row_text))
            print(f"GA {ga}: prefilter kept {len(kept)} / {len(links)}")
            links = kept

        processed = 0
        matched = 0
        for billno, url, _ in tqdm(links, desc=f"GA {ga}", leave=False):
            key = f"{ga}:{billno.upper()}"
            if key in seen_bills:
                continue
            processed += 1
            row = process_bill(billno, url)
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
'''#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Iowa Legislature bill scraper — single-term test (GA 91, "Immigration")

Install:
    pip install requests beautifulsoup4 lxml tenacity tqdm pdfminer.six

Run:
    python iowa_legis_scrape_ga91_immigration.py
"""

import csv
import io
import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup
from tenacity import retry, wait_exponential, stop_after_attempt
from tqdm import tqdm
from pdfminer.high_level import extract_text as pdf_extract_text

# ===================== USER SETTINGS (test) =====================
KEYWORDS = ["Immigration"]  # single-term test
GA_START = 91
GA_END   = 91

# Limit to common bill types while testing (you can add HJR/SJR/HCR/SCR/HR/SR later)
LEG_TYPES = ["HF", "SF", "HSB", "SSB"]

# Polite rate limits
REQUESTS_PER_MINUTE = 10            # ~1 request every 6 seconds
JITTER_RANGE_SECONDS = (0.6, 1.8)   # small random jitter
PAUSE_EVERY_N_REQUESTS = 40         # periodic pause
PAUSE_DURATION_SECONDS = 20

# IMPORTANT: no prefilter so we actually hit BillBook pages
PREFILTER_FROM_DIRECTORY = False

# Optional: cap bills per GA during testing
MAX_BILLS_PER_GA = None  # set to 300 to test faster; None = all

OUT_CSV = "iowa_bills_keywords.csv"
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
    "Policy name",
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
    r = _session.get(url, timeout=40)
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

# ---- Attachment fetching (HTML + PDF) ----
ATTACHMENT_PAT = re.compile(r"/docs/publications/[^\"'>]+?\.(?:html?|pdf)\b", flags=re.I)

def fetch_attachment_texts(billbook_html: str) -> List[str]:
    soup = soupify(billbook_html)
    texts: List[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        url = BASE + href if href.startswith("/") else (href if href.startswith("http") else "")
        if not url or not ATTACHMENT_PAT.search(url):
            continue
        try:
            r = fetch(url)
        except Exception:
            continue
        if re.search(r"\.html?$", url, flags=re.I):
            try:
                sub_soup = soupify(r.text)
                txt = clean(sub_soup.get_text(" "))
                if txt:
                    texts.append(txt)
            except Exception:
                pass
        elif re.search(r"\.pdf$", url, flags=re.I):
            try:
                pdf_bytes = io.BytesIO(r.content)
                txt = pdf_extract_text(pdf_bytes) or ""
                txt = clean(txt)
                if txt:
                    texts.append(txt)
            except Exception:
                pass
    return texts

# ---- Status parsing ----
def parse_status_fields(page_text: str) -> Dict[str, str]:
    out = {k: "" for k in [
        "Policy sponsor","Policy sponsor party","Cosponsor",
        "Introduced (Y/N)","Introduced date","Chamber where introduced",
        "Effective (Y/N)","Effective date",
        "Passed introduced chamber (Y/N)","Passed introduced chamber date",
        "Passed second chamber (Y/N)","Passed second chamber date",
        "Pending (Y/N)","Dead (Y/N)","Chamber where died","Dead date",
        "Enacted (Y/N)","Act identifier","Enacted Date"
    ]}

    m = re.search(r"Introduced(?: in)?(?: the)?\s*(House|Senate)?\s*on\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})", page_text, flags=re.I)
    if m:
        out["Introduced (Y/N)"] = "Y"
        out["Chamber where introduced"] = (m.group(1) or "").title()
        out["Introduced date"] = m.group(2)

    h = re.search(r"Passed\s+House\s+on\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", page_text, flags=re.I)
    s = re.search(r"Passed\s+Senate\s+on\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", page_text, flags=re.I)
    if h:
        out["Passed introduced chamber (Y/N)"] = "Y"
        out["Passed introduced chamber date"] = h.group(1)
    if s:
        if out["Passed introduced chamber (Y/N)"] == "Y":
            out["Passed second chamber (Y/N)"] = "Y"
            out["Passed second chamber date"] = s.group(1)
        else:
            out["Passed introduced chamber (Y/N)"] = "Y"
            out["Passed introduced chamber date"] = s.group(1)

    g = re.search(r"Signed by Governor\s+on\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", page_text, flags=re.I)
    if g:
        out["Enacted (Y/N)"] = "Y"
        out["Enacted Date"] = g.group(1)
    act = re.search(r"(?:Chapter|Act)\s+(\d+)", page_text, flags=re.I)
    if act:
        out["Act identifier"] = f"Chapter {act.group(1)}"

    eff = re.search(r"Effective\s+(?:Date|on)\s*:?[\s]*([A-Za-z]+\s+\d{1,2},\s+\d{4})", page_text, flags=re.I)
    if eff:
        out["Effective (Y/N)"] = "Y"
        out["Effective date"] = eff.group(1)

    dd = re.search(r"(died in (House|Senate).+?on\s+([A-Za-z]+\s+\d{1,2},\s+\d{4}))|(withdrawn\s+on\s+([A-Za-z]+\s+\d{1,2},\s+\d{4}))", page_text, flags=re.I)
    if dd:
        out["Dead (Y/N)"] = "Y"
        c = re.search(r"died in (House|Senate)", dd.group(0), flags=re.I)
        if c:
            out["Chamber where died"] = c.group(1).title()
        d = re.search(r"on\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", dd.group(0), flags=re.I)
        if d:
            out["Dead date"] = d.group(1)

    sp = re.search(r"(?:Sponsor|By|Sponsors?)\s*:\s*([^\n]+)", page_text, flags=re.I)
    if sp:
        sponsor_line = clean(sp.group(1))
        out["Policy sponsor"] = sponsor_line
        pm = re.search(r"\(([RDI])\)", sponsor_line)
        if pm:
            out["Policy sponsor party"] = pm.group(1)

    cos = re.search(r"(?:Co-?sponsors?)\s*:\s*([^\n]+)", page_text, flags=re.I)
    if cos:
        out["Cosponsor"] = clean(cos.group(1))

    if out["Enacted (Y/N)"] != "Y" and out["Dead (Y/N)"] != "Y":
        out["Pending (Y/N)"] = "Y"

    return out

def first_heading_text(soup: BeautifulSoup) -> str:
    for tag in ("h1", "h2", "h3"):
        el = soup.find(tag)
        if el:
            return clean(el.get_text(" "))
    return clean(soup.title.get_text()) if soup.title else ""

# ---- Row model ----
@dataclass
class Row:
    State: str = "Iowa"
    Policy_name: str = ""
    Policy_bill_identifier: str = ""
    Policy_sponsor: str = ""
    Policy_sponsor_party: str = ""
    Link_to_bill: str = ""
    bill_text: str = ""
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
            "Policy name": self.Policy_name,
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
            "Dead date": self.Dead_date,  # <-- FIXED (no stray quote)
            "Enacted (Y/N)": self.Enacted_YN,
            "Act identifier": self.Act_identifier,
            "Enacted Date": self.Enacted_Date,
            "Matched keywords": self.Matched_keywords,
        }

# ---- Bill processing ----
ATTACHMENT_PAT = re.compile(r"/docs/publications/[^\"'>]+?\.(?:html?|pdf)\b", flags=re.I)

def fetch_attachment_texts(billbook_html: str) -> List[str]:
    soup = soupify(billbook_html)
    texts: List[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        url = BASE + href if href.startswith("/") else (href if href.startswith("http") else "")
        if not url or not ATTACHMENT_PAT.search(url):
            continue
        try:
            r = fetch(url)
        except Exception:
            continue
        if re.search(r"\.html?$", url, flags=re.I):
            try:
                sub_soup = soupify(r.text)
                txt = clean(sub_soup.get_text(" "))
                if txt:
                    texts.append(txt)
            except Exception:
                pass
        elif re.search(r"\.pdf$", url, flags=re.I):
            try:
                pdf_bytes = io.BytesIO(r.content)
                txt = pdf_extract_text(pdf_bytes) or ""
                txt = clean(txt)
                if txt:
                    texts.append(txt)
            except Exception:
                pass
    return texts

def process_bill(billno: str, url: str) -> Optional[Row]:
    try:
        r = fetch(url)
    except Exception:
        return None
    soup = soupify(r.text)
    page_text = clean(soup.get_text(" "))
    attach_texts = fetch_attachment_texts(r.text)
    combined_text = page_text + " " + " ".join(attach_texts)
    hits = keywords_found(combined_text)
    if not hits:
        return None

    row = Row()
    row.Link_to_bill = url
    row.Policy_bill_identifier = billno
    row.Policy_name = first_heading_text(soup)

    status = parse_status_fields(combined_text)
    row.Policy_sponsor = status["Policy sponsor"]
    row.Policy_sponsor_party = status["Policy sponsor party"]
    row.Cosponsor = status["Cosponsor"]
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

    row.bill_text = combined_text[:15000]
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

        if PREFILTER_FROM_DIRECTORY:
            kept = []
            for billno, url, row_text in links:
                if keywords_found(row_text):
                    kept.append((billno, url, row_text))
            print(f"GA {ga}: prefilter kept {len(kept)} / {len(links)}")
            links = kept

        processed = 0
        matched = 0
        for billno, url, _ in tqdm(links, desc=f"GA {ga}", leave=False):
            key = f"{ga}:{billno.upper()}"
            if key in seen_bills:
                continue
            processed += 1
            row = process_bill(billno, url)
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
'''