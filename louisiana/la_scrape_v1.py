#!/usr/bin/env python3
"""
Louisiana Legislature Bill Scraper — FULL TEXT ONLY
=====================================================
Enumerates every bill in a session, downloads each bill's full-text
PDF, and keyword-scans the extracted text locally.
Only bills with keyword matches are saved to disk and the CSV.
The site's own search/index is NOT used — only the raw bill text matters.

Requirements:
    pip install requests beautifulsoup4 pdfminer.six lxml

Usage:
    python la_bill_scraper.py --session 26rs
    python la_bill_scraper.py --session 25rs --output my_bills.csv
    python la_bill_scraper.py --list
"""

import re
import csv
import time
import random
import logging
import argparse
import sys
import os
from pathlib import Path
from io import BytesIO
from urllib.parse import urljoin, urlencode

import requests
from bs4 import BeautifulSoup

# ── PDF text extraction — REQUIRED ────────────────────────────────────────────
try:
    from pdfminer.high_level import extract_text as pdf_extract_text
    PDF_SUPPORT = True
except ImportError:
    print(
        "\n[ERROR] pdfminer.six is required.\n"
        "        Install it with:  pip install pdfminer.six\n"
    )
    sys.exit(1)


# ══════════════════════════════════════════════════════════════════════════════
# ❶  SET SESSION HERE  (or pass --session on the command line)
# ══════════════════════════════════════════════════════════════════════════════
SESSION_TO_RUN = "23rs"


# ══════════════════════════════════════════════════════════════════════════════
# COMPLETE SESSION LIST — every session on legis.la.gov (1997–2026)
# ══════════════════════════════════════════════════════════════════════════════
ALL_SESSIONS = [
    ("26rs",   "2026 Regular Session"),
    ("251es",  "2025 First Extraordinary Session"),
    ("25rs",   "2025 Regular Session"),
    ("243es",  "2024 Third Extraordinary Session"),
    ("24rs",   "2024 Regular Session"),
    ("242es",  "2024 Second Extraordinary Session"),
    ("241es",  "2024 First Extraordinary Session"),
    ("24o",    "2024 Organizational Session"),
    ("23vs",   "2023 Veto Session"),
    ("23rs",   "2023 Regular Session"),
    ("231es",  "2023 First Extraordinary Session"),
    ("222es",  "2022 Second Extraordinary Session"),
    ("22vs",   "2022 Veto Session"),
    ("22rs",   "2022 Regular Session"),
    ("221es",  "2022 First Extraordinary Session"),
    ("21vs",   "2021 Veto Session"),
    ("21rs",   "2021 Regular Session"),
    ("202es",  "2020 Second Extraordinary Session"),
    ("201es",  "2020 First Extraordinary Session"),
    ("20rs",   "2020 Regular Session"),
    ("20o",    "2020 Organizational Session"),
    ("19rs",   "2019 Regular Session"),
    ("183es",  "2018 Third Extraordinary Session"),
    ("182es",  "2018 Second Extraordinary Session"),
    ("18rs",   "2018 Regular Session"),
    ("181es",  "2018 First Extraordinary Session"),
    ("172es",  "2017 Second Extraordinary Session"),
    ("17rs",   "2017 Regular Session"),
    ("171es",  "2017 First Extraordinary Session"),
    ("162es",  "2016 Second Extraordinary Session"),
    ("16rs",   "2016 Regular Session"),
    ("161es",  "2016 First Extraordinary Session"),
    ("16o",    "2016 Organizational Session"),
    ("15rs",   "2015 Regular Session"),
    ("14rs",   "2014 Regular Session"),
    ("13rs",   "2013 Regular Session"),
    ("12rs",   "2012 Regular Session"),
    ("12o",    "2012 Organizational Session"),
    ("11rs",   "2011 Regular Session"),
    ("111es",  "2011 First Extraordinary Session"),
    ("10rs",   "2010 Regular Session"),
    ("09rs",   "2009 Regular Session"),
    ("08rs",   "2008 Regular Session"),
    ("082es",  "2008 Second Extraordinary Session"),
    ("081es",  "2008 First Extraordinary Session"),
    ("08o",    "2008 Organizational Session"),
    ("07rs",   "2007 Regular Session"),
    ("062es",  "2006 Second Extraordinary Session"),
    ("06rs",   "2006 Regular Session"),
    ("061es",  "2006 First Extraordinary Session"),
    ("051es",  "2005 First Extraordinary Session"),
    ("05rs",   "2005 Regular Session"),
    ("04rs",   "2004 Regular Session"),
    ("041es",  "2004 First Extraordinary Session"),
    ("04o",    "2004 Organizational Session"),
    ("03rs",   "2003 Regular Session"),
    ("02rs",   "2002 Regular Session"),
    ("021es",  "2002 First Extraordinary Session"),
    ("012es",  "2001 Second Extraordinary Session"),
    ("01rs",   "2001 Regular Session"),
    ("011es",  "2001 First Extraordinary Session"),
    ("002es",  "2000 Second Extraordinary Session"),
    ("00rs",   "2000 Regular Session"),
    ("001es",  "2000 First Extraordinary Session"),
    ("00o",    "2000 Organizational Session"),
    ("99rs",   "1999 Regular Session"),
    ("98rs",   "1998 Regular Session"),
    ("981es",  "1998 First Extraordinary Session"),
    ("97rs",   "1997 Regular Session"),
]

SESSION_LABELS = {code: label for code, label in ALL_SESSIONS}


# ══════════════════════════════════════════════════════════════════════════════
# KEYWORDS — whole-word matching only
# "Visa" matches "visa" but NOT "advisable" or "television"
# ══════════════════════════════════════════════════════════════════════════════
KEYWORDS = [
    "Immigration", "Citizenship", "Alien", "Migrant",
    "Undocumented", "Visa", "Border", "Foreign",
]

KEYWORD_PATTERNS = {
    kw: re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE)
    for kw in KEYWORDS
}


# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════
BASE_URL      = "https://www.legis.la.gov"
BILL_INFO_URL = f"{BASE_URL}/legis/BillInfo.aspx"
OUTPUT_CSV    = "louisiana_immigration_bills.csv"
FULL_TEXT_DIR = Path("bill_full_texts")

BILL_TYPES             = ["HB", "SB", "HCR", "SCR", "HR", "SR"]
MAX_CONSECUTIVE_MISSES = 15
MIN_DELAY              = 2.0   # seconds — courtesy pause between requests
MAX_DELAY              = 4.0

CSV_COLUMNS = [
    "State",
    "GA",
    "Policy (bill) identifier",
    "Policy sponsor",
    "Policy sponsor party",
    "Link to bill",
    "bill text",
    "Cosponsor",
    "Act identifier",
    "Matched keywords",
    "Introduced date",
    "Effective date",
    "Passed introduced chamber date",
    "Passed second chamber date",
    "Dead date",
    "Enacted (Y/N)",
    "Enacted Date",
]


# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("la_scraper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# HTTP SESSION
# ══════════════════════════════════════════════════════════════════════════════
HTTP = requests.Session()
HTTP.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (compatible; LA-Bill-Research-Bot/1.0; "
        "+https://example.com/research)"
    ),
    "Accept-Language": "en-US,en;q=0.9",
})


def polite_get(url: str, **kwargs):
    """
    HTTP GET with a random courtesy pause before every request,
    up to 3 retries, and exponential back-off on rate-limit errors.
    Returns the Response object or None on failure.
    """
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
    for attempt in range(1, 4):
        try:
            r = HTTP.get(url, timeout=45, **kwargs)
            r.raise_for_status()
            return r
        except requests.HTTPError as exc:
            code = exc.response.status_code
            log.warning("HTTP %s on %s (attempt %d/3)", code, url, attempt)
            if code in (429, 503):
                wait = 30 * attempt
                log.info("Rate-limited — backing off %ds…", wait)
                time.sleep(wait)
            else:
                return None
        except requests.RequestException as exc:
            log.warning("Network error on %s (attempt %d/3): %s", url, attempt, exc)
            time.sleep(10 * attempt)
    log.error("Giving up on %s", url)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# KEYWORD MATCHING — full text only, whole words only
# ══════════════════════════════════════════════════════════════════════════════
def find_keywords(text: str) -> list:
    """
    Scan text and return a sorted list of matched keywords.
    \b word boundaries make partial matches impossible:
      "advisable" will NOT match "Visa"
      "alienate"  will NOT match "Alien"
    """
    return sorted(kw for kw, pat in KEYWORD_PATTERNS.items() if pat.search(text))


# ══════════════════════════════════════════════════════════════════════════════
# BILL ENUMERATION
# ══════════════════════════════════════════════════════════════════════════════
def bill_page_is_valid(soup, page_text: str) -> bool:
    """
    Return True if the BillInfo page represents a real bill.
    The site returns a normal-looking page with error text when a
    bill number doesn't exist.
    """
    not_found_phrases = [
        "no instrument",
        "not found",
        "does not exist",
        "invalid bill",
        "no bills found",
        "no record",
    ]
    lowered = page_text.lower()
    if any(phrase in lowered for phrase in not_found_phrases):
        return False
    has_history = soup.find("table", id=re.compile(r"ListViewHistory", re.I))
    has_heading = soup.find(string=re.compile(r'\b(HB|SB|HCR|SCR|HR|SR)\s*\d+', re.I))
    return bool(has_history or has_heading)


def enumerate_bills(session_code: str) -> list:
    """
    Discover every bill in the session by incrementing bill numbers
    for each bill type until MAX_CONSECUTIVE_MISSES in a row.

    Returns a list of dicts: {bill_id, bill_url, session, soup, page_text}
    The soup and page_text are cached so we don't re-fetch the BillInfo
    page later when building the CSV row.
    """
    all_bills = []

    for bill_type in BILL_TYPES:
        log.info("  Enumerating %s bills…", bill_type)
        num                = 1
        consecutive_misses = 0

        while True:
            bill_id  = f"{bill_type}{num}"
            params   = {"s": session_code, "b": bill_id, "sbi": "y"}
            bill_url = f"{BILL_INFO_URL}?{urlencode(params)}"

            resp = polite_get(bill_url)

            if resp is None:
                consecutive_misses += 1
                log.warning("    No response for %s — miss %d/%d",
                            bill_id, consecutive_misses, MAX_CONSECUTIVE_MISSES)
            else:
                soup      = BeautifulSoup(resp.text, "lxml")
                page_text = soup.get_text(" ", strip=True)

                if bill_page_is_valid(soup, page_text):
                    consecutive_misses = 0
                    all_bills.append({
                        "bill_id":   bill_id,
                        "bill_url":  bill_url,
                        "session":   session_code,
                        "soup":      soup,
                        "page_text": page_text,
                    })
                    log.debug("    Found: %s", bill_id)
                else:
                    consecutive_misses += 1
                    log.debug("    Not found: %s — miss %d/%d",
                              bill_id, consecutive_misses, MAX_CONSECUTIVE_MISSES)

            if consecutive_misses >= MAX_CONSECUTIVE_MISSES:
                log.info("    %s: stopped at %s (%d consecutive misses)",
                         bill_type, bill_id, MAX_CONSECUTIVE_MISSES)
                break

            num += 1

    log.info("  Enumeration complete — %d bills found in session %s",
             len(all_bills), session_code)
    return all_bills


# ══════════════════════════════════════════════════════════════════════════════
# PDF DOWNLOAD — fetch into memory only, do NOT save to disk yet
# ══════════════════════════════════════════════════════════════════════════════
def get_doc_url(soup) -> str:
    """Pull the ViewDocument.aspx link from a BillInfo page."""
    for a in soup.find_all("a", href=True):
        if "ViewDocument.aspx" in a["href"]:
            return urljoin(BASE_URL + "/legis/", a["href"])
    return ""


def fetch_bill_text_only(doc_url: str) -> tuple:
    """
    Download the bill's full-text document into memory and extract its text.
    Does NOT write anything to disk — that only happens if keywords match.

    Handles three cases:
      1. ViewDocument returns a PDF directly
      2. ViewDocument returns an HTML wrapper containing a PDF link
      3. Older bills where the document is plain HTML (no PDF)

    Returns (raw_bytes_or_html_str, extracted_text, file_extension)
      file_extension is either "pdf" or "txt" so the caller knows
      which format to save if keywords are found.
    Returns (None, "", "") on failure.
    """
    resp = polite_get(doc_url)
    if resp is None:
        return None, "", ""

    content_type = resp.headers.get("Content-Type", "").lower()

    # Case 1: Direct PDF response
    if "pdf" in content_type:
        try:
            text = pdf_extract_text(BytesIO(resp.content))
            return resp.content, text, "pdf"
        except Exception as exc:
            log.warning("    PDF text extraction failed: %s", exc)
            return resp.content, "", "pdf"

    # Case 2: HTML wrapper — look for a PDF link inside it
    inner_soup = BeautifulSoup(resp.text, "lxml")
    pdf_link   = inner_soup.find("a", href=re.compile(r'\.pdf', re.I))
    if pdf_link:
        direct_url = urljoin(BASE_URL, pdf_link["href"])
        pdf_resp   = polite_get(direct_url)
        if pdf_resp and "pdf" in pdf_resp.headers.get("Content-Type", "").lower():
            try:
                text = pdf_extract_text(BytesIO(pdf_resp.content))
                return pdf_resp.content, text, "pdf"
            except Exception as exc:
                log.warning("    PDF text extraction failed: %s", exc)
                return pdf_resp.content, "", "pdf"

    # Case 3: The bill text IS the HTML (common for older sessions)
    log.info("    Bill text is HTML — extracting inline text")
    html_text = inner_soup.get_text(" ", strip=True)
    return html_text, html_text, "txt"


def save_bill_file(content, file_ext: str, bill_id: str, session: str) -> str:
    """
    Save the bill's full-text content to disk now that we know it matched.
    Returns the local file path string, or "" on failure.
    """
    FULL_TEXT_DIR.mkdir(parents=True, exist_ok=True)

    safe_id   = re.sub(r'[^A-Za-z0-9_\-]', '_', bill_id)
    safe_sess = re.sub(r'[^A-Za-z0-9_\-]', '_', session)
    filepath  = FULL_TEXT_DIR / f"{safe_sess}_{safe_id}.{file_ext}"

    try:
        if file_ext == "pdf":
            filepath.write_bytes(content)
        else:
            filepath.write_text(content, encoding="utf-8")
        log.info("    Saved %s → %s", file_ext.upper(), filepath)
        return str(filepath)
    except OSError as exc:
        log.error("    Could not write %s: %s", filepath, exc)
        return ""


# ══════════════════════════════════════════════════════════════════════════════
# SPONSOR EXTRACTION
# ══════════════════════════════════════════════════════════════════════════════
def extract_sponsors(soup, page_text: str) -> tuple:
    """Return (sponsor_name, party, cosponsor_names_string)."""
    sponsor = cosponsor = sponsor_party = ""

    for label_text in ("Author:", "Authors:", "By:", "Sponsor:"):
        node = soup.find(string=re.compile(re.escape(label_text), re.I))
        if node:
            parent = node.find_parent()
            if parent:
                names = [
                    a.get_text(strip=True)
                    for a in parent.find_all("a")
                    if a.get_text(strip=True)
                ]
                if names:
                    sponsor   = names[0]
                    cosponsor = "; ".join(names[1:])
                    break

    if not sponsor:
        for td in soup.find_all("td"):
            if re.search(r'\bAuthor\b', td.get_text(), re.I):
                sib = td.find_next_sibling("td")
                if sib:
                    sponsor = sib.get_text(strip=True)
                    break

    m = re.search(r'\(([RD])\)', page_text[:800])
    if m:
        sponsor_party = "Republican" if m.group(1) == "R" else "Democrat"

    return sponsor, sponsor_party, cosponsor


# ══════════════════════════════════════════════════════════════════════════════
# HISTORY TABLE PARSER
# ══════════════════════════════════════════════════════════════════════════════
_RE_INTRODUCED = re.compile(r'(Introduced|Prefiled|Read (first|1st) time)', re.I)
_RE_PASSED     = re.compile(
    r'(finally passed|passed by a vote|ordered.*sent to the (House|Senate))', re.I)
_RE_ENACTED    = re.compile(r'Signed by the Governor', re.I)
_RE_EFFECTIVE  = re.compile(r'Effective date', re.I)
_RE_DEAD       = re.compile(
    r'(Withdrawn|Involuntarily deferred|Tabled|Failed to pass|Vetoed)', re.I)


def _session_year(session_code: str) -> int:
    m = re.match(r'^(\d{2,4})', session_code)
    if m:
        yr = int(m.group(1))
        if yr > 999:
            return yr
        return 1900 + yr if yr >= 97 else 2000 + yr
    return 2000


def _full_date(raw: str, year: int) -> str:
    raw = raw.strip()
    if not raw or raw == " ":
        return ""
    if re.match(r'^\d{1,2}/\d{1,2}$', raw):
        return f"{raw}/{year}"
    return raw


def parse_history_table(soup, session_code: str) -> dict:
    out = {k: "" for k in [
        "introduced", "effective", "passed_intro_chamber",
        "passed_second_chamber", "dead", "enacted",
    ]}
    year = _session_year(session_code)

    table = soup.find("table", id=re.compile(r"ListViewHistory", re.I))
    if not table:
        for t in soup.find_all("table"):
            if t.find("th", string=re.compile(r"Action", re.I)):
                table = t
                break
    if not table:
        return out

    intro_chamber = None
    for row in table.find_all("tr", valign="top"):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        raw_date = cells[0].get_text(strip=True)
        chamber  = cells[1].get_text(strip=True)
        action   = cells[3].get_text(" ", strip=True)
        date     = _full_date(raw_date, year)

        if not out["introduced"] and _RE_INTRODUCED.search(action):
            out["introduced"] = date
            intro_chamber     = chamber

        if (not out["passed_intro_chamber"]
                and intro_chamber
                and chamber == intro_chamber
                and _RE_PASSED.search(action)):
            out["passed_intro_chamber"] = date

        if (not out["passed_second_chamber"]
                and intro_chamber
                and chamber != intro_chamber
                and _RE_PASSED.search(action)):
            out["passed_second_chamber"] = date

        if not out["enacted"] and _RE_ENACTED.search(action):
            out["enacted"] = date

        if not out["effective"] and _RE_EFFECTIVE.search(action):
            inline = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', action)
            out["effective"] = inline.group(1) if inline else date

        if not out["dead"] and _RE_DEAD.search(action):
            out["dead"] = date

    return out


# ══════════════════════════════════════════════════════════════════════════════
# BUILD CSV ROW
# ══════════════════════════════════════════════════════════════════════════════
def build_row(bill_meta: dict, matched_keywords: list, local_file: str) -> dict:
    soup      = bill_meta["soup"]
    page_text = bill_meta["page_text"]
    session   = bill_meta["session"]

    sponsor, sponsor_party, cosponsor = extract_sponsors(soup, page_text)
    history = parse_history_table(soup, session)

    act_id    = ""
    act_match = re.search(r'Act\s+No\.?\s*(\d+)', page_text, re.I)
    if act_match:
        act_id = f"Act {act_match.group(1)}"

    return {
        "State":                          "Louisiana",
        "GA":                             SESSION_LABELS.get(session, session),
        "Policy (bill) identifier":       bill_meta["bill_id"],
        "Policy sponsor":                 sponsor,
        "Policy sponsor party":           sponsor_party,
        "Link to bill":                   bill_meta["bill_url"],
        "bill text":                      local_file,
        "Cosponsor":                      cosponsor,
        "Act identifier":                 act_id,
        "Matched keywords":               "; ".join(matched_keywords),
        "Introduced date":                history.get("introduced", ""),
        "Effective date":                 history.get("effective", ""),
        "Passed introduced chamber date": history.get("passed_intro_chamber", ""),
        "Passed second chamber date":     history.get("passed_second_chamber", ""),
        "Dead date":                      history.get("dead", ""),
        "Enacted (Y/N)":                  "Y" if act_id else "N",
        "Enacted Date":                   history.get("enacted", ""),
    }


# ══════════════════════════════════════════════════════════════════════════════
# CSV — append mode so each session run adds to the master file
# ══════════════════════════════════════════════════════════════════════════════
def append_to_csv(rows: list, output_path: str) -> None:
    file_has_data = os.path.isfile(output_path) and os.path.getsize(output_path) > 0
    with open(output_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        if not file_has_data:
            writer.writeheader()
        writer.writerows(rows)
    log.info("Appended %d row(s) to %s", len(rows), output_path)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main(session_code: str, output_csv: str) -> None:
    session_code = session_code.lower().strip()

    if session_code not in SESSION_LABELS:
        log.error(
            "'%s' is not a recognised session code.\n"
            "Run with --list to see all valid codes.", session_code
        )
        sys.exit(1)

    label = SESSION_LABELS[session_code]
    log.info("═" * 65)
    log.info("Louisiana Legislature Bill Scraper — FULL TEXT EDITION")
    log.info("Session  : %s  (%s)", session_code, label)
    log.info("Keywords : %s", KEYWORDS)
    log.info("Strategy : Full-text PDF scan — files saved ONLY on keyword match")
    log.info("Output   : %s  (append mode)", output_csv)
    log.info("═" * 65)

    # ── Step 1: Find every bill number in this session ────────────────────────
    log.info("Step 1 — Enumerating all bills in session %s…", session_code)
    all_bills = enumerate_bills(session_code)
    log.info("Step 1 complete — %d bills to scan", len(all_bills))

    # ── Step 2: Fetch, scan, and save only matched bills ──────────────────────
    log.info("Step 2 — Scanning full text (saving PDFs only on keyword match)…")
    results = []
    total   = len(all_bills)

    for i, bill_meta in enumerate(all_bills, 1):
        bill_id = bill_meta["bill_id"]
        log.info("[%d/%d] %s", i, total, bill_id)

        # Get the ViewDocument link from the cached bill info page
        doc_url = get_doc_url(bill_meta["soup"])
        if not doc_url:
            log.info("  ✗  No document link — skipping")
            continue

        # ── Fetch into memory and extract text (nothing written to disk yet) ──
        content, full_text, file_ext = fetch_bill_text_only(doc_url)

        if not full_text:
            log.info("  ✗  Could not extract text — skipping")
            continue

        # ── Keyword scan against the extracted text ───────────────────────────
        matched = find_keywords(full_text)

        if not matched:
            # No match — content is discarded, nothing saved
            log.info("  ✗  No keywords found — discarding")
            continue

        # ── Keywords found — NOW save the file to disk ────────────────────────
        local_file = save_bill_file(content, file_ext, bill_id, session_code)

        row = build_row(bill_meta, matched, local_file)
        results.append(row)
        log.info("  ✓  Matched: %s", "; ".join(matched))

    # ── Step 3: Write results to CSV ──────────────────────────────────────────
    log.info("═" * 65)
    log.info("Session %s complete — %d matching bills out of %d total",
             session_code, len(results), total)
    append_to_csv(results, output_csv)
    log.info("Done.")


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape one Louisiana legislative session — full text keyword scan only."
    )
    parser.add_argument(
        "--session",
        default=SESSION_TO_RUN,
        metavar="CODE",
        help="Session code to scrape (e.g. 26rs).  Default: %(default)s",
    )
    parser.add_argument(
        "--output",
        default=OUTPUT_CSV,
        metavar="FILE",
        help="CSV file to append results to.  Default: %(default)s",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Print all valid session codes and exit.",
    )
    args = parser.parse_args()

    if args.list:
        print(f"\n  {'CODE':<10}  LABEL")
        print(f"  {'-'*10}  {'-'*45}")
        for code, lbl in ALL_SESSIONS:
            print(f"  {code:<10}  {lbl}")
        print()
        sys.exit(0)

    main(session_code=args.session, output_csv=args.output)