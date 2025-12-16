#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ILGA Phase 2 — Enrich from ILGA Bill Status pages. 1
Priority: link on Full-Text page -> FTP HTML -> FTP XML -> ASP w/ GAID -> other ASP.

Input CSV (Phase 1):
  State, GA, Bill Identifier, URL, Path to full text, Keywords

Output CSV:
  State,GA,Policy (bill) identifier,Policy sponsor,Policy sponsor party,Link to bill,
  bill text,Cosponsor,Act identifier,Matched keywords,Introduced date,Effective date,
  Passed introduced chamber date,Passed second chamber date,Dead date,Enacted (Y/N),Enacted Date
"""

import argparse, csv, logging, random, re, time, xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, FeatureNotFound

BASE = "https://www.ilga.gov"

# -------- HTTP / Politeness --------
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
REQUEST_DELAY = (0.25, 0.55)

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("ilga_enrich")

def sleep_politely():
    time.sleep(random.uniform(*REQUEST_DELAY))

def soupify(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(html, "html.parser")

def clean(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

# -------- GA -> GAID (for ASP fallback) --------
GA_TO_GAID: Dict[int, int] = {
    103: 17, 102: 16, 101: 15, 100: 14,  99: 13,  98: 12,  97: 11,  96: 10,
     95:  9,  94:  8,  93:  3,  92:  2,  91:  1,  90:  7,  89:  6,  88:  5,
     87:  4,  86: -1,  85: -2,  84: -3,  83: -4,  82: -5,  81: -6,  80: -7,
     79: -8,  78: -9,  77: -10,
}

# -------- Candidate URL builders --------
def ftp_html_candidates(ga: int, doctype: str, docnum: int) -> List[str]:
    # https://www.ilga.gov/ftp/legislation/93/BillStatus/HTML/09300HB0019.html | .htm
    base = f"{BASE}/ftp/legislation/{ga}/BillStatus/HTML/{ga:03d}00{doctype}{docnum:04d}"
    return [base + ".html", base + ".htm"]

def ftp_xml_candidates(ga: int, doctype: str, docnum: int) -> List[str]:
    # https://www.ilga.gov/ftp/legislation/93/BillStatus/XML/09300HB0019.xml
    base = f"{BASE}/ftp/legislation/{ga}/BillStatus/XML/{ga:03d}00{doctype}{docnum:04d}"
    return [base + ".xml"]

def asp_gaid_candidates(ga: int, doctype: str, docnum: int) -> List[str]:
    gaid = GA_TO_GAID.get(ga, 0)
    d = str(int(docnum))
    return [
        f"{BASE}/legislation/BillStatus.asp?DocNum={d}&DocTypeID={doctype}&GAID={gaid}&GA={ga}",
        f"{BASE}/Legislation/BillStatus.asp?DocNum={d}&DocTypeID={doctype}&GAID={gaid}&GA={ga}",
        f"{BASE}/Legislation/BillStatus?DocNum={d}&DocTypeID={doctype}&GAID={gaid}&GA={ga}",
    ]

def asp_misc_candidates(ga: int, doctype: str, docnum: int) -> List[str]:
    d = str(int(docnum))
    return [
        f"{BASE}/Legislation/BillStatus.asp?DocNum={d}&DocTypeID={doctype}&GA={ga}",
        f"{BASE}/legislation/BillStatus.asp?DocNum={d}&DocTypeID={doctype}&GA={ga}",
    ]

# -------- Resolve BillStatus from Full-Text page --------
JS_STATUS_HREF_RE = re.compile(
    r"""(?:window\.location\s*=|location\.href\s*=|document\.location\s*=)\s*['"]([^'"]*BillStatus[^'"]+)['"]""",
    re.IGNORECASE
)
PLAIN_STATUS_URL_RE = re.compile(r"""https?://[^"'>\s]+BillStatus[^"'>\s]+""", re.IGNORECASE)

def resolve_status_url_from_fulltext(fulltext_url: str) -> Optional[str]:
    try:
        sleep_politely()
        r = SESSION.get(fulltext_url, timeout=(10, 45), allow_redirects=True)
        if r.status_code == 404:
            log.info(f"[resolver] 404 on full-text: {fulltext_url}")
            return None
        r.raise_for_status()
    except requests.RequestException as e:
        log.info(f"[resolver] Request error on full-text: {fulltext_url} :: {e}")
        return None

    s = soupify(r.text)

    for a in s.select("a[href]"):
        href = a.get("href", "")
        if "billstatus" in href.lower():
            target = urljoin(fulltext_url, href)
            log.info(f"[resolver] href match: {target}")
            return target

    for a in s.find_all("a"):
        txt = (a.get_text(" ", strip=True) or "").lower()
        href = a.get("href")
        if "bill status" in txt and href:
            target = urljoin(fulltext_url, href)
            log.info(f"[resolver] text match: {target}")
            return target

    for tag in s.find_all(["script", "a"]):
        onclick = (tag.get("onclick") or "")
        m = JS_STATUS_HREF_RE.search(onclick)
        if m:
            target = urljoin(fulltext_url, m.group(1))
            log.info(f"[resolver] onclick JS match: {target}")
            return target
        if tag.name == "script":
            scr = tag.string or tag.text or ""
            m2 = JS_STATUS_HREF_RE.search(scr)
            if m2:
                target = urljoin(fulltext_url, m2.group(1))
                log.info(f"[resolver] <script> JS match: {target}")
                return target

    m_abs = PLAIN_STATUS_URL_RE.search(r.text)
    if m_abs:
        log.info(f"[resolver] plain text URL match: {m_abs.group(0)}")
        return m_abs.group(0)

    log.info(f"[resolver] No Bill Status link found on: {fulltext_url}")
    return None

# -------- Parsing helpers (common) --------
ACTION_ROW_RE = re.compile(r"^\s*(\d{1,2}/\d{1,2}/\d{4})\s+(\w+)\s+(.*)$")
PA_RE       = re.compile(r"Public Act\s+(\d{2,3}-\d{4})", re.IGNORECASE)
EFF_DATE_RE = re.compile(r"Effective Date\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", re.IGNORECASE)
EFFECTIVE_IN_TEXT_RE = re.compile(r"\bEffective\s+([A-Za-z]+\s+\d{1,2},\s+\d{4}|\w+\s+\d{1,2},\s+\d{4})", re.IGNORECASE)

def parse_actions_table_any(soup: BeautifulSoup) -> List[str]:
    for hdr in soup.find_all(["h2", "h3", "h4", "h5"]):
        if "action" in hdr.get_text(" ", strip=True).lower():
            tbl = hdr.find_next("table")
            if tbl:
                rows = [clean(tr.get_text(" ")) for tr in tbl.select("tr")]
                return [r for r in rows if r]
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
        chamber = chamber.capitalize()
        if not introduced_date and (("Prefiled" in action) or ("First Reading" in action and chamber == origin_chamber)):
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
    if not effective_literal:
        m_eff2 = EFFECTIVE_IN_TEXT_RE.search(txt)
        if m_eff2:
            effective_literal = m_eff2.group(1)
    return act_identifier, effective_literal

def fetch_member_party(member_url: str) -> str:
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

def extract_sponsors_blocks(soup: BeautifulSoup) -> Tuple[str, str]:
    # Works for FTP HTML “House Sponsors”, “Senate Sponsors”, “Co-Sponsors”
    sponsor_texts, cosponsor_texts = [], []
    txt = soup.get_text("\n")
    # lines like: "House Sponsors" on one line then the names on the next line
    lines = [l.strip() for l in txt.splitlines()]
    for i, line in enumerate(lines):
        lc = line.lower()
        if lc.startswith("house sponsors") or lc.startswith("senate sponsors"):
            # next non-empty line usually has the names
            j = i + 1
            while j < len(lines) and not lines[j]:
                j += 1
            if j < len(lines):
                sponsor_texts.append(lines[j])
        if re.match(r"^co-?sponsors?:", line, flags=re.IGNORECASE):
            cosponsor_texts.append(re.sub(r"^co-?sponsors?:\s*", "", line, flags=re.IGNORECASE))
    sponsor_line = " | ".join(clean(s) for s in sponsor_texts if s)
    cosponsor_line = " | ".join(clean(s) for s in cosponsor_texts if s)
    return sponsor_line, cosponsor_line

def cosponsors_from_actions(actions_text: List[str]) -> List[str]:
    names = []
    for line in actions_text:
        if "Added Co-Sponsor" in line or "Added Chief Co-Sponsor" in line:
            # extract after the phrase
            m = re.search(r"Added (Chief )?Co-Sponsor\s+(.*)$", line)
            if m:
                names.append(clean(m.group(2)))
    return names

# -------- Fetch + parse helpers per source --------
def fetch_text(url: str) -> Optional[str]:
    try:
        log.info(f"[try] {url}")
        sleep_politely()
        r = SESSION.get(url, timeout=(10, 45), allow_redirects=True)
        log.info(f"[try] -> HTTP {r.status_code}")
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.text
    except requests.RequestException as e:
        log.info(f"[fail] {url} :: {e}")
        return None

def parse_ftp_html(html: str, doctype: str) -> Dict[str, str]:
    s = soupify(html)
    sponsor_line, cosponsor_inline = extract_sponsors_blocks(s)
    actions_lines = parse_actions_table_any(s)
    origin = "House" if doctype == "HB" else "Senate"
    dates = parse_actions_for_dates(actions_lines, origin)
    act_identifier, effective_literal = extract_public_act_and_effective_text(s)
    if effective_literal and not dates["Effective date"]:
        dates["Effective date"] = effective_literal
    # Merge cosponsors from actions
    cos_from_actions = cosponsors_from_actions(actions_lines)
    cosponsor = " | ".join([c for c in [cosponsor_inline] + cos_from_actions if c])
    # Party via first sponsor link (FTP HTML seldom has member links; we’ll leave party blank here)
    sponsor_party = ""
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

def parse_ftp_xml(xml_text: str, doctype: str) -> Dict[str, str]:
    # Minimal XML parser (fields vary by GA; try common tags)
    out = {
        "Policy sponsor": "", "Policy sponsor party": "", "Cosponsor": "", "Act identifier": "",
        "Introduced date": "", "Effective date": "", "Passed introduced chamber date": "",
        "Passed second chamber date": "", "Dead date": "", "Enacted (Y/N)": "", "Enacted Date": "",
    }
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return out

    def text_or(el, default=""):
        return (el.text or "").strip() if el is not None else default

    # Sponsors (comma join)
    sponsors = []
    for tag in ["HouseSponsors", "SenateSponsors", "Sponsors"]:
        node = root.find(f".//{tag}")
        if node is not None:
            val = " ".join(node.itertext()).strip()
            if val:
                sponsors.append(val)
    out["Policy sponsor"] = " | ".join(sponsors)

    # Co-sponsors
    cos = root.find(".//CoSponsors")
    if cos is not None:
        out["Cosponsor"] = " ".join(cos.itertext()).strip()

    # Public Act
    pa = root.find(".//PublicAct")
    if pa is not None:
        pa_num = text_or(root.find(".//PublicAct/Act"))
        if pa_num:
            out["Act identifier"] = pa_num
            out["Enacted (Y/N)"] = "Y"

    # Actions list for dates
    origin = "House" if doctype == "HB" else "Senate"
    introduced = ""
    passed_first = ""
    passed_second = ""
    effective = ""
    enacted = ""
    dead = ""

    for act in root.findall(".//Action"):
        date = text_or(act.find("Date"))
        chamber = text_or(act.find("Chamber"))
        action = text_or(act.find("ActionDescription"))
        if not introduced and (("Prefiled" in action) or ("First Reading" in action and chamber == origin)):
            introduced = date
        if (not passed_first) and ("Third Reading" in action and "Passed" in action and chamber == origin):
            passed_first = date
        if (not passed_second) and ("Third Reading" in action and "Passed" in action and chamber != origin and chamber):
            passed_second = date
        if ("Governor Approved" in action) and not enacted:
            enacted = date
        if ("Effective Date" in action) and not effective:
            effective = date
        if any(tag in action for tag in ["Session Sine Die", "Re-referred to Rules Committee", "Rule 19(a)", "Rule 3-9(a)", "Vetoed", "Amendatory Veto Overridden - Fail"]):
            dead = date

    out["Introduced date"] = introduced
    out["Passed introduced chamber date"] = passed_first
    out["Passed second chamber date"] = passed_second
    out["Effective date"] = effective
    out["Enacted Date"] = enacted
    out["Dead date"] = dead
    return out

# -------- Enrich a single bill --------
def enrich_from_bill_status(ga: int, bill_identifier: str, fulltext_url_str: str) -> Dict[str, str]:
    doctype = bill_identifier[:2].upper()
    docnum = int(bill_identifier[2:])

    urls: List[Tuple[str, str]] = []  # (kind, url)

    # 1) Try to resolve from Full-Text page
    resolved = resolve_status_url_from_fulltext(fulltext_url_str)
    if resolved:
        urls.append(("resolved", resolved))

    # 2) FTP HTML (most reliable for GA 77–100+)
    for u in ftp_html_candidates(ga, doctype, docnum):
        urls.append(("ftp_html", u))

    # 3) FTP XML (structured fallback)
    for u in ftp_xml_candidates(ga, doctype, docnum):
        urls.append(("ftp_xml", u))

    # 4) ASP GAID
    for u in asp_gaid_candidates(ga, doctype, docnum):
        urls.append(("asp_gaid", u))

    # 5) ASP misc
    for u in asp_misc_candidates(ga, doctype, docnum):
        urls.append(("asp_misc", u))

    # Now fetch in order and parse
    for kind, url in urls:
        text = fetch_text(url)
        if not text:
            continue

        if kind == "ftp_html" or (kind == "resolved" and "/ftp/legislation/" in url and url.lower().endswith((".htm", ".html"))):
            data = parse_ftp_html(text, doctype)
        elif kind == "ftp_xml" or (kind == "resolved" and url.lower().endswith(".xml")):
            data = parse_ftp_xml(text, doctype)
        else:
            # Generic HTML parser for ASP/static
            s = soupify(text)
            sponsor_line, sponsor_party = extract_sponsors_blocks(s)
            actions_lines = parse_actions_table_any(s)
            dates = parse_actions_for_dates(actions_lines, "House" if doctype == "HB" else "Senate")
            act_identifier, effective_literal = extract_public_act_and_effective_text(s)
            if effective_literal and not dates["Effective date"]:
                dates["Effective date"] = effective_literal
            cos = " | ".join(cosponsors_from_actions(actions_lines))
            data = {
                "Policy sponsor": sponsor_line,
                "Policy sponsor party": sponsor_party,  # often blank here
                "Cosponsor": cos,
                "Act identifier": act_identifier,
                "Introduced date": dates["Introduced date"],
                "Effective date": dates["Effective date"],
                "Passed introduced chamber date": dates["Passed introduced chamber date"],
                "Passed second chamber date": dates["Passed second chamber date"],
                "Dead date": dates["Dead date"],
                "Enacted (Y/N)": "Y" if act_identifier else "",
                "Enacted Date": dates["Enacted Date"],
            }

        # If we actually got content, return it
        if any(data.values()):
            log.info(f"[ok] Enriched from ({kind}): {url}")
            return data

    log.info(f"[miss] Could not enrich bill {bill_identifier} (GA {ga}) from any source.")
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

# -------- Main --------
OUT_COLUMNS = [
    "State","GA","Policy (bill) identifier","Policy sponsor","Policy sponsor party","Link to bill","bill text",
    "Cosponsor","Act identifier","Matched keywords","Introduced date","Effective date","Passed introduced chamber date",
    "Passed second chamber date","Dead date","Enacted (Y/N)","Enacted Date",
]

def main():
    ap = argparse.ArgumentParser(description="Enrich Illinois bill rows using ILGA Bill Status (FTP HTML/XML + ASP fallbacks).")
    ap.add_argument("--in", dest="inp", required=True, help="Input CSV from Phase 1")
    ap.add_argument("--out", dest="outp", required=True, help="Output enriched CSV path")
    ap.add_argument("--only-ga", nargs="*", type=int, default=None,
                    help="Limit to these GA numbers, e.g., --only-ga 93 or --only-ga 92 93 94.")
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
            link = (row.get("URL") or "").strip()
            bill_text_path = (row.get("Path to full text") or "").strip()
            matched_keywords = (row.get("Keywords") or "").strip()

            ga_str = (row.get("GA") or "").strip()
            if ga_str.isdigit():
                ga = int(ga_str)
            else:
                m = re.search(r"/Documents/legislation/(\d{2,3})/", link)
                ga = int(m.group(1)) if m else 0

            if not bill_id or not ga:
                continue
            if only_gas and ga not in only_gas:
                continue

            enriched = enrich_from_bill_status(ga, bill_id, link)

            out_row = {
                "State": state,
                "GA": str(ga),
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
                log.info(f"Wrote {written} rows…")

    log.info(f"Done. Processed {processed} input rows. Wrote {written} rows to {outp}")

if __name__ == "__main__":
    main()
