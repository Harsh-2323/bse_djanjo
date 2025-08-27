# -*- coding: utf-8 -*-
"""
Scrape NSE Corporate Filings (Announcements) and save to **Database** (no Excel).

- Cleans Subject (strips "Time Taken" etc.)
- Captures PDF + XBRL links into separate columns
- Parses XBRL (works with /api/xbrl/{id} JSON, ZIPs, XML/XBRL)
- Shares Selenium cookies with requests so API calls behave like the browser
"""

import os, re, time, io, zipfile, json
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
import xml.etree.ElementTree as ET

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

try:
    from webdriver_manager.chrome import ChromeDriverManager
except Exception:
    ChromeDriverManager = None

# --- Model import (support either app layout) ---
try:
    # if both models live in selenium_scrape.models
    from selenium_scrape.models import NseAnnouncement  # type: ignore
except Exception:
    from nse_scrape.models import NseAnnouncement  # type: ignore

URL = "https://www.nseindia.com/companies-listing/corporate-filings-announcements"
HOMEPAGE = "https://www.nseindia.com"

DT_RE = r"\d{1,2}-[A-Za-z]{3}-\d{4}\s+\d{2}:\d{2}:\d{2}"
TIME_RE = r"\d{2}:\d{2}:\d{2}"

BROADCAST_LABEL_RE = re.compile(r"(Exchange\s*Received\s*Time|Exchange\s*Dissemination\s*Time|Time\s*Taken)", re.I)
TIME_TAKEN_BLOCK_RE = re.compile(r"Time\s*Taken[:\s]*" + TIME_RE, re.I)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

# ---------- selenium & session helpers ----------
def _setup_driver(headless: bool):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--window-size=1400,1000")
    opts.add_argument(f"user-agent={UA}")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.page_load_strategy = "normal"

    if ChromeDriverManager:
        service = Service(ChromeDriverManager().install())
    else:
        service = Service(os.environ.get("CHROMEDRIVER", "chromedriver"))
    return webdriver.Chrome(service=service, options=opts)


def _session_from_driver(driver) -> requests.Session:
    """Clone NSE cookies from Selenium into a requests.Session."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "*/*",
        "Referer": "https://www.nseindia.com/",
        "Connection": "keep-alive",
    })
    for c in driver.get_cookies():
        dom = c.get("domain") or "www.nseindia.com"
        if "nseindia.com" not in dom:
            continue
        s.cookies.set(
            c.get("name"), c.get("value", ""),
            domain=dom, path=c.get("path", "/")
        )
    return s


def _wait_table(driver, timeout=30):
    wait = WebDriverWait(driver, timeout)
    wait.until(EC.presence_of_element_located((By.ID, "CFanncEquityTable")))
    wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, "#CFanncEquityTable tbody tr")) >= 1)
    return driver.find_element(By.ID, "CFanncEquityTable")


def _load_enough_rows(driver, max_rows: int, pause: float, stall_tolerance: int):
    scroll_el = _wait_table(driver, timeout=30)
    prev = 0
    stalled = 0
    while True:
        rows = driver.find_elements(By.CSS_SELECTOR, "#CFanncEquityTable tbody tr")
        if len(rows) >= max_rows:
            break
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", scroll_el)
        time.sleep(pause)
        new_count = len(driver.find_elements(By.CSS_SELECTOR, "#CFanncEquityTable tbody tr"))
        if new_count == prev:
            stalled += 1
            if stalled >= stall_tolerance:
                break
        else:
            stalled = 0
            prev = new_count


# ---------- table parsing helpers ----------
def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _build_header_map(soup: BeautifulSoup) -> dict:
    headers = soup.select("#CFanncEquityTable thead th")
    colmap = {}
    for idx, th in enumerate(headers):
        key = _norm(th.get_text(" ", strip=True))
        if key:
            colmap[key] = idx
    return colmap


def _cell_text(td):
    txt = td.get_text(" ", strip=True) if td else ""
    txt = txt.replace("Read More", "").replace("Read Less", "").strip()
    return re.sub(r"\s{2,}", " ", txt)


def _has_pdf_link(td) -> bool:
    return bool(td and td.select_one('a[href*=".pdf" i]'))


def _has_xbrl_link(td) -> bool:
    if not td:
        return False
    sel = td.select_one('a[href*=".xml" i], a[href*=".xbrl" i], a[href*=".zip" i], a[href*="xbrl" i]')
    if not sel:
        return False
    href = (sel.get("href") or "").lower()
    return (".xml" in href or ".xbrl" in href or "xbrl" in href or href.endswith(".zip"))


def _looks_like_symbol(s: str) -> bool:
    if not s or len(s) > 20:
        return False
    if re.search(DT_RE, s):
        return False
    return bool(re.fullmatch(r"[A-Z0-9&/\-\. ]{1,20}", s)) and any(ch.isalpha() for ch in s)


def _looks_like_company(s: str) -> bool:
    if not s:
        return False
    if _is_broadcast_text(s):
        return False
    if s.strip().lower() in {"data", "details"}:
        return False
    if len(s.split()) < 2:
        return False
    if re.search(r"(limited|ltd|industries|bank|services|international|private|plc|labs|pharma|technolog|finance|infra|steel|engineer|chemical|cement|energy|motors|foods|capital)", s, flags=re.I):
        return True
    alpha_ratio = sum(ch.isalpha() for ch in s) / max(1, len(s))
    return alpha_ratio > 0.5 and not s.isupper()


def _is_broadcast_text(s: str) -> bool:
    if not s:
        return False
    if BROADCAST_LABEL_RE.search(s):
        return True
    if TIME_TAKEN_BLOCK_RE.search(s):
        return True
    return len(re.findall(DT_RE, s)) >= 2


def _parse_broadcast_text(s: str):
    res = {"Exchange Received Time": "", "Exchange Dissemination Time": "", "Time Taken": ""}
    if not s:
        return res
    s1 = re.sub(r"\s+", " ", s.strip())

    rec = re.search(r"Exchange\s*Received\s*Time[:\s]+(" + DT_RE + r")", s1, flags=re.I)
    dis = re.search(r"Exchange\s*Dissemination\s*Time[:\s]+(" + DT_RE + r")", s1, flags=re.I)
    taken = re.search(r"Time\s*Taken[:\s]+(" + TIME_RE + r")", s1, flags=re.I)

    if not (rec and dis):
        dts = re.findall(DT_RE, s1)
        if not rec and len(dts) >= 1:
            res["Exchange Received Time"] = dts[0]
        if not dis and len(dts) >= 2:
            res["Exchange Dissemination Time"] = dts[1]
    else:
        res["Exchange Received Time"] = rec.group(1)
        res["Exchange Dissemination Time"] = dis.group(1)

    if taken:
        res["Time Taken"] = taken.group(1)
    return res


def _strip_broadcast_bits(s: str) -> str:
    if not s:
        return s
    s2 = re.sub(r"Exchange\s*Received\s*Time.*?(?=Exchange|Time\s*Taken|$)", "", s, flags=re.I | re.DOTALL).strip()
    s2 = re.sub(r"Exchange\s*Dissemination\s*Time.*?(?=Exchange|Time\s*Taken|$)", "", s2, flags=re.I | re.DOTALL).strip()
    s2 = re.sub(r"Time\s*Taken[:\s]*" + TIME_RE + r".*?$", "", s2, flags=re.I | re.DOTALL).strip()
    s2 = re.sub(r"\s+" + TIME_RE + r"(?:\s|$)", " ", s2, flags=re.I).strip()
    s2 = re.sub(r"(?:Exchange|Time\s*Taken).*?$", "", s2, flags=re.I).strip()
    return s2


def _clean_subject(s: str) -> str:
    if not s:
        return s
    s2 = _strip_broadcast_bits(s)
    s2 = re.sub(r"\s+" + DT_RE + r"\s*$", "", s2).strip()
    s2 = re.sub(r"\s+" + TIME_RE + r"\s*$", "", s2).strip()
    s2 = re.sub(r"(?:Exchange\s*(?:Received|Dissemination)\s*Time|Time\s*Taken).*?$", "", s2, flags=re.I).strip()
    if s2.strip().lower() in {"data", "details", "time", "taken", "exchange"}:
        return ""
    if re.fullmatch(r"[\d:\s]+", s2.strip()):
        return ""
    return s2


def _in_range(i, arr) -> bool:
    return i is not None and 0 <= i < len(arr)


def _pick_subject_candidate(texts, banned_idx):
    best_idx, best_val, best_len = None, "", -1
    for i, t in enumerate(texts):
        if i in banned_idx:
            continue
        if not t:
            continue
        if _is_broadcast_text(t):
            continue
        if re.search(TIME_RE, t) and len(t.strip()) < 50:
            continue
        tt = _clean_subject(t)
        if not tt:
            continue
        if len(tt) > best_len:
            best_idx, best_val, best_len = i, tt, len(tt)
    return best_idx, best_val


# ---------- XBRL utils ----------
def _iter_localname(e):
    """Yield (localname, element) for all descendants including root."""
    stack = [e]
    while stack:
        cur = stack.pop()
        local = cur.tag.split('}', 1)[-1] if '}' in cur.tag else cur.tag
        yield local, cur
        stack.extend(list(cur))


def _find_first_text(root, names):
    for lname, el in _iter_localname(root):
        if lname in names:
            t = (el.text or "").strip()
            if t:
                return t
    return ""


def _choose_zip_member(namelist):
    """Pick the most likely XBRL instance file inside a zip."""
    candidates = [n for n in namelist if n.lower().endswith((".xml", ".xbrl"))]
    if not candidates:
        return None
    pri = sorted(
        candidates,
        key=lambda n: (
            0 if "inst" in n.lower() or "instance" in n.lower() else 1,
            0 if "capmkt" in n.lower() else 1,
            len(n)
        )
    )
    return pri[0]


def _parse_xbrl_bytes(xml_bytes, debug=False):
    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        return None, f"parse_error: {e}"

    data = {
        "XBRL NSE Symbol": _find_first_text(root, {"NSESymbol", "Symbol"}),
        "XBRL Company Name": _find_first_text(root, {"NameOfTheCompany", "CompanyName", "NameOfCompany"}),
        "XBRL Subject": _find_first_text(root, {"SubjectOfAnnouncement", "Subject"}),
        "XBRL Description": _find_first_text(root, {"DescriptionOfAnnouncement", "Description"}),
        "XBRL Attachment URL": _find_first_text(root, {"AttachmentURL", "AttachmentUrl", "AttachmentLink", "PdfURL", "PDFURL"}),
        "XBRL DateTime": _find_first_text(root, {"DateAndTimeOfSubmission", "DateOfSubmission", "DateTime"}),
        "XBRL Category": _find_first_text(root, {"CategoryOfAnnouncement", "Category"}),
    }
    for k in list(data.keys()):
        data[k] = (data[k] or "").strip()
    ok = any(data.values())
    return (data if ok else None), ("ok" if ok else "empty_xbrl")


def _find_urls_in_json(obj):
    """Yield all strings in a JSON that look like URLs (pref xbrl/xml/zip)."""
    stack = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            stack.extend(cur.values())
        elif isinstance(cur, list):
            stack.extend(cur)
        elif isinstance(cur, str):
            s = cur.strip()
            if s.startswith("http"):
                yield s


def _prefer_xbrl_url(urls):
    """Pick best candidate pointing to actual instance file."""
    pri = sorted(
        urls,
        key=lambda u: (
            0 if re.search(r"\.(xbrl|xml)(?:$|\?)", u.lower()) else 1,
            0 if "xbrl" in u.lower() else 1,
            0 if u.lower().endswith(".zip") else 2,
            len(u)
        )
    )
    return pri[0] if pri else None


def _extract_fields_from_json(j):
    """Best-effort field pull when the API returns decoded XBRL as JSON."""
    flat = {}
    stack = [(None, j)]
    while stack:
        k, v = stack.pop()
        if isinstance(v, dict):
            stack.extend(v.items())
        elif isinstance(v, list):
            stack.extend([(k, x) for x in v])
        else:
            key = (k or "").lower()
            if isinstance(v, str):
                if "symbol" in key and "nse" in key: flat["XBRL NSE Symbol"] = v
                elif "company" in key and "name" in key: flat["XBRL Company Name"] = v
                elif "subject" in key: flat["XBRL Subject"] = v
                elif "description" in key: flat["XBRL Description"] = v
                elif "attachment" in key and ("url" in key or "link" in key): flat["XBRL Attachment URL"] = v
                elif "datetime" in key or ("date" in key and "time" in key): flat["XBRL DateTime"] = v
                elif "category" in key: flat["XBRL Category"] = v
    return {k: v for k, v in flat.items() if v}


def _fetch_and_parse_xbrl(url, session: requests.Session, debug=False):
    """Fetch URL that may be an API JSON, a ZIP, or an XML/XBRL, then parse."""
    try:
        r = session.get(url, timeout=30, allow_redirects=True)
        r.raise_for_status()
    except Exception as e:
        return None, f"http_error: {e}"

    ct = (r.headers.get("Content-Type") or "").lower()
    body = r.content or b""

    # CASE A: JSON wrapper like /api/xbrl/{id}
    is_jsonish = "json" in ct or (body[:1] in (b"{", b"["))
    if is_jsonish:
        try:
            j = r.json()
        except Exception:
            try:
                j = json.loads(body.decode("utf-8", "ignore"))
            except Exception as e:
                return None, f"json_error: {e}"

        urls = list(_find_urls_in_json(j))
        cand = _prefer_xbrl_url(urls)
        if cand:
            return _fetch_and_parse_xbrl(urljoin(URL, cand), session, debug=debug)

        data = _extract_fields_from_json(j)
        if data:
            return data, "ok_json"

        return None, "json_no_urls"

    # CASE B: ZIP package
    if url.lower().endswith(".zip") or "zip" in ct:
        try:
            with zipfile.ZipFile(io.BytesIO(body)) as zf:
                member = _choose_zip_member(zf.namelist())
                if not member:
                    return None, "zip_no_xml"
                xml_bytes = zf.read(member)
        except Exception as e:
            return None, f"zip_error: {e}"
        return _parse_xbrl_bytes(xml_bytes, debug=debug)

    # CASE C: XML/XBRL inline
    return _parse_xbrl_bytes(body, debug=debug)


# ---------- extraction ----------
def _extract_table_html(driver, max_rows: int, debug: bool=False, xbrl_parse: bool=True, http_sess: requests.Session | None=None) -> pd.DataFrame:
    soup = BeautifulSoup(driver.page_source, "lxml")
    colmap = _build_header_map(soup)
    if debug:
        print("Detected headers:", colmap)

    rows = soup.select("#CFanncEquityTable tbody tr")
    out = []

    # HTTP session (shared cookies)
    sess = http_sess or requests.Session()
    sess.headers.update({"User-Agent": UA, "Referer": URL})

    for r in rows[:max_rows]:
        tds = r.find_all("td")
        if not tds:
            continue

        texts = [_cell_text(td) for td in tds]

        # 1) detect broadcast cell by content
        b_idx = next((i for i, t in enumerate(texts) if _is_broadcast_text(t)), None)

        # 2) detect attachment
        idx_attach = colmap.get("attachment")
        if not _in_range(idx_attach, tds):
            idx_attach = next((i for i, td in enumerate(tds) if _has_pdf_link(td) or _has_xbrl_link(td)), None)

        # 3) symbol/company via headers or heuristics
        idx_symbol = colmap.get("symbol")
        if not _in_range(idx_symbol, tds) or not _looks_like_symbol(texts[idx_symbol]):
            idx_symbol = next((i for i, t in enumerate(texts)
                               if _looks_like_symbol(t) and i not in {b_idx, idx_attach}), None)

        idx_company = colmap.get("companyname") or colmap.get("company")
        if not _in_range(idx_company, tds) or not _looks_like_company(texts[idx_company]):
            cand = None
            if _in_range(idx_symbol, tds):
                for nb in (idx_symbol + 1, idx_symbol - 1):
                    if _in_range(nb, tds) and _looks_like_company(texts[nb]) and nb not in {b_idx, idx_attach}:
                        cand = nb
                        break
            if cand is None:
                cand = next((i for i, t in enumerate(texts)
                             if _looks_like_company(t) and i not in {b_idx, idx_attach, idx_symbol}), None)
            idx_company = cand

        # 4) subject: header if valid; else longest clean text
        idx_subject = colmap.get("subject")
        if not _in_range(idx_subject, tds) or _is_broadcast_text(texts[idx_subject]):
            banned = {x for x in [b_idx, idx_attach, idx_symbol, idx_company] if _in_range(x, tds)}
            idx_subject, subj_text = _pick_subject_candidate(texts, banned)
        else:
            subj_text = _clean_subject(texts[idx_subject])

        symbol = texts[idx_symbol] if _in_range(idx_symbol, tds) and _looks_like_symbol(texts[idx_symbol]) else ""
        company_name = texts[idx_company] if _in_range(idx_company, tds) and _looks_like_company(texts[idx_company]) else ""

        subject = _clean_subject(subj_text or "")
        subject = _strip_broadcast_bits(subject)

        # broadcast split
        rec = dis = taken = ""
        if _in_range(b_idx, tds):
            bd = _parse_broadcast_text(texts[b_idx])
            rec, dis, taken = bd["Exchange Received Time"], bd["Exchange Dissemination Time"], bd["Time Taken"]

        # --- attachments (PDF + XBRL) ---
        pdf_links, xbrl_links = [], []

        def _scan_td_for_links(td):
            urls = []
            for a in td.select('a[href]'):
                href = a.get("href") or ""
                if not href:
                    continue
                urls.append(urljoin(URL, href))
            return urls

        if _in_range(idx_attach, tds):
            for u in _scan_td_for_links(tds[idx_attach]):
                lu = u.lower()
                if ".pdf" in lu:
                    pdf_links.append(u)
                elif any(x in lu for x in [".xml", ".xbrl", "xbrl", ".zip"]):
                    xbrl_links.append(u)
        else:
            for td in tds:
                for u in _scan_td_for_links(td):
                    lu = u.lower()
                    if ".pdf" in lu:
                        pdf_links.append(u)
                    elif any(x in lu for x in [".xml", ".xbrl", "xbrl", ".zip"]):
                        xbrl_links.append(u)

        # de-dup keep order
        def _dedup(seq):
            seen, outl = set(), []
            for x in seq:
                if x not in seen:
                    seen.add(x)
                    outl.append(x)
            return outl

        pdf_links = _dedup(pdf_links)
        xbrl_links = _dedup(xbrl_links)

        row = {
            "Symbol": symbol,
            "Company Name": company_name,
            "Subject": subject,
            "Exchange Received Time": rec,
            "Exchange Dissemination Time": dis,
            "Time Taken": taken,
            "Attachment Size": re.sub(r"\s*PDF\s*", "", texts[idx_attach], flags=re.I).strip() if _in_range(idx_attach, tds) else None,
            "Attachment Link": " | ".join(pdf_links) if pdf_links else "",
            "XBRL Link": " | ".join(xbrl_links) if xbrl_links else "",
            "Has XBRL": bool(xbrl_links),

            # parsed XBRL fields (filled later if available)
            "XBRL NSE Symbol": "",
            "XBRL Company Name": "",
            "XBRL Subject": "",
            "XBRL Description": "",
            "XBRL Attachment URL": "",
            "XBRL DateTime": "",
            "XBRL Category": "",
            "XBRL Parse Status": "no_xbrl" if not xbrl_links else "skipped" if not xbrl_parse else "",
        }

        if xbrl_parse and xbrl_links:
            primary = xbrl_links[0]
            data, status = _fetch_and_parse_xbrl(primary, sess, debug=debug)
            row["XBRL Parse Status"] = status
            if data:
                row.update({k: v for k, v in data.items() if v})
                if not row["Subject"] and data.get("XBRL Subject"):
                    row["Subject"] = data["XBRL Subject"]

        out.append(row)

    return pd.DataFrame(out)


# ---------- command ----------
class Command(BaseCommand):
    help = "Scrape NSE announcements → Database (cleans Subject; splits times; includes & parses XBRL)."

    def add_arguments(self, parser):
        parser.add_argument("--max-rows", type=int, default=100)
        parser.add_argument("--headless", action="store_true", default=False)
        parser.add_argument("--pause", type=float, default=1.2)
        parser.add_argument("--stall", type=int, default=4)
        parser.add_argument("--debug", action="store_true")
        parser.add_argument("--xbrl-parse", dest="xbrl_parse", action="store_true", default=True)
        parser.add_argument("--no-xbrl-parse", dest="xbrl_parse", action="store_false")

    def handle(self, *args, **opts):
        max_rows  = int(opts["max_rows"])
        headless  = bool(opts["headless"])
        pause     = float(opts["pause"])
        stall     = int(opts["stall"])
        debug     = bool(opts.get("debug"))
        xbrl_parse = bool(opts.get("xbrl_parse"))

        self.stdout.write(f"→ Launching Chrome (headless={headless})")
        driver = None
        try:
            driver = _setup_driver(headless=headless)
        except Exception as e:
            raise CommandError(f"Failed to launch Chrome/Driver: {e}")

        df = pd.DataFrame()
        try:
            self.stdout.write("→ Opening NSE homepage (cookie preflight)")
            driver.get(HOMEPAGE)
            WebDriverWait(driver, 20).until(lambda d: d.execute_script("return document.readyState") == "complete")

            self.stdout.write("→ Opening announcements page")
            driver.get(URL)
            _wait_table(driver, 30)

            # build HTTP session with current NSE cookies
            sess = _session_from_driver(driver)

            _load_enough_rows(driver, max_rows=max_rows, pause=pause, stall_tolerance=stall)
            df = _extract_table_html(driver, max_rows=max_rows, debug=debug, xbrl_parse=xbrl_parse, http_sess=sess)

        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

        if df.empty:
            self.stdout.write(self.style.WARNING("❌ No announcements scraped"))
            return

        # ------------------------------
        # Save to Database
        # ------------------------------
        def _none_if_blank(v):
            if v is None:
                return None
            if isinstance(v, float) and pd.isna(v):
                return None
            if isinstance(v, str) and not v.strip():
                return None
            return v

        count_new, count_existing = 0, 0
        for _, row in df.iterrows():
            unique_key = {
                "symbol": _none_if_blank(row.get("Symbol")),
                "subject": _none_if_blank(row.get("Subject")),
                "exchange_dissemination_time": _none_if_blank(row.get("Exchange Dissemination Time")),
            }
            defaults = {
                "company_name": _none_if_blank(row.get("Company Name")),
                "exchange_received_time": _none_if_blank(row.get("Exchange Received Time")),
                "time_taken": _none_if_blank(row.get("Time Taken")),
                "attachment_size": _none_if_blank(row.get("Attachment Size")),
                "attachment_link": _none_if_blank(row.get("Attachment Link")),
                "xbrl_link": _none_if_blank(row.get("XBRL Link")),
                "has_xbrl": bool(row.get("Has XBRL")),
                "xbrl_nse_symbol": _none_if_blank(row.get("XBRL NSE Symbol")),
                "xbrl_company_name": _none_if_blank(row.get("XBRL Company Name")),
                "xbrl_subject": _none_if_blank(row.get("XBRL Subject")),
                "xbrl_description": _none_if_blank(row.get("XBRL Description")),
                "xbrl_attachment_url": _none_if_blank(row.get("XBRL Attachment URL")),
                "xbrl_datetime": _none_if_blank(row.get("XBRL DateTime")),
                "xbrl_category": _none_if_blank(row.get("XBRL Category")),
                "xbrl_parse_status": _none_if_blank(row.get("XBRL Parse Status")),
            }

            with transaction.atomic():
                obj, created = NseAnnouncement.objects.update_or_create(
                    **unique_key,
                    defaults=defaults
                )
            if created:
                count_new += 1
            else:
                count_existing += 1

        self.stdout.write(self.style.SUCCESS(
            f"✅ {count_new} new NSE records inserted, {count_existing} already existed"
        ))
