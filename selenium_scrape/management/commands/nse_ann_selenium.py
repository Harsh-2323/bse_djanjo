# -*- coding: utf-8 -*-
"""
Scrape NSE Corporate Filings (Announcements) and save to Excel (no DB).

Fixed version that prevents "Time Taken" from appearing in Subject column.
Now also captures XBRL links and writes them to a separate Excel column.

Usage:
  python manage.py nse_ann_selenium --max-rows 80 --debug
"""
import os, re, time
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup
from django.core.management.base import BaseCommand, CommandError

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


URL = "https://www.nseindia.com/companies-listing/corporate-filings-announcements"
HOMEPAGE = "https://www.nseindia.com"

DT_RE = r"\d{1,2}-[A-Za-z]{3}-\d{4}\s+\d{2}:\d{2}:\d{2}"
TIME_RE = r"\d{2}:\d{2}:\d{2}"

BROADCAST_LABEL_RE = re.compile(r"(Exchange\s*Received\s*Time|Exchange\s*Dissemination\s*Time|Time\s*Taken)", re.I)
TIME_TAKEN_BLOCK_RE = re.compile(r"Time\s*Taken[:\s]*" + TIME_RE, re.I)


# ---------- helpers ----------
def _setup_driver(headless: bool):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--window-size=1400,1000")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.page_load_strategy = "normal"

    if ChromeDriverManager:
        service = Service(ChromeDriverManager().install())
    else:
        service = Service(os.environ.get("CHROMEDRIVER", "chromedriver"))
    return webdriver.Chrome(service=service, options=opts)


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
    """
    Treat .xml/.xbrl as XBRL; also pick zipped XBRL packages and links containing 'xbrl'.
    """
    if not td:
        return False
    sel = td.select_one(
        'a[href*=".xml" i], a[href*=".xbrl" i], a[href*=".zip" i], a[href*="xbrl" i]'
    )
    if not sel:
        return False
    href = (sel.get("href") or "").lower()
    # keep generic zip only if it looks like xbrl-ish
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
    """Identify any broadcast-ish cell."""
    if not s:
        return False
    if BROADCAST_LABEL_RE.search(s):
        return True
    # has explicit 'Time Taken hh:mm:ss'?
    if TIME_TAKEN_BLOCK_RE.search(s):
        return True
    # or contains at least two full datetimes
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


# 🔒 Enhanced broadcast bit stripper - more aggressive patterns
def _strip_broadcast_bits(s: str) -> str:
    if not s:
        return s

    # Remove complete broadcast patterns
    s2 = re.sub(r"Exchange\s*Received\s*Time.*?(?=Exchange|Time\s*Taken|$)", "", s, flags=re.I | re.DOTALL).strip()
    s2 = re.sub(r"Exchange\s*Dissemination\s*Time.*?(?=Exchange|Time\s*Taken|$)", "", s2, flags=re.I | re.DOTALL).strip()
    s2 = re.sub(r"Time\s*Taken[:\s]*" + TIME_RE + r".*?$", "", s2, flags=re.I | re.DOTALL).strip()

    # Remove any standalone time patterns (HH:MM:SS)
    s2 = re.sub(r"\s+" + TIME_RE + r"(?:\s|$)", " ", s2, flags=re.I).strip()

    # Remove any remaining broadcast keywords with following content
    s2 = re.sub(r"(?:Exchange|Time\s*Taken).*?$", "", s2, flags=re.I).strip()

    return s2


def _clean_subject(s: str) -> str:
    if not s:
        return s

    # First pass - remove broadcast segments
    s2 = _strip_broadcast_bits(s)

    # Remove trailing datetimes
    s2 = re.sub(r"\s+" + DT_RE + r"\s*$", "", s2).strip()

    # Remove any remaining time patterns at the end
    s2 = re.sub(r"\s+" + TIME_RE + r"\s*$", "", s2).strip()

    # Final cleanup - remove any text that looks like broadcast labels
    s2 = re.sub(r"(?:Exchange\s*(?:Received|Dissemination)\s*Time|Time\s*Taken).*?$", "", s2, flags=re.I).strip()

    # Reject if what remains is just placeholder text
    if s2.strip().lower() in {"data", "details", "time", "taken", "exchange"}:
        return ""

    # Reject if it's just numbers and colons (likely a time)
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

        # Additional check: skip if text contains time patterns
        if re.search(TIME_RE, t) and len(t.strip()) < 50:  # Short text with time pattern is likely broadcast
            continue

        tt = _clean_subject(t)
        if not tt:
            continue
        if len(tt) > best_len:
            best_idx, best_val, best_len = i, tt, len(tt)
    return best_idx, best_val


# ---------- extraction ----------
def _extract_table_html(driver, max_rows: int, debug: bool = False) -> pd.DataFrame:
    soup = BeautifulSoup(driver.page_source, "lxml")
    colmap = _build_header_map(soup)
    if debug:
        print("Detected headers:", colmap)

    rows = soup.select("#CFanncEquityTable tbody tr")
    out = []

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

        # final values (with enhanced safety)
        symbol = texts[idx_symbol] if _in_range(idx_symbol, tds) and _looks_like_symbol(texts[idx_symbol]) else ""
        company_name = texts[idx_company] if _in_range(idx_company, tds) and _looks_like_company(texts[idx_company]) else ""

        # Enhanced subject cleaning with multiple passes
        subject = _clean_subject(subj_text or "")
        subject = _strip_broadcast_bits(subject)  # Double-check

        # Final validation: if subject still contains broadcast-like content, clear it
        if subject and (_is_broadcast_text(subject) or re.search(r"Time\s*Taken", subject, re.I)):
            if debug:
                print(f"WARNING: Clearing subject with broadcast content: {subject}")
            subject = ""

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
            # parse only the attachment cell
            for u in _scan_td_for_links(tds[idx_attach]):
                lu = u.lower()
                if ".pdf" in lu:
                    pdf_links.append(u)
                elif any(x in lu for x in [".xml", ".xbrl", "xbrl", ".zip"]):
                    xbrl_links.append(u)
        else:
            # fallback: scan all tds
            for td in tds:
                for u in _scan_td_for_links(td):
                    lu = u.lower()
                    if ".pdf" in lu:
                        pdf_links.append(u)
                    elif any(x in lu for x in [".xml", ".xbrl", "xbrl", ".zip"]):
                        xbrl_links.append(u)

        # de-dup while keeping order
        def _dedup(seq):
            seen = set()
            outl = []
            for x in seq:
                if x not in seen:
                    seen.add(x)
                    outl.append(x)
            return outl

        pdf_links = _dedup(pdf_links)
        xbrl_links = _dedup(xbrl_links)

        # Optional size text shown in attachment cell (keep as-is for PDF column)
        size_text = None
        if _in_range(idx_attach, tds):
            size_text = re.sub(r"\s*PDF\s*", "", texts[idx_attach], flags=re.I).strip() or None

        out.append({
            "Symbol": symbol,
            "Company Name": company_name,
            "Subject": subject,
            "Exchange Received Time": rec,
            "Exchange Dissemination Time": dis,
            "Time Taken": taken,
            "Attachment Size": size_text,                  # PDF size-ish text if present
            "Attachment Link": " | ".join(pdf_links) if pdf_links else "",   # PDF(s)
            "XBRL Link": " | ".join(xbrl_links) if xbrl_links else "",       # XBRL(s)
            "Has XBRL": bool(xbrl_links),
        })

    return pd.DataFrame(out)


def _save_excel(df: pd.DataFrame, out_path: str):
    parent = os.path.dirname(out_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Announcements")
        ws = writer.sheets["Announcements"]

        # make clickable hyperlinks for single-link cells
        if "Attachment Link" in df.columns:
            link_col = df.columns.get_loc("Attachment Link")
            for r, val in enumerate(df["Attachment Link"]):
                if isinstance(val, str) and val.startswith("http") and " | " not in val:
                    ws.write_url(r + 1, link_col, val, string="Open PDF")

        if "XBRL Link" in df.columns:
            xcol = df.columns.get_loc("XBRL Link")
            for r, val in enumerate(df["XBRL Link"]):
                if isinstance(val, str) and val.startswith("http") and " | " not in val:
                    ws.write_url(r + 1, xcol, val, string="Open XBRL")

        # tidy column widths
        def _set_if(col_idx, width):
            try:
                ws.set_column(col_idx, col_idx, width)
            except Exception:
                pass

        header_idx = {name: i for i, name in enumerate(df.columns)}
        _set_if(header_idx.get("Symbol", 0), 16)
        _set_if(header_idx.get("Company Name", 1), 40)
        _set_if(header_idx.get("Subject", 2), 60)
        _set_if(header_idx.get("Exchange Received Time", 3), 22)
        _set_if(header_idx.get("Exchange Dissemination Time", 4), 24)
        _set_if(header_idx.get("Time Taken", 5), 12)
        _set_if(header_idx.get("Attachment Size", 6), 14)
        _set_if(header_idx.get("Attachment Link", 7), 50)
        _set_if(header_idx.get("XBRL Link", 8), 50)
        _set_if(header_idx.get("Has XBRL", 9), 10)


# ---------- command ----------
class Command(BaseCommand):
    help = "Scrape NSE announcements and save to Excel (clean Company & Subject; split times; include XBRL)."

    def add_arguments(self, parser):
        parser.add_argument("--max-rows", type=int, default=100)
        parser.add_argument("--headless", action="store_true", default=False)
        parser.add_argument("--pause", type=float, default=1.2)
        parser.add_argument("--stall", type=int, default=4)
        parser.add_argument("--xlsx", type=str, default="outputs/NSE_Announcements.xlsx")
        parser.add_argument("--debug", action="store_true")

    def handle(self, *args, **opts):
        max_rows = int(opts["max_rows"])
        headless = bool(opts["headless"])
        pause = float(opts["pause"])
        stall = int(opts["stall"])
        xlsx = (opts.get("xlsx") or "outputs/NSE_Announcements.xlsx").strip()
        debug = bool(opts.get("debug"))

        self.stdout.write(f"→ Launching Chrome (headless={headless})")
        driver = None
        try:
            driver = _setup_driver(headless=headless)
        except Exception as e:
            raise CommandError(f"Failed to launch Chrome/Driver: {e}")

        try:
            self.stdout.write("→ Opening NSE homepage (cookie preflight)")
            driver.get(HOMEPAGE)
            WebDriverWait(driver, 20).until(lambda d: d.execute_script("return document.readyState") == "complete")

            self.stdout.write("→ Opening announcements page")
            driver.get(URL)

            _wait_table(driver, 30)
            _load_enough_rows(driver, max_rows=max_rows, pause=pause, stall_tolerance=stall)
            df = _extract_table_html(driver, max_rows=max_rows, debug=debug)

        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

        self.stdout.write(f"→ Saving Excel to {xlsx}")
        _save_excel(df, xlsx)
        self.stdout.write(self.style.SUCCESS(f"Saved {len(df)} rows to {xlsx}"))
