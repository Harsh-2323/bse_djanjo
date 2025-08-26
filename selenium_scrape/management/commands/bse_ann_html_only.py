import os
import re
import time
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urljoin

# --- Django bootstrap ---
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "bse_api.settings")
import django
django.setup()

import pandas as pd
import requests
from bs4 import BeautifulSoup

from django.core.management.base import BaseCommand
from django.db import transaction

from selenium_scrape.models import SeleniumAnnouncement

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ----------------------------------------------------
# Config (defaults; can be overridden via CLI options)
# ----------------------------------------------------
BSE_URL = "https://www.bseindia.com/corporates/ann.html"

DEFAULT_MAX_ENTRIES = 10
DEFAULT_DOWNLOAD_PDFS = True
DEFAULT_PDF_DIR = "pdfs"
DEFAULT_OUTPUT_XLSX = "outputs/BSE_Announcements_Output.xlsx"


# -----------------------------
# Selenium setup
# -----------------------------
def setup_driver(headless: bool = True, download_dir: Optional[str] = None):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2,
        "plugins.always_open_pdf_externally": True,
    }
    if download_dir:
        prefs.update({
            "download.default_directory": str(Path(download_dir).absolute()),
            "download.prompt_for_download": False,
        })
    opts.add_experimental_option("prefs", prefs)

    return webdriver.Chrome(options=opts)


# -----------------------------
# Helpers
# -----------------------------
def safe_filename(name: str, max_len: int = 150) -> str:
    name = re.sub(r'[\\/*?:"<>|]', "_", name or "")
    name = re.sub(r"\s+", " ", name).strip()
    return name[:max_len] or "announcement"


def download_pdf(pdf_url: str, outfile_path: str, timeout: int = 30) -> bool:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": BSE_URL,
            "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
        }
        with requests.get(pdf_url, headers=headers, timeout=timeout, stream=True, allow_redirects=True) as r:
            if not r.ok:
                return False
            ctype = (r.headers.get("Content-Type") or "").lower()
            if "pdf" not in ctype and not pdf_url.lower().endswith(".pdf"):
                return False

            Path(outfile_path).parent.mkdir(parents=True, exist_ok=True)
            with open(outfile_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        return True
    except Exception:
        return False


def _extract_category_from_table(table) -> str:
    """
    Attempt to read the 'Category' shown on the right of the first row
    of each announcement table (e.g., 'Company Update').
    We take the last non-empty cell text of the first data row,
    skipping file-size tokens and the literal 'XBRL'.
    """
    try:
        rows = table.find_all("tr")
        if not rows:
            return ""
        tds = rows[0].find_all("td")
        for td in reversed(tds):
            txt = (td.get_text(" ", strip=True) or "").strip()
            if not txt:
                continue
            # skip sizes like "0.67 MB" and the literal XBRL
            if re.search(r"\b\d+(\.\d+)?\s*(KB|MB)\b", txt, flags=re.I):
                continue
            if txt.upper() == "XBRL":
                continue
            return txt
    except Exception:
        pass
    return ""


# -----------------------------
# Core scrape
# -----------------------------
def scrape_bse_announcements_like_reference(
    max_entries: int,
    download_pdfs: bool,
    pdf_dir: str,
    headless: bool = True,
) -> pd.DataFrame:
    driver = setup_driver(headless=headless, download_dir=pdf_dir if download_pdfs else None)
    try:
        driver.get(BSE_URL)

        # Wait for the Angular tables to appear
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table[ng-repeat='cann in CorpannData.Table']"))
        )

        # Keep scrolling until enough announcements are loaded
        last_count = 0
        for _ in range(20):  # max 20 scrolls safeguard
            tables_now = driver.find_elements(By.CSS_SELECTOR, "table[ng-repeat='cann in CorpannData.Table']")
            if len(tables_now) >= max_entries:
                break
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.5)
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table[ng-repeat='cann in CorpannData.Table']"))
            )
            if len(tables_now) == last_count:
                break
            last_count = len(tables_now)

        soup = BeautifulSoup(driver.page_source, "lxml")
        tables = soup.find_all("table", {"ng-repeat": "cann in CorpannData.Table"})
    finally:
        driver.quit()

    records: List[Dict] = []
    for table in tables:
        if len(records) >= max_entries:
            break
        try:
            newssub_tag = table.find("span", {"ng-bind-html": "cann.NEWSSUB"})
            headline_tag = table.find("span", {"ng-bind-html": "cann.HEADLINE"})
            pdf_tag = table.find("a", class_="tablebluelink", href=True)

            newssub = (newssub_tag.get_text(strip=True) if newssub_tag else "") or ""
            headline = (headline_tag.get_text(strip=True) if headline_tag else "") or ""
            category = _extract_category_from_table(table)  # <-- NEW
            pdf_link = urljoin(BSE_URL, pdf_tag["href"]) if pdf_tag else ""

            # second-last row carries the timestamps on BSE
            all_rows = table.find_all("tr")
            time_row_text = all_rows[-2].get_text(strip=True) if len(all_rows) >= 2 else ""

            match_received = re.search(
                r"Exchange Received Time\s*(\d{2}-\d{2}-\d{4})\s*(\d{2}:\d{2}:\d{2})",
                time_row_text
            )
            match_disseminated = re.search(
                r"Exchange Disseminated Time\s*(\d{2}-\d{2}-\d{4})\s*(\d{2}:\d{2}:\d{2})",
                time_row_text
            )

            received_date = match_received.group(1) if match_received else ""
            received_time = match_received.group(2) if match_received else ""
            disseminated_date = match_disseminated.group(1) if match_disseminated else ""
            disseminated_time = match_disseminated.group(2) if match_disseminated else ""

            company_name = newssub.split("-")[0].strip() if newssub else ""
            code_match = re.search(r"\b(\d{6})\b", newssub)
            company_code = code_match.group(1) if code_match else ""

            local_pdf_path = ""
            if download_pdfs and pdf_link:
                code_for_name = company_code or (re.search(r"\d{6}", newssub).group() if re.search(r"\d{6}", newssub) else "NA")
                date_compact = received_date.replace("-", "") if received_date else "NA"
                base = f"{len(records)+1:03d}{code_for_name}{date_compact}_{safe_filename(headline)}.pdf"
                local_pdf_path = os.path.abspath(os.path.join(pdf_dir, base))
                if not download_pdf(pdf_link, local_pdf_path):
                    local_pdf_path = ""

            records.append({
                # NEW fields
                "Headline": headline,
                "Category": category,

                # Existing fields
                "Company Name": company_name,
                "Company Code": company_code,
                "Announcement Text": headline,  # keep legacy column the same
                "Exchange Received Date": received_date,
                "Exchange Received Time": received_time,
                "Exchange Disseminated Date": disseminated_date,
                "Exchange Disseminated Time": disseminated_time,
                "PDF Link (web)": pdf_link,
                "PDF Path (local)": local_pdf_path,
            })
        except Exception as e:
            print(f"Error parsing entry {len(records)+1}: {e}")

    return pd.DataFrame(records)


# -----------------------------
# Excel writer (with hyperlinks)
# -----------------------------
def save_to_excel_with_links(df: pd.DataFrame, out_path: str):
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="BSE")
        ws = writer.sheets["BSE"]

        cols = {name: idx for idx, name in enumerate(df.columns)}
        web_col = cols.get("PDF Link (web)")
        local_col = cols.get("PDF Path (local)")

        for r in range(len(df)):
            # Web link
            if web_col is not None:
                web = df.iat[r, web_col]
                if isinstance(web, str) and web.startswith("http"):
                    ws.write_url(r + 1, web_col, web, string="Open PDF (web)")

            # Local file link
            if local_col is not None:
                lp = df.iat[r, local_col]
                if isinstance(lp, str) and lp:
                    ws.write_url(r + 1, local_col, f"external:{lp}", string="Open PDF (local)")

        # Column widths
        def _setcol(c, w):
            if c is not None:
                ws.set_column(c, c, w)

        # give new columns sensible widths if present
        _setcol(cols.get("Headline"), 60)
        _setcol(cols.get("Category"), 28)

        ws.set_column(0, 0, 28)   # Company Name
        ws.set_column(1, 1, 12)   # Company Code
        ws.set_column(2, 2, 60)   # Announcement Text
        ws.set_column(3, 6, 20)   # Dates/Times
        _setcol(web_col, 22)
        _setcol(local_col, 26)


# -----------------------------
# Django management command
# -----------------------------
class Command(BaseCommand):
    help = "Scrape BSE Corporate Announcements and save into Postgres (deduplicated)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--max-pages", "--max-entries",
            dest="max_pages",
            type=int,
            default=1,
            help="Number of announcements to scrape"
        )
        parser.add_argument(
            "--debug",
            action="store_true",
            help="Enable debug output (show browser)"
        )

    def handle(self, *args, **options):
        self.stdout.write("🚀 Starting Enhanced BSE Announcements Scraper...")

        items = scrape_bse_announcements_like_reference(
            max_entries=options["max_pages"],
            download_pdfs=True,
            pdf_dir=DEFAULT_PDF_DIR,
            headless=not options["debug"]
        )

        if items.empty:
            self.stdout.write(self.style.WARNING("❌ No data scraped"))
            return

        # Optional: quick sanity print
        try:
            self.stdout.write("\nSample:\n" + items[["Headline","Category","Company Name"]].head(3).to_string(index=False))
        except Exception:
            pass

        count_new, count_existing = 0, 0

        for _, row in items.iterrows():
            unique_key = {
                "company_code": row.get("Company Code"),
                "announcement_text": row.get("Announcement Text"),
                "exchange_disseminated_date": row.get("Exchange Disseminated Date"),
                "exchange_disseminated_time": row.get("Exchange Disseminated Time"),
            }

            # Include new fields in defaults so they persist
            defaults = {
                "company_name": row.get("Company Name"),
                "pdf_link_web": row.get("PDF Link (web)"),
                "pdf_path_local": row.get("PDF Path (local)"),
                "exchange_received_date": row.get("Exchange Received Date"),
                "exchange_received_time": row.get("Exchange Received Time"),
                # NEW:
                "headline": row.get("Headline") or None,
                "category": row.get("Category") or None,
            }

            with transaction.atomic():
                obj, created = SeleniumAnnouncement.objects.update_or_create(
                    **unique_key,
                    defaults=defaults,
                )

            if created:
                count_new += 1
            else:
                count_existing += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"✅ {count_new} new records inserted, {count_existing} already existed (skipped)"
            )
        )
