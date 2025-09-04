from datetime import datetime
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from typing import List, Dict, Optional
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
import time
import os
import boto3
from botocore.client import Config
from django.core.management.base import BaseCommand
from django.db import transaction
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_scrape.models import SeleniumAnnouncement

# Constants
BSE_URL = "https://www.bseindia.com/corporates/ann.html"

# Cloudflare R2 Configuration (from environment variables)
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT")
R2_BUCKET_NAME = os.getenv("R2_BUCKET")
R2_PUBLIC_BASEURL = os.getenv("R2_PUBLIC_BASEURL")
R2_BASE_PATH = "bse_announcements"

# -----------------------------
# Selenium setup
# -----------------------------
def setup_driver(headless: bool = True):
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
    opts.add_experimental_option("prefs", prefs)

    return webdriver.Chrome(options=opts)

# -----------------------------
# Helpers
# -----------------------------
def safe_filename(name: str, max_len: int = 150) -> str:
    name = re.sub(r'[\\/*?:"<>|]', "_", name or "")
    name = re.sub(r"\s+", " ", name).strip()
    return name[:max_len] or "announcement"

def upload_pdf_to_r2(pdf_url: str, r2_path: str, timeout: int = 30) -> Optional[str]:
    """Fetch PDF from pdf_url and upload to Cloudflare R2, return the R2 public URL."""
    try:
        # Initialize R2 client
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
            endpoint_url=R2_ENDPOINT_URL,
            config=Config(signature_version="s3v4")
        )

        # Fetch PDF content
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": BSE_URL,
            "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
        }
        with requests.get(pdf_url, headers=headers, timeout=timeout, stream=True, allow_redirects=True) as r:
            if not r.ok:
                print(f"Failed to fetch PDF from {pdf_url}: Status {r.status_code}")
                return None
            ctype = (r.headers.get("Content-Type") or "").lower()
            if "pdf" not in ctype and not pdf_url.lower().endswith(".pdf"):
                print(f"Invalid content type for {pdf_url}: {ctype}")
                return None

            # Upload to R2
            s3_client.upload_fileobj(
                Fileobj=r.raw,
                Bucket=R2_BUCKET_NAME,
                Key=r2_path,
                ExtraArgs={"ContentType": "application/pdf"}
            )

            # Construct public URL
            r2_url = f"{R2_PUBLIC_BASEURL}/{r2_path}"
            print(f"Successfully uploaded {pdf_url} to {r2_url}")
            return r2_url
    except Exception as e:
        print(f"Error uploading PDF to R2 for {pdf_url}: {e}")
        return None

def _extract_category_from_table(table) -> str:
    try:
        rows = table.find_all("tr")
        if not rows:
            return ""
        tds = rows[0].find_all("td")
        for td in reversed(tds):
            txt = (td.get_text(" ", strip=True) or "").strip()
            if not txt:
                continue
            if re.search(r"\b\d+(\.\d+)?\s*(KB|MB)\b", txt, flags=re.I):
                continue
            if txt.upper() == "XBRL":
                continue
            return txt
    except Exception:
        pass
    return ""

def extract_announcement_fields_from_table(driver, table):
    """Extract headline and announcement text from the correct locations."""
    headline = ""
    announcement_text = ""
    
    try:
        # Extract headline using the CSS selector pattern
        headline_element = table.select_one("tr:nth-child(4) td table:nth-child(1) tr:nth-child(1) td:nth-child(1)")
        if headline_element:
            headline = headline_element.get_text(strip=True)
        
        # For announcement text, look for divs with UUID-like IDs
        announcement_divs = table.find_all("div", id=True)
        for div in announcement_divs:
            div_id = div.get("id", "")
            if re.match(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', div_id):
                announcement_text = div.get_text(strip=True)
                break
        
        # Fallback: try other potential containers
        if not announcement_text:
            for selector in [
                "div[id*='-']",
                ".announcement-text",
                "td[colspan]",
            ]:
                element = table.select_one(selector)
                if element:
                    text = element.get_text(strip=True)
                    if text and text != headline and len(text) > 10:
                        announcement_text = text
                        break
        
        # Fallback to original method
        if not announcement_text:
            headline_tag = table.find("span", {"ng-bind-html": "cann.HEADLINE"})
            if headline_tag:
                announcement_text = headline_tag.get_text(strip=True)
    
    except Exception as e:
        print(f"Error extracting announcement fields: {e}")
        try:
            headline_tag = table.find("span", {"ng-bind-html": "cann.HEADLINE"})
            if headline_tag:
                headline = headline_tag.get_text(strip=True)
                announcement_text = headline
        except Exception:
            pass
    
    return headline, announcement_text

# -----------------------------
# Core scrape
# -----------------------------
def scrape_bse_announcements_like_reference(
    target_date: str = "28-08-2025",
    headless: bool = True,
    limit: Optional[int] = None
) -> pd.DataFrame:
    driver = setup_driver(headless=headless)
    records: List[Dict] = []
    try:
        driver.get(BSE_URL)

        # Wait for the Angular tables to appear
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table[ng-repeat='cann in CorpannData.Table']"))
        )

        while True:
            # Parse current page
            soup = BeautifulSoup(driver.page_source, "lxml")
            tables = soup.find_all("table", {"ng-repeat": "cann in CorpannData.Table"})

            for table in tables:
                try:
                    newssub_tag = table.find("span", {"ng-bind-html": "cann.NEWSSUB"})
                    pdf_tag = table.find("a", class_="tablebluelink", href=True)

                    newssub = (newssub_tag.get_text(strip=True) if newssub_tag else "") or ""
                    
                    # Extract headline and announcement text
                    headline, announcement_text = extract_announcement_fields_from_table(driver, table)
                    
                    category = _extract_category_from_table(table)
                    pdf_link = urljoin(BSE_URL, pdf_tag["href"]) if pdf_tag else ""

                    # Extract timestamps
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

                    # Only process announcements for the target date
                    if disseminated_date != target_date:
                        continue

                    company_name = newssub.split("-")[0].strip() if newssub else ""
                    code_match = re.search(r"\b(\d{6})\b", newssub)
                    company_code = code_match.group(1) if code_match else ""

                    # Check for duplicate before fetching/uploading PDF
                    unique_key = {
                        "company_code": company_code,
                        "announcement_text": announcement_text,
                        "exchange_disseminated_date": disseminated_date,
                        "exchange_disseminated_time": disseminated_time,
                    }
                    if SeleniumAnnouncement.objects.filter(**unique_key).exists():
                        continue  # Skip duplicates to avoid fetching/uploading PDF

                    pdf_path_cloud = ""
                    pdf_r2_path = ""
                    if pdf_link:
                        code_for_name = company_code or (re.search(r"\d{6}", newssub).group() if re.search(r"\d{6}", newssub) else "NA")
                        date_compact = received_date.replace("-", "") if received_date else "NA"
                        r2_filename = f"{len(records)+1:03d}{code_for_name}{date_compact}_{safe_filename(headline)[:50]}.pdf"
                        pdf_r2_path = f"{R2_BASE_PATH}/{r2_filename}"
                        pdf_path_cloud = upload_pdf_to_r2(pdf_link, pdf_r2_path)
                        if not pdf_path_cloud:
                            print(f"Failed to upload PDF for {headline}")

                    records.append({
                        "Headline": headline,
                        "Category": category,
                        "Company Name": company_name,
                        "Company Code": company_code,
                        "Announcement Text": announcement_text,
                        "Exchange Received Date": received_date,
                        "Exchange Received Time": received_time,
                        "Exchange Disseminated Date": disseminated_date,
                        "Exchange Disseminated Time": disseminated_time,
                        "PDF Link (web)": pdf_link,
                        "PDF Path (cloud)": pdf_path_cloud,
                        "PDF R2 Path": pdf_r2_path,
                    })

                    # Stop if limit is reached
                    if limit and len(records) >= limit:
                        return pd.DataFrame(records)

                except Exception as e:
                    print(f"Error parsing entry {len(records)+1}: {e}")

            # Check for and click the "Next" button
            try:
                next_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "idnext"))
                )
                ActionChains(driver).move_to_element(next_button).click().perform()
                time.sleep(2)
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table[ng-repeat='cann in CorpannData.Table']"))
                )
            except (TimeoutException, NoSuchElementException):
                print("No more pages to scrape (Next button not found).")
                break

    finally:
        driver.quit()

    return pd.DataFrame(records)

# -----------------------------
# Django management command
# -----------------------------
class Command(BaseCommand):
    help = "Scrape BSE Corporate Announcements for a specific date and save into Postgres (deduplicated)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--date",
            type=str,
            default="28-08-2025",
            help="Target date for announcements (format: DD-MM-YYYY, e.g., 28-08-2025)",
        )
        parser.add_argument(
            "--debug",
            action="store_true",
            help="Enable debug output (show browser)"
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Limit the number of records to scrape (e.g., 10 for testing)",
        )

    def handle(self, *args, **options):
        target_date = options["date"]
        limit = options.get("limit")
        
        if limit:
            self.stdout.write(f"🚀 Starting Enhanced BSE Announcements Scraper for {target_date} (LIMITED to {limit} records)...")
        else:
            self.stdout.write(f"🚀 Starting Enhanced BSE Announcements Scraper for {target_date}...")

        # Validate date format
        try:
            datetime.strptime(target_date, "%d-%m-%Y")
        except ValueError:
            self.stdout.write(self.style.ERROR("❌ Invalid date format. Use DD-MM-YYYY (e.g., 28-08-2025)"))
            return

        items = scrape_bse_announcements_like_reference(
            target_date=target_date,
            headless=not options["debug"],
            limit=limit
        )

        if items.empty:
            self.stdout.write(self.style.WARNING("❌ No data scraped for the specified date"))
            return

        # Optional: quick sanity print
        try:
            self.stdout.write("\nSample:\n" + items[["Headline", "Category", "Company Name"]].head(3).to_string(index=False))
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

            # Skip if record already exists
            if SeleniumAnnouncement.objects.filter(**unique_key).exists():
                count_existing += 1
                continue

            # Create new record
            with transaction.atomic():
                SeleniumAnnouncement.objects.create(
                    company_name=row.get("Company Name"),
                    company_code=row.get("Company Code"),
                    headline=row.get("Headline") or None,
                    category=row.get("Category") or None,
                    announcement_text=row.get("Announcement Text"),
                    exchange_received_date=row.get("Exchange Received Date"),
                    exchange_received_time=row.get("Exchange Received Time"),
                    exchange_disseminated_date=row.get("Exchange Disseminated Date"),
                    exchange_disseminated_time=row.get("Exchange Disseminated Time"),
                    pdf_link_web=row.get("PDF Link (web)"),
                    pdf_path_cloud=row.get("PDF Path (cloud)"),
                    pdf_r2_path=row.get("PDF R2 Path"),
                )
                count_new += 1

            # Stop if limit is reached
            if limit and count_new >= limit:
                break

        self.stdout.write(
            self.style.SUCCESS(
                f"✅ {count_new} new records inserted, {count_existing} already existed (skipped)"
            )
        )