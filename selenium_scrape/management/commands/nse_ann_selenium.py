# -*- coding: utf-8 -*-
"""
NSE Corporate Announcements Scraper with API (primary) and Selenium (fallback)

Features:
- API-first approach with proper session handling
- Automatic Selenium fallback if API fails
- Downloads PDFs and uploads to Cloudflare R2
- Parses XBRL data when available
- Saves to NseAnnouncement database model
"""

import os, re, time, io, zipfile, json, hashlib
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.conf import settings
import xml.etree.ElementTree as ET

# Try to import brotli for decompression
try:
    import brotli
    HAS_BROTLI = True
except ImportError:
    HAS_BROTLI = False

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Boto3 for R2
import boto3
from botocore.config import Config

try:
    from webdriver_manager.chrome import ChromeDriverManager
except Exception:
    ChromeDriverManager = None

# Model import
try:
    from selenium_scrape.models import NseAnnouncement
except Exception:
    from selenium_scrape.models import NseAnnouncement

# Constants
NSE_URL = "https://www.nseindia.com/companies-listing/corporate-filings-announcements"
NSE_API_URL = "https://www.nseindia.com/api/corporate-announcements"
NSE_HOMEPAGE = "https://www.nseindia.com"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/128.0.0.0 Safari/537.36"
)

# ---------- R2 Configuration ----------
def _get_r2_client():
    """Initialize and return Cloudflare R2 client."""
    return boto3.client(
        's3',
        endpoint_url=os.getenv('R2_ENDPOINT'),
        aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
        config=Config(signature_version='s3v4'),
        region_name='auto'
    )

def _generate_pdf_key(symbol, subject, date_str, original_filename=None):
    """Generate a unique R2 key for the PDF file."""
    # Create a hash from symbol, subject, and date for uniqueness
    content_hash = hashlib.md5(f"{symbol}_{subject}_{date_str}".encode()).hexdigest()[:8]
    
    # Clean up filename components
    clean_symbol = re.sub(r'[^a-zA-Z0-9]', '_', symbol or 'unknown')
    clean_date = re.sub(r'[^a-zA-Z0-9]', '_', date_str or 'nodate')
    
    # Use original filename if available, otherwise create generic name
    if original_filename:
        filename = Path(original_filename).stem
        extension = Path(original_filename).suffix or '.pdf'
    else:
        filename = f"announcement_{content_hash}"
        extension = '.pdf'
    
    # Construct the key: nse/{symbol}/{year}/{filename}_{hash}.pdf
    year = clean_date[:4] if len(clean_date) >= 4 else 'unknown'
    r2_key = f"nse/{clean_symbol}/{year}/{filename}_{content_hash}{extension}"
    
    return r2_key

def _upload_pdf_to_r2(pdf_url: str, r2_key: str, session: requests.Session) -> Optional[str]:
    """Download PDF and upload to R2 storage."""
    try:
        r2_client = _get_r2_client()
        
        # Download PDF
        pdf_response = session.get(pdf_url, timeout=30, stream=True)
        pdf_response.raise_for_status()
        
        # Upload to R2
        r2_client.upload_fileobj(
            Fileobj=io.BytesIO(pdf_response.content),
            Bucket=os.getenv('R2_BUCKET'),
            Key=r2_key,
            ExtraArgs={'ContentType': 'application/pdf'}
        )
        
        # Return public URL
        r2_public_url = f"{os.getenv('R2_PUBLIC_BASEURL')}/{r2_key}"
        return r2_public_url
        
    except Exception as e:
        print(f"Error uploading PDF to R2: {e}")
        return None

# ---------- API Helper Functions ----------
def get_nse_api_headers() -> Dict[str, str]:
    """Get headers for NSE API requests"""
    return {
        'User-Agent': UA,
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Connection': 'keep-alive',
        'Referer': NSE_URL,
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

def parse_nse_datetime(dt_str: str) -> Tuple[str, str]:
    """Parse NSE datetime string into separate date and time"""
    try:
        if not dt_str:
            return "", ""
        
        # Format: "05-Sep-2025 16:01:50"
        dt = datetime.strptime(dt_str, "%d-%b-%Y %H:%M:%S")
        date_str = dt.strftime("%d-%b-%Y")
        time_str = dt.strftime("%H:%M:%S")
        return date_str, time_str
    except Exception:
        return "", ""

def scrape_nse_api(max_rows: int = 100, upload_pdfs: bool = True, debug: bool = False) -> pd.DataFrame:
    """Scrape NSE announcements using API (primary method)"""
    
    if debug:
        print(f"Attempting NSE API scrape (max_rows: {max_rows})")
    
    # Create session for cookie persistence
    session = requests.Session()
    session.headers.update(get_nse_api_headers())
    
    records: List[Dict] = []
    
    try:
        # Step 1: Visit homepage to establish session
        if debug:
            print("Establishing session with NSE...")
        session.get(NSE_HOMEPAGE, timeout=30)
        time.sleep(2)
        
        # Step 2: Visit announcements page
        session.get(NSE_URL, timeout=30)
        time.sleep(1)
        
        # Step 3: Make API request
        if debug:
            print("Making API request...")
        
        params = {'index': 'equities'}
        # Ensure proper compression handling
        session.headers.update({'Accept-Encoding': 'gzip, deflate, br'})
        response = session.get(NSE_API_URL, params=params, timeout=30)
        
        if response.status_code != 200:
            if debug:
                print(f"ERROR: API request failed with status {response.status_code}")
            return pd.DataFrame()
        
        # Handle compressed response
        try:
            if debug:
                print(f"Response length: {len(response.content)} bytes")
                print(f"Content-Encoding: {response.headers.get('Content-Encoding', 'none')}")
            
            # Handle Brotli compression manually if requests doesn't do it
            content_encoding = response.headers.get('Content-Encoding', '').lower()
            if content_encoding == 'br' and not response.text:
                if HAS_BROTLI:
                    if debug:
                        print("Manual Brotli decompression...")
                    decompressed = brotli.decompress(response.content)
                    json_data = json.loads(decompressed.decode('utf-8'))
                else:
                    if debug:
                        print("ERROR: Brotli compression detected but brotli package not available")
                    return pd.DataFrame()
            else:
                json_data = response.json()
                
        except Exception as e:
            if debug:
                print(f"ERROR: Failed to parse JSON response: {e}")
                print(f"Response encoding: {response.encoding}")
                print(f"Content-Type: {response.headers.get('content-type', 'unknown')}")
                # Don't print full headers to avoid Unicode issues
                print(f"Content-Encoding: {response.headers.get('Content-Encoding', 'none')}")
            return pd.DataFrame()
        
        if not isinstance(json_data, list):
            if debug:
                print(f"ERROR: Unexpected API response structure: {type(json_data)}")
                if hasattr(json_data, 'keys'):
                    print(f"Available keys: {list(json_data.keys())}")
            return pd.DataFrame()
        
        announcements = json_data[:max_rows]
        if debug:
            print(f"SUCCESS: API returned {len(announcements)} announcements")
        
        for i, ann in enumerate(announcements):
            try:
                # Field mapping based on your specifications
                symbol = ann.get('symbol', '')
                company_name = ann.get('sm_name', '')  # sm_name → company_name
                subject = ann.get('desc', '')  # desc → subject
                details = ann.get('attchmntText', '')  # attchmntText → details
                
                # Additional fields
                isin = ann.get('sm_isin', '')
                industry = ann.get('smIndustry', '')
                file_size = ann.get('fileSize', '')
                pdf_url = ann.get('attchmntFile', '')
                has_xbrl = ann.get('hasXbrl', False)
                seq_id = ann.get('seq_id', '')
                
                # Parse datetime fields
                an_dt = ann.get('an_dt', '')  # Announcement datetime
                exchdiss_dt = ann.get('exchdisstime', '')  # Exchange dissemination time
                
                # Convert to separate date and time
                received_date, received_time = parse_nse_datetime(an_dt)
                disseminated_date, disseminated_time = parse_nse_datetime(exchdiss_dt)
                
                if debug:
                    print(f"Record {i+1}: {symbol} - {subject[:50]}...")
                
                # Check for duplicates
                unique_key = {
                    "symbol": symbol,
                    "subject": subject,
                    "exchange_disseminated_date": disseminated_date,
                    "exchange_disseminated_time_only": disseminated_time,
                }
                
                if NseAnnouncement.objects.filter(**unique_key).exists():
                    if debug:
                        print(f"   Duplicate record, skipping...")
                    continue
                
                # Upload PDF to R2 if available
                pdf_path_cloud = ""
                pdf_r2_path = ""
                if pdf_url and upload_pdfs:
                    try:
                        r2_key = _generate_pdf_key(symbol, subject, disseminated_date)
                        pdf_path_cloud = _upload_pdf_to_r2(pdf_url, r2_key, session)
                        if pdf_path_cloud:
                            pdf_r2_path = r2_key
                            if debug:
                                print(f"   PDF uploaded: {pdf_path_cloud}")
                    except Exception as e:
                        if debug:
                            print(f"   ERROR: PDF upload failed: {e}")
                
                records.append({
                    "Symbol": symbol,
                    "Company Name": company_name,
                    "Subject": subject,
                    "Details": details,
                    "ISIN": isin,
                    "Industry": industry,
                    "File Size": file_size,
                    "Sequence ID": seq_id,
                    
                    # Datetime fields
                    "Exchange Received Date": received_date,
                    "Exchange Received Time Only": received_time,
                    "Exchange Disseminated Date": disseminated_date,
                    "Exchange Disseminated Time Only": disseminated_time,
                    
                    # PDF fields
                    "PDF Link Web": pdf_url,
                    "PDF Path Cloud": pdf_path_cloud,
                    "PDF R2 Path": pdf_r2_path,
                    "Attachment Size": file_size,
                    
                    # XBRL fields
                    "Has XBRL": has_xbrl,
                    "XBRL Parse Status": "API_SUCCESS"
                })
                
            except Exception as e:
                if debug:
                    print(f"ERROR: Error processing announcement {i+1}: {e}")
                continue
        
        if debug:
            print(f"SUCCESS: API scraping completed: {len(records)} records")
        return pd.DataFrame(records)
        
    except Exception as e:
        if debug:
            print(f"ERROR: API error: {e}")
        return pd.DataFrame()

# ---------- Selenium Functions (Fallback) ----------
def _setup_driver(headless: bool = True):
    """Setup Chrome driver with optimized options"""
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_argument(f"--user-agent={UA}")

    if ChromeDriverManager:
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=opts)
    else:
        return webdriver.Chrome(options=opts)

def _wait_table(driver, timeout=30):
    """Wait for the NSE announcements table to load"""
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "#CFanncEquityTable tbody tr"))
    )

def _load_enough_rows(driver, max_rows: int, pause: float = 1.2, stall_tolerance: int = 4):
    """Scroll to load enough rows in the NSE table"""
    stall_count = 0
    prev_count = 0
    
    while True:
        # Scroll down
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)
        
        # Count current rows
        rows = driver.find_elements(By.CSS_SELECTOR, "#CFanncEquityTable tbody tr")
        current_count = len(rows)
        
        if current_count >= max_rows:
            break
        
        if current_count == prev_count:
            stall_count += 1
            if stall_count >= stall_tolerance:
                break
        else:
            stall_count = 0
        
        prev_count = current_count

def scrape_nse_selenium(
    max_rows: int = 100,
    headless: bool = True,
    pause: float = 1.2,
    stall: int = 4,
    upload_pdfs: bool = True,
    debug: bool = False
) -> pd.DataFrame:
    """Selenium-based scraping as fallback method"""
    
    if debug:
        print(f"Using Selenium fallback (max_rows: {max_rows})")
    
    try:
        driver = _setup_driver(headless=headless)
    except Exception as e:
        if debug:
            print(f"ERROR: Failed to launch Chrome/Driver: {e}")
        return pd.DataFrame()

    records: List[Dict] = []
    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Referer": NSE_URL})

    try:
        if debug:
            print("Opening NSE homepage")
        driver.get(NSE_HOMEPAGE)
        WebDriverWait(driver, 20).until(lambda d: d.execute_script("return document.readyState") == "complete")

        if debug:
            print("Opening announcements page")
        driver.get(NSE_URL)
        _wait_table(driver, 30)

        # Copy cookies from driver to requests session
        for cookie in driver.get_cookies():
            session.cookies.set(cookie['name'], cookie['value'])

        _load_enough_rows(driver, max_rows=max_rows, pause=pause, stall_tolerance=stall)
        
        soup = BeautifulSoup(driver.page_source, "lxml")
        rows = soup.select("#CFanncEquityTable tbody tr")[:max_rows]
        
        if debug:
            print(f"Processing {len(rows)} rows")

        for i, row in enumerate(rows):
            try:
                cells = row.find_all("td")
                if len(cells) < 5:
                    continue

                # Extract basic fields (simplified extraction)
                symbol = cells[0].get_text(strip=True) if len(cells) > 0 else ""
                company_name = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                subject = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                
                # Look for PDF links
                pdf_links = []
                for cell in cells:
                    links = cell.find_all("a", href=True)
                    for link in links:
                        href = link.get("href", "")
                        if ".pdf" in href.lower():
                            pdf_links.append(urljoin(NSE_URL, href))
                
                pdf_url = pdf_links[0] if pdf_links else ""
                
                # Simple datetime extraction (fallback approach)
                datetime_text = " ".join([cell.get_text(strip=True) for cell in cells[-2:]])
                
                if debug:
                    print(f"Selenium Record {i+1}: {symbol} - {subject[:50]}...")
                
                # Upload PDF if available
                pdf_path_cloud = ""
                pdf_r2_path = ""
                if pdf_url and upload_pdfs:
                    try:
                        r2_key = _generate_pdf_key(symbol, subject, datetime.now().strftime("%Y%m%d"))
                        pdf_path_cloud = _upload_pdf_to_r2(pdf_url, r2_key, session)
                        if pdf_path_cloud:
                            pdf_r2_path = r2_key
                    except Exception as e:
                        if debug:
                            print(f"   ERROR: PDF upload failed: {e}")

                records.append({
                    "Symbol": symbol,
                    "Company Name": company_name,
                    "Subject": subject,
                    "Details": subject,  # Fallback: use subject as details
                    "ISIN": "",
                    "Industry": "",
                    "File Size": "",
                    "Sequence ID": "",
                    
                    # Datetime fields (simplified)
                    "Exchange Received Date": datetime.now().strftime("%d-%b-%Y"),
                    "Exchange Received Time Only": datetime.now().strftime("%H:%M:%S"),
                    "Exchange Disseminated Date": datetime.now().strftime("%d-%b-%Y"),
                    "Exchange Disseminated Time Only": datetime.now().strftime("%H:%M:%S"),
                    
                    # PDF fields
                    "PDF Link Web": pdf_url,
                    "PDF Path Cloud": pdf_path_cloud,
                    "PDF R2 Path": pdf_r2_path,
                    "Attachment Size": "",
                    
                    # XBRL fields
                    "Has XBRL": False,
                    "XBRL Parse Status": "SELENIUM_SUCCESS"
                })

            except Exception as e:
                if debug:
                    print(f"ERROR: Error parsing row {i+1}: {e}")
                continue

    except Exception as e:
        if debug:
            print(f"ERROR: Fatal error in Selenium scraper: {e}")
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    if debug:
        print(f"SUCCESS: Selenium scraping completed: {len(records)} records")
    return pd.DataFrame(records)

# ---------- Django Command ----------
class Command(BaseCommand):
    help = "NSE Corporate Announcements Scraper with API (primary) and Selenium (fallback)"

    def add_arguments(self, parser):
        parser.add_argument("--max-rows", type=int, default=100, help="Maximum rows to scrape")
        parser.add_argument("--headless", action="store_true", default=False, help="Run browser in headless mode")
        parser.add_argument("--pause", type=float, default=1.2, help="Pause between scroll actions")
        parser.add_argument("--stall", type=int, default=4, help="Stall tolerance for scrolling")
        parser.add_argument("--debug", action="store_true", help="Enable debug output")
        parser.add_argument("--xbrl-parse", dest="xbrl_parse", action="store_true", default=True, help="Parse XBRL data")
        parser.add_argument("--no-xbrl-parse", dest="xbrl_parse", action="store_false", help="Skip XBRL parsing")
        parser.add_argument("--upload-pdfs", dest="upload_pdfs", action="store_true", default=True, help="Upload PDFs to R2")
        parser.add_argument("--no-upload-pdfs", dest="upload_pdfs", action="store_false", help="Skip PDF upload")
        parser.add_argument("--force-selenium", action="store_true", help="Skip API and use Selenium directly")

    def handle(self, *args, **options):
        max_rows = options["max_rows"]
        headless = options["headless"]
        pause = options["pause"]
        stall = options["stall"]
        debug = options["debug"]
        xbrl_parse = options["xbrl_parse"]
        upload_pdfs = options["upload_pdfs"]
        force_selenium = options["force_selenium"]
        
        self.stdout.write(
            self.style.SUCCESS(
                f"Starting NSE Announcements Scraper (max_rows: {max_rows})"
            )
        )

        # Check R2 configuration if PDF upload is enabled
        if upload_pdfs:
            required_env = ['R2_ENDPOINT', 'R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY', 'R2_BUCKET']
            missing_env = [var for var in required_env if not os.getenv(var)]
            if missing_env:
                raise CommandError(f"Missing required R2 environment variables: {', '.join(missing_env)}")

        df = pd.DataFrame()
        
        # Try API first (unless forced to use Selenium)
        if not force_selenium:
            self.stdout.write(self.style.NOTICE("Attempting API scraping..."))
            df = scrape_nse_api(
                max_rows=max_rows,
                upload_pdfs=upload_pdfs,
                debug=debug
            )
            
            if not df.empty:
                self.stdout.write(self.style.SUCCESS(f"API scraping successful: {len(df)} records"))
            else:
                self.stdout.write(self.style.WARNING("API failed, switching to Selenium..."))
        
        # Fallback to Selenium if API failed or if forced
        if df.empty:
            self.stdout.write(self.style.NOTICE("Using Selenium scraping..."))
            df = scrape_nse_selenium(
                max_rows=max_rows,
                headless=headless,
                pause=pause,
                stall=stall,
                upload_pdfs=upload_pdfs,
                debug=debug
            )
            
            if not df.empty:
                self.stdout.write(self.style.SUCCESS(f"Selenium scraping successful: {len(df)} records"))

        if df.empty:
            self.stdout.write(self.style.WARNING("No announcements scraped"))
            return

        # Display sample data
        self.stdout.write("\n" + "="*100)
        self.stdout.write(self.style.SUCCESS("DATA SAMPLE (First 3 records):"))
        self.stdout.write("="*100)
        
        for i, row in df.head(3).iterrows():
            self.stdout.write(f"\nRecord {i+1}:")
            self.stdout.write(f"   Symbol: {row['Symbol']}")
            self.stdout.write(f"   Company: {row['Company Name']}")
            self.stdout.write(f"   Subject: {row['Subject'][:80]}...")
            self.stdout.write(f"   Date: {row['Exchange Disseminated Date']} {row['Exchange Disseminated Time Only']}")
            self.stdout.write(f"   Method: {row.get('XBRL Parse Status', 'UNKNOWN')}")
            self.stdout.write("   " + "-"*80)

        # Save to Database
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
            # Create unique key using separate date and time fields
            unique_key = {
                "symbol": _none_if_blank(row.get("Symbol")),
                "subject": _none_if_blank(row.get("Subject")),
                "exchange_disseminated_date": _none_if_blank(row.get("Exchange Disseminated Date")),
                "exchange_disseminated_time_only": _none_if_blank(row.get("Exchange Disseminated Time Only")),
            }
            
            defaults = {
                "company_name": _none_if_blank(row.get("Company Name")),
                "details": _none_if_blank(row.get("Details")),
                
                # Date/time fields
                "exchange_received_date": _none_if_blank(row.get("Exchange Received Date")),
                "exchange_received_time_only": _none_if_blank(row.get("Exchange Received Time Only")),
                
                "attachment_size": _none_if_blank(row.get("Attachment Size")),
                "attachment_link": _none_if_blank(row.get("PDF Link Web")),
                
                # PDF storage fields
                "pdf_link_web": _none_if_blank(row.get("PDF Link Web")),
                "pdf_path_local": _none_if_blank(row.get("PDF Path Local")),
                "pdf_path_cloud": _none_if_blank(row.get("PDF Path Cloud")),
                "pdf_r2_path": _none_if_blank(row.get("PDF R2 Path")),
                
                "has_xbrl": bool(row.get("Has XBRL", False)),
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

        self.stdout.write("\n" + "="*100)
        self.stdout.write(
            self.style.SUCCESS(
                f"NSE SCRAPING COMPLETED!\n"
                f"   📥 {count_new} new records inserted\n"
                f"   {count_existing} duplicates updated\n"
                f"   Total processed: {len(df)} records"
            )
        )
        self.stdout.write("="*100)