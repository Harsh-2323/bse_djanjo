from datetime import datetime, timedelta
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from typing import List, Dict, Optional, Tuple
import pandas as pd
import requests
import json
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
BSE_API_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"

# Cloudflare R2 Configuration
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT")
R2_BUCKET_NAME = os.getenv("R2_BUCKET")
R2_PUBLIC_BASEURL = os.getenv("R2_PUBLIC_BASEURL")
R2_BASE_PATH = "bse_announcements"

# =====================================================
# API HELPER FUNCTIONS
# =====================================================

def get_api_headers() -> Dict[str, str]:
    """Get browser-like headers for BSE API access"""
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://www.bseindia.com/',
        'Origin': 'https://www.bseindia.com',
        'Sec-Ch-Ua': '"Not A(Brand)";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache'
    }

def convert_date_format(date_str: str, from_format: str = "%d-%m-%Y", to_format: str = "%Y%m%d") -> str:
    """Convert date from DD-MM-YYYY to YYYYMMDD format for API"""
    try:
        date_obj = datetime.strptime(date_str, from_format)
        return date_obj.strftime(to_format)
    except ValueError:
        return date_str

def convert_api_datetime(api_datetime: str) -> Tuple[str, str]:
    """Convert API datetime to separate date and time strings"""
    try:
        if not api_datetime:
            return "", ""
        
        # API format: "2025-09-05T14:41:23.32"
        dt = datetime.fromisoformat(api_datetime.replace('T', ' ').split('.')[0])
        date_str = dt.strftime("%d-%m-%Y")  # DD-MM-YYYY
        time_str = dt.strftime("%H:%M:%S")  # HH:MM:SS
        return date_str, time_str
    except Exception:
        return "", ""

def format_attachment_size(size_bytes: int) -> str:
    """Format attachment size from bytes to readable format"""
    try:
        if not size_bytes:
            return ""
        
        # Convert bytes to appropriate unit
        if size_bytes >= 1024 * 1024:  # MB
            size_mb = size_bytes / (1024 * 1024)
            return f"{size_mb:.2f} MB"
        elif size_bytes >= 1024:  # KB
            size_kb = size_bytes / 1024
            return f"{size_kb:.2f} KB"
        else:
            return f"{size_bytes} B"
    except Exception:
        return ""

def get_pdf_url_from_attachment(attachment_name: str) -> str:
    """Generate PDF URL from attachment name"""
    if not attachment_name:
        return ""
    
    # BSE PDF URL pattern
    base_url = "https://www.bseindia.com/xml-data/corpfiling/AttachHis/"
    return f"{base_url}{attachment_name}"

# =====================================================
# SELENIUM HELPER FUNCTIONS
# =====================================================

def setup_driver(headless: bool = True):
    """Setup Chrome driver with optimized options"""
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

def clean_text(text: str) -> str:
    """Clean and normalize text"""
    if not text:
        return ""
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Remove common artifacts
    text = re.sub(r'Read less\.\.', '', text)
    text = re.sub(r'\.{3,}', '...', text)  # Normalize ellipsis
    
    return text

def extract_company_details(newssub: str) -> Tuple[str, str]:
    """Extract company name and code from NEWSSUB field"""
    if not newssub:
        return "", ""
    
    # Pattern: "Company Name - Code - Description"
    # Try to find company code (6 digits)
    code_match = re.search(r'\b(\d{6})\b', newssub)
    company_code = code_match.group(1) if code_match else ""
    
    # Extract company name (everything before first " - ")
    if " - " in newssub:
        company_name = newssub.split(" - ")[0].strip()
    else:
        # Fallback: remove code from end if present
        company_name = re.sub(r'\s*-?\s*\d{6}\s*-?.*$', '', newssub).strip()
    
    return company_name, company_code

def extract_attachment_size(table) -> str:
    """
    Extracts the attachment size (as rendered on the page) from an announcement table.
    Returns a string like '0.45 MB' or '460.80 KB'; empty string if not found.
    """
    try:
        text = table.get_text(" ", strip=True)
        matches = re.findall(r'(\d+(?:\.\d{1,2})?)\s*(MB|KB)\b', text, flags=re.I)
        if not matches:
            return ""
        sizes = [f"{val} {unit.upper()}" for (val, unit) in matches]
        mb = next((s for s in sizes if s.endswith("MB")), None)
        if mb:
            return mb
        kb = next((s for s in sizes if s.endswith("KB")), None)
        return kb or ""
    except Exception:
        return ""

def extract_announcement_data(table) -> Dict[str, str]:
    """Extract announcement data from HTML table for Selenium scraping"""
    data = {
        'headline': '',
        'announcement_text': '',
        'category': '',
        'company_name': '',
        'company_code': '',
        'attachment_size': ''
    }
    
    try:
        # Extract NEWSSUB (Company info)
        newssub_tag = table.find("span", {"ng-bind-html": "cann.NEWSSUB"})
        newssub = clean_text(newssub_tag.get_text(strip=True)) if newssub_tag else ""
        
        if newssub:
            data['company_name'], data['company_code'] = extract_company_details(newssub)
            data['headline'] = newssub  # Use NEWSSUB as headline
        
        # Extract announcement content from UUID div
        uuid_regex = re.compile(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$')
        announcement_div = table.find("div", id=uuid_regex)
        
        if announcement_div:
            announcement_content = clean_text(announcement_div.get_text(strip=True))
            if announcement_content and announcement_content != data['headline']:
                data['announcement_text'] = announcement_content
            else:
                data['announcement_text'] = data['headline']
        else:
            data['announcement_text'] = data['headline']
        
        # Extract CATEGORY
        try:
            category_td = table.select_one("td.tdcolumngrey.ng-binding.ng-scope[ng-if*=\"cann.CATEGORYNAME\"]")
            if category_td:
                data['category'] = category_td.get_text(strip=True)
            else:
                data['category'] = ""
        except Exception:
            data['category'] = ""

        # Extract attachment size
        try:
            data['attachment_size'] = extract_attachment_size(table)
        except Exception:
            data['attachment_size'] = ""
    
    except Exception as e:
        print(f"Error in extract_announcement_data: {e}")
        if not data['announcement_text'] and not data['headline']:
            data['announcement_text'] = "Error extracting announcement content"
            data['headline'] = "Error extracting headline"
            data['category'] = ""
    
    return data

# =====================================================
# COMMON FUNCTIONS
# =====================================================

def safe_filename(name: str, max_len: int = 150) -> str:
    """Create safe filename from text"""
    if not name:
        return "announcement"
    
    # Clean the name
    name = re.sub(r'[\\/*?:"<>|]', "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    
    # Truncate if too long
    if len(name) > max_len:
        name = name[:max_len].rstrip()
    
    return name or "announcement"

def upload_pdf_to_r2(pdf_url: str, r2_path: str, timeout: int = 30) -> Optional[str]:
    """Fetch PDF from pdf_url and upload to Cloudflare R2, return the R2 public URL."""
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
            endpoint_url=R2_ENDPOINT_URL,
            config=Config(signature_version="s3v4")
        )

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": BSE_URL,
            "Accept": "application/pdf,application/octet-stream;q=0.9,/;q=0.8",
        }
        with requests.get(pdf_url, headers=headers, timeout=timeout, stream=True, allow_redirects=True) as r:
            if not r.ok:
                print(f"Failed to fetch PDF from {pdf_url}: Status {r.status_code}")
                return None
            ctype = (r.headers.get("Content-Type") or "").lower()
            if "pdf" not in ctype and not pdf_url.lower().endswith(".pdf"):
                print(f"Invalid content type for {pdf_url}: {ctype}")
                return None

            s3_client.upload_fileobj(
                Fileobj=r.raw,
                Bucket=R2_BUCKET_NAME,
                Key=r2_path,
                ExtraArgs={"ContentType": "application/pdf"}
            )

            r2_url = f"{R2_PUBLIC_BASEURL}/{r2_path}"
            print(f"Successfully uploaded PDF to R2: {r2_url}")
            return r2_url
    except Exception as e:
        print(f"Error uploading PDF to R2: {e}")
        return None

# =====================================================
# MAIN SCRAPING FUNCTIONS
# =====================================================

def scrape_bse_api(
    target_date: str = "05-09-2025",
    limit: Optional[int] = None,
    timeout: int = 30
) -> pd.DataFrame:
    """Scrape BSE announcements using API (primary method)"""
    
    print(f"🌐 Attempting API scrape for date: {target_date}")
    
    # Convert date format for API (DD-MM-YYYY -> YYYYMMDD)
    api_date = convert_date_format(target_date)
    
    params = {
        'pageno': 1,
        'strCat': -1,
        'strPrevDate': api_date,
        'strScrip': '',
        'strSearch': 'P',
        'strToDate': api_date,
        'strType': 'C',
        'subcategory': -1
    }
    
    headers = get_api_headers()
    records: List[Dict] = []
    
    try:
        print(f"📡 Making API request...")
        response = requests.get(BSE_API_URL, params=params, headers=headers, timeout=timeout)
        
        if response.status_code != 200:
            print(f"❌ API request failed with status {response.status_code}")
            return pd.DataFrame()
        
        json_data = response.json()
        
        if not isinstance(json_data, dict) or 'Table' not in json_data:
            print(f"❌ Unexpected API response structure")
            return pd.DataFrame()
        
        announcements = json_data['Table']
        print(f"✅ API returned {len(announcements)} announcements")
        
        for i, announcement in enumerate(announcements):
            try:
                # Field mapping - YOUR EXACT SPECIFICATIONS
                company_name = announcement.get('SLONGNAME', '')  
                company_code = str(announcement.get('SCRIP_CD', ''))  
                headline = announcement.get('NEWSSUB', '')  # Using NEWSSUB for headline
                category = announcement.get('CATEGORYNAME', '')  
                announcement_text = announcement.get('HEADLINE', '')  
                
                # Time parsing
                news_dt = announcement.get('NEWS_DT', '')  
                dissem_dt = announcement.get('DissemDT', '')  
                
                # Convert API datetime to separate date and time
                received_date, received_time = convert_api_datetime(news_dt)
                disseminated_date, disseminated_time = convert_api_datetime(dissem_dt)
                
                # PDF and attachment handling
                attachment_name = announcement.get('ATTACHMENTNAME', '')
                attachment_size_bytes = announcement.get('Fld_Attachsize', 0)
                attachment_size = format_attachment_size(attachment_size_bytes)
                pdf_link = get_pdf_url_from_attachment(attachment_name) if attachment_name else ""
                
                print(f"📄 Record {i+1}: {company_name} ({company_code})")
                
                # Check for duplicates
                unique_key = {
                    "company_code": company_code,
                    "announcement_text": announcement_text,
                    "exchange_disseminated_date": disseminated_date,
                    "exchange_disseminated_time": disseminated_time,
                }
                
                if SeleniumAnnouncement.objects.filter(**unique_key).exists():
                    print(f"   ⚠️  Duplicate record, skipping...")
                    continue
                
                # Upload PDF to R2 cloud storage
                pdf_path_cloud = ""
                pdf_r2_path = ""
                if pdf_link:
                    try:
                        code_for_name = company_code or "UNKNOWN"
                        date_compact = api_date
                        safe_headline = safe_filename(headline or announcement_text)[:50]
                        r2_filename = f"{len(records)+1:03d}_{code_for_name}_{date_compact}_{safe_headline}.pdf"
                        pdf_r2_path = f"{R2_BASE_PATH}/{r2_filename}"
                        pdf_path_cloud = upload_pdf_to_r2(pdf_link, pdf_r2_path)
                    except Exception as e:
                        print(f"   ❌ PDF upload failed: {e}")
                
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
                    "Attachment Size": attachment_size,
                    "PDF Link (web)": pdf_link,
                    "PDF Path (cloud)": pdf_path_cloud,
                    "PDF R2 Path": pdf_r2_path,
                    # Additional fields for model
                    "XBRL Parse Status": "API_SUCCESS"
                })
                
                # Check limit
                if limit and len(records) >= limit:
                    print(f"🎯 Reached limit of {limit} records")
                    break
                    
            except Exception as e:
                print(f"❌ Error processing announcement {i+1}: {e}")
                continue
        
        print(f"✅ API scraping completed: {len(records)} records")
        return pd.DataFrame(records)
        
    except Exception as e:
        print(f"❌ API error: {e}")
        return pd.DataFrame()

def scrape_bse_selenium(
    target_date: str = "05-09-2025",
    headless: bool = True,
    limit: Optional[int] = None
) -> pd.DataFrame:
    """Selenium-based scraping as fallback method"""
    
    print(f"🔄 Using Selenium fallback for date: {target_date}")
    
    driver = setup_driver(headless=headless)
    records: List[Dict] = []
    
    try:
        driver.get(BSE_URL)
        
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table[ng-repeat='cann in CorpannData.Table']"))
        )

        page_count = 0
        while True:
            page_count += 1
            print(f"\n--- Processing Page {page_count} ---")
            
            soup = BeautifulSoup(driver.page_source, "lxml")
            tables = soup.find_all("table", {"ng-repeat": "cann in CorpannData.Table"})

            for i, table in enumerate(tables):
                try:
                    # Extract announcement data
                    announcement_data = extract_announcement_data(table)
                    
                    # Extract PDF link
                    pdf_tag = table.find("a", class_="tablebluelink", href=True)
                    pdf_link = urljoin(BSE_URL, pdf_tag["href"]) if pdf_tag else ""

                    # Extract time information
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

                    # Only process records for target date
                    if disseminated_date != target_date:
                        continue

                    if not announcement_data['headline'] and not announcement_data['announcement_text']:
                        print(f"Skipping record {len(records)+1}: No content")
                        continue

                    # Check for duplicates
                    unique_key = {
                        "company_code": announcement_data['company_code'],
                        "announcement_text": announcement_data['announcement_text'],
                        "exchange_disseminated_date": disseminated_date,
                        "exchange_disseminated_time": disseminated_time,
                    }
                    if SeleniumAnnouncement.objects.filter(**unique_key).exists():
                        print(f"Skipping duplicate record for {announcement_data['company_name']}")
                        continue

                    # Upload PDF if available
                    pdf_path_cloud = ""
                    pdf_r2_path = ""
                    if pdf_link:
                        code_for_name = announcement_data['company_code'] or "UNKNOWN"
                        date_compact = received_date.replace("-", "") if received_date else "NA"
                        safe_headline = safe_filename(announcement_data['headline'])[:50]
                        r2_filename = f"{len(records)+1:03d}_{code_for_name}_{date_compact}_{safe_headline}.pdf"
                        pdf_r2_path = f"{R2_BASE_PATH}/{r2_filename}"
                        pdf_path_cloud = upload_pdf_to_r2(pdf_link, pdf_r2_path)

                    print(f"📄 Selenium Record {len(records)+1}: {announcement_data['company_name']} ({announcement_data['company_code']})")

                    records.append({
                        "Headline": announcement_data['headline'],
                        "Category": announcement_data['category'],
                        "Company Name": announcement_data['company_name'],
                        "Company Code": announcement_data['company_code'],
                        "Announcement Text": announcement_data['announcement_text'],
                        "Exchange Received Date": received_date,
                        "Exchange Received Time": received_time,
                        "Exchange Disseminated Date": disseminated_date,
                        "Exchange Disseminated Time": disseminated_time,
                        "Attachment Size": announcement_data.get('attachment_size', ''),
                        "PDF Link (web)": pdf_link,
                        "PDF Path (cloud)": pdf_path_cloud,
                        "PDF R2 Path": pdf_r2_path,
                        "XBRL Parse Status": "SELENIUM_SUCCESS"
                    })

                    if limit and len(records) >= limit:
                        print(f"\n✅ Reached limit of {limit} records")
                        return pd.DataFrame(records)

                except Exception as e:
                    print(f"❌ Error parsing entry {len(records)+1}: {e}")
                    continue

            # Try to go to next page
            try:
                next_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "idnext"))
                )
                ActionChains(driver).move_to_element(next_button).click().perform()
                time.sleep(2)
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table[ng-repeat='cann in CorpannData.Table']"))
                )
                print(f"➡ Moving to page {page_count + 1}")
            except (TimeoutException, NoSuchElementException):
                print("🏁 No more pages to scrape.")
                break

    except Exception as e:
        print(f"❌ Fatal error in Selenium scraper: {e}")
    finally:
        driver.quit()

    return pd.DataFrame(records)

# =====================================================
# DJANGO COMMAND
# =====================================================

class Command(BaseCommand):
    help = "BSE Corporate Announcements Scraper with API (primary) and Selenium (fallback)"

    def add_arguments(self, parser):
        today_date = datetime.now().strftime("%d-%m-%Y")
        parser.add_argument(
            "--date",
            type=str,
            default=today_date,
            help=f"Target date for announcements (format: DD-MM-YYYY, e.g., {today_date})",
        )
        parser.add_argument(
            "--debug",
            action="store_true",
            help="Enable debug output (show browser for Selenium)"
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Limit the number of records to scrape (e.g., 10 for testing)",
        )
        parser.add_argument(
            "--force-selenium",
            action="store_true",
            help="Skip API and use Selenium directly"
        )

    def handle(self, *args, **options):
        target_date = options["date"]
        limit = options.get("limit")
        force_selenium = options.get("force_selenium", False)
        
        self.stdout.write(
            self.style.SUCCESS(
                f"🚀 Starting BSE Announcements Scraper for {target_date}"
                f"{f' (LIMITED to {limit} records)' if limit else ''}"
            )
        )

        try:
            datetime.strptime(target_date, "%d-%m-%Y")
        except ValueError:
            self.stdout.write(self.style.ERROR("❌ Invalid date format. Use DD-MM-YYYY"))
            return

        items = pd.DataFrame()
        
        # Try API first (unless forced to use Selenium)
        if not force_selenium:
            self.stdout.write(self.style.NOTICE("🌐 Attempting API scraping..."))
            items = scrape_bse_api(
                target_date=target_date,
                limit=limit
            )
            
            if not items.empty:
                self.stdout.write(self.style.SUCCESS(f"✅ API scraping successful: {len(items)} records"))
            else:
                self.stdout.write(self.style.WARNING("⚠️ API failed, switching to Selenium..."))
        
        # Fallback to Selenium if API failed or if forced
        if items.empty:
            self.stdout.write(self.style.NOTICE("🔄 Using Selenium scraping..."))
            items = scrape_bse_selenium(
                target_date=target_date,
                headless=not options["debug"],
                limit=limit
            )
            
            if not items.empty:
                self.stdout.write(self.style.SUCCESS(f"✅ Selenium scraping successful: {len(items)} records"))

        if items.empty:
            self.stdout.write(self.style.ERROR("❌ No data scraped for the specified date"))
            return

        # Display sample data
        self.stdout.write("\n" + "="*100)
        self.stdout.write(self.style.SUCCESS("📊 DATA SAMPLE (First 3 records):"))
        self.stdout.write("="*100)
        
        for i, row in items.head(3).iterrows():
            self.stdout.write(f"\n🔸 Record {i+1}:")
            self.stdout.write(f"   🏢 Company: {row['Company Name']} ({row['Company Code']})")
            self.stdout.write(f"   📰 Headline: {row['Headline'][:80]}...")
            self.stdout.write(f"   📂 Category: {row['Category']}")
            self.stdout.write(f"   📅 Date: {row['Exchange Disseminated Date']} {row['Exchange Disseminated Time']}")
            self.stdout.write(f"   📎 Size: {row.get('Attachment Size') or 'N/A'}")
            self.stdout.write(f"   🔧 Method: {row.get('XBRL Parse Status', 'UNKNOWN')}")
            self.stdout.write("   " + "-"*80)

        # Save to database
        count_new, count_existing = 0, 0

        for _, row in items.iterrows():
            unique_key = {
                "company_code": row.get("Company Code"),
                "announcement_text": row.get("Announcement Text"),
                "exchange_disseminated_date": row.get("Exchange Disseminated Date"),
                "exchange_disseminated_time": row.get("Exchange Disseminated Time"),
            }

            if SeleniumAnnouncement.objects.filter(**unique_key).exists():
                count_existing += 1
                continue

            try:
                with transaction.atomic():
                    SeleniumAnnouncement.objects.create(
                        company_name=row.get("Company Name") or None,
                        company_code=row.get("Company Code") or None,
                        headline=row.get("Headline") or None,
                        category=row.get("Category") or None,
                        announcement_text=row.get("Announcement Text") or None,
                        exchange_received_date=row.get("Exchange Received Date") or None,
                        exchange_received_time=row.get("Exchange Received Time") or None,
                        exchange_disseminated_date=row.get("Exchange Disseminated Date") or None,
                        exchange_disseminated_time=row.get("Exchange Disseminated Time") or None,
                        pdf_link_web=row.get("PDF Link (web)") or None,
                        pdf_path_cloud=row.get("PDF Path (cloud)") or None,
                        pdf_r2_path=row.get("PDF R2 Path") or None,
                        attachment_size=row.get("Attachment Size") or None,
                        # Additional XBRL fields (optional)
                        xbrl_parse_status=row.get("XBRL Parse Status") or None,
                    )
                    count_new += 1
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"❌ Error saving record: {e}")
                )
                continue

            if limit and count_new >= limit:
                break

        self.stdout.write("\n" + "="*100)
        self.stdout.write(
            self.style.SUCCESS(
                f"✅ SCRAPING COMPLETED!\n"
                f"   📥 {count_new} new records inserted\n"
                f"   🔄 {count_existing} duplicates skipped\n"
                f"   📊 Total processed: {len(items)} records"
            )
        )
        self.stdout.write("="*100)