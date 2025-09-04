from datetime import datetime
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from typing import List, Dict, Optional, Tuple
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

# Cloudflare R2 Configuration
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT")
R2_BUCKET_NAME = os.getenv("R2_BUCKET")
R2_PUBLIC_BASEURL = os.getenv("R2_PUBLIC_BASEURL")
R2_BASE_PATH = "bse_announcements"

# Enhanced Category Mapping
CATEGORY_MAPPING = {
    'agm': 'Annual General Meeting',
    'annual general meeting': 'Annual General Meeting',
    'egm': 'Extraordinary General Meeting',
    'extraordinary general meeting': 'Extraordinary General Meeting',
    'board meeting': 'Board Meeting',
    'board': 'Board Meeting',
    'dividend': 'Dividend',
    'bonus': 'Bonus Issue',
    'split': 'Stock Split',
    'rights': 'Rights Issue',
    'result': 'Financial Results',
    'financial result': 'Financial Results',
    'quarterly result': 'Financial Results',
    'annual result': 'Financial Results',
    'disclosure': 'Corporate Disclosure',
    'intimation': 'Corporate Intimation',
    'allotment': 'Share Allotment',
    'newspaper': 'Newspaper Publication',
    'advertisement': 'Advertisement',
    'cessation': 'Management Changes',
    'appointment': 'Management Changes',
    'resignation': 'Management Changes',
    'compliance': 'Regulatory Compliance',
    'regulation 30': 'Regulatory Compliance',
    'regulation 29': 'Regulatory Compliance',
    'takeover': 'Takeover/Acquisition',
    'acquisition': 'Takeover/Acquisition',
    'merger': 'Merger',
    'voting': 'Voting Results',
    'scrutinizer': 'Voting Results',
    'e-voting': 'E-Voting',
    'share transfer': 'Share Transfer',
    'register': 'Share Transfer',
    'annual report': 'Annual Report',
    'outcome': 'Meeting Outcome',
    'closure': 'Book Closure',
    'record date': 'Record Date'
}

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

def categorize_announcement(text: str) -> str:
    """Intelligently categorize announcement based on content"""
    if not text:
        return "General"
    
    text_lower = text.lower()
    
    # Check against category mapping
    for keyword, category in CATEGORY_MAPPING.items():
        if keyword in text_lower:
            return category
    
    # Additional pattern matching
    if re.search(r'\b(esop|employee stock option)\b', text_lower):
        return "Employee Stock Options"
    
    if re.search(r'\b(debenture|bond|debt)\b', text_lower):
        return "Debt Securities"
    
    if re.search(r'\b(credit rating|rating)\b', text_lower):
        return "Credit Rating"
    
    return "General"

def extract_announcement_data(table) -> Dict[str, str]:
    """
    Enhanced extraction function with better field separation
    """
    data = {
        'headline': '',
        'announcement_text': '',
        'category': '',
        'company_name': '',
        'company_code': ''
    }
    
    try:
        # 1. Extract NEWSSUB (Company info)
        newssub_tag = table.find("span", {"ng-bind-html": "cann.NEWSSUB"})
        newssub = clean_text(newssub_tag.get_text(strip=True)) if newssub_tag else ""
        
        if newssub:
            data['company_name'], data['company_code'] = extract_company_details(newssub)
        
        # 2. Extract HEADLINE (Primary content)
        headline_tag = table.find("span", {"ng-bind-html": "cann.HEADLINE"})
        if headline_tag:
            headline_text = clean_text(headline_tag.get_text(strip=True))
            
            # Remove company info from headline if it exists
            if data['company_name'] and headline_text.startswith(data['company_name']):
                headline_text = headline_text[len(data['company_name']):].lstrip(' -')
            
            # Remove company code from headline
            if data['company_code']:
                headline_text = re.sub(fr'\b{data["company_code"]}\b\s*-?\s*', '', headline_text)
            
            data['headline'] = clean_text(headline_text)
        
        # 3. Extract announcement content from UUID div
        uuid_regex = re.compile(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$')
        announcement_div = table.find("div", id=uuid_regex)
        
        if announcement_div:
            announcement_content = clean_text(announcement_div.get_text(strip=True))
            
            # If announcement content is different from headline, use it
            if announcement_content and announcement_content != data['headline']:
                data['announcement_text'] = announcement_content
            else:
                data['announcement_text'] = data['headline']
        else:
            # Fallback: use headline as announcement text
            data['announcement_text'] = data['headline']
        
        # 4. Generate category based on content
        content_for_categorization = f"{data['headline']} {data['announcement_text']}"
        data['category'] = categorize_announcement(content_for_categorization)
        
        # 5. Ensure we have minimum required content
        if not data['announcement_text'] and not data['headline']:
            # Last resort: try to extract from any meaningful text in the table
            all_text_elements = table.find_all(text=True)
            meaningful_texts = []
            
            for text in all_text_elements:
                cleaned = clean_text(text)
                if (len(cleaned) > 20 and 
                    not re.match(r'^\d{2}-\d{2}-\d{4}', cleaned) and  # Not date
                    not re.match(r'^\d{2}:\d{2}:\d{2}', cleaned) and  # Not time
                    'exchange' not in cleaned.lower() and
                    'pdf' not in cleaned.lower() and
                    'view' not in cleaned.lower()):
                    meaningful_texts.append(cleaned)
            
            if meaningful_texts:
                # Use the longest meaningful text
                data['announcement_text'] = max(meaningful_texts, key=len)
                if not data['headline']:
                    # Create headline from first 100 chars
                    data['headline'] = data['announcement_text'][:100] + "..." if len(data['announcement_text']) > 100 else data['announcement_text']
        
        # 6. Final cleanup and validation
        if data['headline'] and len(data['headline']) > 300:
            data['headline'] = data['headline'][:297] + "..."
        
        if data['category'] and len(data['category']) > 100:
            data['category'] = data['category'][:100]
        
    except Exception as e:
        print(f"Error in extract_announcement_data: {e}")
        # Ensure we have some basic data even on error
        if not data['announcement_text'] and not data['headline']:
            data['announcement_text'] = "Error extracting announcement content"
            data['headline'] = "Error extracting headline"
            data['category'] = "General"
    
    return data

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

def scrape_bse_announcements_enhanced(
    target_date: str = "04-09-2025",
    headless: bool = True,
    limit: Optional[int] = None
) -> pd.DataFrame:
    """Enhanced scraping function with improved data quality"""
    
    driver = setup_driver(headless=headless)
    records: List[Dict] = []
    
    try:
        driver.get(BSE_URL)

        # Wait for the Angular tables to appear
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table[ng-repeat='cann in CorpannData.Table']"))
        )

        page_count = 0
        while True:
            page_count += 1
            print(f"\n--- Processing Page {page_count} ---")
            
            # Parse current page
            soup = BeautifulSoup(driver.page_source, "lxml")
            tables = soup.find_all("table", {"ng-repeat": "cann in CorpannData.Table"})

            for i, table in enumerate(tables):
                try:
                    # Extract all data using enhanced function
                    announcement_data = extract_announcement_data(table)
                    
                    # Extract PDF link
                    pdf_tag = table.find("a", class_="tablebluelink", href=True)
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

                    # Skip if we don't have essential data
                    if not announcement_data['announcement_text'] and not announcement_data['headline']:
                        print(f"Skipping record {len(records)+1}: No content")
                        continue

                    # Check for duplicate before processing PDF
                    unique_key = {
                        "company_code": announcement_data['company_code'],
                        "announcement_text": announcement_data['announcement_text'],
                        "exchange_disseminated_date": disseminated_date,
                        "exchange_disseminated_time": disseminated_time,
                    }
                    if SeleniumAnnouncement.objects.filter(**unique_key).exists():
                        print(f"Skipping duplicate record for {announcement_data['company_name']}")
                        continue

                    # Handle PDF upload
                    pdf_path_cloud = ""
                    pdf_r2_path = ""
                    if pdf_link:
                        code_for_name = announcement_data['company_code'] or "UNKNOWN"
                        date_compact = received_date.replace("-", "") if received_date else "NA"
                        safe_headline = safe_filename(announcement_data['headline'])[:50]
                        r2_filename = f"{len(records)+1:03d}{code_for_name}{date_compact}_{safe_headline}.pdf"
                        pdf_r2_path = f"{R2_BASE_PATH}/{r2_filename}"
                        pdf_path_cloud = upload_pdf_to_r2(pdf_link, pdf_r2_path)

                    # Enhanced debug output
                    print(f"\n📄 Record {len(records)+1} (Page {page_count}, Item {i+1}):")
                    print(f"   🏢 Company: {announcement_data['company_name']} ({announcement_data['company_code']})")
                    print(f"   📰 Headline: {announcement_data['headline'][:100]}{'...' if len(announcement_data['headline']) > 100 else ''}")
                    print(f"   📂 Category: {announcement_data['category']}")
                    print(f"   📝 Content: {announcement_data['announcement_text'][:80]}{'...' if len(announcement_data['announcement_text']) > 80 else ''}")
                    print(f"   🕐 Time: {disseminated_date} {disseminated_time}")
                    print("   " + "="*80)

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
                        "PDF Link (web)": pdf_link,
                        "PDF Path (cloud)": pdf_path_cloud,
                        "PDF R2 Path": pdf_r2_path,
                    })

                    # Stop if limit is reached
                    if limit and len(records) >= limit:
                        print(f"\n✅ Reached limit of {limit} records")
                        return pd.DataFrame(records)

                except Exception as e:
                    print(f"❌ Error parsing entry {len(records)+1}: {e}")
                    continue

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
                print(f"➡ Moving to page {page_count + 1}")
            except (TimeoutException, NoSuchElementException):
                print("🏁 No more pages to scrape (Next button not found).")
                break

    except Exception as e:
        print(f"❌ Fatal error in scraper: {e}")
    finally:
        driver.quit()

    return pd.DataFrame(records)

class Command(BaseCommand):
    help = "Enhanced BSE Corporate Announcements Scraper with improved data quality"

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
        
        self.stdout.write(
            self.style.SUCCESS(
                f"🚀 Starting ENHANCED BSE Announcements Scraper v2.0 for {target_date}"
                f"{f' (LIMITED to {limit} records)' if limit else ''}"
            )
        )

        # Validate date format
        try:
            datetime.strptime(target_date, "%d-%m-%Y")
        except ValueError:
            self.stdout.write(self.style.ERROR("❌ Invalid date format. Use DD-MM-YYYY"))
            return

        # Run enhanced scraper
        items = scrape_bse_announcements_enhanced(
            target_date=target_date,
            headless=not options["debug"],
            limit=limit
        )

        if items.empty:
            self.stdout.write(self.style.WARNING("❌ No data scraped for the specified date"))
            return

        # Display improved sample
        self.stdout.write("\n" + "="*100)
        self.stdout.write(self.style.SUCCESS("📊 ENHANCED DATA SAMPLE (First 3 records):"))
        self.stdout.write("="*100)
        
        for i, row in items.head(3).iterrows():
            self.stdout.write(f"\n🔸 Record {i+1}:")
            self.stdout.write(f"   🏢 Company: {row['Company Name']} ({row['Company Code']})")
            self.stdout.write(f"   📰 Headline: {row['Headline']}")
            self.stdout.write(f"   📂 Category: {row['Category']}")
            self.stdout.write(f"   📝 Content: {row['Announcement Text'][:100]}{'...' if len(str(row['Announcement Text'])) > 100 else ''}")
            self.stdout.write(f"   📅 Date: {row['Exchange Disseminated Date']} {row['Exchange Disseminated Time']}")
            self.stdout.write("   " + "-"*80)

        # Save to database with transaction safety
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
                    )
                    count_new += 1
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"❌ Error saving record: {e}")
                )
                continue

            # Stop if limit is reached
            if limit and count_new >= limit:
                break

        # Final summary
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