from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from datetime import datetime, timedelta
from selenium.webdriver.common.keys import Keys
import json
import re
import time
import requests
import boto3
from pathlib import Path
from urllib.parse import urljoin, urlparse
from io import BytesIO
import hashlib
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# Import your models
from selenium_scrape.models import BseAnnouncementAggregate  # Replace 'selenium_scrape' with your app name

# =========== repo paths ===========
BASE_DIR = Path(__file__).resolve().parents[4]   # Adjust as needed
DOWNLOADS_DIR = BASE_DIR / "downloads" / "announcements"

# =========== R2 Configuration ===========
R2_ENDPOINT = getattr(settings, 'R2_ENDPOINT', None)
R2_ACCESS_KEY = getattr(settings, 'R2_ACCESS_KEY_ID', None)
R2_SECRET_KEY = getattr(settings, 'R2_SECRET_ACCESS_KEY', None)
R2_BUCKET = getattr(settings, 'R2_BUCKET', None)
R2_PUBLIC_BASEURL = getattr(settings, 'R2_PUBLIC_BASEURL', None)

# Company code to name mapping (optional fallback)
COMPANY_CODE_MAPPING = {
    "532268": "Accelya Solutions India Ltd",
    "500112": "STATE BANK OF INDIA",
    "500002": "ABB India Ltd",
    # Add other known company codes and names
}

def get_r2_client():
    """Initialize R2 client"""
    return boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name='auto'
    )

def download_pdf_from_url(pdf_url, timeout=30):
    """Download PDF from URL and return bytes"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.bseindia.com/corporates/ann.html',
            'Accept': 'application/pdf,application/octet-stream;q=0.9,*/*;q=0.8',
        }
        with requests.get(pdf_url, headers=headers, timeout=timeout, stream=True, allow_redirects=True) as response:
            response.raise_for_status()
            ctype = response.headers.get('content-type', '').lower()
            if 'pdf' not in ctype and not pdf_url.lower().endswith('.pdf'):
                raise Exception(f"Downloaded content doesn't appear to be a PDF. Content-Type: {ctype}")
            return response.content
    except Exception as e:
        raise Exception(f"Failed to download PDF from {pdf_url}: {e}")

def upload_pdf_to_r2(pdf_bytes, r2_key):
    """Upload PDF bytes to R2 and return public URL"""
    try:
        r2_client = get_r2_client()
        r2_client.upload_fileobj(
            BytesIO(pdf_bytes),
            R2_BUCKET,
            r2_key,
            ExtraArgs={'ContentType': 'application/pdf'}
        )
        public_url = f"{R2_PUBLIC_BASEURL}/{r2_key}"
        return public_url
    except Exception as e:
        raise Exception(f"Failed to upload PDF to R2: {e}")

def generate_pdf_filename(announcement, company_code):
    """Generate a unique filename for PDF"""
    content_hash = hashlib.md5(
        f"{announcement.get('headline', '')}{announcement.get('exchange_disseminated_date', '')}{announcement.get('exchange_disseminated_time', '')}".encode()
    ).hexdigest()[:8]
    company_clean = re.sub(r'[^\w\s-]', '', announcement.get('company_name', 'unknown')).strip()
    company_clean = re.sub(r'[-\s]+', '_', company_clean)[:30]
    date_str = announcement.get('exchange_disseminated_date', '').replace('-', '')
    return f"ann_{company_code}_{company_clean}_{date_str}_{content_hash}.pdf"

class Command(BaseCommand):
    help = 'BSE announcements scraper with R2 cloud storage - combines form filling + table parsing'

    def add_arguments(self, parser):
        parser.add_argument(
            '--company-code',
            type=str,
            required=True,
            help='6-digit BSE company code (e.g., 500112 for SBI)'
        )
        parser.add_argument(
            '--start-date',
            type=str,
            help='Start date in DD-MM-YYYY format (default: 3 months ago)'
        )
        parser.add_argument(
            '--end-date',
            type=str,
            help='End date in DD-MM-YYYY format (default: today)'
        )
        parser.add_argument(
            '--date',
            type=str,
            help='Single target date in DD-MM-YYYY format (alternative to start/end dates)'
        )
        parser.add_argument(
            '--max-pages',
            type=int,
            default=20,
            help='Maximum pages to scrape (default: 20)'
        )
        parser.add_argument(
            '--debug',
            action='store_true',
            help='Show browser window for debugging'
        )
        parser.add_argument(
            '--skip-pdf-download',
            action='store_true',
            help='Skip downloading PDFs (useful for testing)'
        )
        parser.add_argument(
            '--save-json',
            type=str,
            help='Save results to JSON file'
        )

    def handle(self, *args, **options):
        company_code = options['company_code']
        start_date = options.get('start_date')
        end_date = options.get('end_date')
        single_date = options.get('date')
        max_pages = options['max_pages']
        debug_mode = options['debug']
        skip_pdf_download = options.get('skip_pdf_download', False)
        save_json = options.get('save_json')

        # Validate company code
        if not re.match(r'^\d{6}$', company_code):
            raise CommandError('Company code must be exactly 6 digits')

        # Handle date logic
        if single_date:
            start_date = end_date = single_date
        else:
            if not start_date or not end_date:
                end_dt = datetime.now()
                start_dt = end_dt - timedelta(days=90)
                start_date = start_date or start_dt.strftime('%d-%m-%Y')
                end_date = end_date or end_dt.strftime('%d-%m-%Y')

        # Validate date formats
        try:
            start_dt = datetime.strptime(start_date, '%d-%m-%Y')
            end_dt = datetime.strptime(end_date, '%d-%m-%Y')
        except ValueError:
            raise CommandError('Dates must be in DD-MM-YYYY format')

        if start_dt > end_dt:
            raise CommandError('Start date cannot be after end date')

        # Convert to BSE format
        bse_start_date = start_date.replace('-', '/')
        bse_end_date = end_date.replace('-', '/')

        self.stdout.write(
            self.style.SUCCESS(f'BSE Announcements Scraper - Combined Version\n') +
            f'Company Code: {company_code}\n' +
            f'Date Range: {start_date} to {end_date}\n' +
            f'Max Pages: {max_pages}\n' +
            f'Skip PDF Download: {skip_pdf_download}'
        )

        # Create downloads directory
        DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

        # Run scraping
        result = self.run_scraper(company_code, bse_start_date, bse_end_date, max_pages, debug_mode)
        
        if result['status'] == 'success' and result['announcements']:
            # Process PDFs and save to database
            self.process_and_save_announcements(result, company_code, start_date, end_date, skip_pdf_download)
        else:
            self.stdout.write(self.style.WARNING(f"No announcements found or scraping failed: {result.get('status', 'unknown')}"))

        # Display results
        self.display_results(result)
        
        # Save to JSON if requested
        if save_json:
            with open(save_json, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            self.stdout.write(self.style.SUCCESS(f'Results saved to: {save_json}'))

    # ============ FORM FILLING METHODS ============
    
    def setup_driver(self, headless: bool = True):
        """Setup Chrome driver"""
        self.stdout.write(f"Setting up Chrome driver (headless: {headless})")
        opts = Options()
        if headless:
            opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1366,768")
        opts.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option('useAutomationExtension', False)
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values.notifications": 2,
            "plugins.always_open_pdf_externally": True,
        }
        opts.add_experimental_option("prefs", prefs)
        return webdriver.Chrome(options=opts)

    def wait_for_angular_ready(self, driver, timeout=30):
        """Wait for Angular to load"""
        self.stdout.write("Waiting for Angular to load...")
        try:
            WebDriverWait(driver, timeout).until(
                lambda d: d.execute_script(
                    "return angular.element(document.body).injector() && "
                    "angular.element(document.body).scope() !== undefined"
                )
            )
            self.stdout.write("Angular ready")
        except Exception as e:
            self.stdout.write(f"Angular load timeout: {e}")

    def find_company_field(self, driver):
        """Find company search field"""
        self.stdout.write("Finding company search field...")
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[ng-include*='SmartSearch']"))
            )
            time.sleep(2)
            smart_search_div = driver.find_element(By.CSS_SELECTOR, "div[ng-include*='SmartSearch']")
            input_field = smart_search_div.find_element(By.CSS_SELECTOR, "input[type='text']")
            
            if input_field.is_displayed() and input_field.is_enabled():
                field_id = input_field.get_attribute("id")
                self.stdout.write(f"Found company field: {field_id}")
                return input_field
        except Exception as e:
            self.stdout.write(f"Failed to find company field: {e}")
        return None

    def handle_company_dropdown(self, driver):
        """Handle dropdown selection"""
        self.stdout.write("Handling company dropdown...")
        try:
            search_field = self.find_company_field(driver)
            if search_field:
                search_field.send_keys(Keys.ARROW_DOWN)
                time.sleep(1)
                search_field.send_keys(Keys.ENTER)
                time.sleep(2)
                self.stdout.write("Company dropdown handled successfully")
                return True
        except Exception as e:
            self.stdout.write(f"Dropdown handling failed: {e}")
        return False

    def set_date_fields(self, driver, start_date, end_date):
        """Set from and to date fields for date range"""
        self.stdout.write(f"Setting date range: {start_date} to {end_date}")
        try:
            driver.execute_script("""
                var fromField = document.getElementById('txtFromDt');
                var toField = document.getElementById('txtToDt');
                fromField.value = arguments[0];
                toField.value = arguments[1];
                fromField.dispatchEvent(new Event('input', { bubbles: true }));
                fromField.dispatchEvent(new Event('change', { bubbles: true }));
                toField.dispatchEvent(new Event('input', { bubbles: true }));
                toField.dispatchEvent(new Event('change', { bubbles: true }));
            """, start_date, end_date)
            time.sleep(2)
            
            from_value = driver.find_element(By.ID, "txtFromDt").get_attribute('value')
            to_value = driver.find_element(By.ID, "txtToDt").get_attribute('value')
            
            if from_value == start_date and to_value == end_date:
                self.stdout.write("Date fields set successfully")
                return True
            else:
                self.stdout.write(f"Date verification failed: {from_value}, {to_value}")
        except Exception as e:
            self.stdout.write(f"Date setting failed: {e}")
        return False

    def set_filters(self, driver, company_code, start_date, end_date):
        """Set company code and date range filters"""
        self.stdout.write(f"Setting filters for company {company_code}")
        
        search_field = self.find_company_field(driver)
        if not search_field:
            return False
            
        try:
            search_field.clear()
            search_field.send_keys(company_code)
            time.sleep(2)
            
            driver.execute_script("""
                var element = arguments[0];
                element.dispatchEvent(new Event('input', { bubbles: true }));
                element.dispatchEvent(new Event('change', { bubbles: true }));
                element.dispatchEvent(new Event('keyup', { bubbles: true }));
                element.focus();
            """, search_field)
            time.sleep(3)
            
            if not self.handle_company_dropdown(driver):
                self.stdout.write("Dropdown failed, continuing anyway...")
                
        except Exception as e:
            self.stdout.write(f"Company setting failed: {e}")
            return False

        if not self.set_date_fields(driver, start_date, end_date):
            return False

        return True

    def submit_form(self, driver):
        """Submit form"""
        self.stdout.write("Submitting form...")
        
        try:
            driver.execute_script("""
                var datePickers = document.querySelectorAll('.ui-datepicker, .ui-widget-overlay');
                datePickers.forEach(function(picker) {
                    picker.style.display = 'none';
                });
                document.body.click();
            """)
            time.sleep(1)
            
            submit_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "btnSubmit"))
            )
            submit_button.click()
            time.sleep(5)
            
            for i in range(30):
                try:
                    WebDriverWait(driver, 2).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "table[ng-repeat='cann in CorpannData.Table']"))
                    )
                    self.stdout.write("Data loaded successfully")
                    return True
                except:
                    if i % 5 == 0:
                        self.stdout.write(f"Waiting for data... ({i+1}/30)")
                    time.sleep(1)
                    
            return False
            
        except Exception as e:
            self.stdout.write(f"Form submission failed: {e}")
            return False

    # ============ TABLE PARSING METHODS ============
    
    def _extract_category_from_table(self, table):
        """Extract category from table structure"""
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

    def scrape_announcements(self, driver, company_code):
        """Extract announcements with accurate company name and code extraction"""
        announcements = []
        
        try:
            soup = BeautifulSoup(driver.page_source, "lxml")
            tables = soup.find_all("table", {"ng-repeat": "cann in CorpannData.Table"})
            # Find the company info row
            company_info_row = soup.find("td", {"class": "tdcolumn ng-binding ng-scope", "colspan": "4", "ng-if": "trIsDisplay!='1'"})
            
            company_name = ""
            extracted_company_code = company_code
            
            if company_info_row:
                # Extract company code
                code_tag = company_info_row.find("b", string=re.compile(r"Security Code :"))
                if code_tag and code_tag.next_sibling:
                    code_text = code_tag.next_sibling.strip()
                    code_match = re.search(r"\b(\d{6})\b", code_text)
                    if code_match:
                        extracted_company_code = code_match.group(1)
                
                # Extract company name
                name_tag = company_info_row.find("b", string=re.compile(r"Company :"))
                if name_tag and name_tag.next_sibling and name_tag.next_sibling.name == "a":
                    company_name = name_tag.next_sibling.get_text(strip=True)
            
            self.stdout.write(f"Extracted company: {company_name}, code: {extracted_company_code}")
            
            self.stdout.write(f"Found {len(tables)} announcement tables")
            
            for idx, table in enumerate(tables):
                try:
                    # Extract core data
                    newssub_tag = table.find("span", {"ng-bind-html": "cann.NEWSSUB"})
                    headline_tag = table.find("span", {"ng-bind-html": "cann.HEADLINE"})
                    pdf_tag = table.find("a", class_="tablebluelink", href=True)

                    newssub = (newssub_tag.get_text(strip=True) if newssub_tag else "") or ""
                    headline = (headline_tag.get_text(strip=True) if headline_tag else "") or ""
                    category = self._extract_category_from_table(table)
                    pdf_link = urljoin("https://www.bseindia.com/corporates/ann.html", pdf_tag["href"]) if pdf_tag else ""

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

                    # Use extracted company name and code unless overridden by table-specific data
                    if not company_name and newssub:
                        name_match = re.search(r"^(.*?)(?=\s*-\s*Announcement|\s*-\s*Board Meeting|\s*-\s*Certificate|\s*-\s*Appointment|$)", newssub)
                        if name_match:
                            company_name = name_match.group(1).strip()
                    if not extracted_company_code and newssub:
                        code_match = re.search(r"\b(\d{6})\b", newssub)
                        if code_match:
                            extracted_company_code = code_match.group(1)

                    # Skip if no essential data
                    if not headline and not newssub:
                        continue

                    announcements.append({
                        "serial_no": len(announcements) + 1,
                        "company_name": company_name or COMPANY_CODE_MAPPING.get(company_code, f"Unknown Company ({company_code})"),
                        "company_code": extracted_company_code,
                        "headline": headline or None,
                        "category": category or None,
                        "announcement_text": headline,
                        "exchange_received_date": received_date,
                        "exchange_received_time": received_time,
                        "exchange_disseminated_date": disseminated_date,
                        "exchange_disseminated_time": disseminated_time,
                        "pdf_link": pdf_link,
                        "source": "combined_parsing"
                    })
                    
                except Exception as e:
                    self.stdout.write(f"Error parsing table {idx+1}: {e}")
                    
        except Exception as e:
            self.stdout.write(f"Data extraction failed: {e}")
        
        return announcements

    # ============ PAGINATION AND NAVIGATION ============
    
    def check_pagination_status(self, driver):
        """Check current pagination status"""
        try:
            pagination_info = {}
            
            try:
                next_button = driver.find_element(By.ID, "idnext")
                pagination_info['next_button_exists'] = True
                pagination_info['next_button_enabled'] = next_button.is_enabled()
                pagination_info['next_button_disabled_attr'] = next_button.get_attribute("disabled")
            except:
                pagination_info['next_button_exists'] = False
            
            self.stdout.write(f"Pagination status: {pagination_info}")
            return pagination_info
            
        except Exception as e:
            self.stdout.write(f"Could not check pagination status: {e}")
            return {}

    def navigate_next_page(self, driver):
        """Navigate to next page"""
        self.stdout.write("Attempting to navigate to next page...")
        
        try:
            next_button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "idnext"))
            )
            
            if next_button.get_attribute("disabled") == "true":
                self.stdout.write("Next button is disabled - no more pages")
                return False
            
            if not next_button.is_enabled():
                self.stdout.write("Next button is not enabled - no more pages")
                return False
                
            driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", next_button)
            time.sleep(1)
            
            try:
                next_button.click()
            except Exception:
                try:
                    driver.execute_script("arguments[0].click();", next_button)
                except Exception:
                    return False
            
            time.sleep(3)
            
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table[ng-repeat='cann in CorpannData.Table']"))
                )
                self.stdout.write("New page data detected")
                return True
            except:
                time.sleep(5)
                tables = driver.find_elements(By.CSS_SELECTOR, "table[ng-repeat='cann in CorpannData.Table']")
                if tables:
                    self.stdout.write(f"Found {len(tables)} tables after additional wait")
                    return True
                return False
            
        except Exception as e:
            self.stdout.write(f"Navigation failed: {e}")
            return False

    def check_for_results(self, driver):
        """Check if results are available"""
        try:
            tables = driver.find_elements(By.CSS_SELECTOR, "table[ng-repeat='cann in CorpannData.Table']")
            if tables:
                return {'status': 'has_data', 'count': len(tables)}
            else:
                return {'status': 'no_data'}
        except Exception as e:
            return {'status': 'error', 'error': str(e)}

    # ============ MAIN SCRAPER WORKFLOW ============
    
    def run_scraper(self, company_code, start_date, end_date, max_pages, debug_mode):
        """Main scraper function - combines form filling + table parsing"""
        driver = None
        all_announcements = []
        
        try:
            driver = self.setup_driver(headless=not debug_mode)
            
            self.stdout.write("Loading BSE page...")
            driver.get("https://www.bseindia.com/corporates/ann.html")
            time.sleep(5)
            
            self.wait_for_angular_ready(driver)
            time.sleep(10)
            
            # Use form filling
            if not self.set_filters(driver, company_code, start_date, end_date):
                return {'status': 'filter_failed', 'announcements': []}
            
            if not self.submit_form(driver):
                return {'status': 'submit_failed', 'announcements': []}
            
            result_status = self.check_for_results(driver)
            
            if result_status['status'] == 'has_data':
                self.stdout.write("Found data, starting pagination...")
                
                page_count = 0
                consecutive_failures = 0
                
                while page_count < max_pages:
                    page_count += 1
                    self.stdout.write(f"Scraping page {page_count}...")
                    
                    time.sleep(2)
                    page_announcements = self.scrape_announcements(driver, company_code)
                    
                    if not page_announcements:
                        consecutive_failures += 1
                        if consecutive_failures >= 2:
                            break
                    else:
                        consecutive_failures = 0
                        all_announcements.extend(page_announcements)
                        self.stdout.write(f"Page {page_count}: {len(page_announcements)} announcements")
                    
                    if page_count < max_pages:
                        if not self.navigate_next_page(driver):
                            break
                
                return {
                    'status': 'success',
                    'announcements': all_announcements,
                    'pages_scraped': page_count,
                    'total_found': len(all_announcements),
                    'company_code': company_code,
                    'date_range': f"{start_date} to {end_date}"
                }
            else:
                return {
                    'status': 'no_data', 
                    'announcements': [],
                    'company_code': company_code,
                    'date_range': f"{start_date} to {end_date}"
                }
                
        except Exception as e:
            return {
                'status': 'error', 
                'announcements': [], 
                'error': str(e),
                'company_code': company_code,
                'date_range': f"{start_date} to {end_date}"
            }
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception as e:
                    self.stdout.write(f"Driver cleanup error: {e}")

    # ============ PDF PROCESSING AND DATABASE SAVE ============
    
    def process_and_save_announcements(self, scrape_result, company_code, start_date, end_date, skip_pdf_download):
        """Process announcements, download PDFs, and save to database"""
        announcements = scrape_result.get('announcements', [])
        if not announcements:
            return

        self.stdout.write(self.style.NOTICE(f"Processing {len(announcements)} announcements..."))

        pdfs_data = []
        processed_announcements = []

        for idx, announcement in enumerate(announcements, 1):
            self.stdout.write(f"Processing announcement {idx}/{len(announcements)}: {announcement.get('headline', 'N/A')[:60]}...")
            
            processed_ann = announcement.copy()
            
            # Handle PDF download if available and not skipped
            if announcement.get('pdf_link') and not skip_pdf_download:
                try:
                    pdf_url = announcement['pdf_link']
                    self.stdout.write(f"  Downloading PDF from: {pdf_url}")
                    
                    # Download PDF
                    pdf_bytes = download_pdf_from_url(pdf_url)
                    pdf_size = len(pdf_bytes)
                    
                    # Generate R2 key
                    pdf_filename = generate_pdf_filename(announcement, company_code)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    r2_key = f"bse_announcements/pdfs/{company_code}/{timestamp}_{pdf_filename}"
                    
                    # Upload to R2
                    cloud_url = upload_pdf_to_r2(pdf_bytes, r2_key)
                    
                    # Store PDF info
                    pdf_info = {
                        'original_url': pdf_url,
                        'r2_key': r2_key,
                        'cloud_url': cloud_url,
                        'filename': pdf_filename,
                        'size_bytes': pdf_size,
                        'uploaded_at': datetime.now().isoformat(),
                        'announcement_id': idx,
                        'headline': announcement.get('headline', '')[:100]
                    }
                    pdfs_data.append(pdf_info)
                    
                    # Update announcement with cloud storage info
                    processed_ann['pdf_r2_key'] = r2_key
                    processed_ann['pdf_cloud_url'] = cloud_url
                    processed_ann['pdf_size_bytes'] = pdf_size
                    processed_ann['pdf_upload_status'] = 'success'
                    
                    self.stdout.write(self.style.SUCCESS(f"  PDF uploaded successfully ({pdf_size:,} bytes): {pdf_filename}"))
                    
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"  PDF processing failed: {e}"))
                    processed_ann['pdf_upload_status'] = 'failed'
                    processed_ann['pdf_error'] = str(e)
            else:
                processed_ann['pdf_upload_status'] = 'skipped' if skip_pdf_download else 'no_pdf'
            
            processed_announcements.append(processed_ann)

        # Extract company information
        company_name = COMPANY_CODE_MAPPING.get(company_code, "Unknown Company")
        if processed_announcements:
            # Prefer the company name from announcements if available
            first_company_name = processed_announcements[0].get('company_name', '')
            if first_company_name and first_company_name != f"Unknown Company ({company_code})":
                company_name = first_company_name

        self.stdout.write(self.style.NOTICE("Saving to database..."))

        # Create or update BseAnnouncementAggregate record
        aggregate, created = BseAnnouncementAggregate.objects.update_or_create(
            bse_code=company_code,
            scrape_start_date=start_date,
            scrape_end_date=end_date,
            defaults={
                'company_name': company_name,
                'announcements_data': processed_announcements,
                'pdfs_data': pdfs_data,
            }
        )

        action_word = "Created" if created else "Updated"
        self.stdout.write(self.style.SUCCESS(
            f"{action_word} BseAnnouncementAggregate record:\n"
            f"  Company: {aggregate.company_name} ({aggregate.bse_code})\n"
            f"  Date Range: {aggregate.scrape_start_date} to {aggregate.scrape_end_date}\n"
            f"  Total Announcements: {aggregate.total_announcements_count}\n"
            f"  Total PDFs Stored: {aggregate.total_pdfs_count}"
        ))

        self.stdout.write(self.style.SUCCESS("Announcements processing completed successfully!"))

    # ============ RESULTS DISPLAY ============
    
    def display_results(self, result):
        """Display results with cloud storage info"""
        status = result.get('status', 'unknown')
        announcements = result.get('announcements', [])
        
        self.stdout.write(f"\n{'='*60}")
        self.stdout.write(f"BSE ANNOUNCEMENTS SCRAPING RESULTS - COMBINED VERSION")
        self.stdout.write(f"{'='*60}")
        self.stdout.write(f"Company Code: {result.get('company_code', 'N/A')}")
        self.stdout.write(f"Date Range: {result.get('date_range', result.get('target_date', 'N/A'))}")
        if result.get('pages_scraped'):
            self.stdout.write(f"Pages Scraped: {result.get('pages_scraped', 0)}")
        self.stdout.write(f"{'='*60}")
        
        if status == 'success':
            self.stdout.write(self.style.SUCCESS(f"Success: {len(announcements)} announcements found"))
            
            # Count PDFs
            pdfs_available = sum(1 for ann in announcements if ann.get('pdf_link'))
            pdfs_uploaded = sum(1 for ann in announcements if ann.get('pdf_upload_status') == 'success')
            
            self.stdout.write(f"PDFs Available: {pdfs_available}")
            self.stdout.write(f"PDFs Successfully Uploaded: {pdfs_uploaded}")
            
            if announcements:
                self.stdout.write(f"\nANNOUNCEMENTS SAMPLE (First 10):")
                self.stdout.write(f"{'-'*60}")
                for i, ann in enumerate(announcements[:10], 1):
                    headline = ann.get('headline', 'N/A')
                    category = ann.get('category', 'N/A')
                    company = ann.get('company_name', 'N/A')
                    diss_date = ann.get('exchange_disseminated_date', 'N/A')
                    diss_time = ann.get('exchange_disseminated_time', 'N/A')
                    
                    pdf_status = "No PDF"
                    if ann.get('pdf_link'):
                        if ann.get('pdf_upload_status') == 'success':
                            pdf_status = f"PDF Uploaded ({ann.get('pdf_size_bytes', 0):,} bytes)"
                        else:
                            pdf_status = f"PDF Failed: {ann.get('pdf_error', 'Unknown error')}"
                    
                    self.stdout.write(f"\n{i}. {headline[:80]}...")
                    self.stdout.write(f"   Company: {company}")
                    self.stdout.write(f"   Category: {category}")
                    self.stdout.write(f"   Disseminated: {diss_date} {diss_time}")
                    self.stdout.write(f"   {pdf_status}")
                
                if len(announcements) > 10:
                    self.stdout.write(f"\n... and {len(announcements) - 10} more announcements")
        
        elif status == 'no_data':
            self.stdout.write(self.style.WARNING("No data found for the specified company and date range"))
        else:
            self.stdout.write(self.style.ERROR(f"Failed: {status}"))
            if result.get('error'):
                self.stdout.write(f"Error: {result['error']}")

    # ============ ADDITIONAL UTILITY METHODS ============
    
    def safe_filename(self, name: str, max_len: int = 150) -> str:
        """Create safe filename from announcement text"""
        name = re.sub(r'[\\/*?:"<>|]', "_", name or "")
        name = re.sub(r"\s+", " ", name).strip()
        return name[:max_len] or "announcement"