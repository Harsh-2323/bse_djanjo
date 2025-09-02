from django.core.management.base import BaseCommand, CommandError
from datetime import datetime, timedelta
from selenium.webdriver.common.keys import Keys
import json
import re
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup
from urllib.parse import urljoin

class Command(BaseCommand):
    help = 'Streamlined BSE corporate announcements scraper - company code and date only'

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
        save_json = options.get('save_json')

        # Validate company code
        if not re.match(r'^\d{6}$', company_code):
            raise CommandError('Company code must be exactly 6 digits')

        # Handle date logic
        if single_date:
            # Single date mode
            start_date = end_date = single_date
        else:
            # Date range mode - set defaults if not provided
            if not start_date or not end_date:
                end_dt = datetime.now()
                start_dt = end_dt - timedelta(days=90)  # 3 months ago
                start_date = start_date or start_dt.strftime('%d-%m-%Y')
                end_date = end_date or end_dt.strftime('%d-%m-%Y')

        # Validate date formats
        try:
            start_dt = datetime.strptime(start_date, '%d-%m-%Y')
            end_dt = datetime.strptime(end_date, '%d-%m-%Y')
        except ValueError:
            raise CommandError('Dates must be in DD-MM-YYYY format')

        # Validate date range
        if start_dt > end_dt:
            raise CommandError('Start date cannot be after end date')

        # Convert to BSE format (DD/MM/YYYY)
        bse_start_date = start_date.replace('-', '/')
        bse_end_date = end_date.replace('-', '/')

        self.stdout.write(
            self.style.SUCCESS(f'Streamlined BSE Scraper\n') +
            f'Company Code: {company_code}\n' +
            f'Date Range: {start_date} to {end_date}\n' +
            f'Max Pages: {max_pages}'
        )

        # Run scraping - FIXED: Now passes correct parameters
        result = self.run_scraper(company_code, bse_start_date, bse_end_date, max_pages, debug_mode)
        
        # Display results
        self.display_results(result)
        
        # Save to JSON if requested
        if save_json:
            with open(save_json, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            self.stdout.write(self.style.SUCCESS(f'Results saved to: {save_json}'))

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
            # Use JavaScript to set both date fields
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
            
            # Verify
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
        
        # Step 1: Set company code
        search_field = self.find_company_field(driver)
        if not search_field:
            return False
            
        try:
            search_field.clear()
            search_field.send_keys(company_code)
            time.sleep(2)
            
            # Trigger events
            driver.execute_script("""
                var element = arguments[0];
                element.dispatchEvent(new Event('input', { bubbles: true }));
                element.dispatchEvent(new Event('change', { bubbles: true }));
                element.dispatchEvent(new Event('keyup', { bubbles: true }));
                element.focus();
            """, search_field)
            time.sleep(3)
            
            # Handle dropdown
            if not self.handle_company_dropdown(driver):
                self.stdout.write("Dropdown failed, continuing anyway...")
                
        except Exception as e:
            self.stdout.write(f"Company setting failed: {e}")
            return False

        # Step 2: Set date range
        if not self.set_date_fields(driver, start_date, end_date):
            return False

        return True

    def submit_form(self, driver):
        """Submit form"""
        self.stdout.write("Submitting form...")
        
        try:
            # Close any date pickers
            driver.execute_script("""
                var datePickers = document.querySelectorAll('.ui-datepicker, .ui-widget-overlay');
                datePickers.forEach(function(picker) {
                    picker.style.display = 'none';
                });
                document.body.click();
            """)
            time.sleep(1)
            
            # Find and click submit button
            submit_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "btnSubmit"))
            )
            submit_button.click()
            time.sleep(5)
            
            # Wait for data to load
            for i in range(30):
                try:
                    # Wait for the Angular tables to appear
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

    def extract_category_from_table(self, table):
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
        """Extract announcements using BeautifulSoup table parsing"""
        announcements = []
        
        try:
            # Parse current page with BeautifulSoup
            soup = BeautifulSoup(driver.page_source, "html.parser")
            tables = soup.find_all("table", {"ng-repeat": "cann in CorpannData.Table"})
            
            self.stdout.write(f"Found {len(tables)} announcement tables")
            
            for idx, table in enumerate(tables):
                try:
                    # Extract main content
                    newssub_tag = table.find("span", {"ng-bind-html": "cann.NEWSSUB"})
                    headline_tag = table.find("span", {"ng-bind-html": "cann.HEADLINE"})
                    pdf_tag = table.find("a", class_="tablebluelink", href=True)

                    newssub = (newssub_tag.get_text(strip=True) if newssub_tag else "") or ""
                    headline = (headline_tag.get_text(strip=True) if headline_tag else "") or ""
                    category = self.extract_category_from_table(table)
                    pdf_link = urljoin("https://www.bseindia.com/corporates/ann.html", pdf_tag["href"]) if pdf_tag else ""

                    # Extract timestamps from the table
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

                    # Extract company information
                    company_name = newssub.split("-")[0].strip() if newssub else ""
                    code_match = re.search(r"\b(\d{6})\b", newssub)
                    extracted_company_code = code_match.group(1) if code_match else company_code

                    announcements.append({
                        "serial_no": len(announcements) + 1,
                        "company_name": company_name,
                        "company_code": extracted_company_code,
                        "headline": headline,
                        "category": category,
                        "announcement_text": headline,
                        "newssub_full": newssub,
                        "exchange_received_date": received_date,
                        "exchange_received_time": received_time,
                        "exchange_disseminated_date": disseminated_date,
                        "exchange_disseminated_time": disseminated_time,
                        "pdf_link": pdf_link,
                        "source": "table_parsing"
                    })
                    
                except Exception as e:
                    self.stdout.write(f"Error parsing table {idx+1}: {e}")
                    
        except Exception as e:
            self.stdout.write(f"Data extraction failed: {e}")
        
        return announcements

    def navigate_next_page(self, driver):
        """Navigate to next page"""
        try:
            next_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.ID, "idnext"))
            )
            
            if next_button.get_attribute("disabled"):
                return False
                
            driver.execute_script("arguments[0].click();", next_button)
            time.sleep(3)
            
            # Wait for new data to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table[ng-repeat='cann in CorpannData.Table']"))
            )
            return True
            
        except:
            return False

    def check_for_results(self, driver):
        """Check if results are available"""
        try:
            # Check for tables
            tables = driver.find_elements(By.CSS_SELECTOR, "table[ng-repeat='cann in CorpannData.Table']")
            if tables:
                return {'status': 'has_data', 'count': len(tables)}
            else:
                return {'status': 'no_data'}
        except Exception as e:
            return {'status': 'error', 'error': str(e)}

    def run_scraper(self, company_code, start_date, end_date, max_pages, debug_mode):
        """Main scraper function with pagination support - FIXED to accept correct parameters"""
        driver = None
        all_announcements = []
        
        try:
            # Setup
            driver = self.setup_driver(headless=not debug_mode)
            
            # Load page
            self.stdout.write("Loading BSE page...")
            driver.get("https://www.bseindia.com/corporates/ann.html")
            time.sleep(5)
            
            # Wait for Angular
            self.wait_for_angular_ready(driver)
            time.sleep(10)  # Wait for components
            
            # Set filters (company code and date range)
            if not self.set_filters(driver, company_code, start_date, end_date):
                return {'status': 'filter_failed', 'announcements': []}
            
            # Submit form
            if not self.submit_form(driver):
                return {'status': 'submit_failed', 'announcements': []}
            
            # Check for results
            result_status = self.check_for_results(driver)
            
            if result_status['status'] == 'has_data':
                self.stdout.write(f"Found data, starting pagination...")
                
                # Scrape pages
                page_count = 0
                
                while page_count < max_pages:
                    page_count += 1
                    self.stdout.write(f"Scraping page {page_count}...")
                    
                    # Wait a bit more for data to load
                    time.sleep(2)
                    
                    # Extract announcements from current page
                    page_announcements = self.scrape_announcements(driver, company_code)
                    
                    if not page_announcements:
                        self.stdout.write(f"No announcements found on page {page_count} - stopping")
                        break
                        
                    all_announcements.extend(page_announcements)
                    self.stdout.write(f"Page {page_count}: {len(page_announcements)} announcements")
                    
                    # Try next page (but don't fail if it doesn't work)
                    if page_count < max_pages:
                        if not self.navigate_next_page(driver):
                            self.stdout.write("Cannot navigate to next page - stopping")
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
                driver.quit()

    def display_results(self, result):
        """Display results"""
        status = result.get('status', 'unknown')
        announcements = result.get('announcements', [])
        
        self.stdout.write(f"\n{'='*60}")
        self.stdout.write(f"BSE ANNOUNCEMENTS SCRAPING RESULTS")
        self.stdout.write(f"{'='*60}")
        self.stdout.write(f"Company Code: {result.get('company_code', 'N/A')}")
        self.stdout.write(f"Date Range: {result.get('date_range', result.get('target_date', 'N/A'))}")
        if result.get('pages_scraped'):
            self.stdout.write(f"Pages Scraped: {result.get('pages_scraped', 0)}")
        self.stdout.write(f"{'='*60}")
        
        if status == 'success':
            self.stdout.write(self.style.SUCCESS(f"Success: {len(announcements)} announcements found"))
            
            if announcements:
                self.stdout.write(f"\nANNOUNCEMENTS:")
                self.stdout.write(f"{'-'*60}")
                for i, ann in enumerate(announcements, 1):
                    headline = ann.get('headline', 'N/A')
                    category = ann.get('category', 'N/A')
                    company = ann.get('company_name', 'N/A')
                    diss_date = ann.get('exchange_disseminated_date', 'N/A')
                    diss_time = ann.get('exchange_disseminated_time', 'N/A')
                    pdf_status = "PDF Available" if ann.get('pdf_link') else "No PDF"
                    
                    self.stdout.write(f"\n{i}. {headline[:80]}...")
                    self.stdout.write(f"   Company: {company}")
                    self.stdout.write(f"   Category: {category}")
                    self.stdout.write(f"   Disseminated: {diss_date} {diss_time}")
                    self.stdout.write(f"   {pdf_status}")
                    
                # Print JSON format
                self.stdout.write(f"\n{'='*60}")
                self.stdout.write("JSON FORMAT:")
                self.stdout.write(f"{'='*60}")
                print(json.dumps(result, indent=2, ensure_ascii=False))
        
        elif status == 'no_data':
            self.stdout.write(self.style.WARNING("No data found for the specified company and date range"))
        else:
            self.stdout.write(self.style.ERROR(f"Failed: {status}"))
            if result.get('error'):
                self.stdout.write(f"Error: {result['error']}")