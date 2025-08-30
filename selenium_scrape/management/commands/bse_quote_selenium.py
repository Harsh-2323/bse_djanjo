import re
import time
import sys
import os
from datetime import datetime
from typing import Dict, List, Optional
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone
from selenium import webdriver
from dateutil import parser
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium_scrape.models import BseStockQuote

# Force UTF-8 encoding for console output
if sys.platform.startswith('win'):
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except AttributeError:
        pass

BSE_STOCK_URL = "https://www.bseindia.com/stock-share-price/undefined/undefined/{scripcode}/"

LABELS_TO_SCRAPE = ["Basic Industry", "Security Name"]

LABEL_ALIASES = {
    "Industry": "Basic Industry",
    "Company Name": "Security Name",
    "Name": "Security Name",
}

# Wait times optimized for speed
ENHANCED_WAIT_TIME = 30  # Reduced for faster execution
MINIMAL_SLEEP = 2.0  # Minimal sleep for stabilization
BATCH_SIZE = 5

def clean_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s or None

def is_likely_navigation_text(text: str) -> bool:
    if not text:
        return True
    nav_indicators = [
        "skip to main content", "high contrast", "reset", "select language",
        "group websites", "notices", "media release", "trading holidays",
        "contact us", "feedback", "bse sme", "bseplus", "payments to bse",
        "home", "menu", "login", "register", "search", "help"
    ]
    text_lower = text.lower()
    return any(indicator in text_lower for indicator in nav_indicators)

def extract_company_name(text: str) -> Optional[str]:
    if not text:
        return None
    match = re.match(r"^\s*\(\s*([^|]+?)\s*\|\s*[^|]+\s*\|\s*[^)]+\)\s*$", text)
    if match:
        return clean_text(match.group(1))
    if '|' in text:
        parts = text.split('|')
        if len(parts) >= 1:
            return clean_text(parts[0].strip('()'))
    return clean_text(text)

def is_price_or_percentage_text(text: str) -> bool:
    if not text:
        return False
    price_indicators = [
        r'[-+]?\d+\.?\d*%',
        r'[-+]?\d+\.?\d*\s*[-+]\s*\d+\.?\d*%',
        r'\d+\.\d+\s*[-+]\s*\d+\.\d+%',
        r'(high|low|open|close|volume|ltp|change)',
        r'announcements?\s+financials?\s+meet',
        r'corp\s+announcements',
        r'\d{4}-\d{2}-\d{2}',
        r'^\s*\d+\.\d+\s*$',
        r'volume:\s*\d+',
    ]
    text_lower = text.lower()
    for pattern in price_indicators:
        if re.search(pattern, text_lower):
            return True
    return False

def check_for_block_page(driver) -> Optional[str]:
    try:
        body_text = driver.find_element(By.TAG_NAME, "body").text.lower()
        title_text = driver.title.lower()
        block_indicators = [
            "captcha", "access denied", "please verify you are not a robot", 
            "403 forbidden", "blocked", "security check", "unusual traffic",
            "verify you are human", "cloudflare", "rate limit"
        ]
        full_text = f"{body_text} {title_text}"
        for indicator in block_indicators:
            if indicator in full_text:
                return f"Blocked by {indicator}"
        return None
    except Exception:
        return None

class EnhancedBSEQuoteScraper:
    def __init__(self, headless: bool = True, page_timeout: int = 30):
        self.headless = headless
        self.page_timeout = page_timeout
        self.driver = None

    def __enter__(self):
        self.driver = self._new_driver()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.driver:
                self.driver.quit()
        finally:
            self.driver = None

    def _new_driver(self):
        chrome_opts = ChromeOptions()
        if self.headless:
            chrome_opts.add_argument("--headless=new")
        chrome_opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        chrome_opts.add_argument("--disable-gpu")
        chrome_opts.add_argument("--no-sandbox")
        chrome_opts.add_argument("--disable-dev-shm-usage")
        chrome_opts.add_argument("--disable-extensions")
        chrome_opts.add_argument("--disable-plugins")
        chrome_opts.add_argument("--disable-images")
        chrome_opts.add_argument("--disable-notifications")
        chrome_opts.add_argument("--disable-sync")
        chrome_opts.add_experimental_option("prefs", {
            "profile.default_content_setting_values.notifications": 2,
            "profile.default_content_settings.popups": 0,
            "profile.managed_default_content_settings.images": 2,
        })
        chrome_opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        service = Service(ChromeDriverManager().install(), log_level=0)
        driver = webdriver.Chrome(service=service, options=chrome_opts)
        driver.set_page_load_timeout(self.page_timeout)
        driver.implicitly_wait(1)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return driver

    def enhanced_open_scrip(self, scripcode: str):
        url = BSE_STOCK_URL.format(scripcode=scripcode)
        print(f"Loading: {scripcode}")
        try:
            self.driver.get(url)
            block_message = check_for_block_page(self.driver)
            if block_message:
                raise TimeoutException(f"Blocked by BSE: {block_message}")
            wait = WebDriverWait(self.driver, ENHANCED_WAIT_TIME)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(MINIMAL_SLEEP)
        except TimeoutException as e:
            print(f"Timeout for {scripcode}: {str(e)}")
            raise
        except Exception as e:
            print(f"Failed to load {scripcode}: {str(e)}")
            raise

    def _enhanced_find_security_name(self) -> Optional[str]:
        try:
            elements = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'ng-binding') and contains(text(), '|')]")
            for element in elements:
                text = clean_text(element.text)
                if text and '|' in text and not is_price_or_percentage_text(text) and not is_likely_navigation_text(text):
                    company_name = extract_company_name(text)
                    if company_name and len(company_name) > 3:
                        return company_name
        except Exception:
            pass
        return None

    def _enhanced_find_basic_industry(self) -> Optional[str]:
        print("Trying comprehensive table search...")
        try:
            industry_cells = self.driver.find_elements(By.XPATH, "//td[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'industry')]")
            print(f"Debug: Found {len(industry_cells)} potential 'industry' label cells.")
            for idx, cell in enumerate(industry_cells, start=1):
                label_text = clean_text(cell.text)
                print(f"Debug: Checking cell {idx}/{len(industry_cells)} - Label text: '{label_text}'")
                try:
                    # Get a dynamic XPath for the cell (for location debugging)
                    try:
                        cell_xpath = self.driver.execute_script(
                            "function getXPath(element) {"
                            "   if (element.id !== '') return 'id(\"' + element.id + '\")';"
                            "   if (element === document.body) return '//' + element.tagName.toLowerCase();"
                            "   var ix = 0;"
                            "   var siblings = element.parentNode.childNodes;"
                            "   for (var i = 0; i < siblings.length; i++) {"
                            "       var sibling = siblings[i];"
                            "       if (sibling === element) return getXPath(element.parentNode) + '/' + element.tagName.toLowerCase() + '[' + (ix + 1) + ']';"
                            "       if (sibling.nodeType === 1 && sibling.tagName === element.tagName) ix++;"
                            "   }"
                            "};"
                            "return getXPath(arguments[0]);",
                            cell
                        )
                        print(f"Debug: Label cell location (XPath): {cell_xpath}")
                    except Exception as xpath_err:
                        print(f"Debug: Could not get XPath for label cell: {str(xpath_err)}")
                    
                    # Find next cell
                    next_cell = cell.find_element(By.XPATH, "./following-sibling::td[1]")
                    raw_text = next_cell.text
                    text = clean_text(raw_text)
                    print(f"Debug: Candidate value - Raw: '{raw_text}', Cleaned: '{text}'")
                    
                    # Validation checks with reasons
                    if not text:
                        print("Debug: Rejected - Empty text")
                        continue
                    if is_likely_navigation_text(text):
                        print("Debug: Rejected - Likely navigation text")
                        continue
                    if len(text) >= 200:
                        print("Debug: Rejected - Text too long (>=200 chars)")
                        continue
                    if len(text) <= 2:
                        print("Debug: Rejected - Text too short (<=2 chars)")
                        continue
                    
                    print("Debug: Accepted - Valid candidate found")
                    return text
                except NoSuchElementException:
                    print(f"Debug: No next sibling td for cell {idx}")
                except Exception as cell_err:
                    print(f"Debug: Error processing cell {idx}: {str(cell_err)}")
                    continue
            print("Debug: No valid Basic Industry found after checking all cells")
        except Exception as e:
            print(f"Comprehensive table search failed: {e}")
        return None

    def enhanced_extract_data(self) -> Dict[str, Optional[str]]:
        security_name = self._enhanced_find_security_name()
        basic_industry = self._enhanced_find_basic_industry()
        print(f"Extraction results:")
        print(f"  Security Name: {'Found' if security_name else 'Missing'} - '{security_name}'")
        print(f"  Basic Industry: {'Found' if basic_industry else 'Missing'} - '{basic_industry}'")
        return {
            "Security Name": security_name,
            "Basic Industry": basic_industry
        }

    def scrape_scripcode_enhanced(self, scripcode: str) -> Dict[str, Optional[str]]:
        try:
            self.enhanced_open_scrip(scripcode)
            data = self.enhanced_extract_data()
            data["scripcode"] = scripcode
            data["scraped_at"] = timezone.now()  # Use timezone-aware datetime directly
            return data
        except Exception as e:
            print(f"Failed for {scripcode}: {str(e)}")
            return {
                "scripcode": scripcode,
                "scraped_at": timezone.now(),  # Use timezone-aware datetime
                "error": str(e),
                "Security Name": None,
                "Basic Industry": None
            }

class Command(BaseCommand):
    help = "Streamlined BSE stock quotes scraper using Comprehensive Table Search for Basic Industry."

    def add_arguments(self, parser):
        parser.add_argument("--scripcode", type=str, required=True, help="BSE scrip code, e.g. 500325")
        parser.add_argument("--sleep", type=float, default=2.0, help="Sleep after page load (default: 2.0 seconds)")
        parser.add_argument("--headful", action="store_true", help="Run Chrome in non-headless mode")
        parser.add_argument("--delay", type=float, default=2.0, help="Delay between requests (default: 2.0 seconds)")
        parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help=f"Batch size (default: {BATCH_SIZE})")

    def handle(self, *args, **options):
        scripcode = options["scripcode"].strip()
        post_sleep = float(options["sleep"])
        headful = bool(options["headful"])
        delay = float(options["delay"])
        batch_size = int(options["batch_size"])

        if not scripcode:
            raise CommandError("Provide a scripcode via --scripcode")

        if not scripcode.isdigit():
            self.stdout.write(self.style.WARNING(f"Warning: '{scripcode}' may not be a valid BSE scrip code"))

        self.stdout.write(self.style.NOTICE(f"Scraping scripcode: {scripcode}"))

        start_time = time.time()

        try:
            with EnhancedBSEQuoteScraper(headless=not headful, page_timeout=30) as scraper:
                self.stdout.write(f"Processing: {scripcode}")
                row = scraper.scrape_scripcode_enhanced(scripcode)

                try:
                    with transaction.atomic():
                        scraped_datetime = row['scraped_at']  # Already timezone-aware
                        error_msg = row.get('error')

                        if error_msg:
                            stock_quote, created = BseStockQuote.objects.update_or_create(
                                scripcode=scripcode,
                                defaults={
                                    'error_message': error_msg,
                                    'scraped_at': scraped_datetime,
                                    'security_name': None,
                                    'basic_industry': None,
                                }
                            )
                            self.stderr.write(self.style.ERROR(f"Error for {scripcode}: {error_msg}"))
                        else:
                            security_name = row.get('Security Name')
                            basic_industry = row.get('Basic Industry')
                            stock_quote, created = BseStockQuote.objects.update_or_create(
                                scripcode=scripcode,
                                defaults={
                                    'security_name': security_name,
                                    'basic_industry': basic_industry,
                                    'scraped_at': scraped_datetime,
                                    'error_message': None,
                                }
                            )
                            action = "Created" if created else "Updated"
                            if security_name and basic_industry:
                                self.stdout.write(self.style.SUCCESS(f"{action} COMPLETE: {scripcode}"))
                                print(f"  ✓ Name: {security_name}")
                                print(f"  ✓ Industry: {basic_industry}")
                            else:
                                self.stdout.write(self.style.WARNING(f"{action} PARTIAL: {scripcode}"))
                                print(f"  ✓ Name: {'NOT FOUND' if not security_name else security_name}")
                                print(f"  ✗ Industry: {'NOT FOUND' if not basic_industry else basic_industry}")

                except Exception as db_error:
                    error_msg = f"Database error: {str(db_error)}"
                    self.stderr.write(self.style.ERROR(error_msg))
                    raise CommandError(error_msg)

                if delay > 0:
                    time.sleep(delay)

        except Exception as e:
            error_msg = f"Scraping failed: {str(e)}"
            self.stderr.write(self.style.ERROR(error_msg))
            try:
                with transaction.atomic():
                    BseStockQuote.objects.update_or_create(
                        scripcode=scripcode,
                        defaults={
                            'error_message': error_msg,
                            'scraped_at': timezone.now(),
                            'security_name': None,
                            'basic_industry': None,
                        }
                    )
            except Exception:
                pass
            raise CommandError(error_msg)

        elapsed = time.time() - start_time
        self.stdout.write(self.style.SUCCESS(f"Completed {scripcode} in {elapsed:.2f} seconds"))