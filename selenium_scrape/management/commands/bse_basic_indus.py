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

LABELS_TO_SCRAPE = ["Basic Industry", "Security Name", "Company Name"]

LABEL_ALIASES = {
    "Industry": "Basic Industry",
    "Company Name": "Company Name",
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

    def _enhanced_find_security_name(self) -> Dict[str, Optional[str]]:
        company_name = None
        security_name = None
        
        # ENHANCED COMPANY NAME EXTRACTION
        
        # Strategy 1: Enhanced H1 extraction with better cleaning
        try:
            wait = WebDriverWait(self.driver, ENHANCED_WAIT_TIME)
            h1_element = wait.until(
                EC.presence_of_element_located((By.XPATH, "//h1[@class='stockreach_title ng-binding']"))
            )
            
            h1_text_methods = [
                lambda: h1_element.text,
                lambda: h1_element.get_attribute('textContent'),
                lambda: h1_element.get_attribute('innerText'),
                lambda: self.driver.execute_script("return arguments[0].textContent;", h1_element),
                lambda: self.driver.execute_script("return arguments[0].innerText;", h1_element)
            ]
            
            for method_idx, method in enumerate(h1_text_methods):
                try:
                    h1_text = clean_text(method())
                    print(f"[H1-{method_idx + 1}] Raw text: '{h1_text}'")
                    
                    if h1_text and len(h1_text) > 2:
                        # Clean up common prefixes and extract actual company name
                        cleaned_h1 = h1_text
                        
                        # Remove common prefixes
                        prefixes_to_remove = [
                            r'^Industry Classification of\s+',
                            r'^Stock Quote of\s+',
                            r'^Share Price of\s+',
                            r'^Quote of\s+',
                            r'^BSE\s+',
                            r'^Stock\s+',
                            r'^Share\s+'
                        ]
                        
                        for prefix in prefixes_to_remove:
                            before_cleanup = cleaned_h1
                            cleaned_h1 = re.sub(prefix, '', cleaned_h1, flags=re.IGNORECASE).strip()
                            if before_cleanup != cleaned_h1:
                                print(f"[H1-{method_idx + 1}] Removed prefix, now: '{cleaned_h1}'")
                        
                        # Extract company name from patterns like "ABB India Ltd(500002)"
                        patterns = [
                            r'^(.+?)\s*\(\s*\d+\s*\)$',  # "Company Name(scripcode)"
                            r'^(.+?)\s*-\s*\d+$',        # "Company Name - scripcode"
                            r'^(.+?)\s*\|\s*\d+$',       # "Company Name | scripcode"
                            r'^(.+?)(?=\s*(?:\(|\-|\|)\s*\d)',  # Company name before scripcode
                            r'^(.+)$'  # Fallback - use as is if no patterns match
                        ]
                        
                        for pattern_idx, pattern in enumerate(patterns):
                            match = re.search(pattern, cleaned_h1)
                            if match:
                                extracted_name = clean_text(match.group(1))
                                print(f"[H1-{method_idx + 1}] Pattern {pattern_idx + 1} extracted: '{extracted_name}'")
                                
                                if (extracted_name and len(extracted_name) > 3 and 
                                    not is_likely_navigation_text(extracted_name) and 
                                    not is_price_or_percentage_text(extracted_name) and
                                    not extracted_name.isdigit()):
                                    company_name = extracted_name
                                    print(f"*** COMPANY NAME FOUND (H1 Method {method_idx + 1}): '{company_name}' ***")
                                    break
                        
                        if company_name:
                            break
                            
                except Exception as method_err:
                    print(f"[H1-{method_idx + 1}] Failed: {str(method_err)}")
                    continue
                    
        except TimeoutException:
            print("[H1] Element not found within timeout")
        except Exception as e:
            print(f"[H1] Search error: {str(e)}")

        # Strategy 2: Enhanced page title extraction
        if not company_name:
            try:
                print("[TITLE] Trying page title extraction...")
                page_title = self.driver.title
                if page_title:
                    print(f"[TITLE] Page title: '{page_title}'")
                    
                    # Clean the title first
                    cleaned_title = page_title
                    
                    # Remove common BSE suffixes
                    suffixes_to_remove = [
                        r'\s*-\s*BSE.*$',
                        r'\s*\|\s*BSE.*$',
                        r'\s*-\s*Stock.*$',
                        r'\s*-\s*Share.*$',
                        r'\s*-\s*Quote.*$',
                        r'\s*-\s*Price.*$'
                    ]
                    
                    for suffix in suffixes_to_remove:
                        before_cleanup = cleaned_title
                        cleaned_title = re.sub(suffix, '', cleaned_title, flags=re.IGNORECASE).strip()
                        if before_cleanup != cleaned_title:
                            print(f"[TITLE] After cleanup: '{cleaned_title}'")
                    
                    # Extract company name from patterns
                    title_patterns = [
                        r'^(.+?)\s*\(\s*\d+\s*\)$',  # "Company Name(scripcode)"
                        r'^(.+?)\s*-\s*\d+$',        # "Company Name - scripcode"
                        r'^(.+?)\s*\|\s*\d+$',       # "Company Name | scripcode"
                        r'^(.+?)(?=\s*(?:\(|\-|\|)\s*\d)',  # Company name before scripcode
                        r'^(.+)$'  # Use cleaned title as is
                    ]
                    
                    for pattern_idx, pattern in enumerate(title_patterns):
                        match = re.search(pattern, cleaned_title)
                        if match:
                            title_company_name = clean_text(match.group(1))
                            print(f"[TITLE] Pattern {pattern_idx + 1} extracted: '{title_company_name}'")
                            
                            if (title_company_name and len(title_company_name) > 3 and
                                not is_likely_navigation_text(title_company_name) and
                                not is_price_or_percentage_text(title_company_name) and
                                not title_company_name.isdigit()):
                                company_name = title_company_name
                                print(f"*** COMPANY NAME FOUND (Page Title): '{company_name}' ***")
                                break
                                
            except Exception as e:
                print(f"[TITLE] Extraction error: {str(e)}")

        # Strategy 3: Look for specific company name elements
        if not company_name:
            try:
                print("[ELEMENTS] Searching for dedicated company name elements...")
                
                company_selectors = [
                    "//span[contains(@class, 'companyname')]",
                    "//div[contains(@class, 'companyname')]",
                    "//span[contains(@class, 'company-name')]",
                    "//div[contains(@class, 'company-name')]",
                    "//span[contains(@class, 'stockname')]",
                    "//div[contains(@class, 'stockname')]",
                    "//h2[contains(@class, 'ng-binding')]",
                    "//h3[contains(@class, 'ng-binding')]",
                    "//span[@class='ng-binding'][contains(text(), 'Ltd') or contains(text(), 'Limited') or contains(text(), 'Inc') or contains(text(), 'Corp')]"
                ]
                
                for selector_idx, selector in enumerate(company_selectors):
                    try:
                        elements = self.driver.find_elements(By.XPATH, selector)
                        print(f"[ELEMENTS] Selector {selector_idx + 1} found {len(elements)} elements")
                        
                        for idx, element in enumerate(elements):
                            element_text = clean_text(element.text)
                            if element_text:
                                print(f"[ELEMENTS] Element {idx + 1}: '{element_text}'")
                                
                                # Clean and extract company name
                                cleaned_text = element_text
                                
                                # Remove unwanted prefixes
                                prefixes_to_remove = [
                                    r'^Industry Classification of\s+',
                                    r'^Stock Quote of\s+',
                                    r'^Share Price of\s+',
                                    r'^Quote of\s+',
                                    r'^Price of\s+'
                                ]
                                
                                for prefix in prefixes_to_remove:
                                    before_cleanup = cleaned_text
                                    cleaned_text = re.sub(prefix, '', cleaned_text, flags=re.IGNORECASE).strip()
                                    if before_cleanup != cleaned_text:
                                        print(f"[ELEMENTS] After prefix removal: '{cleaned_text}'")
                                
                                # Extract company name (remove scripcode in parentheses)
                                patterns = [
                                    r'^(.+?)\s*\(\s*\d+\s*\)$',  # "Company Name(scripcode)"
                                    r'^(.+?)(?=\s*\(\s*\d+\s*\))',  # Company name before (scripcode)
                                    r'^(.+)$'  # Use as is if no scripcode found
                                ]
                                
                                for pattern_idx, pattern in enumerate(patterns):
                                    match = re.search(pattern, cleaned_text)
                                    if match:
                                        extracted_name = clean_text(match.group(1))
                                        print(f"[ELEMENTS] Pattern {pattern_idx + 1} extracted: '{extracted_name}'")
                                        
                                        if (extracted_name and len(extracted_name) > 3 and 
                                            not is_likely_navigation_text(extracted_name) and 
                                            not is_price_or_percentage_text(extracted_name) and
                                            not extracted_name.isdigit() and
                                            re.search(r'[a-zA-Z]{3,}', extracted_name)):
                                            company_name = extracted_name
                                            print(f"*** COMPANY NAME FOUND (Element Search): '{company_name}' ***")
                                            break
                                
                                if company_name:
                                    break
                        
                        if company_name:
                            break
                            
                    except Exception as e:
                        print(f"[ELEMENTS] Selector {selector_idx + 1} error: {str(e)}")
                        continue
                        
            except Exception as e:
                print(f"[ELEMENTS] Search error: {str(e)}")

        # Strategy 4: Enhanced meta tag extraction
        if not company_name:
            try:
                print("[META] Trying enhanced meta tag extraction...")
                meta_queries = [
                    ("og:title", "return document.querySelector('meta[property=\"og:title\"]')?.content || null;"),
                    ("title", "return document.querySelector('meta[name=\"title\"]')?.content || null;"),
                    ("description", "return document.querySelector('meta[name=\"description\"]')?.content || null;"),
                    ("og:description", "return document.querySelector('meta[property=\"og:description\"]')?.content || null;")
                ]
                
                for query_name, query in meta_queries:
                    try:
                        meta_content = self.driver.execute_script(query)
                        if meta_content:
                            print(f"[META-{query_name}] Content: '{meta_content}'")
                            
                            # Clean meta content
                            cleaned_meta = meta_content
                            
                            # Remove BSE-specific prefixes and suffixes
                            cleaners = [
                                r'^.*?Industry Classification of\s+',
                                r'^.*?Stock Quote of\s+',
                                r'^.*?Share Price of\s+',
                                r'\s*-\s*BSE.*$',
                                r'\s*\|\s*BSE.*$',
                                r'\s*-\s*Stock.*$',
                                r'\s*-\s*Share.*$'
                            ]
                            
                            for cleaner in cleaners:
                                before_cleanup = cleaned_meta
                                cleaned_meta = re.sub(cleaner, '', cleaned_meta, flags=re.IGNORECASE).strip()
                                if before_cleanup != cleaned_meta:
                                    print(f"[META-{query_name}] After cleanup: '{cleaned_meta}'")
                            
                            # Extract company name
                            meta_patterns = [
                                r'^(.+?)\s*\(\s*\d+\s*\)$',  # "Company Name(scripcode)"
                                r'^(.+?)(?=\s*\(\s*\d+\s*\))',  # Company name before (scripcode)
                                r'^(.+)$'  # Use cleaned content as is
                            ]
                            
                            for pattern_idx, pattern in enumerate(meta_patterns):
                                match = re.search(pattern, cleaned_meta)
                                if match:
                                    meta_company_name = clean_text(match.group(1))
                                    print(f"[META-{query_name}] Pattern {pattern_idx + 1} extracted: '{meta_company_name}'")
                                    
                                    if (meta_company_name and len(meta_company_name) > 3 and
                                        not is_likely_navigation_text(meta_company_name) and
                                        not is_price_or_percentage_text(meta_company_name) and
                                        re.search(r'[a-zA-Z]{3,}', meta_company_name)):
                                        company_name = meta_company_name
                                        print(f"*** COMPANY NAME FOUND (Meta Tag {query_name}): '{company_name}' ***")
                                        break
                            
                            if company_name:
                                break
                                
                    except Exception as e:
                        print(f"[META-{query_name}] Error: {str(e)}")
                        continue
                        
            except Exception as e:
                print(f"[META] Extraction error: {str(e)}")

        # Strategy 5: JavaScript-based company name search
        if not company_name:
            try:
                print("[JS] Trying JavaScript-based company name extraction...")
                js_company_search = """
                        for (var j = 0; j < elements.length; j++) {
                            var text = elements[j].textContent || elements[j].innerText || '';
                            text = text.trim();
                            
                            if (text.length > 5 && text.length < 150) {
                                // Remove common prefixes
                                text = text.replace(/^Industry Classification of\\s+/i, '');
                                text = text.replace(/^Stock Quote of\\s+/i, '');
                                text = text.replace(/^Share Price of\\s+/i, '');
                                
                                // Extract company name (remove scripcode)
                                var patterns = [
                                    /^(.+?)\\s*\\(\\s*\\d+\\s*\\)$/,  // "Company Name(scripcode)"
                                    /^(.+?)(?=\\s*\\(\\s*\\d+\\s*\\))/,  // Company name before (scripcode)
                                    /^(.+)$/  // Use as is
                                ];
                                
                                for (var p = 0; p < patterns.length; p++) {
                                    var match = text.match(patterns[p]);
                                    if (match && match[1]) {
                                        var candidate = match[1].trim();
                                        if (candidate.length > 3 && 
                                            /[a-zA-Z]{3,}/.test(candidate) &&
                                            !/(click|menu|home|login|search|button)/i.test(candidate) &&
                                            !/^[\\d\\s\\-\\.%]+$/.test(candidate)) {
                                            return candidate;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    return null;
                }
                return findCompanyName();
                """
                
                js_company_result = self.driver.execute_script(js_company_search)
                if js_company_result:
                    js_company_name = clean_text(js_company_result)
                    print(f"[JS] JavaScript found: '{js_company_name}'")
                    
                    if (js_company_name and len(js_company_name) > 3 and
                        not is_likely_navigation_text(js_company_name) and
                        not is_price_or_percentage_text(js_company_name)):
                        company_name = js_company_name
                        print(f"*** COMPANY NAME FOUND (JavaScript Search): '{company_name}' ***")
                        
            except Exception as e:
                print(f"[JS] Search error: {str(e)}")

        # Final validation and cleanup
        if company_name:
            # Final cleanup of the company name
            original_name = company_name
            company_name = company_name.strip()
            
            # Remove any remaining unwanted patterns
            final_cleaners = [
                r'\s*\(\s*\d+\s*\)$',  # Remove trailing (scripcode)
                r'^Industry Classification of\s+',
                r'^Stock Quote of\s+',
                r'^Share Price of\s+'
            ]
            
            for cleaner in final_cleaners:
                before_final = company_name
                company_name = re.sub(cleaner, '', company_name, flags=re.IGNORECASE).strip()
                if before_final != company_name:
                    print(f"[FINAL] Final cleanup: '{before_final}' -> '{company_name}'")
            
            print(f"*** FINAL COMPANY NAME: '{company_name}' ***")
        else:
            print("*** NO COMPANY NAME FOUND ***")

        # SECURITY NAME EXTRACTION - UNCHANGED (since it's working)
        try:
            elements = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'ng-binding') and contains(text(), '|')]")
            for element in elements:
                text = clean_text(element.text)
                if text and '|' in text and not is_price_or_percentage_text(text) and not is_likely_navigation_text(text):
                    security_name = extract_company_name(text)
                    if security_name and len(security_name) > 3:
                        print(f"*** SECURITY NAME FOUND: '{security_name}' ***")
                        break
        except Exception as e:
            print(f"[SECURITY] Error finding security name: {str(e)}")

        return {
            "Company Name": company_name,
            "Security Name": security_name
        }

    def _enhanced_find_basic_industry(self) -> Optional[str]:
        print("Trying comprehensive table search...")
        try:
            industry_cells = self.driver.find_elements(By.XPATH, "//td[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'industry')]")
            print(f"Debug: Found {len(industry_cells)} potential 'industry' label cells.")
            for idx, cell in enumerate(industry_cells, start=1):
                label_text = clean_text(cell.text)
                print(f"Debug: Checking cell {idx}/{len(industry_cells)} - Label text: '{label_text}'")
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
                    
                    next_cell = cell.find_element(By.XPATH, "./following-sibling::td[1]")
                    raw_text = next_cell.text
                    text = clean_text(raw_text)
                    print(f"Debug: Candidate value - Raw: '{raw_text}', Cleaned: '{text}'")
                    
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
                    print(f"*** BASIC INDUSTRY FOUND: '{text}' ***")
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
        name_data = self._enhanced_find_security_name()
        basic_industry = self._enhanced_find_basic_industry()
        print(f"=== EXTRACTION SUMMARY ===")
        print(f"  Company Name: {'FOUND' if name_data['Company Name'] else 'MISSING'} - '{name_data['Company Name']}'")
        print(f"  Security Name: {'FOUND' if name_data['Security Name'] else 'MISSING'} - '{name_data['Security Name']}'")
        print(f"  Basic Industry: {'FOUND' if basic_industry else 'MISSING'} - '{basic_industry}'")
        print(f"========================")
        return {
            "Company Name": name_data["Company Name"],
            "Security Name": name_data["Security Name"],
            "Basic Industry": basic_industry
        }

    def scrape_scripcode_enhanced(self, scripcode: str) -> Dict[str, Optional[str]]:
        try:
            self.enhanced_open_scrip(scripcode)
            data = self.enhanced_extract_data()
            data["scripcode"] = scripcode
            data["scraped_at"] = timezone.now()
            return data
        except Exception as e:
            print(f"Failed for {scripcode}: {str(e)}")
            return {
                "scripcode": scripcode,
                "scraped_at": timezone.now(),
                "error": str(e),
                "Company Name": None,
                "Security Name": None,
                "Basic Industry": None
            }

class Command(BaseCommand):
    help = "Streamlined BSE stock quotes scraper for Company Name, Security Name, and Basic Industry."

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

                # Print scraped data to terminal for verification
                self.stdout.write(self.style.NOTICE("Scraped Data:"))
                print(f"  Scripcode: {row['scripcode']}")
                print(f"  Company Name: {'NOT FOUND' if not row.get('Company Name') else row['Company Name']}")
                print(f"  Security Name: {'NOT FOUND' if not row.get('Security Name') else row['Security Name']}")
                print(f"  Basic Industry: {'NOT FOUND' if not row.get('Basic Industry') else row['Basic Industry']}")
                print(f"  Scraped At: {row['scraped_at']}")
                if row.get('error'):
                    print(f"  Error: {row['error']}")

                try:
                    with transaction.atomic():
                        scraped_datetime = row['scraped_at']
                        error_msg = row.get('error')

                        if error_msg:
                            stock_quote, created = BseStockQuote.objects.update_or_create(
                                scripcode=scripcode,
                                defaults={
                                    'error_message': error_msg,
                                    'scraped_at': scraped_datetime,
                                    'security_name': None,
                                    'company_name': None,
                                    'basic_industry': None,
                                }
                            )
                            self.stderr.write(self.style.ERROR(f"Error for {scripcode}: {error_msg}"))
                        else:
                            company_name = row.get('Company Name')
                            security_name = row.get('Security Name')
                            basic_industry = row.get('Basic Industry')
                            stock_quote, created = BseStockQuote.objects.update_or_create(
                                scripcode=scripcode,
                                defaults={
                                    'security_name': security_name,
                                    'company_name': company_name,
                                    'basic_industry': basic_industry,
                                    'scraped_at': scraped_datetime,
                                    'error_message': None,
                                }
                            )
                            action = "Created" if created else "Updated"
                            if company_name and security_name and basic_industry:
                                self.stdout.write(self.style.SUCCESS(f"{action} COMPLETE: {scripcode}"))
                                print(f"  ✓ Company Name: {company_name}")
                                print(f"  ✓ Security Name: {security_name}")
                                print(f"  ✓ Industry: {basic_industry}")
                            else:
                                self.stdout.write(self.style.WARNING(f"{action} PARTIAL: {scripcode}"))
                                print(f"  ✓ Company Name: {'NOT FOUND' if not company_name else company_name}")
                                print(f"  ✓ Security Name: {'NOT FOUND' if not security_name else security_name}")
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
                            'company_name': None,
                            'basic_industry': None,
                        }
                    )
            except Exception:
                pass
            raise CommandError(error_msg)

        elapsed = time.time() - start_time
        self.stdout.write(self.style.SUCCESS(f"Completed {scripcode} in {elapsed:.2f} seconds"))