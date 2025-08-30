import re
import time
import json
from datetime import datetime
from typing import Dict, List, Optional

from django.core.management.base import BaseCommand, CommandError

# Selenium / Browser
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Driver manager (auto installs compatible ChromeDriver)
from webdriver_manager.chrome import ChromeDriverManager

# Excel / Data
import pandas as pd

# API fallback
import requests


BSE_STOCK_URL = "https://www.bseindia.com/stock-share-price/undefined/undefined/{scripcode}/"

# Labels we want to scrape from the rendered DOM.
LABELS_TO_SCRAPE = [
    # Price / OHLC
    "LTP",
    "Change",
    "Prev Close",
    "Open",
    "High",
    "Low",
    "VWAP",

    # 52W / Bands
    "52W High",
    "52W Low",
    "Upper Price Band",
    "Lower Price Band",

    # Vol / Value
    "TTQ",
    "Turnover (Lakh)",
    "Avg Qty 2W",

    # Market Cap
    "Mcap Full (Cr.)",
    "Mcap FF (Cr.)",

    # Fundamentals
    "EPS (TTM)",
    "CEPS (TTM)",
    "PE",
    "PB",
    "ROE",
    "Face Value",

    # Classification
    "Category",
    "Group",
    "Index",
    "Basic Industry",
]

# Map aliases → canonical label (when DOM uses slightly different text)
LABEL_ALIASES = {
    "Turnover (Lac)": "Turnover (Lakh)",
    "Turnover (Lakhs)": "Turnover (Lakh)",
    "TTQ (Qty)": "TTQ",
    "52 Week High": "52W High",
    "52 Week Low": "52W Low",
    "FaceVal": "Face Value",
    "Index Name": "Index",
    "Industry": "Basic Industry",
    "Mcap Full (Cr)": "Mcap Full (Cr.)",
    "Mcap FF (Cr)": "Mcap FF (Cr.)",
    "2W Avg Qty": "Avg Qty 2W",
}


# ----------------- Utils -----------------

def clean_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s or None


def as_float_or_str(v: Optional[str]) -> Optional[str]:
    """Keep numeric-looking values tidy; leave others as-is."""
    if v is None:
        return None
    tv = v.replace(",", "").strip()
    # Handle negative values and percentages
    if re.fullmatch(r"[+-]?\d+(?:\.\d+)?%?", tv):
        return tv.replace("%", "")  # Remove % for cleaner numeric data
    return v


def is_likely_navigation_text(text: str) -> bool:
    """Filter out navigation/header text that shouldn't be data values."""
    if not text:
        return True
    
    nav_indicators = [
        "skip to main content",
        "high contrast",
        "reset",
        "select language",
        "group websites",
        "notices",
        "media release",
        "trading holidays",
        "contact us",
        "feedback",
        "bse sme",
        "bseplus",
        "payments to bse"
    ]
    
    text_lower = text.lower()
    return any(indicator in text_lower for indicator in nav_indicators)


def extract_numeric_from_combined(text: str, target_label: str) -> Optional[str]:
    """Extract specific numeric values from combined text like '52 Wk High 1,003.20 52 Wk Low 530.50'."""
    if not text or not target_label:
        return None
    
    patterns = {
        "52W High": [r"52\s*W(?:eek)?\s*High\s*:?\s*([\d,]+(?:\.\d+)?)", r"52\s*Wk\s*High\s*:?\s*([\d,]+(?:\.\d+)?)"],
        "52W Low": [r"52\s*W(?:eek)?\s*Low\s*:?\s*([\d,]+(?:\.\d+)?)", r"52\s*Wk\s*Low\s*:?\s*([\d,]+(?:\.\d+)?)"],
        "Change": [r"Change\s*:?\s*([+-]?[\d,]+(?:\.\d+)?(?:\s*\([+-]?[\d,]+(?:\.\d+)?%?\))?)", r"Chg\s*:?\s*([+-]?[\d,]+(?:\.\d+)?)"],
        "PE": [r"PE\s*[:/]?\s*([+-]?[\d,]+(?:\.\d+)?)", r"P/E\s*:?\s*([+-]?[\d,]+(?:\.\d+)?)"],
        "PB": [r"PB\s*[:/]?\s*([+-]?[\d,]+(?:\.\d+)?)", r"P/B\s*:?\s*([+-]?[\d,]+(?:\.\d+)?)"],
        "ROE": [r"ROE\s*:?\s*([+-]?[\d,]+(?:\.\d+)?%?)", r"Return\s+on\s+Equity\s*:?\s*([+-]?[\d,]+(?:\.\d+)?%?)"],
        "Basic Industry": [r"Basic\s+Industry\s*:?\s*([A-Za-z\s&,-]+)(?:\s|$)", r"Industry\s*:?\s*([A-Za-z\s&,-]+)(?:\s|$)"],
        "Group": [r"Group\s*[:/]?\s*([A-Za-z0-9\s/+.-]+?)(?:\s+[A-Z]|$)", r"Settlement\s+Type\s*:?\s*([A-Za-z0-9\s/+.-]+?)(?:\s|$)"],
    }
    
    if target_label in patterns:
        for pattern in patterns[target_label]:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                result = match.group(1).strip()
                if target_label in ["Basic Industry", "Group"] and len(result) > 50:
                    result = result[:50].strip()
                return result
    
    escaped_label = re.escape(target_label).replace(r"\ ", r"\s+")
    generic_patterns = [
        rf"{escaped_label}\s*:?\s*([\d,]+(?:\.\d+)?%?)",
        rf"{escaped_label}\s*[:/]\s*([A-Za-z0-9\s,.-]+?)(?:\s+[A-Z]|$)",
    ]
    
    for pattern in generic_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    
    return None


# ----------------- Industry Classification Helpers -----------------

def _click_industry_info_button(driver, debug: bool = False) -> Dict[str, str]:
    """Find and click the 'i' button to get industry classification info."""
    try:
        info_buttons = driver.find_elements(By.XPATH, "//td[@class='textsr' and contains(text(), 'Basic Industry')]/a[@data-bs-toggle='modal' and @data-bs-target='#catinfo']")
        
        if not info_buttons:
            info_buttons = driver.find_elements(By.XPATH, 
                "//*[contains(text(), 'Basic Industry')]/a[@data-bs-toggle='modal' and contains(@class, 'social-icon') and descendant::img[@src='/include/images/iicon.png']]")
        
        for button in info_buttons:
            try:
                driver.execute_script("arguments[0].scrollIntoView(true);", button)
                time.sleep(0.5)
                button.click()
                WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.ID, "catinfo")))
                industry_info = _extract_industry_classification_modal(driver, debug=debug)
                if industry_info:
                    _close_modal(driver)
                    return industry_info
            except Exception as e:
                if debug:
                    print(f"Error clicking info button: {e}")
                continue
        if debug:
            print("No industry info button found")
        return {}
    except Exception as e:
        if debug:
            print(f"Error finding industry info button: {e}")
        return {}


def _extract_industry_classification_modal(driver, debug: bool = False) -> Dict[str, str]:
    """Extract industry classification information from the opened modal."""
    try:
        industry_info = {
            "Macro Economic Indicator": "",
            "Sector": "",
            "Industry": "",
            "Basic Industry": ""
        }
        
        modal = driver.find_element(By.ID, "catinfo")
        table = modal.find_element(By.TAG_NAME, "table")
        rows = table.find_elements(By.TAG_NAME, "tr")
        
        if debug:
            print(f"Modal rows found: {len(rows)}")
        
        data_rows = [row for row in rows if len(row.find_elements(By.TAG_NAME, "td")) == 3]
        for row in data_rows:
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) == 3:
                key = clean_text(cells[0].text).lower()
                value = clean_text(cells[2].text)
                if "macro-economic indicator" in key:
                    industry_info["Macro Economic Indicator"] = value
                elif "sector" in key:
                    industry_info["Sector"] = value
                elif "industry" in key and "basic" not in key:
                    industry_info["Industry"] = value
                elif "basic industry" in key:
                    industry_info["Basic Industry"] = value
        
        if debug:
            print(f"Extracted industry info: {industry_info}")
        return industry_info
    except Exception as e:
        if debug:
            print(f"Error extracting industry classification: {e}")
        return {}


def _close_modal(driver):
    """Close any open modal/popup."""
    try:
        close_button = driver.find_element(By.CLASS_NAME, "btn-close")
        close_button.click()
        time.sleep(0.5)
    except Exception:
        from selenium.webdriver.common.keys import Keys
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
        time.sleep(0.5)


def _fetch_industry_from_api(scripcode: str) -> Dict[str, str]:
    """
    Read classification directly from BSE 'ComHeadernew' API and map it to the four fields
    shown in the 'i' modal. Returns empty strings if anything is missing.
    """
    out = {
        "Macro Economic Indicator": "",
        "Sector": "",
        "Industry": "",
        "Basic Industry": "",
    }
    try:
        url = f"https://api.bseindia.com/BseIndiaAPI/api/ComHeadernew/w?quotetype=EQ&scripcode={scripcode}&seriesid="
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        j = r.json() if r.content else {}

        # Modal "Macro-Economic Indicator"  ← API 'Sector'
        # Modal "Sector"                    ← API 'IndustryNew'
        # Modal "Industry"                  ← API 'IGroup'
        # Modal "Basic Industry"            ← API 'Industry' (fallback 'ISubGroup')
        out["Macro Economic Indicator"] = (j.get("Sector") or "").strip()
        out["Sector"] = (j.get("IndustryNew") or "").strip()
        out["Industry"] = (j.get("IGroup") or "").strip()
        out["Basic Industry"] = (j.get("Industry") or j.get("ISubGroup") or "").strip()
    except Exception:
        pass
    return out


# ----------------- Scraper -----------------

class BSEQuoteScraper:
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
        chrome_opts.add_argument("--disable-gpu")
        chrome_opts.add_argument("--no-sandbox")
        chrome_opts.add_argument("--window-size=1400,1000")
        chrome_opts.add_argument("--disable-dev-shm-usage")
        chrome_opts.add_argument("--disable-blink-features=AutomationControlled")
        chrome_opts.add_argument("--lang=en-US,en")
        chrome_opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")

        chrome_prefs = {"profile.default_content_setting_values.notifications": 2}
        chrome_opts.add_experimental_option("prefs", chrome_prefs)

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_opts)
        driver.set_page_load_timeout(self.page_timeout)
        driver.implicitly_wait(0)
        return driver

    def open_scrip(self, scripcode: str, total_wait: int = 20):
        url = BSE_STOCK_URL.format(scripcode=scripcode)
        print(f"Opening URL: {url}")
        self.driver.get(url)

        wait = WebDriverWait(self.driver, total_wait)
        success = False
        wait_strategies = [
            "//div[contains(@class, 'stock-detail') or contains(@class, 'quote')]",
            "//*[contains(text(), 'LTP') or contains(text(), 'Last Traded Price')]",
            "//table[.//td[contains(text(), 'LTP')]]",
            "//*[contains(text(), 'BSE') and contains(text(), 'Stock')]"
        ]
        for strategy in wait_strategies:
            try:
                wait.until(EC.presence_of_element_located((By.XPATH, strategy)))
                success = True
                print(f"Page loaded successfully using strategy: {strategy}")
                break
            except TimeoutException:
                continue
        if not success:
            print("Warning: Could not confirm page loaded properly")
        time.sleep(2)

    def _find_value_by_multiple_strategies(self, label: str) -> Optional[str]:
        d = self.driver
        
        table_strategies = [
            f"//table//tr[td[normalize-space(text())='{label}'] or td[normalize-space(text())='{label}:']]/td[2]",
            f"//table//tr[th[normalize-space(text())='{label}'] or th[normalize-space(text())='{label}:']]/td[1]",
            f"//table//tr[td[normalize-space(text())='{label}'] or td[normalize-space(text())='{label}:']]/td[last()]",
        ]
        for xpath in table_strategies:
            try:
                element = d.find_element(By.XPATH, xpath)
                text = clean_text(element.text)
                if text and not is_likely_navigation_text(text):
                    return text
            except NoSuchElementException:
                continue
        
        div_strategies = [
            f"//div[span[normalize-space(text())='{label}:' or normalize-space(text())='{label}']]/span[last()]",
            f"//div[contains(@class, 'data') or contains(@class, 'info')][.//text()[normalize-space()='{label}:' or normalize-space()='{label}']]//*[self::span or self::div][last()]",
            f"//*[normalize-space(text())='{label}:']/following-sibling::*[1]",
            f"//*[normalize-space(text())='{label}']/following-sibling::*[1]",
            f"//*[normalize-space(text())='{label}:']/following::*[self::span or self::div or self::strong or self::b][1]",
        ]
        for xpath in div_strategies:
            try:
                element = d.find_element(By.XPATH, xpath)
                text = clean_text(element.text)
                if text and not is_likely_navigation_text(text) and text.lower() != label.lower():
                    return text
            except NoSuchElementException:
                continue
        
        bse_specific_strategies = [
            f"//*[contains(@class, 'value') or contains(@class, 'price') or contains(@class, 'data')][preceding-sibling::*[normalize-space(text())='{label}:' or normalize-space(text())='{label}']]",
            f"//div[contains(@class, 'quote') or contains(@class, 'stock')]//*[normalize-space(text())='{label}:']/following-sibling::*[1]",
            f"//*[text()[normalize-space()='{label}:' or normalize-space()='{label}']]/following-sibling::text()[1]",
        ]
        for xpath in bse_specific_strategies:
            try:
                if xpath.endswith("/text()[1]"):
                    elements = d.find_elements(By.XPATH, xpath.replace("/text()[1]", ""))
                    for element in elements:
                        script = """
                        var walker = document.createTreeWalker(
                            arguments[0].parentNode,
                            NodeFilter.SHOW_TEXT,
                            null,
                            false
                        );
                        var node = walker.nextNode();
                        while (node && node !== arguments[0].firstChild) {
                            node = walker.nextNode();
                        }
                        if (node) node = walker.nextNode();
                        return node ? node.textContent.trim() : '';
                        """
                        try:
                            text = d.execute_script(script, element)
                            if text and not is_likely_navigation_text(text):
                                return text
                        except Exception:
                            continue
                else:
                    element = d.find_element(By.XPATH, xpath)
                    text = clean_text(element.text)
                    if text and not is_likely_navigation_text(text):
                        return text
            except NoSuchElementException:
                continue
        
        try:
            page_text = d.find_element(By.TAG_NAME, "body").text
            extracted = extract_numeric_from_combined(page_text, label)
            if extracted:
                return extracted
        except Exception:
            pass
        
        flexible_strategies = [
            f"//*[contains(normalize-space(text()), '{label}')]/following::*[self::span or self::div or self::td][1]",
            f"//*[self::div or self::span or self::li][contains(normalize-space(.), '{label}:')]/following::*[self::span or self::div or self::strong][1]",
            f"//li[contains(normalize-space(.), '{label}')]//*[self::span or self::div][last()]",
            f"//*[@title='{label}' or @aria-label='{label}']/following-sibling::*[1]",
        ]
        for xpath in flexible_strategies:
            try:
                element = d.find_element(By.XPATH, xpath)
                text = clean_text(element.text)
                if text and not is_likely_navigation_text(text) and len(text) < 50 and text.lower() != label.lower():
                    if re.match(r'^[+-]?\d+(?:[.,]\d+)?(?:\s*%)?$|^[A-Za-z0-9\s/.-]+$', text):
                        return text
            except NoSuchElementException:
                continue
        
        return None

    def extract_tiles(self) -> Dict[str, Optional[str]]:
        data: Dict[str, Optional[str]] = {}

        # Extract timestamp
        timestamp = None
        timestamp_strategies = [
            "//*[contains(text(), 'As on') or contains(text(), 'As Of')]/text()",
            "//*[contains(text(), 'Updated')]/text()",
            "//*[@class='timestamp' or contains(@class, 'time')]/text()",
        ]
        for xpath in timestamp_strategies:
            try:
                element = self.driver.find_element(By.XPATH, xpath.replace("/text()", ""))
                text = clean_text(element.text)
                if text and ("as on" in text.lower() or "updated" in text.lower()):
                    timestamp = text
                    break
            except NoSuchElementException:
                continue
        data["as_of"] = timestamp

        # Extract industry classification via modal
        industry_info = _click_industry_info_button(self.driver, debug=True)
        data.update({
            "Macro Economic Indicator": industry_info.get("Macro Economic Indicator", ""),
            "Sector": industry_info.get("Sector", ""),
            "Industry": industry_info.get("Industry", ""),
            "Basic Industry": industry_info.get("Basic Industry", data.get("Basic Industry", ""))
        })

        # Extract all the data fields
        for label in LABELS_TO_SCRAPE:
            value = self._find_value_by_multiple_strategies(label)
            
            if value is None:
                for alias, canonical in LABEL_ALIASES.items():
                    if canonical == label:
                        value = self._find_value_by_multiple_strategies(alias)
                        if value:
                            break
            
            if value is None:
                if label == "Change":
                    try:
                        page_text = self.driver.find_element(By.TAG_NAME, "body").text
                        change_patterns = [
                            r"Change\s*:?\s*([+-]?\d+(?:\.\d+)?)\s*\(\s*([+-]?\d+(?:\.\d+)?)%\s*\)",
                            r"([+-]?\d+(?:\.\d+)?)\s*\(\s*([+-]?\d+(?:\.\d+)?)%\s*\)",
                            r"Chg\s*:?\s*([+-]?\d+(?:\.\d+)?)",
                        ]
                        for pattern in change_patterns:
                            match = re.search(pattern, page_text)
                            if match:
                                if len(match.groups()) >= 2:
                                    data["ChangeAbs"] = match.group(1)
                                    data["ChangePct"] = match.group(2)
                                    value = f"{match.group(1)} ({match.group(2)}%)"
                                else:
                                    value = match.group(1)
                                break
                    except Exception:
                        pass
                elif label in ["52W High", "52W Low"]:
                    try:
                        page_text = self.driver.find_element(By.TAG_NAME, "body").text
                        extracted = extract_numeric_from_combined(page_text, label)
                        if extracted:
                            value = extracted
                    except Exception:
                        pass
                elif label == "PE":
                    try:
                        page_text = self.driver.find_element(By.TAG_NAME, "body").text
                        pe_patterns = [
                            r"PE[/\s]*PB\s*([\d,]+(?:\.\d+)?)\s*/\s*([\d,]+(?:\.\d+)?)",
                            r"PE\s*:?\s*([\d,]+(?:\.\d+)?)",
                            r"P/E\s*:?\s*([\d,]+(?:\.\d+)?)",
                        ]
                        for pattern in pe_patterns:
                            match = re.search(pattern, page_text, re.IGNORECASE)
                            if match:
                                value = match.group(1)
                                if len(match.groups()) >= 2 and data.get("PB") is None:
                                    data["PB"] = match.group(2)
                                break
                    except Exception:
                        pass
            
            if value:
                if label == "PE" and "/" in value and not data.get("ChangeAbs"):
                    parts = value.split("/")
                    if len(parts) >= 1:
                        value = clean_text(parts[0])
                elif label == "PB" and "/" in value and not data.get("ChangeAbs"):
                    parts = value.split("/")
                    if len(parts) >= 2:
                        value = clean_text(parts[1])
                if label == "ROE" and "%" in value:
                    value = value.replace("%", "").strip()
            
            data[label] = as_float_or_str(clean_text(value))

        if not data.get("ChangeAbs") and not data.get("ChangePct"):
            chg_str = data.get("Change")
            if chg_str and "(" in chg_str and "%" in chg_str:
                m = re.search(r"([+-]?\d+(?:\.\d+)?)\s*\(\s*([+-]?\d+(?:\.\d+)?)\s*%?\s*\)", chg_str)
                if m:
                    data["ChangeAbs"] = m.group(1)
                    data["ChangePct"] = m.group(2)
        
        return data

    def scrape_scripcode(self, scripcode: str, post_load_sleep: float = 1.0) -> Dict[str, Optional[str]]:
        self.open_scrip(scripcode)
        time.sleep(post_load_sleep)
        row = self.extract_tiles()
        row["scripcode"] = scripcode
        row["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Fill missing classification fields from API (non-invasive)
        api_cls = _fetch_industry_from_api(scripcode)
        for k in ["Macro Economic Indicator", "Sector", "Industry", "Basic Industry"]:
            if not row.get(k):
                row[k] = api_cls.get(k, "")

        # Debug: Print what we found
        print(f"Scraped data for {scripcode}:")
        for key, value in row.items():
            if value and key not in ["scraped_at", "scripcode"]:
                print(f"  {key}: {value}")
        
        return row


# ----------------- Django command -----------------

class Command(BaseCommand):
    help = "Scrape BSE stock tiles via Selenium-rendered DOM and export to Excel & JSON."

    def add_arguments(self, parser):
        parser.add_argument(
            "--scripcodes",
            type=str,
            required=True,
            help="Comma-separated BSE scrip codes, e.g. 530549,500325,532540",
        )
        parser.add_argument(
            "--out",
            type=str,
            default="bse_quotes.xlsx",
            help="Path to output Excel file (default: bse_quotes.xlsx)",
        )
        parser.add_argument(
            "--json-out",
            type=str,
            default="bse_quotes.json",
            help="Path to output JSON file (default: bse_quotes.json)",
        )
        parser.add_argument(
            "--jsonl-out",
            type=str,
            default="bse_quotes.jsonl",
            help="Path to output JSONL file (default: bse_quotes.jsonl)",
        )
        parser.add_argument(
            "--sleep",
            type=float,
            default=1.0,
            help="Extra seconds to sleep after load before reading tiles (default: 1.0)",
        )
        parser.add_argument(
            "--headful",
            action="store_true",
            help="Run Chrome in non-headless mode for debugging",
        )
        parser.add_argument(
            "--delay",
            type=float,
            default=2.0,
            help="Delay between scraping different stocks (default: 2.0 seconds)",
        )

    def handle(self, *args, **options):
        raw_codes = options["scripcodes"]
        xlsx_path = options["out"]
        json_path = options["json_out"]
        jsonl_path = options["jsonl_out"]
        post_sleep = float(options["sleep"])
        headful = bool(options["headful"])
        delay = float(options["delay"])

        scripcodes = [c.strip() for c in raw_codes.split(",") if c.strip()]
        if not scripcodes:
            raise CommandError("Provide at least one scripcode via --scripcodes")

        # Validate scrip codes (basic check - should be numeric)
        for code in scripcodes:
            if not code.isdigit():
                self.stdout.write(self.style.WARNING(f"Warning: '{code}' doesn't look like a valid BSE scrip code"))

        rows: List[Dict[str, Optional[str]]] = []

        self.stdout.write(self.style.NOTICE(f"Starting Selenium scrape for {len(scripcodes)} scripcode(s)…"))

        with BSEQuoteScraper(headless=not headful, page_timeout=30) as scraper:
            for i, code in enumerate(scripcodes):
                try:
                    self.stdout.write(f"  → {code} … ({i+1}/{len(scripcodes)})")
                    row = scraper.scrape_scripcode(code, post_load_sleep=post_sleep)
                    rows.append(row)
                    self.stdout.write(self.style.SUCCESS(f"    OK {code}"))
                    
                    if i < len(scripcodes) - 1:
                        time.sleep(delay)
                        
                except Exception as e:
                    self.stderr.write(self.style.ERROR(f"    FAIL {code}: {e}"))
                    # Do not create an 'error' field; just record minimal row
                    rows.append({
                        "scripcode": code,
                        "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    })

        if not rows:
            raise CommandError("No rows scraped; aborting.")

        # ----- Build DataFrame with stable schema -----
        priority_cols = [
            "scraped_at",
            "scripcode",
            "as_of",

            # Prices
            "LTP",
            "Change",
            "ChangeAbs",
            "ChangePct",
            "Prev Close",
            "Open",
            "High",
            "Low",
            "VWAP",

            # 52W / Bands
            "52W High",
            "52W Low",
            "Upper Price Band",
            "Lower Price Band",

            # Vol / Value
            "TTQ",
            "Turnover (Lakh)",
            "Avg Qty 2W",

            # Market Cap
            "Mcap Full (Cr.)",
            "Mcap FF (Cr.)",

            # Fundamentals
            "EPS (TTM)",
            "CEPS (TTM)",
            "PE",
            "PB",
            "ROE",
            "Face Value",

            # Classification
            "Category",
            "Group",
            "Index",
            "Macro Economic Indicator",
            "Sector",
            "Industry",
            "Basic Industry",
        ]

        df = pd.DataFrame(rows)

        # Canonicalize known alternates before ordering
        for alias, canonical in LABEL_ALIASES.items():
            if alias in df.columns and canonical not in df.columns:
                df[canonical] = df[alias]

        # Ensure all priority columns exist
        for col in priority_cols:
            if col not in df.columns:
                df[col] = None

        # --- create exact hyphenated label for outputs ---
        if "Macro-Economic Indicator" not in df.columns:
            df["Macro-Economic Indicator"] = df["Macro Economic Indicator"]

        # Drop unwanted columns if they somehow appeared
        drop_cols = ["company_name", "ISIN", "Security Code", "Security Id", "error"]
        for c in drop_cols:
            if c in df.columns:
                df = df.drop(columns=[c])

        # Reorder: priority first, then extras
        extra_cols = [c for c in df.columns if c not in priority_cols and c != "Macro-Economic Indicator"]
        ordered = priority_cols.copy()
        insert_after = ordered.index("Macro Economic Indicator") + 1
        ordered.insert(insert_after, "Macro-Economic Indicator")
        df = df[[c for c in ordered if c in df.columns] + [c for c in extra_cols if c not in ordered]]

        # ----- Write Excel -----
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Quotes", freeze_panes=(1, 0))
        self.stdout.write(self.style.SUCCESS(f"Excel saved → {xlsx_path}"))

        # ----- Write JSON & JSONL -----
        json_records = json.loads(df.to_json(orient="records", date_format="iso"))
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(json_records, f, ensure_ascii=False, indent=2)
        self.stdout.write(self.style.SUCCESS(f"JSON saved  → {json_path}"))

        with open(jsonl_path, "w", encoding="utf-8") as f:
            for rec in json_records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        self.stdout.write(self.style.SUCCESS(f"JSONL saved → {jsonl_path}"))

        # Preview first record (if any)
        if json_records:
            preview = json.dumps(json_records[0], ensure_ascii=False, indent=2)
            self.stdout.write(self.style.HTTP_INFO("Sample successful row:"))
            self.stdout.write(preview)
        else:
            self.stdout.write(self.style.WARNING("No successful records to preview"))
   