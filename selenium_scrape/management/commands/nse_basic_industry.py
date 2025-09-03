from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Optional

from django.core.management.base import BaseCommand, CommandError
from django.db import IntegrityError, transaction

# ⬇️ CHANGE THIS IMPORT to your app path
from selenium_scrape.models import NseStockQuote

import requests  # NEW

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)


@dataclass
class CompanyInfo:
    symbol: str
    name: str
    basic_industry: str


class Command(BaseCommand):
    help = "Scrape NSE company details (Company Name & Basic Industry) using Selenium and store/update in DB."

    def add_arguments(self, parser):
        parser.add_argument("symbol", type=str, help="Stock symbol, e.g., MODISONLTD")
        parser.add_argument("--timeout", type=int, default=25, help="Element wait timeout (s)")
        parser.add_argument("--retries", type=int, default=2, help="Page-level retries")
        parser.add_argument("--no-headless", action="store_true", help="Run Chrome with UI")
        parser.add_argument("--debug", action="store_true", help="Verbose logs")

    def handle(self, *args, **options):
        symbol = options["symbol"].upper().strip()
        timeout: int = options["timeout"]
        retries: int = options["retries"]
        headless: bool = not options["no_headless"]
        debug: bool = options["debug"]

        if not symbol:
            raise CommandError("Symbol must be a non-empty string.")

        url = f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}"

        chrome_options = self._build_chrome_options(headless=headless)

        try:
            driver = webdriver.Chrome(options=chrome_options)
        except WebDriverException as e:
            raise CommandError(f"Could not start Chrome driver: {e}") from e

        try:
            info = self._scrape_with_retries(
                driver=driver,
                url=url,
                symbol=symbol,
                timeout=timeout,
                retries=retries,
                debug=debug,
            )
            if info is None:
                raise CommandError(f"Failed to scrape data for symbol: {symbol}")

            # ---- Save to DB (create or update by symbol) ----
            try:
                with transaction.atomic():
                    obj, created = NseStockQuote.objects.update_or_create(
                        symbol=info.symbol,
                        defaults={
                            "company_name": info.name,
                            "basic_industry": info.basic_industry,
                        },
                    )
                action = "Created" if created else "Updated"
                self.stdout.write(self.style.SUCCESS(
                    f"{action} NSE record → symbol={obj.symbol}, company_name={obj.company_name}, basic_industry={obj.basic_industry}"
                ))
            except IntegrityError as e:
                raise CommandError(f"Database error while saving {symbol}: {e}") from e

            # Console output
            self.stdout.write(self.style.SUCCESS(f"Symbol: {info.symbol}"))
            self.stdout.write(self.style.SUCCESS(f"Company Name: {info.name}"))
            self.stdout.write(self.style.SUCCESS(f"Basic Industry: {info.basic_industry}"))

        finally:
            try:
                driver.quit()
            except Exception:
                pass

    # -----------------------------
    # Internal helpers
    # -----------------------------

    def _build_chrome_options(self, headless: bool) -> Options:
        opts = Options()
        if headless:
            opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--window-size=1400,1000")
        opts.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        )
        # Mild anti-bot hints
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        opts.add_argument("--lang=en-US,en;q=0.9")
        return opts

    def _scrape_with_retries(
        self,
        driver: webdriver.Chrome,
        url: str,
        symbol: str,
        timeout: int,
        retries: int,
        debug: bool,
    ) -> Optional[CompanyInfo]:
        base = "https://www.nseindia.com/"

        for attempt in range(1, retries + 2):
            try:
                if debug:
                    self.stdout.write(self.style.WARNING(f"[Attempt {attempt}] Warm-up {base}"))

                # Warm up to seed cookies / tokens
                driver.get(base)
                self._wait_ready(driver, timeout)

                if debug:
                    self.stdout.write(self.style.WARNING(f"[Attempt {attempt}] Load {url}"))

                driver.get(url)
                self._wait_ready(driver, timeout)

                # Wait until we have at least a non-empty title or an h1
                WebDriverWait(driver, timeout).until(
                    lambda d: (d.title and d.title.strip()) or d.find_elements(By.TAG_NAME, "h1")
                )

                # Try to read company name from DOM/meta/title
                name = self._extract_company_name(driver, timeout, debug)

                # Make sure the Securities Information table exists and is hydrated
                WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((By.ID, "securities_info_table"))
                )
                WebDriverWait(driver, timeout).until(
                    EC.text_to_be_present_in_element(
                        (By.ID, "Securities_Info_New"), "Securities Information"
                    )
                )

                basic_industry = self._extract_table_value(
                    driver,
                    table_id="securities_info_table",
                    row_key="Basic Industry",
                    timeout=timeout,
                    debug=debug,
                )

                # If the DOM/meta/title approach failed, fall back to the JSON API with cookies
                if (not name) or (name == "Not Found"):
                    if debug:
                        self.stdout.write(self.style.WARNING("Falling back to NSE API for company name..."))
                    api_name, api_industry = self._fetch_company_via_api(driver, symbol, debug=debug)
                    if api_name:
                        name = api_name
                    # If basic industry was missing, fill from API too (when available)
                    if (not basic_industry or basic_industry == "Not Found") and api_industry:
                        basic_industry = api_industry

                if not name and not basic_industry:
                    raise TimeoutException("Key elements not found")

                return CompanyInfo(
                    symbol=symbol,
                    name=name or "Not Found",
                    basic_industry=basic_industry or "Not Found",
                )

            except (TimeoutException, NoSuchElementException, WebDriverException) as e:
                if attempt <= retries:
                    if debug:
                        self.stdout.write(self.style.WARNING(f"[Attempt {attempt}] Error: {e}. Retrying..."))
                    time.sleep(1.5 * attempt)
                    continue
                if debug:
                    self.stdout.write(self.style.ERROR(f"[Attempt {attempt}] Failed: {e}"))
                return None

    def _wait_ready(self, driver: webdriver.Chrome, timeout: int):
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

    def _extract_company_name(
        self, driver: webdriver.Chrome, timeout: int, debug: bool
    ) -> Optional[str]:
        """
        Robustly resolve the company name from various locations:
        1) Header <h1> (and some common fallbacks)
        2) <meta property="og:title">
        3) document.title pattern: "<SYMBOL> | <Company Name> share Price - NSE India"
        """
        # 1) Primary: <h1> variants
        header_xpaths = [
            "//h1",
            "//div[contains(@class,'company-name')]//h1",
            "//div[contains(@class,'symbol-page')]//h1",
            "//*[@id='securityName' or @id='companyName']",
            "//h1[contains(., 'LIMITED') or contains(., 'Ltd') or contains(., 'LTD')]",
        ]
        for xp in header_xpaths:
            try:
                el = WebDriverWait(driver, int(timeout / 2)).until(
                    EC.presence_of_element_located((By.XPATH, xp))
                )
                txt = (el.text or "").strip()
                if txt:
                    return txt
            except TimeoutException:
                continue

        # 2) Meta og:title
        try:
            meta = driver.find_element(By.CSS_SELECTOR, "meta[property='og:title']")
            content = (meta.get_attribute("content") or "").strip()
            if content:
                parsed = self._parse_company_from_title_like(content)
                if parsed:
                    return parsed
        except NoSuchElementException:
            pass

        # 3) Fallback: document.title parsing
        title = (driver.title or "").strip()
        if title:
            parsed = self._parse_company_from_title_like(title)
            if parsed:
                return parsed

        if debug:
            self.stdout.write(self.style.WARNING("Company name not found via h1/meta/title."))
        return None

    def _parse_company_from_title_like(self, text: str) -> Optional[str]:
        """
        Given strings like:
          "RUPA | Rupa & Company Ltd. share Price - NSE India"
        extract "Rupa & Company Ltd."
        """
        m = re.search(r"\|\s*(.*?)\s+(?:share|Share)\b", text)
        if m and m.group(1).strip():
            return m.group(1).strip()
        m2 = re.search(r"\|\s*(.*)$", text)
        if m2 and m2.group(1).strip():
            cleaned = re.sub(r"\s*[-–]\s*NSE India.*$", "", m2.group(1)).strip()
            cleaned = re.sub(r"\s*(?:Share|share)\s*Price.*$", "", cleaned).strip()
            if cleaned:
                return cleaned
        return None

    def _fetch_company_via_api(self, driver: webdriver.Chrome, symbol: str, debug: bool = False) -> tuple[Optional[str], Optional[str]]:
        """
        Use the browser's cookies to call NSE's JSON API:
          GET https://www.nseindia.com/api/quote-equity?symbol=<SYMBOL>
        Returns (company_name, basic_industry_from_api)
        """
        api_url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"

        # Move Selenium cookies into a requests session
        sess = requests.Session()
        for c in driver.get_cookies():
            # some cookies are essential (like akamai/consent); set all we have
            sess.cookies.set(c.get("name"), c.get("value"), domain=c.get("domain"))

        # headers matter for this endpoint
        sess.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}",
            "Connection": "keep-alive",
        })

        try:
            r = sess.get(api_url, timeout=15)
            if debug:
                self.stdout.write(self.style.WARNING(f"NSE API status: {r.status_code}"))
            if r.status_code != 200:
                return None, None

            data = r.json()
            # Typical structure: { "info": { "companyName": "...", "industry": "..." }, ... }
            info = data.get("info") or {}
            company = (info.get("companyName") or "").strip() or None
            industry = (info.get("industry") or "").strip() or None
            return company, industry
        except Exception as e:
            if debug:
                self.stdout.write(self.style.WARNING(f"NSE API error: {e}"))
            return None, None

    def _extract_table_value(
        self,
        driver: webdriver.Chrome,
        table_id: str,
        row_key: str,
        timeout: int,
        debug: bool,
    ) -> Optional[str]:
        """
        Generic key→value extraction for a 2-column table.
        """
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.ID, table_id))
        )
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, f"//table[@id='{table_id}']//tr"))
        )

        xpaths = [
            f"//table[@id='{table_id}']//td[normalize-space(.)='{row_key}']/following-sibling::td[1]",
            f"//table[@id='{table_id}']//th[normalize-space(.)='{row_key}']/following-sibling::td[1]",
            f"//table[@id='{table_id}']//td[contains(normalize-space(.), '{row_key}')]/following-sibling::td[1]",
        ]

        for xp in xpaths:
            try:
                cell = WebDriverWait(driver, int(timeout / 2)).until(
                    EC.presence_of_element_located((By.XPATH, xp))
                )
                value = (cell.text or "").strip()
                if value:
                    return value
            except TimeoutException:
                continue

        if debug:
            self.stdout.write(self.style.WARNING(f"Row '{row_key}' not found in table '{table_id}'."))

        return None
