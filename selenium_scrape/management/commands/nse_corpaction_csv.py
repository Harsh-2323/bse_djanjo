from __future__ import annotations

import os
import time
import glob
from django.core.management.base import BaseCommand, CommandError
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

class Command(BaseCommand):
    help = "Download NSE corporate actions CSV for a symbol by clicking the download button."

    def add_arguments(self, parser):
        parser.add_argument("symbol", type=str, help="Stock symbol, e.g., ASAHIINDIA")
        parser.add_argument("--timeout", type=int, default=30, help="Element wait timeout (s)")
        parser.add_argument("--retries", type=int, default=3, help="Page-level retries")
        parser.add_argument("--no-headless", action="store_true", help="Run Chrome with UI")
        parser.add_argument("--debug", action="store_true", help="Verbose logs including page source")
        parser.add_argument("--download-dir", type=str, default="downloads", help="Directory to save downloads")

    def handle(self, *args, **options):
        symbol = options["symbol"].upper().strip()
        timeout: int = options["timeout"]
        retries: int = options["retries"]
        headless: bool = not options["no_headless"]
        debug: bool = options["debug"]
        download_dir: str = options["download_dir"]

        if not symbol:
            raise CommandError("Symbol must be a non-empty string.")

        url = f"https://www.nseindia.com/companies-listing/corporate-filings-actions?symbol={symbol}&tabIndex=equity"

        # Create download directory
        os.makedirs(download_dir, exist_ok=True)

        chrome_options = self._build_chrome_options(headless=headless, download_dir=os.path.abspath(download_dir))

        try:
            driver = webdriver.Chrome(options=chrome_options)
        except WebDriverException as e:
            raise CommandError(f"Could not start Chrome driver: {e}") from e

        try:
            csv_file = self._download_csv_with_retries(
                driver=driver,
                url=url,
                symbol=symbol,
                timeout=timeout,
                retries=retries,
                debug=debug,
                download_dir=download_dir,
            )
            if not csv_file:
                raise CommandError(f"Failed to download CSV for symbol: {symbol}")

            self.stdout.write(self.style.SUCCESS(f"CSV downloaded for {symbol}: {csv_file}"))

        finally:
            try:
                driver.quit()
            except Exception:
                pass

    def _build_chrome_options(self, headless: bool, download_dir: str) -> Options:
        opts = Options()
        if headless:
            opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
        )
        opts.add_experimental_option("prefs", {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        })
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        opts.add_argument("--lang=en-US,en;q=0.9")
        return opts

    def _download_csv_with_retries(
        self,
        driver: webdriver.Chrome,
        url: str,
        symbol: str,
        timeout: int,
        retries: int,
        debug: bool,
        download_dir: str,
    ) -> str | None:
        base = "https://www.nseindia.com/"
        if debug:
            self.stdout.write(self.style.WARNING(f"Starting _download_csv_with_retries for {symbol}"))

        for attempt in range(1, retries + 1):
            try:
                if debug:
                    self.stdout.write(self.style.WARNING(f"[Attempt {attempt}/{retries}] Warming up: {base}"))
                driver.get(base)
                self._wait_ready(driver, timeout, debug)
                if debug:
                    self.stdout.write(self.style.SUCCESS("Base page loaded successfully."))

                if debug:
                    self.stdout.write(self.style.WARNING(f"[Attempt {attempt}/{retries}] Loading target URL: {url}"))
                driver.get(url)
                self._wait_ready(driver, timeout, debug)
                if debug:
                    self.stdout.write(self.style.SUCCESS(f"Target page loaded. Title: {driver.title}"))

                # Check for "Service Temporarily Unavailable"
                page_source = driver.page_source
                if "Service Temporarily Unavailable" in page_source:
                    self.stdout.write(self.style.ERROR("Service Temporarily Unavailable detected. Consider using a VPN with an Indian IP."))
                    self._save_page_source(driver, download_dir, debug)
                    time.sleep(5 * attempt)
                    continue

                # Check for SweetAlert popup
                try:
                    error_popup = driver.find_element(By.CLASS_NAME, "swal-icon--error")
                    self.stdout.write(self.style.ERROR("SweetAlert error popup detected. Possible bot detection."))
                    self._save_page_source(driver, download_dir, debug)
                    time.sleep(5 * attempt)
                    continue
                except NoSuchElementException:
                    pass

                if debug:
                    self.stdout.write(self.style.WARNING(f"Page source (snippet):\n{page_source[:1000]}..."))

                # Scroll to make button visible
                if debug:
                    self.stdout.write(self.style.WARNING("Scrolling to ensure download button is visible"))
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)  # Wait for scroll

                # Click CSV download button
                try:
                    if debug:
                        self.stdout.write(self.style.WARNING("Searching for download button with ID: CFcorpactionsEquity-download"))
                    download_link = WebDriverWait(driver, timeout).until(
                        EC.element_to_be_clickable((By.ID, "CFcorpactionsEquity-download"))
                    )
                    if debug:
                        self.stdout.write(self.style.SUCCESS(f"Found download link: {download_link.get_attribute('outerHTML')}"))
                    
                    # Get list of files before clicking
                    before_files = set(glob.glob(os.path.join(download_dir, "*.csv")))
                    
                    if debug:
                        self.stdout.write(self.style.WARNING("Clicking download button in _download_csv_with_retries"))
                    driver.execute_script("arguments[0].click();", download_link)
                    time.sleep(10)  # Wait for download to complete
                    
                    # Check for new CSV file
                    after_files = set(glob.glob(os.path.join(download_dir, "*.csv")))
                    new_files = after_files - before_files
                    if new_files:
                        csv_file = new_files.pop()
                        if debug:
                            self.stdout.write(self.style.SUCCESS(f"CSV download completed in _download_csv_with_retries. File: {csv_file}"))
                        return csv_file
                    else:
                        self.stdout.write(self.style.ERROR("No new CSV file detected after clicking download button"))
                        self._save_page_source(driver, download_dir, debug)
                except TimeoutException as e:
                    self.stdout.write(self.style.ERROR(f"Download button not found: {e}"))
                    self._save_page_source(driver, download_dir, debug)

            except (TimeoutException, NoSuchElementException, WebDriverException) as e:
                self.stdout.write(self.style.ERROR(f"[Attempt {attempt}/{retries}] Error in _download_csv_with_retries: {e}"))
                self._save_page_source(driver, download_dir, debug)
                time.sleep(3 * attempt)

        return None

    def _wait_ready(self, driver: webdriver.Chrome, timeout: int, debug: bool = False):
        if debug:
            self.stdout.write(self.style.WARNING("Waiting for page to be fully loaded in _wait_ready"))
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        if debug:
            self.stdout.write(self.style.SUCCESS("Page fully loaded in _wait_ready"))

    def _save_page_source(self, driver: webdriver.Chrome, download_dir: str, debug: bool):
        if debug:
            source_path = os.path.join(download_dir, "page_source.html")
            with open(source_path, "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            self.stdout.write(self.style.WARNING(f"Saved page source to: {source_path}"))