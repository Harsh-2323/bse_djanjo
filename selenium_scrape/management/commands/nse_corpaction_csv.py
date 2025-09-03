from __future__ import annotations

import os
import time
import glob
import csv
import json
from pathlib import Path
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
    help = "Download NSE corporate actions CSVs for Equity and SME tabs, scrape them, and output as JSON."

    def add_arguments(self, parser):
        parser.add_argument("symbol", type=str, help="Stock symbol, e.g., ASAHIINDIA")
        parser.add_argument("--timeout", type=int, default=30, help="Element wait timeout (s)")
        parser.add_argument("--retries", type=int, default=3, help="Page-level retries")
        parser.add_argument("--no-headless", action="store_true", help="Run Chrome with UI")
        parser.add_argument("--debug", action="store_true", help="Verbose logs including page source")
        parser.add_argument("--download-dir", type=str, default="downloads", help="Directory to save downloads")
        parser.add_argument("--equity-only", action="store_true", help="Download only equity CSV")
        parser.add_argument("--sme-only", action="store_true", help="Download only SME CSV")

    def handle(self, *args, **options):
        symbol = options["symbol"].upper().strip()
        timeout: int = options["timeout"]
        retries: int = options["retries"]
        headless: bool = not options["no_headless"]
        debug: bool = options["debug"]
        download_dir: str = options["download_dir"]
        equity_only: bool = options["equity_only"]
        sme_only: bool = options["sme_only"]

        if not symbol:
            raise CommandError("Symbol must be a non-empty string.")

        # Create download directory
        os.makedirs(download_dir, exist_ok=True)
        
        # Clear any existing CSV and JSON files to avoid confusion
        self._clear_existing_files(download_dir, debug, ["*.csv", "*.json"])

        chrome_options = self._build_chrome_options(headless=headless, download_dir=os.path.abspath(download_dir))

        try:
            driver = webdriver.Chrome(options=chrome_options)
        except WebDriverException as e:
            raise CommandError(f"Could not start Chrome driver: {e}") from e

        try:
            equity_csv, sme_csv = self._download_csv_with_retries(
                driver=driver,
                symbol=symbol,
                timeout=timeout,
                retries=retries,
                debug=debug,
                download_dir=download_dir,
                equity_only=equity_only,
                sme_only=sme_only,
            )
            
            # Scrape CSVs and convert to JSON
            json_output = {
                "symbol": symbol,
                "equity": None,
                "sme": None
            }
            
            if equity_csv and not sme_only:
                try:
                    json_output["equity"] = self._parse_csv(equity_csv, debug)
                    self.stdout.write(self.style.SUCCESS(f"Equity CSV parsed for {symbol}: {equity_csv}"))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Failed to parse Equity CSV {equity_csv}: {e}"))
            
            if sme_csv and not equity_only:
                try:
                    json_output["sme"] = self._parse_csv(sme_csv, debug)
                    self.stdout.write(self.style.SUCCESS(f"SME CSV parsed for {symbol}: {sme_csv}"))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Failed to parse SME CSV {sme_csv}: {e}"))
            
            # Output JSON to console
            json_string = json.dumps(json_output, indent=2, ensure_ascii=False)
            self.stdout.write(self.style.SUCCESS(f"JSON output for {symbol}:\n{json_string}"))
            
            # Save JSON to files
            if equity_csv and not sme_only:
                equity_json_path = os.path.join(download_dir, f"{symbol}_equity.json")
                with open(equity_json_path, "w", encoding="utf-8") as f:
                    json.dump({"symbol": symbol, "equity": json_output["equity"]}, f, indent=2, ensure_ascii=False)
                self.stdout.write(self.style.SUCCESS(f"Equity JSON saved to: {equity_json_path}"))
            
            if sme_csv and not equity_only:
                sme_json_path = os.path.join(download_dir, f"{symbol}_sme.json")
                with open(sme_json_path, "w", encoding="utf-8") as f:
                    json.dump({"symbol": symbol, "sme": json_output["sme"]}, f, indent=2, ensure_ascii=False)
                self.stdout.write(self.style.SUCCESS(f"SME JSON saved to: {sme_json_path}"))
            
            # Report download status
            success_count = 0
            if equity_csv and not sme_only:
                self.stdout.write(self.style.SUCCESS(f"Equity CSV downloaded for {symbol}: {equity_csv}"))
                success_count += 1
            if sme_csv and not equity_only:
                self.stdout.write(self.style.SUCCESS(f"SME CSV downloaded for {symbol}: {sme_csv}"))
                success_count += 1
                
            if success_count == 0:
                raise CommandError(f"Failed to download any CSV for symbol: {symbol}")
                
            if not equity_csv and not sme_only:
                self.stdout.write(self.style.WARNING(f"No Equity CSV downloaded for {symbol}"))
            if not sme_csv and not equity_only:
                self.stdout.write(self.style.WARNING(f"No SME CSV downloaded for {symbol}. May not have SME data."))

        finally:
            try:
                driver.quit()
            except Exception:
                pass

    def _clear_existing_files(self, download_dir: str, debug: bool, patterns: list[str]):
        """Clear existing files matching patterns to avoid confusion during download detection"""
        for pattern in patterns:
            files = glob.glob(os.path.join(download_dir, pattern))
            for file in files:
                try:
                    os.remove(file)
                    if debug:
                        self.stdout.write(self.style.WARNING(f"Removed existing file: {file}"))
                except OSError:
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

    def _parse_csv(self, csv_path: str, debug: bool) -> list[dict]:
        """Parse a CSV file into a list of dictionaries"""
        if debug:
            self.stdout.write(self.style.WARNING(f"Parsing CSV: {csv_path}"))
        
        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    self.stdout.write(self.style.WARNING(f"CSV file {csv_path} is empty or has no headers"))
                    return []
                
                data = [row for row in reader]
                if debug:
                    self.stdout.write(self.style.SUCCESS(f"Parsed {len(data)} rows from {csv_path}"))
                return data
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error parsing CSV {csv_path}: {e}"))
            return []

    def _download_csv_with_retries(
        self,
        driver: webdriver.Chrome,
        symbol: str,
        timeout: int,
        retries: int,
        debug: bool,
        download_dir: str,
        equity_only: bool = False,
        sme_only: bool = False,
    ) -> tuple[str | None, str | None]:
        base = "https://www.nseindia.com/"
        equity_csv = None
        sme_csv = None
        
        if debug:
            self.stdout.write(self.style.WARNING(f"Starting download process for {symbol}"))

        for attempt in range(1, retries + 1):
            try:
                # Warm up with base page
                if debug:
                    self.stdout.write(self.style.WARNING(f"[Attempt {attempt}/{retries}] Warming up: {base}"))
                driver.get(base)
                self._wait_ready(driver, timeout, debug)
                
                # Download Equity CSV if requested
                if not sme_only:
                    equity_csv = self._download_tab_csv(
                        driver, symbol, "equity", timeout, debug, download_dir
                    )
                
                # Download SME CSV if requested
                if not equity_only:
                    sme_csv = self._download_tab_csv(
                        driver, symbol, "sme", timeout, debug, download_dir
                    )
                
                # Return if we got what we needed
                if (equity_only and equity_csv) or (sme_only and sme_csv) or (equity_csv and sme_csv):
                    return equity_csv, sme_csv
                elif not equity_only and not sme_only and (equity_csv or sme_csv):
                    return equity_csv, sme_csv

            except (TimeoutException, NoSuchElementException, WebDriverException) as e:
                self.stdout.write(self.style.ERROR(f"[Attempt {attempt}/{retries}] Error: {e}"))
                self._save_page_source(driver, download_dir, debug)
                time.sleep(3 * attempt)

        return equity_csv, sme_csv

    def _download_tab_csv(
        self, 
        driver: webdriver.Chrome, 
        symbol: str, 
        tab_type: str, 
        timeout: int, 
        debug: bool, 
        download_dir: str
    ) -> str | None:
        """Download CSV for a specific tab (equity or sme)"""
        try:
            # Construct URL for specific tab
            if tab_type.lower() == "equity":
                url = f"https://www.nseindia.com/companies-listing/corporate-filings-actions?symbol={symbol}&tabIndex=equity"
                download_button_id = "CFcorpactionsEquity-download"
                tab_name = "Equity"
            else:  # SME
                url = f"https://www.nseindia.com/companies-listing/corporate-filings-actions?symbol={symbol}&tabIndex=sme"
                download_button_id = "CFcorpactionsSME-download"
                tab_name = "SME"
            
            if debug:
                self.stdout.write(self.style.WARNING(f"Loading {tab_name} page: {url}"))
            
            driver.get(url)
            self._wait_ready(driver, timeout, debug)
            
            if debug:
                self.stdout.write(self.style.SUCCESS(f"{tab_name} page loaded. Title: {driver.title}"))

            # Check for common error conditions
            page_source = driver.page_source
            if "Service Temporarily Unavailable" in page_source:
                self.stdout.write(self.style.ERROR("Service Temporarily Unavailable detected. Consider using a VPN with an Indian IP."))
                return None

            # Check for SweetAlert popup
            try:
                error_popup = driver.find_element(By.CLASS_NAME, "swal-icon--error")
                self.stdout.write(self.style.ERROR("SweetAlert error popup detected. Possible bot detection."))
                return None
            except NoSuchElementException:
                pass

            # Scroll and wait a bit
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            # Wait for the specific tab content to be loaded
            if tab_type.lower() == "sme":
                try:
                    # Wait for SME tab content
                    WebDriverWait(driver, timeout).until(
                        EC.presence_of_element_located((By.ID, "Corporate_Actions_sme"))
                    )
                except TimeoutException:
                    self.stdout.write(self.style.WARNING(f"SME tab content not found for {symbol}. May not have SME data."))
                    return None

            # Find and click download button
            if debug:
                self.stdout.write(self.style.WARNING(f"Searching for {tab_name} download button: {download_button_id}"))
            
            # Get files before download
            before_files = set(glob.glob(os.path.join(download_dir, "*.csv")))
            
            download_button = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.ID, download_button_id))
            )
            
            if debug:
                self.stdout.write(self.style.SUCCESS(f"Found {tab_name} download button"))
            
            # Click download button
            driver.execute_script("arguments[0].click();", download_button)
            
            # Wait for download with multiple checks
            download_file = self._wait_for_download(download_dir, before_files, timeout, debug, tab_name)
            
            if download_file:
                if debug:
                    self.stdout.write(self.style.SUCCESS(f"{tab_name} CSV downloaded: {download_file}"))
                return download_file
            else:
                self.stdout.write(self.style.WARNING(f"No {tab_name} CSV file detected after download"))
                return None
                
        except TimeoutException as e:
            self.stdout.write(self.style.ERROR(f"{tab_name} download failed - timeout: {e}"))
            return None
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"{tab_name} download failed: {e}"))
            return None

    def _wait_for_download(
        self, 
        download_dir: str, 
        before_files: set, 
        timeout: int, 
        debug: bool, 
        tab_name: str
    ) -> str | None:
        """Wait for download to complete and return the downloaded file path"""
        end_time = time.time() + timeout
        
        while time.time() < end_time:
            time.sleep(1)
            
            # Check for new CSV files
            current_files = set(glob.glob(os.path.join(download_dir, "*.csv")))
            new_files = current_files - before_files
            
            if new_files:
                # Check if any new files are complete (not being downloaded)
                for file_path in new_files:
                    try:
                        # Check if file is not being written to
                        initial_size = os.path.getsize(file_path)
                        time.sleep(0.5)
                        final_size = os.path.getsize(file_path)
                        
                        if initial_size == final_size and final_size > 0:
                            if debug:
                                self.stdout.write(self.style.SUCCESS(f"{tab_name} download completed: {file_path}"))
                            return file_path
                    except OSError:
                        continue
            
            # Check for .crdownload files (Chrome partial downloads)
            partial_files = glob.glob(os.path.join(download_dir, "*.crdownload"))
            if partial_files and debug:
                self.stdout.write(self.style.WARNING(f"Download in progress: {len(partial_files)} partial file(s)"))
        
        return None

    def _wait_ready(self, driver: webdriver.Chrome, timeout: int, debug: bool):
        if debug:
            self.stdout.write(self.style.WARNING("Waiting for page to be fully loaded"))
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(1)  # Additional wait for dynamic content
        if debug:
            self.stdout.write(self.style.SUCCESS("Page fully loaded"))

    def _save_page_source(self, driver: webdriver.Chrome, download_dir: str, debug: bool):
        if debug:
            source_path = os.path.join(download_dir, f"page_source_{int(time.time())}.html")
            with open(source_path, "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            self.stdout.write(self.style.WARNING(f"Saved page source to: {source_path}"))