from __future__ import annotations

import csv
import datetime
import os
import re
import time
from dataclasses import dataclass
from typing import List, Optional

from django.core.management.base import BaseCommand, CommandError

import requests
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
class CorporateAction:
    announcement_date: str
    purpose: str
    record_date: str
    ex_date: str
    attachment_url: Optional[str] = None

class Command(BaseCommand):
    help = "Scrape NSE corporate actions for a symbol, download CSV/PDFs, with full logging."

    def add_arguments(self, parser):
        parser.add_argument("symbol", type=str, help="Stock symbol, e.g., CONCORDBIO")
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
            actions = self._scrape_with_retries(
                driver=driver,
                url=url,
                symbol=symbol,
                timeout=timeout,
                retries=retries,
                debug=debug,
                download_dir=download_dir,
            )
            if actions is None:
                raise CommandError(f"Failed to scrape data for symbol: {symbol}")

            # Save to CSV
            csv_path = os.path.join(download_dir, f"{symbol}_corporate_actions.csv")
            self._save_to_csv(actions, csv_path)
            self.stdout.write(self.style.SUCCESS(f"Downloaded CSV: {csv_path}"))

            # Output scraped data
            self.stdout.write(self.style.SUCCESS(f"Scraped {len(actions)} corporate actions for {symbol}:"))
            for action in actions:
                self.stdout.write(f"- Announcement: {action.announcement_date}, Purpose: {action.purpose}, "
                                 f"Record: {action.record_date}, Ex: {action.ex_date}")
                if action.attachment_url:
                    self.stdout.write(self.style.SUCCESS(f"  Downloaded PDF: {action.attachment_url}"))

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

    def _scrape_with_retries(
        self,
        driver: webdriver.Chrome,
        url: str,
        symbol: str,
        timeout: int,
        retries: int,
        debug: bool,
        download_dir: str,
    ) -> Optional[List[CorporateAction]]:
        base = "https://www.nseindia.com/"
        current_date = datetime.date.today().strftime("%d-%m-%Y")
        from_date = "01-01-2000"
        api_url = f"https://www.nseindia.com/api/corporates-corporateActions?index=equities&symbol={symbol}&from_date={from_date}&to_date={current_date}"

        for attempt in range(1, retries + 1):
            try:
                if debug:
                    self.stdout.write(self.style.WARNING(f"[Attempt {attempt}/{retries}] Warming up: {base}"))
                driver.get(base)
                self._wait_ready(driver, timeout)
                if debug:
                    self.stdout.write(self.style.SUCCESS("Base page loaded."))

                if debug:
                    self.stdout.write(self.style.WARNING(f"[Attempt {attempt}/{retries}] Loading: {url}"))
                driver.get(url)
                self._wait_ready(driver, timeout)
                if debug:
                    self.stdout.write(self.style.SUCCESS(f"Target page loaded. Title: {driver.title}"))

                # Check for SweetAlert error popup
                try:
                    error_popup = driver.find_element(By.CLASS_NAME, "swal-icon--error")
                    self.stdout.write(self.style.ERROR("SweetAlert error popup detected. Possible bot detection."))
                    if attempt == retries:
                        self._save_page_source(driver, download_dir, debug)
                        return None
                    time.sleep(5 * attempt)  # Longer delay on popup
                    continue
                except NoSuchElementException:
                    pass

                # Log page source snippet
                page_source = driver.page_source
                if debug:
                    self.stdout.write(self.style.WARNING(f"Page source (full):\n{page_source}"))
                else:
                    self.stdout.write(self.style.WARNING(f"Page source (snippet):\n{page_source[:1000]}..."))

                # Try API first
                actions = self._fetch_via_api(driver, api_url, symbol, debug)
                if actions:
                    for action in actions:
                        if action.attachment_url:
                            self._download_file(action.attachment_url, download_dir, debug)
                    return actions

                # Fallback: Scrape table
                self.stdout.write(self.style.WARNING("API failed, falling back to table scraping."))
                actions = self._scrape_table(driver, timeout, debug)
                if actions:
                    # Try downloading CSV
                    try:
                        download_link = WebDriverWait(driver, timeout).until(
                            EC.element_to_be_clickable((By.ID, "CFcorpactionsEquity-download"))
                        )
                        if debug:
                            self.stdout.write(self.style.SUCCESS(f"Found download link: {download_link.get_attribute('outerHTML')}"))
                        driver.execute_script("arguments[0].click();", download_link)
                        time.sleep(10)  # Wait for download
                        self.stdout.write(self.style.SUCCESS("CSV download triggered."))
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f"Download button failed: {e}"))
                    return actions

            except (TimeoutException, NoSuchElementException, WebDriverException) as e:
                self.stdout.write(self.style.ERROR(f"[Attempt {attempt}/{retries}] Error: {e}"))
                self._save_page_source(driver, download_dir, debug)
                time.sleep(3 * attempt)
                if attempt == retries:
                    return None

        return None

    def _wait_ready(self, driver: webdriver.Chrome, timeout: int):
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

    def _save_page_source(self, driver: webdriver.Chrome, download_dir: str, debug: bool):
        if debug:
            source_path = os.path.join(download_dir, "page_source.html")
            with open(source_path, "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            self.stdout.write(self.style.WARNING(f"Saved page source to: {source_path}"))

    def _fetch_via_api(self, driver: webdriver.Chrome, api_url: str, symbol: str, debug: bool) -> List[CorporateAction]:
        sess = requests.Session()
        for c in driver.get_cookies():
            sess.cookies.set(c.get("name"), c.get("value"))

        sess.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": f"https://www.nseindia.com/companies-listing/corporate-filings-actions?symbol={symbol}&tabIndex=equity",
            "Connection": "keep-alive",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
        })

        try:
            if debug:
                self.stdout.write(self.style.WARNING(f"Calling API: {api_url}"))
            r = sess.get(api_url, timeout=15)
            if debug:
                self.stdout.write(self.style.WARNING(f"API status: {r.status_code}, Response: {r.text}"))
            if r.status_code != 200:
                raise ValueError(f"API failed with status {r.status_code}")

            data = r.json()
            actions = []
            for item in data:
                # Updated keys based on typical NSE API structure
                announcement_date = item.get('an_dt', '') or item.get('announcementDate', '') or ''
                purpose = item.get('purp', '') or item.get('purpose', '') or ''
                record_date = item.get('rec_dt', '') or item.get('recordDate', '') or ''
                ex_date = item.get('ex_dt', '') or item.get('exDate', '') or ''
                attachment_url = item.get('attchmntFile', '') or item.get('attachment', '') or ''
                if attachment_url and not attachment_url.startswith('http'):
                    attachment_url = f"https://www.nseindia.com{attachment_url}"
                actions.append(CorporateAction(announcement_date, purpose, record_date, ex_date, attachment_url))
            return actions
        except Exception as e:
            if debug:
                self.stdout.write(self.style.ERROR(f"API error: {e}"))
            return []

    def _scrape_table(self, driver: webdriver.Chrome, timeout: int, debug: bool) -> List[CorporateAction]:
        try:
            # Use provided selector for table container
            table_container = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#Corporate_Actions_equity"))
            )
            if debug:
                self.stdout.write(self.style.SUCCESS(f"Found table container: {table_container.get_attribute('outerHTML')[:500]}..."))

            # Find table within container
            table = table_container.find_element(By.TAG_NAME, "table")
            rows = table.find_elements(By.XPATH, ".//tr")[1:]  # Skip header
            actions = []
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 4:
                    announcement_date = cells[0].text.strip()
                    purpose = cells[1].text.strip()
                    record_date = cells[2].text.strip()
                    ex_date = cells[3].text.strip()
                    attachment_url = None
                    try:
                        link = cells[-1].find_element(By.TAG_NAME, "a")
                        attachment_url = link.get_attribute("href")
                        if attachment_url and attachment_url.endswith(".pdf"):
                            self._download_file(attachment_url, self.download_dir, debug)
                    except NoSuchElementException:
                        pass
                    actions.append(CorporateAction(announcement_date, purpose, record_date, ex_date, attachment_url))
            return actions
        except Exception as e:
            if debug:
                self.stdout.write(self.style.ERROR(f"Table scraping failed: {e}"))
            return []

    def _download_file(self, url: str, download_dir: str, debug: bool):
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                filename = url.split("/")[-1]
                path = os.path.join(download_dir, filename)
                with open(path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                if debug:
                    self.stdout.write(self.style.SUCCESS(f"Downloaded: {path}"))
        except Exception as e:
            if debug:
                self.stdout.write(self.style.ERROR(f"Failed to download {url}: {e}"))

    def _save_to_csv(self, actions: List[CorporateAction], csv_path: str):
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Announcement Date', 'Purpose', 'Record Date', 'Ex Date', 'Attachment URL'])
            for action in actions:
                writer.writerow([action.announcement_date, action.purpose, action.record_date, action.ex_date, action.attachment_url])