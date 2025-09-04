from __future__ import annotations

import os
import time
import csv
import json
import boto3
from datetime import datetime
from io import StringIO
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
from selenium_scrape.models import NSECorporateAction, NseStockQuote, NseAnnouncement

class Command(BaseCommand):
    help = "Scrape NSE corporate actions, save to DB, optionally scrape announcements, and upload JSON to R2 without local storage."

    def add_arguments(self, parser):
        parser.add_argument("symbol", type=str, help="Stock symbol, e.g., ASAHIINDIA")
        parser.add_argument("--timeout", type=int, default=30, help="Element wait timeout (s)")
        parser.add_argument("--retries", type=int, default=3, help="Page-level retries")
        parser.add_argument("--no-headless", action="store_true", help="Run Chrome with UI")
        parser.add_argument("--debug", action="store_true", help="Verbose logs including page source")
        parser.add_argument("--equity-only", action="store_true", help="Scrape only equity data")
        parser.add_argument("--sme-only", action="store_true", help="Scrape only SME data")
        parser.add_argument("--scrape-announcements", action="store_true", help="Scrape announcements and save to NseAnnouncement")

    def handle(self, *args, **options):
        symbol = options["symbol"].upper().strip()
        timeout: int = options["timeout"]
        retries: int = options["retries"]
        headless: bool = not options["no_headless"]
        debug: bool = options["debug"]
        equity_only: bool = options["equity_only"]
        sme_only: bool = options["sme_only"]
        scrape_announcements: bool = options["scrape_announcements"]

        if not symbol:
            raise CommandError("Symbol must be a non-empty string.")

        chrome_options = self._build_chrome_options(headless=headless)

        try:
            driver = webdriver.Chrome(options=chrome_options)
        except WebDriverException as e:
            raise CommandError(f"Could not start Chrome driver: {e}") from e

        try:
            # Get company name from NseStockQuote or scrape from website
            company_name = self._get_company_name(driver, symbol, timeout, debug)

            # Scrape corporate actions tables
            equity_data, sme_data = self._scrape_corporate_actions(
                driver=driver,
                symbol=symbol,
                timeout=timeout,
                retries=retries,
                debug=debug,
                equity_only=equity_only,
                sme_only=sme_only,
            )
            
            # Prepare JSON output
            json_output = {
                "symbol": symbol,
                "equity": equity_data,
                "sme": sme_data
            }
            
            # Save corporate actions to NSECorporateAction
            try:
                nse_corp_action, created = NSECorporateAction.objects.get_or_create(
                    symbol=symbol,
                    defaults={"company_name": company_name or symbol}
                )
                nse_corp_action.actions_data = json_output
                nse_corp_action.save()
                self.stdout.write(self.style.SUCCESS(
                    f"{'Created' if created else 'Updated'} NSECorporateAction for {symbol} with {nse_corp_action.total_actions_count} actions"
                ))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Failed to save to NSECorporateAction for {symbol}: {e}"))

            # Upload JSON to R2
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_filename = f"{symbol}_{timestamp}.json"
            r2_key = f"nse/{symbol}/{json_filename}"
            r2_public_url = f"{os.getenv('R2_PUBLIC_BASEURL')}/{r2_key}"
            
            try:
                s3_client = boto3.client(
                    "s3",
                    endpoint_url=os.getenv("R2_ENDPOINT"),
                    aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
                    aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY")
                )
                json_buffer = StringIO()
                json.dump(json_output, json_buffer, indent=2, ensure_ascii=False)
                json_buffer.seek(0)
                s3_client.upload_fileobj(
                    Fileobj=json_buffer,
                    Bucket=os.getenv("R2_BUCKET"),
                    Key=r2_key,
                    ExtraArgs={"ContentType": "application/json"}
                )
                self.stdout.write(self.style.SUCCESS(f"Uploaded JSON to R2: {r2_key}"))
                
                # Update NSECorporateAction with R2 paths
                nse_corp_action.json_r2_path = r2_key
                nse_corp_action.json_cloud_url = r2_public_url
                nse_corp_action.save()
                self.stdout.write(self.style.SUCCESS(f"Updated NSECorporateAction with R2 path: {r2_key}"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Failed to upload JSON to R2 for {symbol}: {e}"))

            # Scrape announcements if requested
            if scrape_announcements:
                announcements = self._scrape_announcements(driver, symbol, timeout, debug)
                for ann in announcements:
                    try:
                        NseAnnouncement.objects.get_or_create(
                            symbol=symbol,
                            subject=ann.get("subject"),
                            exchange_dissemination_time=ann.get("exchange_dissemination_time"),
                            defaults={
                                "company_name": company_name or symbol,
                                "exchange_received_time": ann.get("exchange_received_time"),
                                "time_taken": ann.get("time_taken"),
                                "attachment_size": ann.get("attachment_size"),
                                "attachment_link": ann.get("attachment_link"),
                                "xbrl_link": ann.get("xbrl_link"),
                                "has_xbrl": bool(ann.get("xbrl_link"))
                            }
                        )
                        self.stdout.write(self.style.SUCCESS(f"Saved announcement for {symbol}: {ann.get('subject')[:60]}"))
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f"Failed to save announcement for {symbol}: {e}"))

            # Output JSON to console
            json_string = json.dumps(json_output, indent=2, ensure_ascii=False)
            self.stdout.write(self.style.SUCCESS(f"JSON output for {symbol}:\n{json_string}"))

            # Report scrape status
            success_count = 0
            if equity_data and not sme_only:
                self.stdout.write(self.style.SUCCESS(f"Equity data scraped for {symbol}: {len(equity_data)} rows"))
                success_count += 1
            if sme_data and not equity_only:
                self.stdout.write(self.style.SUCCESS(f"SME data scraped for {symbol}: {len(sme_data)} rows"))
                success_count += 1
                
            if success_count == 0 and not scrape_announcements:
                raise CommandError(f"Failed to scrape any data for symbol: {symbol}")
                
            if not equity_data and not sme_only:
                self.stdout.write(self.style.WARNING(f"No Equity data scraped for {symbol}"))
            if not sme_data and not equity_only:
                self.stdout.write(self.style.WARNING(f"No SME data scraped for {symbol}. May not have SME data."))

        finally:
            try:
                driver.quit()
            except Exception:
                pass

    def _get_company_name(self, driver: webdriver.Chrome, symbol: str, timeout: int, debug: bool) -> str | None:
        """Get company name from NseStockQuote or scrape from NSE website"""
        try:
            stock_quote = NseStockQuote.objects.filter(symbol=symbol).first()
            if stock_quote and stock_quote.company_name:
                if debug:
                    self.stdout.write(self.style.SUCCESS(f"Found company name in NseStockQuote: {stock_quote.company_name}"))
                return stock_quote.company_name
        except Exception as e:
            self.stdout.write(self.style.WARNING(f"Error checking NseStockQuote for {symbol}: {e}"))

        # Scrape company name from NSE website
        try:
            url = f"https://www.nseindia.com/companies-listing/corporate-filings-actions?symbol={symbol}&tabIndex=equity"
            driver.get(url)
            self._wait_ready(driver, timeout, debug)
            company_name_element = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h2#companyName, h1"))
            )
            company_name = company_name_element.text.strip()
            if debug:
                self.stdout.write(self.style.SUCCESS(f"Scraped company name: {company_name}"))
            return company_name
        except Exception as e:
            self.stdout.write(self.style.WARNING(f"Failed to scrape company name for {symbol}: {e}"))
            return None

    def _scrape_announcements(self, driver: webdriver.Chrome, symbol: str, timeout: int, debug: bool) -> list[dict]:
        """Scrape announcements from NSE website"""
        announcements = []
        try:
            url = f"https://www.nseindia.com/companies-listing/corporate-filings-announcements?symbol={symbol}"
            driver.get(url)
            self._wait_ready(driver, timeout, debug)
            
            if debug:
                self.stdout.write(self.style.WARNING(f"Scraping announcements for {symbol} from {url}"))
            
            # Wait for announcement table
            table = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.ID, "CFannouncements"))
            )
            
            # Find rows in the announcements table
            rows = table.find_elements(By.TAG_NAME, "tr")[1:]  # Skip header
            for row in rows:
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 5:
                        announcement = {
                            "subject": cells[0].text.strip(),
                            "exchange_received_time": cells[1].text.strip(),
                            "exchange_dissemination_time": cells[2].text.strip(),
                            "time_taken": cells[3].text.strip(),
                            "attachment_size": cells[4].text.strip(),
                            "attachment_link": None,
                            "xbrl_link": None
                        }
                        # Get attachment links
                        try:
                            links = cells[4].find_elements(By.TAG_NAME, "a")
                            for link in links:
                                href = link.get_attribute("href")
                                if href and ".pdf" in href.lower():
                                    announcement["attachment_link"] = href
                                elif href and ".xbrl" in href.lower():
                                    announcement["xbrl_link"] = href
                        except:
                            pass
                        announcements.append(announcement)
                except Exception as e:
                    self.stdout.write(self.style.WARNING(f"Error parsing announcement row: {e}"))
            
            if debug:
                self.stdout.write(self.style.SUCCESS(f"Scraped {len(announcements)} announcements for {symbol}"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Failed to scrape announcements for {symbol}: {e}"))
        return announcements

    def _scrape_corporate_actions(
        self,
        driver: webdriver.Chrome,
        symbol: str,
        timeout: int,
        retries: int,
        debug: bool,
        equity_only: bool = False,
        sme_only: bool = False,
    ) -> tuple[list[dict] | None, list[dict] | None]:
        """Scrape corporate actions tables for Equity and/or SME tabs"""
        base = "https://www.nseindia.com/"
        equity_data = None
        sme_data = None
        
        if debug:
            self.stdout.write(self.style.WARNING(f"Starting scrape process for {symbol}"))

        for attempt in range(1, retries + 1):
            try:
                # Warm up with base page
                if debug:
                    self.stdout.write(self.style.WARNING(f"[Attempt {attempt}/{retries}] Warming up: {base}"))
                driver.get(base)
                self._wait_ready(driver, timeout, debug)
                
                # Scrape Equity table if requested
                if not sme_only:
                    equity_data = self._scrape_tab_table(driver, symbol, "equity", timeout, debug)
                
                # Scrape SME table if requested
                if not equity_only:
                    sme_data = self._scrape_tab_table(driver, symbol, "sme", timeout, debug)
                
                # Return if we got what we needed
                if (equity_only and equity_data) or (sme_only and sme_data) or (equity_data and sme_data):
                    return equity_data, sme_data
                elif not equity_only and not sme_only and (equity_data or sme_data):
                    return equity_data, sme_data

            except (TimeoutException, NoSuchElementException, WebDriverException) as e:
                self.stdout.write(self.style.ERROR(f"[Attempt {attempt}/{retries}] Error: {e}"))
                self._save_page_source(driver, debug)
                time.sleep(3 * attempt)

        return equity_data, sme_data

    def _scrape_tab_table(
        self, 
        driver: webdriver.Chrome, 
        symbol: str, 
        tab_type: str, 
        timeout: int, 
        debug: bool
    ) -> list[dict] | None:
        """Scrape corporate actions table for a specific tab (equity or sme)"""
        try:
            # Construct URL for specific tab
            if tab_type.lower() == "equity":
                url = f"https://www.nseindia.com/companies-listing/corporate-filings-actions?symbol={symbol}&tabIndex=equity"
                table_id = "Corporate_Actions"
                tab_name = "Equity"
            else:  # SME
                url = f"https://www.nseindia.com/companies-listing/corporate-filings-actions?symbol={symbol}&tabIndex=sme"
                table_id = "Corporate_Actions_sme"
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

            # Wait for the specific tab table
            try:
                table = WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((By.ID, table_id))
                )
            except TimeoutException:
                self.stdout.write(self.style.WARNING(f"{tab_name} table not found for {symbol}. May not have {tab_name} data."))
                return None

            # Get table headers
            headers = []
            header_row = table.find_element(By.TAG_NAME, "thead").find_element(By.TAG_NAME, "tr")
            for th in header_row.find_elements(By.TAG_NAME, "th"):
                headers.append(th.text.strip())
            
            if not headers:
                self.stdout.write(self.style.WARNING(f"No headers found in {tab_name} table for {symbol}"))
                return None

            # Get table rows
            data = []
            rows = table.find_elements(By.TAG_NAME, "tbody")[0].find_elements(By.TAG_NAME, "tr")
            for row in rows:
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    row_data = {}
                    for i, cell in enumerate(cells):
                        if i < len(headers):
                            row_data[headers[i]] = cell.text.strip()
                    if row_data:
                        data.append(row_data)
                except Exception as e:
                    self.stdout.write(self.style.WARNING(f"Error parsing {tab_name} table row: {e}"))
            
            if debug:
                self.stdout.write(self.style.SUCCESS(f"Scraped {len(data)} rows from {tab_name} table for {symbol}"))
            
            return data if data else None

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"{tab_name} scrape failed: {e}"))
            return None

    def _build_chrome_options(self, headless: bool) -> Options:
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
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        })
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        opts.add_argument("--lang=en-US,en;q=0.9")
        return opts

    def _wait_ready(self, driver: webdriver.Chrome, timeout: int, debug: bool):
        if debug:
            self.stdout.write(self.style.WARNING("Waiting for page to be fully loaded"))
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(1)  # Additional wait for dynamic content
        if debug:
            self.stdout.write(self.style.SUCCESS("Page fully loaded"))

    def _save_page_source(self, driver: webdriver.Chrome, debug: bool):
        if debug:
            source_path = f"page_source_{int(time.time())}.html"
            with open(source_path, "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            self.stdout.write(self.style.WARNING(f"Saved page source to: {source_path}"))