import time
import csv
import json
import boto3
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from io import StringIO, BytesIO

from django.core.management.base import BaseCommand
from django.conf import settings

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# Import your new model
from selenium_scrape.models import CorporateAction  # Replace 'your_app' with actual app name

# =========== repo paths ===========
BASE_DIR = Path(__file__).resolve().parents[4]   # D:\BSE_django\bse_api
DOWNLOADS_DIR = BASE_DIR / "downloads"

DEFAULT_URL = "https://www.bseindia.com/corporates/corporates_act.html"

# =========== R2 Configuration ===========
R2_ENDPOINT = getattr(settings, 'R2_ENDPOINT', None)
R2_ACCESS_KEY = getattr(settings, 'R2_ACCESS_KEY_ID', None)
R2_SECRET_KEY = getattr(settings, 'R2_SECRET_ACCESS_KEY', None)
R2_BUCKET = getattr(settings, 'R2_BUCKET', None)
R2_PUBLIC_BASEURL = getattr(settings, 'R2_PUBLIC_BASEURL', None)

def get_r2_client():
    """Initialize R2 client"""
    return boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name='auto'
    )

# =========== helpers ===========
def build_company_url(url: str | None, scripcode: str | None) -> tuple[str, str | None]:
    """
    Return a proper corporate actions URL and the resolved scripcode (if present).
    Priority:
      1) If --scripcode provided, attach/replace ?scripcode=...
      2) Else if --url provided, pass-through (and extract scripcode if present)
      3) Else DEFAULT_URL (no scripcode)
    """
    if scripcode:
        # sanitize to digits
        scripcode = "".join(ch for ch in scripcode if ch.isdigit())
        parts = list(urlparse(url or DEFAULT_URL))
        qs = parse_qs(parts[4])
        qs["scripcode"] = [scripcode]
        parts[4] = urlencode(qs, doseq=True)
        return urlunparse(parts), scripcode

    if url:
        parts = urlparse(url)
        qs = parse_qs(parts.query)
        resolved = (qs.get("scripcode") or [None])[0]
        return url, resolved

    return DEFAULT_URL, None

# =========== driver helpers ===========
def start_driver(download_dir: Path, headless: bool = True) -> webdriver.Chrome:
    """
    Start a Chrome driver configured to auto-download CSVs to download_dir.
    """
    download_dir.mkdir(parents=True, exist_ok=True)

    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1600,1000")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    # Additional options to reduce detection
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-web-security")
    opts.add_argument("--disable-features=VizDisplayCompositor")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)

    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    opts.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(90)
    # Remove navigator.webdriver property
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def safe_click(driver, element):
    """
    Try multiple click methods to ensure the click works.
    """
    try:
        element.click()
        return True
    except Exception:
        pass

    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
        time.sleep(0.5)
        element.click()
        return True
    except Exception:
        pass

    try:
        driver.execute_script("arguments[0].click();", element)
        return True
    except Exception:
        pass

    try:
        driver.execute_script("""
            var element = arguments[0];
            var scope = window.angular ? angular.element(element).scope() : null;
            if (scope && scope.fn_downloadcsv) {
                scope.fn_downloadcsv();
                scope.$apply();
            } else {
                element.click();
            }
        """, element)
        return True
    except Exception:
        pass

    return False

def wait_for_csv(download_dir: Path, before: set, timeout_sec: int = 180) -> Path:
    """
    Wait for a *new* .csv to appear in download_dir.
    """
    end = time.time() + timeout_sec
    while time.time() < end:
        after = set(download_dir.glob("*.csv"))
        new_files = list(after - before)
        if new_files:
            latest = max(new_files, key=lambda p: p.stat().st_mtime)
            # ensure Chrome finished writing
            if not (download_dir / (latest.name + ".crdownload")).exists():
                return latest
        time.sleep(1)
    raise TimeoutException("Timed out waiting for CSV download.")

def upload_csv_to_r2(csv_path: Path, r2_key: str) -> str:
    """Upload CSV to R2 and return public URL"""
    try:
        r2_client = get_r2_client()
        
        with open(csv_path, 'rb') as file:
            r2_client.upload_fileobj(
                file,
                R2_BUCKET,
                r2_key,
                ExtraArgs={'ContentType': 'text/csv'}
            )
        
        # Construct public URL
        public_url = f"{R2_PUBLIC_BASEURL}/{r2_key}"
        return public_url
    except Exception as e:
        raise Exception(f"Failed to upload CSV to R2: {e}")

def parse_csv_to_actions_data(csv_path: Path) -> list:
    """Parse CSV file and return list of corporate actions"""
    actions_data = []
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Clean up the row data
                cleaned_row = {}
                for key, value in row.items():
                    # Clean up column names and values
                    clean_key = key.strip() if key else ""
                    clean_value = value.strip() if value else ""
                    cleaned_row[clean_key] = clean_value
                
                actions_data.append(cleaned_row)
    
    except Exception as e:
        raise Exception(f"Failed to parse CSV: {e}")
    
    return actions_data

# =========== management command ===========
class Command(BaseCommand):
    help = "Download BSE Corporate Actions CSV and store in database with R2 cloud storage"

    def add_arguments(self, parser):
        parser.add_argument("--url", default=None, help="Corporate Actions page URL")
        parser.add_argument("--scripcode", default=None, help="BSE scripcode, e.g. 500325")
        parser.add_argument("--headful", action="store_true", help="Run visible browser (not headless)")

    def handle(self, *args, **opts):
        # Build the final URL + resolved scripcode
        final_url, resolved_scrip = build_company_url(opts.get("url"), opts.get("scripcode"))
        headless = not opts["headful"]

        driver = start_driver(DOWNLOADS_DIR, headless=headless)
        wait = WebDriverWait(driver, 30)

        try:
            self.stdout.write(self.style.NOTICE(f"Opening: {final_url}"))
            driver.get(final_url)

            # Wait for basic page load
            self.stdout.write(self.style.NOTICE("Waiting for page to load..."))
            wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
            time.sleep(5)

            # Optional: confirm that the page is the company-specific view if scripcode provided
            if resolved_scrip:
                self.stdout.write(self.style.NOTICE(f"Expecting company page for scripcode: {resolved_scrip}"))

            # Find download button (using same logic from original code)
            self.stdout.write(self.style.NOTICE("Looking for download button..."))
            
            download_btn = None
            successful_method = None

            # Strategy 1: Direct ng-click selector
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, "[ng-click*='downloadcsv']")
                if elements:
                    download_btn = elements[0]
                    successful_method = f"ng-click selector (found {len(elements)} elements)"
            except Exception:
                pass

            # Strategy 2: XPath with ng-click (simplified from original)
            if not download_btn:
                try:
                    download_btn = driver.find_element(By.XPATH, "//*[@ng-click and contains(@ng-click, 'downloadcsv')]")
                    successful_method = "XPath ng-click"
                except Exception:
                    pass

            if not download_btn:
                raise TimeoutException("Could not find download button. Try running with --headful to inspect.")

            self.stdout.write(self.style.NOTICE(f"Found download button via: {successful_method}"))

            # Snapshot existing files
            before_files = set(DOWNLOADS_DIR.glob("*.csv"))

            # Click download button
            if safe_click(driver, download_btn):
                self.stdout.write(self.style.SUCCESS("Download button clicked successfully"))
            else:
                raise Exception("All click methods failed")

            time.sleep(3)

            # Wait for CSV file
            self.stdout.write(self.style.NOTICE("Waiting for CSV download..."))
            csv_path = wait_for_csv(DOWNLOADS_DIR, before_files, timeout_sec=120)

            file_size = csv_path.stat().st_size
            self.stdout.write(self.style.SUCCESS(f"Downloaded CSV: {csv_path.name} ({file_size:,} bytes)"))

            # Parse CSV to get actions data
            self.stdout.write(self.style.NOTICE("Parsing CSV data..."))
            actions_data = parse_csv_to_actions_data(csv_path)

            # Extract company information from the first row (if available)
            company_name = None
            security_name = None
            bse_code = resolved_scrip

            if actions_data and len(actions_data) > 0:
                first_action = actions_data[0]
                company_name = first_action.get('Company Name', '')
                security_name = first_action.get('Security Name', '')
                if not bse_code:
                    bse_code = first_action.get('Security Code', '')

            # Generate R2 key for file storage
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            tag = f"_{bse_code}" if bse_code else ""
            r2_key = f"corporate_actions/csv/corp_actions{tag}_{timestamp}.csv"

            # Upload CSV to R2
            self.stdout.write(self.style.NOTICE("Uploading CSV to R2 cloud storage..."))
            cloud_url = upload_csv_to_r2(csv_path, r2_key)

            # Create or update CorporateAction record
            corp_action, created = CorporateAction.objects.update_or_create(
                bse_code=bse_code,
                defaults={
                    'company_name': company_name,
                    'security_name': security_name,
                    'actions_data': actions_data,
                    'csv_r2_path': r2_key,
                    'csv_cloud_url': cloud_url,
                }
            )

            action_word = "Created" if created else "Updated"
            self.stdout.write(self.style.SUCCESS(
                f"{action_word} CorporateAction record: {corp_action.company_name} "
                f"({corp_action.bse_code}) with {corp_action.total_actions_count} actions"
            ))
            self.stdout.write(self.style.SUCCESS(f"CSV stored at: {cloud_url}"))

            # Clean up local CSV file
            csv_path.unlink()
            self.stdout.write(self.style.NOTICE("Local CSV file cleaned up"))

            self.stdout.write(self.style.SUCCESS("Corporate Actions processing completed successfully!"))

        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Error: {e}"))
            self.stderr.write(self.style.ERROR(
                "\nTroubleshooting:\n"
                "1. Try: python manage.py bse_corpactions_csv --headful\n"
                "2. Check your internet connection\n"
                "3. Verify the BSE website is accessible\n"
                "4. Check R2 credentials in settings"
            ))
            raise

        finally:
            driver.quit()