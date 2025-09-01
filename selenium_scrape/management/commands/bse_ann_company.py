from django.core.management.base import BaseCommand, CommandError
from datetime import datetime, timedelta
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from typing import List, Dict, Optional
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

class Command(BaseCommand):
    help = 'Scrape BSE corporate announcements for a specific company'

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
            help='Start date in DD-MM-YYYY format (default: 1 year ago)'
        )
        parser.add_argument(
            '--end-date',
            type=str,
            help='End date in DD-MM-YYYY format (default: today)'
        )
        parser.add_argument(
            '--max-pages',
            type=int,
            default=10,
            help='Maximum pages to scrape (default: 10)'
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
        parser.add_argument(
            '--interactive',
            action='store_true',
            help='Run interactive debugging session'
        )

    def handle(self, *args, **options):
        company_code = options['company_code']
        start_date = options.get('start_date')
        end_date = options.get('end_date')
        max_pages = options['max_pages']
        debug_mode = options['debug']
        save_json = options.get('save_json')
        interactive = options['interactive']

        # Validate company code
        if not re.match(r'^\d{6}$', company_code):
            raise CommandError('Company code must be exactly 6 digits')

        # Set default dates if not provided
        if not start_date or not end_date:
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=365)  # 1 year ago
            start_date = start_dt.strftime('%d-%m-%Y')
            end_date = end_dt.strftime('%d-%m-%Y')

        # Validate date format
        try:
            datetime.strptime(start_date, '%d-%m-%Y')
            datetime.strptime(end_date, '%d-%m-%Y')
        except ValueError:
            raise CommandError('Dates must be in DD-MM-YYYY format')

        # Convert to format expected by BSE (DD/MM/YYYY)
        bse_start_date = start_date.replace('-', '/')
        bse_end_date = end_date.replace('-', '/')

        self.stdout.write(
            self.style.SUCCESS(f'BSE Corporate Announcements Scraper\n') +
            f'Company Code: {company_code}\n' +
            f'Date Range: {start_date} to {end_date}\n' +
            f'Max Pages: {max_pages}\n' +
            f'Debug Mode: {debug_mode}\n' +
            f'Interactive: {interactive}'
        )

        if interactive:
            # Run interactive debug session
            self.run_interactive_debug(company_code)
        else:
            # Run normal scraping
            result = self.run_enhanced_scraper(
                company_code, bse_start_date, bse_end_date, max_pages, debug_mode
            )

            # Display results
            self.display_results(result)

            # Save to JSON if requested
            if save_json:
                with open(save_json, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=2, ensure_ascii=False)
                self.stdout.write(self.style.SUCCESS(f'Results saved to: {save_json}'))

    def setup_driver(self, headless: bool = True):
        """Setup Chrome driver with Angular-friendly configuration"""
        opts = Options()

        if headless:
            opts.add_argument("--headless=new")

        # Essential arguments for Angular apps
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-web-security")
        opts.add_argument("--allow-running-insecure-content")
        opts.add_argument("--window-size=1366,768")

        # User agent
        opts.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        # Preferences
        prefs = {
            "profile.default_content_setting_values.notifications": 2,
            "profile.default_content_settings.popups": 0,
        }
        opts.add_experimental_option("prefs", prefs)
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option('useAutomationExtension', False)

        return webdriver.Chrome(options=opts)

    def wait_for_angular_ready(self, driver, timeout=30):
        """Wait for Angular application to be ready"""
        self.stdout.write("Waiting for Angular to load...")

        # Wait for Angular to be defined
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return typeof angular !== 'undefined'")
        )

        # Wait for the Angular app to bootstrap
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script(
                "return angular.element(document.body).injector() && "
                "angular.element(document.body).scope() !== undefined"
            )
        )

        self.stdout.write("Angular is ready!")

    def find_company_search_field(self, driver):
        """Find the company search field using multiple strategies"""
        self.stdout.write("Searching for company input field...")

        # Strategy 1: Wait for SmartSearch component to load
        try:
            # Wait for the ng-include to load the SmartSearch component
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[ng-include*='SmartSearch']"))
            )

            # Give time for the component to render
            time.sleep(5)

            # Look for text inputs in the SmartSearch area
            smart_search_div = driver.find_element(By.CSS_SELECTOR, "div[ng-include*='SmartSearch']")
            inputs = smart_search_div.find_elements(By.TAG_NAME, "input")

            for inp in inputs:
                if inp.get_attribute("type") == "text" and inp.is_displayed():
                    self.stdout.write("Found company search field in SmartSearch component")
                    return inp

        except Exception as e:
            self.stdout.write(f"SmartSearch strategy failed: {e}")

        # Strategy 2: Look for any visible text input that could be company search
        try:
            all_text_inputs = driver.find_elements(By.CSS_SELECTOR, "input[type='text']")

            # Filter out date inputs and find the most likely company search field
            for inp in all_text_inputs:
                inp_id = inp.get_attribute("id")

                # Skip known date fields
                if inp_id in ["txtFromDt", "txtToDt"]:
                    continue

                # If it's visible and not a date field, it's likely the company search
                if inp.is_displayed() and inp.is_enabled():
                    self.stdout.write(f"Found potential company search field with ID: {inp_id}")
                    return inp

        except Exception as e:
            self.stdout.write(f"Text input strategy failed: {e}")

        # Strategy 3: Use JavaScript to find the field through Angular
        try:
            search_field = driver.execute_script("""
                // Look for inputs in the SmartSearch area
                var smartSearchDiv = document.querySelector('div[ng-include*="SmartSearch"]');
                if (smartSearchDiv) {
                    var inputs = smartSearchDiv.querySelectorAll('input[type="text"]');
                    for (var i = 0; i < inputs.length; i++) {
                        if (inputs[i].offsetParent !== null) { // visible
                            return inputs[i];
                        }
                    }
                }

                // Fallback: find any visible text input that's not a date field
                var allInputs = document.querySelectorAll('input[type="text"]');
                for (var i = 0; i < allInputs.length; i++) {
                    var inp = allInputs[i];
                    if (inp.id !== 'txtFromDt' && inp.id !== 'txtToDt' && inp.offsetParent !== null) {
                        return inp;
                    }
                }
                return null;
            """)

            if search_field:
                self.stdout.write("Found company search field via JavaScript")
                return search_field

        except Exception as e:
            self.stdout.write(f"JavaScript strategy failed: {e}")

        return None

    def handle_dropdown_selection(self, driver, company_code):
        """Handle dropdown selection after typing company code"""
        self.stdout.write("Looking for dropdown suggestions...")

        # Wait a bit for dropdown to appear
        time.sleep(2)

        # Multiple selectors for dropdown options
        dropdown_selectors = [
            "ul.dropdown-menu li",
            ".autocomplete-suggestion",
            ".typeahead-option",
            "ul[role='listbox'] li",
            ".suggestion-item",
            "li[role='option']",
            ".smart-search-option",
            "ul li[ng-repeat]"
        ]

        dropdown_found = False

        for selector in dropdown_selectors:
            try:
                options = driver.find_elements(By.CSS_SELECTOR, selector)
                visible_options = [opt for opt in options if opt.is_displayed()]

                if visible_options:
                    self.stdout.write(f"Found {len(visible_options)} dropdown options with selector: {selector}")

                    # Look for exact match first, then partial match
                    for option in visible_options:
                        option_text = option.get_text().strip()
                        self.stdout.write(f"  Option: {option_text[:50]}...")

                        # Check if this option contains our company code
                        if company_code in option_text:
                            self.stdout.write(f"Selecting option containing company code: {company_code}")
                            try:
                                # Try different click methods
                                try:
                                    option.click()
                                except:
                                    driver.execute_script("arguments[0].click();", option)

                                time.sleep(2)
                                dropdown_found = True
                                self.stdout.write("Successfully selected dropdown option")
                                return True
                            except Exception as e:
                                self.stdout.write(f"Failed to click option: {e}")
                                continue

                    # If no exact match, try first option
                    if not dropdown_found and visible_options:
                        try:
                            self.stdout.write("No exact match found, trying first option...")
                            first_option = visible_options[0]
                            driver.execute_script("arguments[0].click();", first_option)
                            time.sleep(2)
                            dropdown_found = True
                            self.stdout.write("Selected first dropdown option")
                            return True
                        except Exception as e:
                            self.stdout.write(f"Failed to click first option: {e}")

                    break  # Exit selector loop if we found options

            except Exception as e:
                continue  # Try next selector

        if not dropdown_found:
            self.stdout.write("No dropdown options found or could not select")
            return False

        return dropdown_found

    def set_filters(self, driver, company_code, start_date, end_date):
        """Set all form filters"""
        success_count = 0

        # 1. Set company code with proper dropdown selection
        self.stdout.write(f"Setting company code: {company_code}")
        search_field = self.find_company_search_field(driver)

        if search_field:
            try:
                search_field.clear()
                time.sleep(0.5)
                search_field.send_keys(company_code)
                time.sleep(3)  # Wait longer for dropdown to populate

                # Trigger events for Angular
                driver.execute_script("""
                    arguments[0].dispatchEvent(new Event('input', { bubbles: true }));
                    arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
                    arguments[0].dispatchEvent(new Event('keyup', { bubbles: true }));
                """, search_field)

                time.sleep(3)  # Wait for dropdown to appear

                # Handle dropdown selection
                if self.handle_dropdown_selection(driver, company_code):
                    success_count += 1
                    self.stdout.write("Company code set successfully with dropdown selection")
                else:
                    self.stdout.write(self.style.WARNING("Could not select from dropdown, trying alternative approach..."))

                    # Alternative: Try to find company by name instead of code
                    try:
                        # Clear field and try searching by partial company name
                        search_field.clear()
                        time.sleep(1)

                        # Map common company codes to names for better search
                        company_names = {
                            "500112": "State Bank",
                            "500325": "Reliance",
                            "500209": "Infosys",
                            "532540": "TCS",
                            "500180": "HDFC"
                        }

                        search_term = company_names.get(company_code, company_code)
                        search_field.send_keys(search_term)
                        time.sleep(3)

                        driver.execute_script("""
                            arguments[0].dispatchEvent(new Event('input', { bubbles: true }));
                            arguments[0].dispatchEvent(new Event('keyup', { bubbles: true }));
                        """, search_field)

                        time.sleep(3)

                        if self.handle_dropdown_selection(driver, company_code):
                            success_count += 1
                            self.stdout.write("Company set successfully using name search")
                        else:
                            self.stdout.write(self.style.WARNING("Both code and name search failed"))

                    except Exception as e:
                        self.stdout.write(self.style.WARNING(f"Alternative search failed: {e}"))

            except Exception as e:
                self.stdout.write(self.style.WARNING(f"Failed to set company code: {e}"))
        else:
            self.stdout.write(self.style.WARNING("Could not find company search field"))

        # 2. Set date range
        self.stdout.write(f"Setting date range: {start_date} to {end_date}")
        try:
            # From Date
            from_date_input = driver.find_element(By.ID, "txtFromDt")
            driver.execute_script(f"arguments[0].value = '{start_date}';", from_date_input)
            driver.execute_script("arguments[0].dispatchEvent(new Event('change', { bubbles: true }));", from_date_input)

            # To Date
            to_date_input = driver.find_element(By.ID, "txtToDt")
            driver.execute_script(f"arguments[0].value = '{end_date}';", to_date_input)
            driver.execute_script("arguments[0].dispatchEvent(new Event('change', { bubbles: true }));", to_date_input)

            success_count += 1
            self.stdout.write("Date range set successfully")
            time.sleep(2)

        except Exception as e:
            self.stdout.write(self.style.WARNING(f"Failed to set date range: {e}"))

        # 3. Set segment to Equity
        try:
            segment_dropdown = Select(driver.find_element(By.ID, "ddlAnnType"))
            segment_dropdown.select_by_value("C")  # Equity
            success_count += 1
            self.stdout.write("Segment set to Equity")
            time.sleep(1)
        except Exception as e:
            self.stdout.write(self.style.WARNING(f"Failed to set segment: {e}"))

        # 4. Set announcement type
        try:
            ann_type_dropdown = Select(driver.find_element(By.ID, "ddlAnnsubmType"))
            ann_type_dropdown.select_by_value("0")  # Announcement
            success_count += 1
            self.stdout.write("Announcement type set")
            time.sleep(1)
        except Exception as e:
            self.stdout.write(self.style.WARNING(f"Failed to set announcement type: {e}"))

        return success_count >= 2  # Need at least company and dates to be successful

    def submit_form(self, driver):
        """Submit the form and handle any alerts"""
        self.stdout.write("Submitting form...")

        try:
            # Check for any existing alerts first and dismiss them
            try:
                alert = driver.switch_to.alert
                alert_text = alert.text
                self.stdout.write(f"Dismissing existing alert: {alert_text}")
                alert.accept()
                time.sleep(1)
            except:
                pass  # No alert present

            # Find submit button
            submit_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "input[value='Submit']"))
            )

            # Click submit
            driver.execute_script("arguments[0].click();", submit_button)
            self.stdout.write("Submit button clicked")

            # Wait and check for alerts
            time.sleep(3)

            try:
                alert = driver.switch_to.alert
                alert_text = alert.text
                self.stdout.write(f"Alert appeared: {alert_text}")

                if "Please Enter or Select valid Security Name" in alert_text:
                    self.stdout.write("Company selection validation failed - accepting alert and retrying...")
                    alert.accept()
                    time.sleep(2)

                    # Try to fix company selection and resubmit
                    return self.retry_company_selection_and_submit(driver)
                else:
                    # Some other alert
                    alert.accept()
                    return False

            except:
                # No alert - form submitted successfully
                self.stdout.write("Form submitted without alerts")
                pass

            # Wait for page to process
            time.sleep(5)

            # Wait for Angular to finish loading
            self.stdout.write("Waiting for results to load...")

            for attempt in range(20):  # Wait up to 40 seconds
                time.sleep(2)

                # Check if data is loaded
                data_state = driver.execute_script("""
                    try {
                        var scope = angular.element(document.body).scope();
                        if (scope && scope.loader) {
                            return scope.loader.CorpAnnState || 'unknown';
                        }
                        return 'no_scope';
                    } catch(e) {
                        return 'error';
                    }
                """)

                self.stdout.write(f"Loading check {attempt + 1}/20: {data_state}")

                if data_state == 'loaded':
                    self.stdout.write("Data loaded successfully!")
                    return True
                elif data_state == 'error':
                    break

            self.stdout.write(self.style.WARNING("Timeout waiting for data to load"))
            return False

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Form submission failed: {e}"))
            return False

    def retry_company_selection_and_submit(self, driver):
        """Retry company selection with different approach after alert"""
        self.stdout.write("Retrying company selection...")

        try:
            # Find the search field again
            search_field = self.find_company_search_field(driver)

            if search_field:
                # Clear the field completely
                search_field.clear()
                time.sleep(1)

                # Try different search approaches
                search_attempts = [
                    ("500112", "State Bank"),
                    ("500112", "SBI"),
                    ("500112", "STATE BANK OF INDIA"),
                ]

                for code, name_search in search_attempts:
                    self.stdout.write(f"Trying search term: {name_search}")

                    search_field.clear()
                    time.sleep(1)
                    search_field.send_keys(name_search)
                    time.sleep(4)

                    # Trigger events
                    driver.execute_script("""
                        arguments[0].dispatchEvent(new Event('input', { bubbles: true }));
                        arguments[0].dispatchEvent(new Event('keyup', { bubbles: true }));
                    """, search_field)

                    time.sleep(4)

                    # Try to select from dropdown
                    if self.handle_dropdown_selection(driver, code):
                        self.stdout.write(f"Successfully selected using search term: {name_search}")

                        # Now try to submit again
                        time.sleep(2)
                        submit_button = driver.find_element(By.CSS_SELECTOR, "input[value='Submit']")
                        driver.execute_script("arguments[0].click();", submit_button)
                        time.sleep(3)

                        # Check for alert again
                        try:
                            alert = driver.switch_to.alert
                            alert_text = alert.text
                            self.stdout.write(f"Alert after retry: {alert_text}")
                            alert.accept()
                            continue  # Try next search term
                        except:
                            # No alert - success!
                            self.stdout.write("Form submitted successfully after retry")
                            return True

                self.stdout.write("All retry attempts failed")
                return False

        except Exception as e:
            self.stdout.write(f"Retry failed: {e}")
            return False

    def check_for_results(self, driver):
        """Check if results are available and what type"""
        try:
            # Check Angular scope for data
            result_info = driver.execute_script("""
                try {
                    var scope = angular.element(document.body).scope();
                    if (scope && scope.CorpannData) {
                        return {
                            hasData: !!scope.CorpannData.Table,
                            tableLength: scope.CorpannData.Table ? scope.CorpannData.Table.length : 0,
                            currentPage: scope.currentPage || 1,
                            totalPages: scope.pagenumber || 1,
                            totalCount: scope.CorpannData.Table1 && scope.CorpannData.Table1[0] ? scope.CorpannData.Table1[0].ROWCNT : 0
                        };
                    }
                    return { hasData: false, error: 'No CorpannData in scope' };
                } catch(e) {
                    return { hasData: false, error: e.message };
                }
            """)

            self.stdout.write(f"Result check: {json.dumps(result_info, indent=2)}")

            if result_info.get('hasData'):
                return {
                    'status': 'has_data',
                    'count': result_info.get('tableLength', 0),
                    'total_count': result_info.get('totalCount', 0),
                    'current_page': result_info.get('currentPage', 1),
                    'total_pages': result_info.get('totalPages', 1)
                }
            else:
                # Check HTML for "No Records Found"
                if "no records found" in driver.page_source.lower():
                    return {'status': 'no_data', 'message': 'No records found'}
                else:
                    return {'status': 'unknown', 'error': result_info.get('error', 'Unknown state')}

        except Exception as e:
            return {'status': 'error', 'error': str(e)}

    def scrape_current_page(self, driver, company_code):
        """Scrape announcements from current page"""
        announcements = []

        try:
            soup = BeautifulSoup(driver.page_source, "lxml")
            tables = soup.find_all("table", {"ng-repeat": re.compile(r"cann in CorpannData\.Table")})

            for table_idx, table in enumerate(tables):
                try:
                    # Extract announcement data
                    newssub_tag = table.find("span", {"ng-bind-html": "cann.NEWSSUB"})
                    headline_tag = table.find("span", {"ng-bind-html": "cann.HEADLINE"})
                    pdf_tag = table.find("a", class_="tablebluelink", href=True)

                    newssub = (newssub_tag.get_text(strip=True) if newssub_tag else "") or ""
                    headline = (headline_tag.get_text(strip=True) if headline_tag else "") or ""
                    pdf_link = urljoin("https://www.bseindia.com/corporates/ann.html", pdf_tag["href"]) if pdf_tag else ""

                    # Extract category
                    category = ""
                    try:
                        rows = table.find_all("tr")
                        if rows:
                            first_row_tds = rows[0].find_all("td")
                            for td in first_row_tds:
                                td_text = td.get_text(strip=True)
                                if td_text and not re.search(r'\d+(\.\d+)?\s*(KB|MB)', td_text, re.I):
                                    if td_text.upper() not in ['XBRL', '']:
                                        category = td_text
                                        break
                    except:
                        pass

                    # Extract timestamps
                    timing_info = {}
                    try:
                        all_rows = table.find_all("tr")
                        for row in all_rows:
                            row_text = row.get_text()

                            received_match = re.search(
                                r"Exchange Received Time\s*(\d{2}-\d{2}-\d{4})\s*(\d{2}:\d{2}:\d{2})",
                                row_text
                            )
                            dissem_match = re.search(
                                r"Exchange Disseminated Time\s*(\d{2}-\d{2}-\d{4})\s*(\d{2}:\d{2}:\d{2})",
                                row_text
                            )

                            if received_match:
                                timing_info["received_date"] = received_match.group(1)
                                timing_info["received_time"] = received_match.group(2)

                            if dissem_match:
                                timing_info["disseminated_date"] = dissem_match.group(1)
                                timing_info["disseminated_time"] = dissem_match.group(2)

                    except:
                        pass

                    # Extract company info
                    company_name = ""
                    found_company_code = ""
                    if newssub:
                        parts = newssub.split("-")
                        if parts:
                            company_name = parts[0].strip()

                        code_match = re.search(r'\b(\d{6})\b', newssub)
                        if code_match:
                            found_company_code = code_match.group(1)

                    # Filter by company if specified
                    if company_code != "ALL":
                        if found_company_code and found_company_code != company_code:
                            continue
                        elif not found_company_code and company_code not in newssub:
                            continue

                    announcements.append({
                        "serial_no": len(announcements) + 1,
                        "headline": headline,
                        "category": category,
                        "company_name": company_name,
                        "company_code": found_company_code or company_code,
                        "announcement_text": headline,
                        "exchange_received_date": timing_info.get("received_date", ""),
                        "exchange_received_time": timing_info.get("received_time", ""),
                        "exchange_disseminated_date": timing_info.get("disseminated_date", ""),
                        "exchange_disseminated_time": timing_info.get("disseminated_time", ""),
                        "pdf_link": pdf_link,
                        "newssub_full": newssub,
                        "table_index": table_idx
                    })

                except Exception as e:
                    self.stdout.write(f"Error parsing table {table_idx}: {e}")
                    continue

        except Exception as e:
            self.stdout.write(f"Error scraping page: {e}")

        return announcements

    def navigate_next_page(self, driver):
        """Navigate to next page if available"""
        try:
            next_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "idnext"))
            )

            if next_button.get_attribute("disabled"):
                return False

            driver.execute_script("arguments[0].click();", next_button)
            time.sleep(5)

            # Wait for new data to load
            WebDriverWait(driver, 20).until(
                lambda d: d.execute_script("""
                    try {
                        var scope = angular.element(document.body).scope();
                        return scope && scope.loader && scope.loader.CorpAnnState === 'loaded';
                    } catch(e) {
                        return false;
                    }
                """)
            )

            return True

        except:
            return False

    def run_enhanced_scraper(self, company_code, start_date, end_date, max_pages, debug_mode):
        """Run the enhanced scraper"""
        driver = None
        all_announcements = []

        try:
            # Setup
            self.stdout.write("Setting up Chrome driver...")
            driver = self.setup_driver(headless=not debug_mode)

            # Load page
            self.stdout.write("Loading BSE announcements page...")
            driver.get("https://www.bseindia.com/corporates/ann.html")
            time.sleep(5)

            # Wait for Angular
            try:
                self.wait_for_angular_ready(driver)
            except Exception as e:
                self.stdout.write(self.style.WARNING(f"Angular wait failed: {e}"))

            # Additional wait for SmartSearch component
            self.stdout.write("Waiting for SmartSearch component...")
            time.sleep(10)

            # Set filters
            filter_success = self.set_filters(driver, company_code, start_date, end_date)

            if not filter_success:
                self.stdout.write(self.style.WARNING("Filter setting had issues, but continuing..."))

            # Submit form
            if self.submit_form(driver):
                # Check results
                result_status = self.check_for_results(driver)

                if result_status['status'] == 'has_data':
                    self.stdout.write(self.style.SUCCESS(f"Found data! Total announcements: {result_status.get('total_count', 'Unknown')}"))

                    # Scrape pages
                    page_count = 0
                    while page_count < max_pages:
                        page_count += 1
                        self.stdout.write(f"Scraping page {page_count}...")

                        page_announcements = self.scrape_current_page(driver, company_code)
                        all_announcements.extend(page_announcements)

                        self.stdout.write(f"Page {page_count}: Found {len(page_announcements)} announcements")

                        # Try next page
                        if page_count < max_pages and not self.navigate_next_page(driver):
                            self.stdout.write("No more pages available")
                            break

                    return {
                        'status': 'success',
                        'announcements': all_announcements,
                        'pages_scraped': page_count,
                        'total_found': len(all_announcements)
                    }

                elif result_status['status'] == 'no_data':
                    return {
                        'status': 'no_data',
                        'announcements': [],
                        'message': 'No announcements found for specified criteria'
                    }
                else:
                    return {
                        'status': 'unknown_state',
                        'announcements': [],
                        'error': result_status.get('error', 'Unknown error')
                    }
            else:
                return {
                    'status': 'submit_failed',
                    'announcements': [],
                    'error': 'Could not submit form'
                }

        except Exception as e:
            # Save debug page
            if driver:
                try:
                    debug_file = f"debug_django_{company_code}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
                    with open(debug_file, "w", encoding="utf-8") as f:
                        f.write(driver.page_source)
                    self.stdout.write(f"Debug page saved: {debug_file}")
                except:
                    pass

            return {
                'status': 'error',
                'announcements': [],
                'error': str(e)
            }

        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

    def run_interactive_debug(self, company_code):
        """Run interactive debugging session"""
        self.stdout.write("Starting interactive debug session...")
        self.stdout.write("Browser will open and remain visible for manual inspection")

        driver = self.setup_driver(headless=False)

        try:
            driver.get("https://www.bseindia.com/corporates/ann.html")
            time.sleep(10)

            # Show page analysis
            self.stdout.write("\nPage Analysis:")

            # Check Angular
            angular_info = driver.execute_script("""
                try {
                    return {
                        angularLoaded: typeof angular !== 'undefined',
                        scopeExists: !!angular.element(document.body).scope(),
                        controllerName: angular.element(document.body).scope().$parent ? 'found' : 'not_found'
                    };
                } catch(e) {
                    return { error: e.message };
                }
            """)
            self.stdout.write(f"Angular Status: {json.dumps(angular_info, indent=2)}")

            # Show form elements
            selects = driver.find_elements(By.TAG_NAME, "select")
            inputs = driver.find_elements(By.TAG_NAME, "input")

            self.stdout.write(f"\nForm Elements Found:")
            self.stdout.write(f"  Select elements: {len(selects)}")
            self.stdout.write(f"  Input elements: {len(inputs)}")

            for inp in inputs:
                inp_id = inp.get_attribute("id")
                inp_type = inp.get_attribute("type")
                if inp_type == "text":
                    self.stdout.write(f"    Text input: {inp_id}")

            self.stdout.write(f"\nBrowser is open at: {driver.current_url}")
            self.stdout.write("You can manually test the form now.")
            self.stdout.write("Browser will close in 120 seconds...")

            time.sleep(120)

        except Exception as e:
            self.stdout.write(f"Debug session error: {e}")
            time.sleep(30)

        finally:
            driver.quit()

    def display_results(self, result):
        """Display scraping results"""
        status = result.get('status', 'unknown')
        announcements = result.get('announcements', [])

        self.stdout.write(f"\nScraping Results:")
        self.stdout.write(f"Status: {status}")

        if status == 'success':
            self.stdout.write(self.style.SUCCESS(f"Successfully scraped {len(announcements)} announcements"))

            if announcements:
                self.stdout.write(f"\nSample Announcements:")
                for i, ann in enumerate(announcements[:5], 1):
                    date = ann.get('exchange_disseminated_date', 'N/A')
                    headline = ann.get('headline', 'N/A')[:80]
                    category = ann.get('category', 'N/A')

                    self.stdout.write(f"{i}. [{date}] {headline}...")
                    self.stdout.write(f"   Category: {category}")

                if len(announcements) > 5:
                    self.stdout.write(f"... and {len(announcements) - 5} more announcements")

                # Category breakdown
                categories = {}
                for ann in announcements:
                    cat = ann.get("category", "Unknown")
                    categories[cat] = categories.get(cat, 0) + 1

                self.stdout.write(f"\nCategory Breakdown:")
                for cat, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
                    self.stdout.write(f"  {cat}: {count}")

        elif status == 'no_data':
            self.stdout.write(self.style.WARNING("No announcements found for the specified criteria"))

        elif status == 'error':
            self.stdout.write(self.style.ERROR(f"Scraping failed: {result.get('error', 'Unknown error')}"))

        elif status == 'submit_failed':
            self.stdout.write(self.style.ERROR("Could not submit the form properly"))

        else:
            self.stdout.write(self.style.WARNING(f"Unknown status: {status}"))