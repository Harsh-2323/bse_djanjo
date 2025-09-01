
from django.core.management.base import BaseCommand, CommandError
from datetime import datetime, timedelta
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.keys import Keys
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
        parser.add_argument(
            '--debug-dates',
            action='store_true',
            help='Run date field diagnostics instead of normal scraping'
        )

    def handle(self, *args, **options):
        company_code = options['company_code']
        start_date = options.get('start_date')
        end_date = options.get('end_date')
        max_pages = options['max_pages']
        debug_mode = options['debug']
        save_json = options.get('save_json')
        interactive = options['interactive']
        debug_dates = options.get('debug_dates', False)

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
            f'Interactive: {interactive}\n' +
            f'Date Debug Mode: {debug_dates}'
        )

        if debug_dates:
            # Run date debugging mode
            self.stdout.write("Running in DATE DEBUGGING mode...")
            driver = self.setup_driver(headless=False)  # Always visible for debugging
            try:
                driver.get("https://www.bseindia.com/corporates/ann.html")
                time.sleep(10)
                # Wait for page to load
                try:
                    self.wait_for_angular_ready(driver)
                except:
                    self.stdout.write("Angular wait failed, proceeding anyway")
                time.sleep(5)
                # Run diagnostics
                success = self.run_date_field_diagnostics(driver, bse_start_date, bse_end_date)
                self.stdout.write(f"\nDate debugging completed. Success: {success}")
                self.stdout.write("Browser will remain open for 60 seconds for manual inspection...")
                time.sleep(60)
            except Exception as e:
                self.stdout.write(f"Date debugging error: {e}")
            finally:
                driver.quit()
        elif interactive:
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
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-web-security")
        opts.add_argument("--allow-running-insecure-content")
        opts.add_argument("--window-size=1366,768")
        opts.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
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
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return typeof angular !== 'undefined'")
        )
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
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[ng-include*='SmartSearch']"))
            )
            time.sleep(5)
            smart_search_div = driver.find_element(By.CSS_SELECTOR, "div[ng-include*='SmartSearch']")
            inputs = smart_search_div.find_elements(By.TAG_NAME, "input")
            for inp in inputs:
                if inp.get_attribute("type") == "text" and inp.is_displayed():
                    self.stdout.write("Found company search field in SmartSearch component")
                    return inp
        except Exception as e:
            self.stdout.write(f"SmartSearch strategy failed: {e}")
        try:
            all_text_inputs = driver.find_elements(By.CSS_SELECTOR, "input[type='text']")
            for inp in all_text_inputs:
                inp_id = inp.get_attribute("id")
                if inp_id in ["txtFromDt", "txtToDt"]:
                    continue
                if inp.is_displayed() and inp.is_enabled():
                    self.stdout.write(f"Found potential company search field with ID: {inp_id}")
                    return inp
        except Exception as e:
            self.stdout.write(f"Text input strategy failed: {e}")
        try:
            search_field = driver.execute_script("""
                var smartSearchDiv = document.querySelector('div[ng-include*="SmartSearch"]');
                if (smartSearchDiv) {
                    var inputs = smartSearchDiv.querySelectorAll('input[type="text"]');
                    for (var i = 0; i < inputs.length; i++) {
                        if (inputs[i].offsetParent !== null) {
                            return inputs[i];
                        }
                    }
                }
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
        """Enhanced dropdown selection - properly detect and select from BSE dropdown"""
        self.stdout.write("Looking for dropdown suggestions...")
        dropdown_appeared = False
        max_wait_time = 10
        for attempt in range(max_wait_time):
            time.sleep(1)
            try:
                dropdown_ul = driver.find_element(By.CSS_SELECTOR, "ul.dropdown-menu")
                if dropdown_ul.is_displayed():
                    dropdown_options = dropdown_ul.find_elements(By.TAG_NAME, "li")
                    visible_options = [opt for opt in dropdown_options if opt.is_displayed() and opt.text.strip()]
                    if visible_options:
                        self.stdout.write(f"Found BSE dropdown with {len(visible_options)} options")
                        dropdown_appeared = True
                        for i, option in enumerate(visible_options[:3]):
                            self.stdout.write(f"  Option {i+1}: {option.text.strip()}")
                        try:
                            first_option = visible_options[0]
                            self.stdout.write(f"Selecting first option: {first_option.text.strip()}")
                            actions = ActionChains(driver)
                            actions.move_to_element(first_option).click().perform()
                            time.sleep(2)
                            self.stdout.write("Successfully selected first dropdown option")
                            return True
                        except Exception as click_error:
                            self.stdout.write(f"ActionChains click failed: {click_error}")
                            try:
                                driver.execute_script("arguments[0].click();", first_option)
                                time.sleep(2)
                                self.stdout.write("Successfully selected with JavaScript click")
                                return True
                            except Exception as js_error:
                                self.stdout.write(f"JavaScript click also failed: {js_error}")
                        break
            except NoSuchElementException:
                if attempt < max_wait_time - 1:
                    self.stdout.write(f"Waiting for dropdown... attempt {attempt + 1}/{max_wait_time}")
                continue
            except Exception as e:
                self.stdout.write(f"Error checking dropdown: {e}")
                continue
        if not dropdown_appeared:
            self.stdout.write("BSE dropdown did not appear - trying alternative approaches")
            try:
                search_field = self.find_company_search_field(driver)
                if search_field:
                    self.stdout.write("Trying Arrow Down + Enter keys...")
                    search_field.send_keys(Keys.ARROW_DOWN)
                    time.sleep(1)
                    search_field.send_keys(Keys.ENTER)
                    time.sleep(2)
                    return True
            except:
                pass
            try:
                search_field = self.find_company_search_field(driver)
                if search_field:
                    self.stdout.write("Trying Tab key...")
                    search_field.send_keys(Keys.TAB)
                    time.sleep(2)
                    return True
            except:
                pass
        self.stdout.write("All dropdown selection methods failed")
        return False

    def enhanced_set_date_fields(self, driver, start_date, end_date):
        """Enhanced method to set date fields with multiple strategies"""
        self.stdout.write(f"Enhanced date setting: From {start_date} to {end_date}")
        success_count = 0
        try:
            # Strategy 1: Direct input
            self.stdout.write("Trying direct input method...")
            from_date_input = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "txtFromDt"))
            )
            from_date_input.click()
            time.sleep(1)
            from_date_input.clear()
            from_date_input.send_keys(start_date)
            time.sleep(1)
            to_date_input = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "txtToDt"))
            )
            to_date_input.click()
            time.sleep(1)
            to_date_input.clear()
            to_date_input.send_keys(end_date)
            time.sleep(1)
            # Verify values
            if (from_date_input.get_attribute('value') == start_date and
                to_date_input.get_attribute('value') == end_date):
                self.stdout.write("Direct input successful")
                success_count += 1
            else:
                self.stdout.write("Direct input verification failed")
        except Exception as e:
            self.stdout.write(f"Direct input failed: {e}")
        # Strategy 2: JavaScript setting
        if success_count == 0:
            self.stdout.write("Trying JavaScript setting method...")
            try:
                driver.execute_script("""
                    var fromField = document.getElementById('txtFromDt');
                    var toField = document.getElementById('txtToDt');
                    fromField.value = arguments[0];
                    toField.value = arguments[1];
                    fromField.dispatchEvent(new Event('input', { bubbles: true }));
                    fromField.dispatchEvent(new Event('change', { bubbles: true }));
                    toField.dispatchEvent(new Event('input', { bubbles: true }));
                    toField.dispatchEvent(new Event('change', { bubbles: true }));
                """, start_date, end_date)
                time.sleep(2)
                from_value = driver.find_element(By.ID, "txtFromDt").get_attribute('value')
                to_value = driver.find_element(By.ID, "txtToDt").get_attribute('value')
                if from_value == start_date and to_value == end_date:
                    self.stdout.write("JavaScript setting successful")
                    success_count += 1
                else:
                    self.stdout.write(f"JavaScript verification failed: From {from_value}, To {to_value}")
            except Exception as e:
                self.stdout.write(f"JavaScript setting failed: {e}")
        # Strategy 3: Slow typing with focus
        if success_count == 0:
            self.stdout.write("Trying slow typing method...")
            try:
                from_date_input = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "txtFromDt"))
                )
                from_date_input.click()
                time.sleep(1)
                from_date_input.send_keys(Keys.CONTROL + "a")
                from_date_input.send_keys(Keys.DELETE)
                time.sleep(1)
                for char in start_date:
                    from_date_input.send_keys(char)
                    time.sleep(0.1)
                to_date_input = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "txtToDt"))
                )
                to_date_input.click()
                time.sleep(1)
                to_date_input.send_keys(Keys.CONTROL + "a")
                to_date_input.send_keys(Keys.DELETE)
                time.sleep(1)
                for char in end_date:
                    to_date_input.send_keys(char)
                    time.sleep(0.1)
                time.sleep(2)
                from_value = from_date_input.get_attribute('value')
                to_value = to_date_input.get_attribute('value')
                if from_value == start_date and to_value == end_date:
                    self.stdout.write("Slow typing successful")
                    success_count += 1
                else:
                    self.stdout.write(f"Slow typing verification failed: From {from_value}, To {to_value}")
            except Exception as e:
                self.stdout.write(f"Slow typing failed: {e}")
        # Close any open date pickers
        try:
            driver.execute_script("""
                var datePickers = document.querySelectorAll('.ui-datepicker, .ui-widget-overlay');
                datePickers.forEach(function(picker) {
                    if (picker.style.display !== 'none') {
                        picker.style.display = 'none';
                    }
                });
            """)
            time.sleep(1)
        except:
            pass
        return success_count > 0

    def set_filters(self, driver, company_code, start_date, end_date):
        """Set all form filters with enhanced company and date selection"""
        success_count = 0
        self.stdout.write(f"Setting company code: {company_code}")
        search_field = self.find_company_search_field(driver)
        if search_field:
            try:
                search_field.clear()
                time.sleep(1)
                self.stdout.write(f"Typing company code: {company_code}")
                search_field.send_keys(company_code)
                time.sleep(2)
                driver.execute_script("""
                    var element = arguments[0];
                    element.dispatchEvent(new Event('input', { bubbles: true }));
                    element.dispatchEvent(new Event('change', { bubbles: true }));
                    element.dispatchEvent(new Event('keyup', { bubbles: true }));
                    element.focus();
                """, search_field)
                time.sleep(3)
                if self.handle_dropdown_selection(driver, company_code):
                    success_count += 1
                    self.stdout.write("Company selected successfully from dropdown")
                    time.sleep(2)
                else:
                    self.stdout.write(self.style.WARNING("Dropdown selection failed, trying alternative..."))
                    try:
                        search_field.send_keys(Keys.TAB)
                        time.sleep(2)
                        success_count += 1
                        self.stdout.write("Company code set with Tab key")
                    except Exception as e:
                        self.stdout.write(self.style.WARNING(f"Tab approach failed: {e}"))
                        success_count += 1
                        self.stdout.write("Continuing with typed company code")
            except Exception as e:
                self.stdout.write(self.style.WARNING(f"Failed to set company code: {e}"))
                return False
        else:
            self.stdout.write(self.style.WARNING("Could not find company search field"))
            return False
        self.stdout.write(f"Setting date range: {start_date} to {end_date}")
        if self.enhanced_set_date_fields(driver, start_date, end_date):
            success_count += 1
            self.stdout.write("Date range set successfully")
        else:
            self.stdout.write(self.style.WARNING("Failed to set date range"))
            return False
        try:
            segment_dropdown = Select(WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "ddlAnnType"))
            ))
            segment_dropdown.select_by_value("C")
            success_count += 1
            self.stdout.write("Segment set to Equity")
            time.sleep(1)
        except Exception as e:
            self.stdout.write(self.style.WARNING(f"Failed to set segment: {e}"))
        try:
            ann_type_dropdown = Select(WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "ddlAnnsubmType"))
            ))
            ann_type_dropdown.select_by_value("0")
            success_count += 1
            self.stdout.write("Announcement type set")
            time.sleep(1)
        except Exception as e:
            self.stdout.write(self.style.WARNING(f"Failed to set announcement type: {e}"))
        return success_count >= 2

    def debug_date_fields(self, driver):
        """Comprehensive debugging function to analyze date field behavior on BSE page"""
        self.stdout.write("\n=== DATE FIELD DEBUGGING ===")
        try:
            debug_info = driver.execute_script("""
                var result = {
                    dateInputs: [],
                    datepickers: [],
                    jqueryDatepickers: [],
                    allInputs: [],
                    angularDateInfo: {}
                };
                var inputs = document.querySelectorAll('input');
                for (var i = 0; i < inputs.length; i++) {
                    var inp = inputs[i];
                    var info = {
                        id: inp.id,
                        name: inp.name,
                        type: inp.type,
                        placeholder: inp.placeholder,
                        className: inp.className,
                        value: inp.value,
                        readonly: inp.readOnly,
                        disabled: inp.disabled,
                        visible: inp.offsetParent !== null
                    };
                    if (inp.type === 'text' || inp.type === 'date') {
                        result.allInputs.push(info);
                        if (inp.id.toLowerCase().includes('date') || 
                            inp.name.toLowerCase().includes('date') ||
                            inp.placeholder.toLowerCase().includes('date') ||
                            inp.id === 'txtFromDt' || inp.id === 'txtToDt') {
                            result.dateInputs.push(info);
                        }
                    }
                }
                if (typeof $ !== 'undefined') {
                    $('.hasDatepicker').each(function(i, elem) {
                        result.jqueryDatepickers.push({
                            id: elem.id,
                            className: elem.className,
                            hasDatepicker: $(elem).hasClass('hasDatepicker'),
                            datepickerOptions: $(elem).datepicker('option', 'dateFormat') || 'unknown'
                        });
                    });
                }
                try {
                    var scope = angular.element(document.body).scope();
                    if (scope) {
                        result.angularDateInfo = {
                            fromDate: scope.txtFromDt || scope.fromDate || 'not found',
                            toDate: scope.txtToDt || scope.toDate || 'not found',
                            dateFormat: scope.dateFormat || 'not found'
                        };
                    }
                } catch(e) {
                    result.angularDateInfo = { error: e.message };
                }
                return result;
            """)
            self.stdout.write("Date Inputs Found:")
            for inp in debug_info.get('dateInputs', []):
                self.stdout.write(f"  ID: {inp.get('id', 'N/A')}, Name: {inp.get('name', 'N/A')}")
                self.stdout.write(f"    Type: {inp.get('type')}, Placeholder: {inp.get('placeholder', 'N/A')}")
                self.stdout.write(f"    Value: {inp.get('value', 'N/A')}, Visible: {inp.get('visible')}")
                self.stdout.write(f"    ReadOnly: {inp.get('readonly')}, Disabled: {inp.get('disabled')}")
                self.stdout.write("")
            self.stdout.write("jQuery Datepickers:")
            for dp in debug_info.get('jqueryDatepickers', []):
                self.stdout.write(f"  ID: {dp.get('id')}, Format: {dp.get('datepickerOptions')}")
            self.stdout.write(f"Angular Date Info: {debug_info.get('angularDateInfo', {})}")
            return debug_info
        except Exception as e:
            self.stdout.write(f"Debug date fields error: {e}")
            return {}

    def test_date_field_interaction(self, driver, field_id, test_date):
        """Test different ways to interact with a specific date field"""
        self.stdout.write(f"\n=== TESTING DATE FIELD: {field_id} ===")
        try:
            field = driver.find_element(By.ID, field_id)
            original_value = field.get_attribute('value')
            self.stdout.write(f"Original value: {original_value}")
            self.stdout.write("Test 1: Basic click and type")
            try:
                field.click()
                time.sleep(1)
                field.clear()
                field.send_keys(test_date)
                time.sleep(2)
                new_value = field.get_attribute('value')
                self.stdout.write(f"  Result: {new_value} (Success: {bool(new_value)})")
                field.clear()
                time.sleep(1)
            except Exception as e:
                self.stdout.write(f"  Failed: {e}")
            self.stdout.write("Test 2: JavaScript direct value")
            try:
                driver.execute_script("arguments[0].value = arguments[1];", field, test_date)
                driver.execute_script("arguments[0].dispatchEvent(new Event('change'));", field)
                time.sleep(1)
                new_value = field.get_attribute('value')
                self.stdout.write(f"  Result: {new_value} (Success: {bool(new_value)})")
                field.clear()
                time.sleep(1)
            except Exception as e:
                self.stdout.write(f"  Failed: {e}")
            self.stdout.write("Test 3: Focus, clear, type with delays")
            try:
                field.click()
                time.sleep(1)
                field.send_keys(Keys.CONTROL + "a")
                field.send_keys(Keys.DELETE)
                time.sleep(1)
                for char in test_date:
                    field.send_keys(char)
                    time.sleep(0.1)
                time.sleep(2)
                new_value = field.get_attribute('value')
                self.stdout.write(f"  Result: {new_value} (Success: {bool(new_value)})")
            except Exception as e:
                self.stdout.write(f"  Failed: {e}")
            self.stdout.write("Test 4: Checking for date picker behavior")
            try:
                field.click()
                time.sleep(2)
                picker_visible = driver.execute_script("""
                    var pickers = document.querySelectorAll('.ui-datepicker, .datepicker, [class*="date-picker"]');
                    for (var i = 0; i < pickers.length; i++) {
                        if (pickers[i].offsetParent !== null) {
                            return {
                                found: true,
                                className: pickers[i].className,
                                id: pickers[i].id,
                                display: getComputedStyle(pickers[i]).display
                            };
                        }
                    }
                    return { found: false };
                """)
                self.stdout.write(f"  Date picker visible: {picker_visible}")
                if picker_visible.get('found'):
                    driver.execute_script("$('.ui-datepicker').hide();")
            except Exception as e:
                self.stdout.write(f"  Failed: {e}")
        except Exception as e:
            self.stdout.write(f"Could not find field {field_id}: {e}")

    def run_date_field_diagnostics(self, driver, start_date, end_date):
        """Run comprehensive diagnostics on date field handling"""
        self.stdout.write("\n" + "="*50)
        self.stdout.write("DATE FIELD DIAGNOSTICS")
        self.stdout.write("="*50)
        debug_info = self.debug_date_fields(driver)
        for field_id in ['txtFromDt', 'txtToDt']:
            test_date = start_date if 'From' in field_id else end_date
            self.test_date_field_interaction(driver, field_id, test_date)
        self.stdout.write("\n=== TESTING FULL DATE SETTING PROCESS ===")
        success = self.enhanced_set_date_fields(driver, start_date, end_date)
        self.stdout.write(f"Full process result: {'SUCCESS' if success else 'FAILED'}")
        final_state = driver.execute_script("""
            try {
                var fromField = document.getElementById('txtFromDt');
                var toField = document.getElementById('txtToDt');
                return {
                    fromValue: fromField ? fromField.value : 'Field not found',
                    toValue: toField ? toField.value : 'Field not found',
                    fromVisible: fromField ? (fromField.offsetParent !== null) : false,
                    toVisible: toField ? (toField.offsetParent !== null) : false
                };
            } catch(e) {
                return { error: e.message };
            }
        """)
        self.stdout.write(f"\nFinal field state: {final_state}")
        self.stdout.write("="*50)
        return success

    def submit_form(self, driver):
        """Submit the form after all filters are set - enhanced to handle overlapping elements"""
        self.stdout.write("Submitting form...")
        try:
            try:
                alert = driver.switch_to.alert
                alert_text = alert.text
                self.stdout.write(f"Dismissing existing alert: {alert_text}")
                alert.accept()
                time.sleep(1)
            except:
                pass
            try:
                self.stdout.write("Closing any open date pickers...")
                driver.execute_script("""
                    var datePickers = document.querySelectorAll('.ui-datepicker, .ui-widget-overlay, .ui-state-highlight');
                    datePickers.forEach(function(picker) {
                        if (picker.style.display !== 'none') {
                            picker.style.display = 'none';
                        }
                    });
                    var dateInputs = document.querySelectorAll('#txtFromDt, #txtToDt');
                    dateInputs.forEach(function(input) {
                        input.blur();
                    });
                    document.body.click();
                """)
                time.sleep(2)
            except Exception as e:
                self.stdout.write(f"Error closing date pickers: {e}")
            submit_button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "btnSubmit"))
            )
            driver.execute_script("""
                arguments[0].scrollIntoView({
                    behavior: 'smooth',
                    block: 'center',
                    inline: 'center'
                });
            """, submit_button)
            time.sleep(2)
            click_successful = False
            try:
                if submit_button.is_enabled() and submit_button.is_displayed():
                    self.stdout.write("Trying regular click on Submit button...")
                    submit_button.click()
                    click_successful = True
                    self.stdout.write("Regular click successful")
                else:
                    self.stdout.write("Submit button is not enabled or visible")
            except Exception as e:
                self.stdout.write(f"Regular click failed: {e}")
            if not click_successful:
                try:
                    self.stdout.write("Trying ActionChains click...")
                    actions = ActionChains(driver)
                    actions.move_to_element(submit_button).click().perform()
                    click_successful = True
                    self.stdout.write("ActionChains click successful")
                except Exception as e:
                    self.stdout.write(f"ActionChains click failed: {e}")
            if not click_successful:
                try:
                    self.stdout.write("Trying JavaScript click...")
                    driver.execute_script("arguments[0].click();", submit_button)
                    click_successful = True
                    self.stdout.write("JavaScript click successful")
                except Exception as e:
                    self.stdout.write(f"JavaScript click failed: {e}")
            if not click_successful:
                try:
                    self.stdout.write("Trying Angular function call...")
                    driver.execute_script("""
                        try {
                            var scope = angular.element(document.body).scope();
                            if (scope && scope.fn_submit) {
                                scope.fn_submit();
                                scope.$apply();
                            }
                        } catch(e) {
                            console.log('Angular submit failed:', e);
                        }
                    """)
                    click_successful = True
                    self.stdout.write("Angular function call successful")
                except Exception as e:
                    self.stdout.write(f"Angular function call failed: {e}")
            if not click_successful:
                self.stdout.write("All click strategies failed")
                return False
            time.sleep(5)
            try:
                WebDriverWait(driver, 5).until(EC.alert_is_present())
                alert = driver.switch_to.alert
                alert_text = alert.text
                self.stdout.write(f"Alert after submission: {alert_text}")
                if "company" in alert_text.lower() or "security" in alert_text.lower():
                    alert.accept()
                    self.stdout.write("Company selection alert - submission failed")
                    return False
                else:
                    alert.accept()
                    time.sleep(2)
            except TimeoutException:
                self.stdout.write("No alerts after submission - likely successful")
            self.stdout.write("Waiting for results to load...")
            data_loaded = False
            for i in range(30):
                try:
                    data_check = driver.execute_script("""
                        try {
                            var scope = angular.element(document.body).scope();
                            if (scope && scope.CorpannData && scope.CorpannData.Table) {
                                return {
                                    hasData: scope.CorpannData.Table.length > 0,
                                    dataLength: scope.CorpannData.Table.length,
                                    loadingComplete: !scope.loader || scope.loader.CorpAnnState === 'loaded'
                                };
                            }
                            return { hasData: false, loadingComplete: false };
                        } catch(e) {
                            return { hasData: false, error: e.message };
                        }
                    """)
                    if data_check.get('loadingComplete'):
                        if data_check.get('hasData'):
                            self.stdout.write(f"Data loaded successfully! Found {data_check.get('dataLength', 0)} records")
                            data_loaded = True
                            break
                        else:
                            if i > 10:
                                self.stdout.write("No data found for the specified criteria")
                                data_loaded = True
                                break
                    if i % 5 == 0:
                        self.stdout.write(f"Still waiting for data... ({i+1}/30 seconds)")
                    time.sleep(1)
                except Exception as e:
                    self.stdout.write(f"Error checking data status: {e}")
                    time.sleep(1)
            if not data_loaded:
                self.stdout.write("Timeout waiting for data to load")
                return False
            return True
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Form submission failed: {e}"))
            return False

    def check_for_results(self, driver):
        """Enhanced result checking with better error handling"""
        try:
            result_info = driver.execute_script("""
                try {
                    var scope = angular.element(document.body).scope();
                    if (scope) {
                        if (scope.CorpannData) {
                            var table = scope.CorpannData.Table || [];
                            var table1 = scope.CorpannData.Table1 || [];
                            return {
                                hasData: table.length > 0,
                                tableLength: table.length,
                                currentPage: scope.currentPage || 1,
                                totalPages: scope.pagenumber || 1,
                                totalCount: table1.length > 0 ? table1[0].ROWCNT : table.length,
                                loadingState: scope.loader ? scope.loader.CorpAnnState : 'unknown',
                                scopeVars: Object.keys(scope).filter(k => k.includes('Corp') || k.includes('ann') || k.includes('data'))
                            };
                        } else {
                            return { 
                                hasData: false, 
                                error: 'No CorpannData in scope',
                                scopeVars: Object.keys(scope).slice(0, 20)
                            };
                        }
                    }
                    return { hasData: false, error: 'No scope found' };
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
                page_text = driver.page_source.lower()
                if any(phrase in page_text for phrase in ["no records found", "no data available", "no announcements"]):
                    return {'status': 'no_data', 'message': 'No records found'}
                else:
                    return {'status': 'unknown', 'error': result_info.get('error', 'Unknown state')}
        except Exception as e:
            return {'status': 'error', 'error': str(e)}

    def scrape_current_page(self, driver, company_code):
        """Enhanced data scraping with multiple fallback strategies"""
        announcements = []
        try:
            self.stdout.write("Attempting Angular scope data extraction...")
            angular_data = driver.execute_script("""
                try {
                    var scope = angular.element(document.body).scope();
                    if (scope && scope.CorpannData && scope.CorpannData.Table) {
                        return scope.CorpannData.Table.map(function(item, index) {
                            return {
                                index: index,
                                headline: item.HEADLINE || '',
                                newssub: item.NEWSSUB || '',
                                category: item.ANNCAT || item.CATEGORY || '',
                                attachment: item.ATTACHMENT || '',
                                disseminated_date: item.DISSEMINATED_DT || item.DISSEM_DT || '',
                                disseminated_time: item.DISSEMINATED_TM || item.DISSEM_TM || '',
                                received_date: item.RECEIVED_DT || '',
                                received_time: item.RECEIVED_TM || '',
                                ann_id: item.ANN_ID || '',
                                scrip_cd: item.SCRIP_CD || '',
                                company_name: item.COMPANY_NAME || item.COMP_NAME || '',
                                full_data: item
                            };
                        });
                    }
                    return null;
                } catch(e) {
                    return { error: e.message };
                }
            """)
            if angular_data and isinstance(angular_data, list):
                self.stdout.write(f"Angular extraction successful! Found {len(angular_data)} records")
                for i, item in enumerate(angular_data):
                    pdf_link = ""
                    if item.get('attachment'):
                        pdf_link = f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{item['attachment']}"
                    found_company_code = item.get('scrip_cd', company_code)
                    announcements.append({
                        "serial_no": len(announcements) + 1,
                        "headline": item.get('headline', ''),
                        "category": item.get('category', ''),
                        "company_name": item.get('company_name', ''),
                        "company_code": found_company_code,
                        "announcement_text": item.get('headline', ''),
                        "exchange_received_date": item.get('received_date', ''),
                        "exchange_received_time": item.get('received_time', ''),
                        "exchange_disseminated_date": item.get('disseminated_date', ''),
                        "exchange_disseminated_time": item.get('disseminated_time', ''),
                        "pdf_link": pdf_link,
                        "newssub_full": item.get('newssub', ''),
                        "ann_id": item.get('ann_id', ''),
                        "table_index": i,
                        "source": "angular_scope"
                    })
                return announcements
            else:
                self.stdout.write("Angular extraction failed, falling back to HTML parsing...")
        except Exception as e:
            self.stdout.write(f"Angular extraction error: {e}")
            self.stdout.write("Falling back to HTML parsing...")
        try:
            self.stdout.write("Attempting enhanced HTML parsing...")
            soup = BeautifulSoup(driver.page_source, "lxml")
            table_selectors = [
                'table[ng-repeat*="cann in CorpannData.Table"]',
                'tr[ng-repeat*="cann in CorpannData.Table"]',
                'div[ng-repeat*="cann in CorpannData.Table"]',
                '.table-responsive table',
                'table.table',
                '#tblData table',
                'table[id*="tbl"]'
            ]
            found_tables = []
            for selector in table_selectors:
                elements = soup.select(selector)
                if elements:
                    self.stdout.write(f"Found {len(elements)} elements with selector: {selector}")
                    found_tables.extend(elements)
                    break
            if not found_tables:
                all_tables = soup.find_all("table")
                self.stdout.write(f"Found {len(all_tables)} total tables on page")
                for table in all_tables:
                    table_text = table.get_text().lower()
                    if any(keyword in table_text for keyword in ['announcement', 'headline', 'corporate', 'disseminated']):
                        found_tables.append(table)
                        break
            if found_tables:
                self.stdout.write(f"Processing {len(found_tables)} data tables...")
                for table_idx, table in enumerate(found_tables):
                    try:
                        ann_data = self.extract_announcement_from_table(table, table_idx, company_code)
                        if ann_data:
                            announcements.append(ann_data)
                    except Exception as e:
                        self.stdout.write(f"Error parsing table {table_idx}: {e}")
                        continue
            else:
                self.stdout.write("No suitable tables found in HTML")
                self.stdout.write("Attempting raw Angular binding extraction...")
                headline_patterns = soup.find_all(attrs={"ng-bind-html": re.compile(r".*HEADLINE.*", re.I)})
                newssub_patterns = soup.find_all(attrs={"ng-bind-html": re.compile(r".*NEWSSUB.*", re.I)})
                if headline_patterns or newssub_patterns:
                    self.stdout.write(f"Found Angular binding patterns: Headlines={len(headline_patterns)}, Newssub={len(newssub_patterns)}")
                    for i, pattern in enumerate(headline_patterns):
                        try:
                            parent = pattern.find_parent('tr') or pattern.find_parent('div') or pattern.find_parent('table')
                            if parent:
                                ann_data = self.extract_from_angular_bindings(parent, i, company_code)
                                if ann_data:
                                    announcements.append(ann_data)
                        except Exception as e:
                            self.stdout.write(f"Error extracting from binding pattern {i}: {e}")
                            continue
        except Exception as e:
            self.stdout.write(f"HTML parsing error: {e}")
        if not announcements:
            try:
                self.stdout.write("Last resort: Attempting direct JavaScript data extraction...")
                js_data = driver.execute_script("""
                    try {
                        var results = [];
                        var body = document.body;
                        var scope = angular.element(body).scope();
                        if (scope) {
                            for (var key in scope) {
                                if (typeof scope[key] === 'object' && scope[key] !== null) {
                                    if (Array.isArray(scope[key])) {
                                        if (scope[key].length > 0 && scope[key][0].HEADLINE) {
                                            results = scope[key];
                                            break;
                                        }
                                    } else if (scope[key].Table && Array.isArray(scope[key].Table)) {
                                        results = scope[key].Table;
                                        break;
                                    }
                                }
                            }
                        }
                        if (results.length === 0) {
                            var headlines = document.querySelectorAll('[ng-bind-html*="HEADLINE"]');
                            var newsubs = document.querySelectorAll('[ng-bind-html*="NEWSSUB"]');
                            for (var i = 0; i < headlines.length; i++) {
                                results.push({
                                    HEADLINE: headlines[i].textContent || '',
                                    NEWSSUB: newsubs[i] ? newsubs[i].textContent : '',
                                    index: i
                                });
                            }
                        }
                        return results.length > 0 ? results : null;
                    } catch(e) {
                        return { error: e.message };
                    }
                """)
                if js_data and isinstance(js_data, list):
                    self.stdout.write(f"JavaScript extraction successful! Found {len(js_data)} records")
                    for i, item in enumerate(js_data):
                        headline = item.get('HEADLINE', '') or item.get('headline', '')
                        newssub = item.get('NEWSSUB', '') or item.get('newssub', '')
                        company_name = ""
                        found_company_code = company_code
                        if newssub:
                            parts = newssub.split("-")
                            if parts:
                                company_name = parts[0].strip()
                            code_match = re.search(r'\b(\d{6})\b', newssub)
                            if code_match:
                                found_company_code = code_match.group(1)
                        announcements.append({
                            "serial_no": len(announcements) + 1,
                            "headline": headline,
                            "category": item.get('ANNCAT', '') or item.get('category', ''),
                            "company_name": company_name,
                            "company_code": found_company_code,
                            "announcement_text": headline,
                            "exchange_received_date": item.get('RECEIVED_DT', ''),
                            "exchange_received_time": item.get('RECEIVED_TM', ''),
                            "exchange_disseminated_date": item.get('DISSEMINATED_DT', '') or item.get('DISSEM_DT', ''),
                            "exchange_disseminated_time": item.get('DISSEMINATED_TM', '') or item.get('DISSEM_TM', ''),
                            "pdf_link": f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{item.get('ATTACHMENT', '')}" if item.get('ATTACHMENT') else "",
                            "newssub_full": newssub,
                            "ann_id": item.get('ANN_ID', ''),
                            "table_index": i,
                            "source": "javascript_extraction"
                        })
                else:
                    self.stdout.write(f"JavaScript extraction result: {js_data}")
            except Exception as e:
                self.stdout.write(f"JavaScript extraction error: {e}")
        if not announcements:
            self.stdout.write("Attempting original HTML parsing method...")
            announcements = self.scrape_current_page_original(driver, company_code)
        return announcements

    def extract_announcement_from_table(self, table, table_idx, company_code):
        """Extract announcement data from a table element"""
        try:
            newssub_tag = table.find("span", {"ng-bind-html": re.compile(r".*NEWSSUB.*", re.I)})
            headline_tag = table.find("span", {"ng-bind-html": re.compile(r".*HEADLINE.*", re.I)})
            pdf_tag = table.find("a", class_="tablebluelink", href=True)
            newssub = (newssub_tag.get_text(strip=True) if newssub_tag else "") or ""
            headline = (headline_tag.get_text(strip=True) if headline_tag else "") or ""
            pdf_link = urljoin("https://www.bseindia.com/corporates/ann.html", pdf_tag["href"]) if pdf_tag else ""
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
            company_name = ""
            found_company_code = ""
            if newssub:
                parts = newssub.split("-")
                if parts:
                    company_name = parts[0].strip()
                code_match = re.search(r'\b(\d{6})\b', newssub)
                if code_match:
                    found_company_code = code_match.group(1)
            if headline or newssub:
                return {
                    "serial_no": table_idx + 1,
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
                    "table_index": table_idx,
                    "source": "html_table_parsing"
                }
        except Exception as e:
            self.stdout.write(f"Error extracting from table {table_idx}: {e}")
        return None

    def extract_from_angular_bindings(self, container, index, company_code):
        """Extract data from Angular binding patterns"""
        try:
            headline = ""
            newssub = ""
            headline_elem = container.find(attrs={"ng-bind-html": re.compile(r".*HEADLINE.*", re.I)})
            if headline_elem:
                headline = headline_elem.get_text(strip=True)
            newssub_elem = container.find(attrs={"ng-bind-html": re.compile(r".*NEWSSUB.*", re.I)})
            if newssub_elem:
                newssub = newssub_elem.get_text(strip=True)
            company_name = ""
            found_company_code = company_code
            if newssub:
                parts = newssub.split("-")
                if parts:
                    company_name = parts[0].strip()
                code_match = re.search(r'\b(\d{6})\b', newssub)
                if code_match:
                    found_company_code = code_match.group(1)
            if headline or newssub:
                return {
                    "serial_no": index + 1,
                    "headline": headline,
                    "category": "",
                    "company_name": company_name,
                    "company_code": found_company_code,
                    "announcement_text": headline,
                    "exchange_received_date": "",
                    "exchange_received_time": "",
                    "exchange_disseminated_date": "",
                    "exchange_disseminated_time": "",
                    "pdf_link": "",
                    "newssub_full": newssub,
                    "table_index": index,
                    "source": "angular_bindings"
                }
        except Exception as e:
            self.stdout.write(f"Error extracting from Angular bindings: {e}")
        return None

    def scrape_current_page_original(self, driver, company_code):
        """Original scraping method as final fallback"""
        announcements = []
        try:
            soup = BeautifulSoup(driver.page_source, "lxml")
            tables = soup.find_all("table", {"ng-repeat": re.compile(r"cann in CorpannData\.Table")})
            for table_idx, table in enumerate(tables):
                try:
                    newssub_tag = table.find("span", {"ng-bind-html": "cann.NEWSSUB"})
                    headline_tag = table.find("span", {"ng-bind-html": "cann.HEADLINE"})
                    pdf_tag = table.find("a", class_="tablebluelink", href=True)
                    newssub = (newssub_tag.get_text(strip=True) if newssub_tag else "") or ""
                    headline = (headline_tag.get_text(strip=True) if headline_tag else "") or ""
                    pdf_link = urljoin("https://www.bseindia.com/corporates/ann.html", pdf_tag["href"]) if pdf_tag else ""
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
                    company_name = ""
                    found_company_code = ""
                    if newssub:
                        parts = newssub.split("-")
                        if parts:
                            company_name = parts[0].strip()
                        code_match = re.search(r'\b(\d{6})\b', newssub)
                        if code_match:
                            found_company_code = code_match.group(1)
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
                        "table_index": table_idx,
                        "source": "original_method"
                    })
                except Exception as e:
                    self.stdout.write(f"Error parsing table {table_idx}: {e}")
                    continue
        except Exception as e:
            self.stdout.write(f"Error in original scraping method: {e}")
        return announcements

    def wait_for_data_load_enhanced(self, driver, max_wait_seconds=60):
        """Enhanced waiting for data load with comprehensive checks"""
        self.stdout.write(f"Enhanced wait for data load (max {max_wait_seconds} seconds)...")
        for second in range(max_wait_seconds):
            try:
                status = driver.execute_script("""
                    try {
                        var scope = angular.element(document.body).scope();
                        var result = {
                            second: arguments[0],
                            angularReady: !!scope,
                            hasCorpannData: !!(scope && scope.CorpannData),
                            hasTable: !!(scope && scope.CorpannData && scope.CorpannData.Table),
                            tableLength: (scope && scope.CorpannData && scope.CorpannData.Table) ? scope.CorpannData.Table.length : 0,
                            loadingState: scope && scope.loader ? scope.loader.CorpAnnState : 'unknown',
                            isLoading: scope && scope.loader ? scope.loader.isLoading : false
                        };
                        var visibleTables = document.querySelectorAll('table[ng-repeat*="CorpannData"]');
                        var angularBindings = document.querySelectorAll('[ng-bind-html*="HEADLINE"]');
                        result.visibleTables = visibleTables.length;
                        result.angularBindings = angularBindings.length;
                        return result;
                    } catch(e) {
                        return { error: e.message, second: arguments[0] };
                    }
                """, second)
                if second % 5 == 0:
                    self.stdout.write(f"Wait status at {second}s: {json.dumps(status, indent=2)}")
                if status.get('hasTable') and status.get('tableLength', 0) > 0:
                    self.stdout.write(f"Data loaded! Found {status['tableLength']} records")
                    return True
                if (status.get('loadingState') == 'loaded' or 
                    status.get('isLoading') == False) and status.get('hasCorpannData'):
                    if status.get('tableLength', 0) == 0:
                        self.stdout.write("Loading complete but no data found")
                        return True
                if status.get('visibleTables', 0) > 0 or status.get('angularBindings', 0) > 0:
                    self.stdout.write(f"Found visible data elements: tables={status.get('visibleTables', 0)}, bindings={status.get('angularBindings', 0)}")
                    return True
                time.sleep(1)
            except Exception as e:
                self.stdout.write(f"Error during data wait check: {e}")
                time.sleep(1)
        self.stdout.write("Timeout waiting for data - proceeding anyway")
        return False

    def navigate_next_page(self, driver):
        """Navigate to next page if available"""
        try:
            next_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "idnext"))
            )
            if next_button.get_attribute("disabled"):
                return False
            driver.execute_script("arguments[0].click();", next_button)
            time.sleep(3)
            self.wait_for_data_load_enhanced(driver, max_wait_seconds=30)
            return True
        except:
            return False

    def run_enhanced_scraper(self, company_code, start_date, end_date, max_pages, debug_mode):
        """Run the enhanced scraper with better error handling"""
        driver = None
        all_announcements = []
        try:
            self.stdout.write("Setting up Chrome driver...")
            driver = self.setup_driver(headless=not debug_mode)
            self.stdout.write("Loading BSE announcements page...")
            driver.get("https://www.bseindia.com/corporates/ann.html")
            time.sleep(5)
            try:
                self.wait_for_angular_ready(driver)
            except Exception as e:
                self.stdout.write(self.style.WARNING(f"Angular wait failed: {e}"))
            self.stdout.write("Waiting for SmartSearch component...")
            time.sleep(10)
            max_attempts = 3
            filter_success = False
            for attempt in range(max_attempts):
                self.stdout.write(f"Filter setting attempt {attempt + 1}/{max_attempts}")
                if self.set_filters(driver, company_code, start_date, end_date):
                    filter_success = True
                    break
                elif attempt < max_attempts - 1:
                    self.stdout.write("Retrying filter setup...")
                    time.sleep(3)
                    driver.refresh()
                    time.sleep(10)
                    try:
                        self.wait_for_angular_ready(driver)
                    except:
                        pass
                    time.sleep(5)
            if not filter_success:
                return {
                    'status': 'filter_failed',
                    'announcements': [],
                    'error': 'Could not set filters after multiple attempts'
                }
            submission_success = False
            for submit_attempt in range(2):
                self.stdout.write(f"Form submission attempt {submit_attempt + 1}/2")
                if self.submit_form(driver):
                    submission_success = True
                    break
                elif submit_attempt < 1:
                    self.stdout.write("Retrying form submission...")
                    time.sleep(3)
            if not submission_success:
                return {
                    'status': 'submit_failed',
                    'announcements': [],
                    'error': 'Could not submit form after multiple attempts'
                }
            self.wait_for_data_load_enhanced(driver, max_wait_seconds=45)
            result_status = self.check_for_results(driver)
            if result_status['status'] == 'has_data':
                self.stdout.write(self.style.SUCCESS(f"Found data! Total announcements: {result_status.get('total_count', 'Unknown')}"))
                page_count = 0
                while page_count < max_pages:
                    page_count += 1
                    self.stdout.write(f"Scraping page {page_count}...")
                    page_announcements = self.scrape_current_page(driver, company_code)
                    all_announcements.extend(page_announcements)
                    self.stdout.write(f"Page {page_count}: Found {len(page_announcements)} announcements")
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
                self.stdout.write("Unknown status but attempting data extraction anyway...")
                page_announcements = self.scrape_current_page(driver, company_code)
                if page_announcements:
                    self.stdout.write(f"Found {len(page_announcements)} announcements despite unknown status")
                    return {
                        'status': 'success',
                        'announcements': page_announcements,
                        'pages_scraped': 1,
                        'total_found': len(page_announcements)
                    }
                else:
                    return {
                        'status': 'unknown_state',
                        'announcements': [],
                        'error': result_status.get('error', 'Unknown error')
                    }
        except Exception as e:
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
        """Enhanced interactive debugging session"""
        self.stdout.write("Starting enhanced interactive debug session...")
        self.stdout.write("Browser will open and remain visible for manual inspection")
        driver = self.setup_driver(headless=False)
        try:
            driver.get("https://www.bseindia.com/corporates/ann.html")
            time.sleep(10)
            self.stdout.write("\n=== ENHANCED PAGE ANALYSIS ===")
            angular_info = driver.execute_script("""
                try {
                    var scope = angular.element(document.body).scope();
                    return {
                        angularLoaded: typeof angular !== 'undefined',
                        scopeExists: !!scope,
                        scopeKeys: scope ? Object.keys(scope).slice(0, 20) : [],
                        hasCorpannData: scope && scope.CorpannData ? true : false,
                        corpannDataKeys: scope && scope.CorpannData ? Object.keys(scope.CorpannData) : []
                    };
                } catch(e) {
                    return { error: e.message };
                }
            """)
            self.stdout.write(f"Angular Status: {json.dumps(angular_info, indent=2)}")
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
            self.stdout.write(f"\n=== TESTING DATA EXTRACTION METHODS ===")
            current_data = self.scrape_current_page(driver, company_code)
            self.stdout.write(f"Current page data extraction test: Found {len(current_data)} items")
            scope_data = driver.execute_script("""
                try {
                    var scope = angular.element(document.body).scope();
                    if (scope) {
                        var result = {};
                        for (var key in scope) {
                            if (typeof scope[key] === 'object' && scope[key] !== null) {
                                if (Array.isArray(scope[key])) {
                                    result[key] = { type: 'array', length: scope[key].length };
                                } else {
                                    result[key] = { type: 'object', keys: Object.keys(scope[key]).slice(0, 5) };
                                }
                            }
                        }
                        return result;
                    }
                    return {};
                } catch(e) {
                    return { error: e.message };
                }
            """)
            self.stdout.write(f"Angular scope analysis: {json.dumps(scope_data, indent=2)}")
            self.stdout.write(f"\nBrowser is open at: {driver.current_url}")
            self.stdout.write("You can manually test the form now.")
            self.stdout.write("Try entering a company code and submitting to see what happens.")
            self.stdout.write("Browser will close in 180 seconds...")
            time.sleep(180)
        except Exception as e:
            self.stdout.write(f"Debug session error: {e}")
            time.sleep(30)
        finally:
            driver.quit()

    def display_results(self, result):
        """Enhanced result display with debugging info"""
        status = result.get('status', 'unknown')
        announcements = result.get('announcements', [])
        self.stdout.write(f"\n=== SCRAPING RESULTS ===")
        self.stdout.write(f"Status: {status}")
        if status == 'success':
            self.stdout.write(self.style.SUCCESS(f"Successfully scraped {len(announcements)} announcements"))
            if announcements:
                sources = {}
                for ann in announcements:
                    source = ann.get('source', 'unknown')
                    sources[source] = sources.get(source, 0) + 1
                self.stdout.write(f"\nData Sources Used:")
                for source, count in sources.items():
                    self.stdout.write(f"  {source}: {count} records")
                self.stdout.write(f"\nSample Announcements:")
                for i, ann in enumerate(announcements[:5], 1):
                    date = ann.get('exchange_disseminated_date', 'N/A')
                    headline = ann.get('headline', 'N/A')[:80]
                    category = ann.get('category', 'N/A')
                    source = ann.get('source', 'N/A')
                    self.stdout.write(f"{i}. [{date}] {headline}...")
                    self.stdout.write(f"   Category: {category} | Source: {source}")
                if len(announcements) > 5:
                    self.stdout.write(f"... and {len(announcements) - 5} more announcements")
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
            if announcements:
                self.stdout.write(f"However, managed to extract {len(announcements)} announcements")
                for i, ann in enumerate(announcements[:3], 1):
                    headline = ann.get('headline', 'N/A')[:60]
                    self.stdout.write(f"  {i}. {headline}...")

    def debug_page_state(self, driver):
        """Debug helper to analyze current page state"""
        debug_info = driver.execute_script("""
            try {
                var scope = angular.element(document.body).scope();
                return {
                    url: window.location.href,
                    title: document.title,
                    angularVersion: angular ? angular.version : null,
                    scopeExists: !!scope,
                    corpannDataExists: !!(scope && scope.CorpannData),
                    tableExists: !!(scope && scope.CorpannData && scope.CorpannData.Table),
                    tableLength: scope && scope.CorpannData && scope.CorpannData.Table ? scope.CorpannData.Table.length : 0,
                    visibleTables: document.querySelectorAll('table').length,
                    visibleRows: document.querySelectorAll('tr').length,
                    angularElements: document.querySelectorAll('[ng-bind-html]').length,
                    loadingElements: document.querySelectorAll('[ng-show*="loader"]').length
                };
            } catch(e) {
                return { error: e.message };
            }
        """)
        return debug_info

