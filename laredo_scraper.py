import os
import time
import json
import csv
import argparse
from datetime import datetime, timedelta

import requests
from slugify import slugify

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException


class Laredo:
    def __init__(
        self,
        headless=True,
        out_dir="files",
        wait_seconds=20,
        max_parties=6,
        days_back=2,
        rescrape_indices=None,
        only_counties=None,
        hard_timeout=None,
    ):
        self.flow_start_time = time.time()
        self.WAIT_DURATION = int(wait_seconds)
        self.wait = None
        self.OUTPUT_DIRECTORY = out_dir
        os.makedirs(self.OUTPUT_DIRECTORY, exist_ok=True)

        self.max_parties = int(max_parties)
        self.days_back = int(days_back)
        self.rescrape_indices = (
            [int(i) for i in str(rescrape_indices).split()] if rescrape_indices else []
        )
        # If provided, only scrape these county names (exact, comma-separated)
        self.only_counties = (
            [c.strip() for c in only_counties.split(",")] if only_counties else None
        )
        self.hard_timeout = int(hard_timeout) if hard_timeout else None

        # Selenium setup
        chrome_options = webdriver.ChromeOptions()
        if headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.maximize_window()
        self.driver.execute_cdp_cmd("Network.enable", {})

        self.action_chains = ActionChains(self.driver)
        self.wait = WebDriverWait(self.driver, self.WAIT_DURATION)
        self.flow_log = {}

    # ----------------- utilities & logging -----------------

    def __write_flow_logs(self):
        try:
            with open("laredo-flow-logs.json", "w") as f:
                f.write(json.dumps(self.flow_log, indent=2))
        except Exception as e:
            print("Error in __write_flow_logs():", e)
            self.__write_log(e)

    def __write_log(self, msg):
        try:
            with open("laredo.logs", "a") as f:
                f.write(str(msg) + "\n")
        except Exception as e:
            print("Error in write_log():", e)

    def __wait_for_element(self, xpath, multiple=False):
        for _ in range(5):
            try:
                if multiple:
                    return self.wait.until(
                        EC.visibility_of_all_elements_located((By.XPATH, xpath))
                    )
                else:
                    return self.wait.until(
                        EC.visibility_of_element_located((By.XPATH, xpath))
                    )
            except TimeoutException:
                print(f"'{xpath}' not found yet, retrying...")
        return False

    # ----------------- auth & navigation -----------------

    def __login(self):
        is_logged_in = False
        try:
            self.driver.get("https://www.laredoanywhere.com/")
            username = self.__wait_for_element(
                "//input[@id='username']", multiple=False
            )
            if not username:
                return False

            username_val = os.getenv("LAREDO_USERNAME") or "YOUGOGIRL"
            password_val = os.getenv("LAREDO_PASSWORD") or "WEINERT!"

            username.send_keys(username_val)
            password = self.__wait_for_element("//input[@id='password']", False)
            password.send_keys(password_val)
            login = self.__wait_for_element("//button", False)
            login.click()
            time.sleep(8)
            if self.__wait_for_element(
                "//div[contains(@class, 'button-wrapper')]", multiple=False
            ):
                is_logged_in = True
                self.flow_log["login_status"] = "success"
        except Exception as e:
            print("Error in login()", e)
            self.__write_log(e)
            self.flow_log["login_status"] = "failed"
        return is_logged_in

    def __logout(self):
        try:
            logout_buttons = self.__wait_for_element(
                "//button[contains(@class,'nav-button')]", True
            )
            if logout_buttons:
                logout_button = list(logout_buttons)[-1]
                if "Sign out" in logout_button.text:
                    logout_button.click()
                    time.sleep(3)
                    yes_button = self.__wait_for_element(
                        "//button[contains(@class,'mobile-dialog-button')]", False
                    )
                    if yes_button:
                        yes_button.click()
                    self.flow_log["logout"] = "success"
                    time.sleep(3)
        except Exception as e:
            print("Error in logout()", e)
            self.__write_log(e)
            self.flow_log["logout"] = "failed"

    def __get_counties(self):
        counties = []
        try:
            counties = self.__wait_for_element(
                "//div[contains(@class, 'button-wrapper')]", True
            )
        except Exception as e:
            print("Error in __get_counties():", e)
            self.__write_log(e)
        return list(counties) if counties else []

    def __connect_county(self, county_name, county_index):
        try:
            counties = self.__get_counties()
            if county_index >= len(counties):
                raise Exception(f"County index {county_index} out of range")
            county = counties[county_index]

            for _ in range(3):
                try:
                    time.sleep(3)
                    self.action_chains.move_to_element(county).perform()
                    county.click()
                    disconnect_button = self.wait.until(
                        EC.visibility_of_element_located(
                            (By.XPATH, "//button[@type='button']")
                        )
                    )
                    if disconnect_button.text.strip() == "Disconnect":
                        self.flow_log[county_name] = {"connected": "success"}
                        return True
                except StaleElementReferenceException:
                    time.sleep(2)
                    counties = self.__get_counties()
                    county = counties[county_index]
            raise Exception("Failed to connect after retries")
        except Exception as e:
            print(f"Error in connect_county() for {county_name}: {e}")
            self.__write_log(e)
            self.flow_log[county_name] = {"connected": "failed"}
            return False

    def __disconnect_county(self, county_name):
        try:
            disconnect_button = self.__wait_for_element(
                "//button[@type='button']", False
            )
            if disconnect_button and disconnect_button.text.strip() == "Disconnect":
                disconnect_button.click()
                self.flow_log[county_name]["disconnected"] = "success"
                time.sleep(4)
        except Exception as e:
            print("Error in disconnect_county()", e)
            self.__write_log(e)
            self.flow_log[county_name]["disconnected"] = "failed"

    def __close_popup(self):
        try:
            time.sleep(2)
            close_popup = self.__wait_for_element(
                "//i[contains(@class, 'fa-xmark')]", False
            )
            if close_popup:
                close_popup.click()
        except Exception as e:
            print("Error in close_popup()", e)
            self.__write_log(e)

    # ----------------- search & intercept -----------------

    def __get_start_date_mmddyyyy(self):
        # start date is today - N days, end date is today (UI defaults end to today)
        start_dt = datetime.today() - timedelta(days=self.days_back)
        return start_dt.strftime("%m%d%Y")

    def __fill_form(self, county_name, second_pass=False):
        try:
            time.sleep(2)
            start_date = self.__wait_for_element(
                "//input[@placeholder='Enter a start date']", False
            )
            if start_date:
                start_date.clear()
                start_date.send_keys(self.__get_start_date_mmddyyyy())

            time.sleep(1)
            search_dropdown_div = self.wait.until(
                EC.visibility_of_element_located(
                    (By.XPATH, "//p-dropdown[@formcontrolname='selectedDocumentType']")
                )
            )
            search_dropdown_div.click()
            time.sleep(1)

            search_input_field = self.wait.until(
                EC.visibility_of_element_located(
                    (By.XPATH, "//input[contains(@class, 'p-dropdown-filter')]")
                )
            )
            # Use existing logic + your special cases
            if second_pass:
                search_input_field.send_keys("RESOLUTION")
            else:
                if county_name.strip() == "Jefferson County":
                    search_input_field.send_keys("APPOINTMENT")
                else:
                    search_input_field.send_keys("Successor")
            time.sleep(1)
            first_search_option = self.wait.until(
                EC.visibility_of_element_located((By.XPATH, "//li[@role='option']"))
            )
            first_search_option.click()

            time.sleep(1)
            run_search_button = self.wait.until(
                EC.visibility_of_element_located(
                    (By.XPATH, "//button[contains(@class,'run-btn')]")
                )
            )
            run_search_button.click()
            # allow results to render
            time.sleep(max(6, self.WAIT_DURATION // 2))
        except Exception as e:
            print("Error in __fill_form()", e)
            self.__write_log(e)

    def __intercept(self):
        """
        Read Chrome performance logs to capture:
          - the 'advance/search' JSON response (docs list)
          - the Authorization header (bearer) used by the site (if visible)
        """
        data = {"docs_list": [], "auth_token": ""}
        try:
            logs = self.driver.get_log("performance")
            for entry in logs:
                try:
                    message = json.loads(entry["message"])["message"]
                    method = message.get("method", "")
                    if method == "Network.responseReceived":
                        params = message.get("params", {})
                        url = params.get("response", {}).get("url", "")
                        if url.endswith("api/advance/search"):
                            request_id = params.get("requestId")
                            body = self.driver.execute_cdp_cmd(
                                "Network.getResponseBody", {"requestId": request_id}
                            )
                            response_body = body.get("body")
                            docs_data = json.loads(response_body) if response_body else {}
                            if "documentList" in docs_data:
                                data["docs_list"] = docs_data["documentList"]

                    elif method == "Network.requestWillBeSent":
                        params = message.get("params", {})
                        headers = params.get("request", {}).get("headers", {})
                        # Sometimes the Authorization header shows up here
                        auth = headers.get("Authorization")
                        if auth and not data["auth_token"]:
                            data["auth_token"] = auth
                except Exception as ex:
                    self.__write_log(f"Error parsing log entry: {ex}")
        except Exception as e:
            self.__write_log(f"Error in __intercept(): {e}")
        return data

    # ----------------- NEW: read dates from the table -----------------

    def __parse_doc_date_text(self, s):
        """
        e.g. 'Sep 10, 2025' -> '09/10/2025'
        """
        s = (s or "").strip()
        if not s:
            return ""
        for fmt in ["%b %d, %Y", "%B %d, %Y"]:
            try:
                dt = datetime.strptime(s, fmt)
                return dt.strftime("%m/%d/%Y")
            except Exception:
                pass
        # if unknown format, just return original
        return s

    def __parse_recorded_date_text(self, s):
        """
        e.g. 'Sep 12, 2025, 8:27 AM' -> '09/12/2025, 08:27 AM'
        """
        s = (s or "").strip()
        if not s:
            return ""
        tried = [
            "%b %d, %Y, %I:%M %p",
            "%B %d, %Y, %I:%M %p",
            "%b %d, %Y %I:%M %p",
            "%B %d, %Y %I:%M %p",
        ]
        for fmt in tried:
            try:
                dt = datetime.strptime(s, fmt)
                return dt.strftime("%m/%d/%Y, %I:%M %p")
            except Exception:
                pass
        # if unknown format, return original
        return s

    def __scrape_table_dates_map(self):
        """
        After the search results render, scrape the table DOM.
        Each result row has multiple <td class="column-data"> elements starting at 'Doc Number'.

        Index mapping within column-data tds:
          0 -> Doc Number
          1 -> Party
          2 -> Book & Page
          3 -> Doc Date
          4 -> Recorded Date
          5 -> Doc Type
          6 -> Assoc Doc
          7 -> Legal Summary
          8 -> Consideration
          9 -> Additional Party
          10 -> Pages
        """
        mapping = {}
        try:
            # Make sure table exists
            _tbody = self.__wait_for_element("//tbody[contains(@class,'p-datatable-tbody')]", False)
            if not _tbody:
                return mapping

            rows = self.driver.find_elements(By.CSS_SELECTOR, "tbody.p-datatable-tbody tr")
            for row in rows:
                cols = row.find_elements(By.CSS_SELECTOR, "td.column-data")
                if len(cols) < 5:
                    continue
                doc_no = cols[0].text.strip()
                doc_date = cols[3].text.strip() if len(cols) > 3 else ""
                rec_date = cols[4].text.strip() if len(cols) > 4 else ""

                parsed_doc = self.__parse_doc_date_text(doc_date)
                parsed_rec = self.__parse_recorded_date_text(rec_date)
                if doc_no:
                    mapping[doc_no] = {
                        "Doc Date": parsed_doc,
                        "Recorded Date": parsed_rec,
                    }
        except Exception as e:
            self.__write_log(f"__scrape_table_dates_map error: {e}")
        return mapping

    # ----------------- detail enrichment -----------------

    def get_doc_details(self, auth_token, search_doc_id):
        """
        Pulls legal descriptions to populate 'addresses' and 'parcels'.
        """
        doc_details = {"addresses": [], "parcels": []}
        try:
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json",
                "Authorization": f"{auth_token}" if auth_token else "",
                "User-Agent": "Mozilla/5.0",
            }
            data = {
                "searchDocId": search_doc_id,
                "searchResultId": None,
                "searchResultAuthCode": None,
            }
            resp = requests.post(
                "https://www.laredoanywhere.com/LaredoAnywhere/LaredoAnywhere.WebService/api/docDetail",
                headers=headers,
                json=data,
                timeout=30,
            )
            if resp.ok:
                jr = resp.json()
                legal_list = (
                    jr.get("document", {}).get("legalList", []) if isinstance(jr, dict) else []
                )
                address_list, parcel_list = [], []
                for legal in legal_list:
                    if legal.get("legalType") == "A":
                        address_list.append(legal.get("description", ""))
                    elif legal.get("legalType") == "P":
                        parcel_list.append(legal.get("description", ""))
                doc_details["addresses"] = address_list
                doc_details["parcels"] = parcel_list
        except Exception as e:
            self.__write_log(f"Error in get_doc_details(): {e}")
        return doc_details

    # ----------------- transformation -----------------

    def get_grouped_data(self, clean_data):
        grouped = {}
        try:
            for record in clean_data:
                doc_number = record.get("Doc Number", "")
                if doc_number in grouped:
                    grouped[doc_number].append(record)
                else:
                    grouped[doc_number] = [record]
        except Exception as e:
            self.__write_log(f"Error in get_grouped_data(): {e}")
        return grouped

    def get_map(self, data_list):
        mapping = {}
        try:
            for data in data_list:
                if data.get("userDocNo") not in mapping:
                    mapping[data.get("userDocNo")] = data.get("searchDocId")
        except Exception as e:
            self.__write_log(f"Error in get_map(): {e}")
        return mapping

    def get_combined_records(self, auth_token, doc_id_map, grouped_data):
        new_records_list = []
        try:
            records_list = []
            max_parties = max_addresses = max_parcels = 0
            for doc_number, records in grouped_data.items():
                old_record = records[0]
                # enrich with details
                if doc_number in doc_id_map:
                    old_record.update(self.get_doc_details(auth_token, doc_id_map[doc_number]))
                records[0] = old_record

                max_parties = max(max_parties, len(records))
                max_addresses = max(max_addresses, len(old_record.get("addresses", [])))
                max_parcels = max(max_parcels, len(old_record.get("parcels", [])))
                records_list.append(records)

            max_parties = max(self.max_parties, max_parties)

            def combine_dummies(max_entities, key_value, linked_records, out_record):
                if key_value == "Party":
                    all_vals = [r["Party"] for r in linked_records]
                    all_vals += [""] * (max_entities - len(all_vals))
                    for idx, val in enumerate(all_vals, 1):
                        out_record[f"Party{idx}"] = val
                else:
                    all_vals = linked_records[0].get(key_value, [])
                    all_vals += [""] * (max_entities - len(all_vals))
                    label = "Address" if key_value == "addresses" else "Parcel"
                    for idx, val in enumerate(all_vals, 1):
                        out_record[f"{label}{idx}"] = val

            for linked in records_list:
                out_rec = {}
                old = linked[0]
                for key in old:
                    if key == "Party":
                        combine_dummies(max_parties, key, linked, out_rec)
                    elif key == "addresses":
                        combine_dummies(len(old.get("addresses", [])), "addresses", linked, out_rec)
                    elif key == "parcels":
                        combine_dummies(len(old.get("parcels", [])), "parcels", linked, out_rec)
                    else:
                        out_rec[key] = old[key]
                new_records_list.append(out_rec)
        except Exception as e:
            self.__write_log(f"Error in get_combined_records(): {e}")
        return new_records_list

    # ----------------- cleaning & writing -----------------

    def __clean_data(self, county_slug, docs_list, table_date_map):
        """
        Build the flat records for grouping. Fill Doc Date / Recorded Date
        from API if present, otherwise from the table_date_map.
        """
        doc_id = 1
        new_docs_list = []
        for doc in docs_list:
            try:
                doc_no = str(doc.get("userDocNo", "")).strip()
                party_one = (doc.get("partyOne") or "").strip()
                party_one_type = (doc.get("partyOneType") or "").strip()
                party = f"{party_one}{f' ({party_one_type})' if party_one_type else ''}"

                new_doc = {
                    "id": f"{county_slug}-{doc_id}",
                    "Doc Number": doc_no,
                    "Party": party,
                    "Book & Page": doc.get("bookPage"),
                    "Doc Date": "",
                    "Recorded Date": "",
                    "Doc Type": doc.get("docType") or "",
                    "Assoc Doc": (doc.get("assocDocSummary") or ""),
                    "Legal Summary": (doc.get("legalSummary") or ""),
                    "Consideration": f"${doc.get('consideration', 0) or 0.0}".replace("$$", "$"),
                    "Pages": doc.get("pages", ""),
                }

                # Try API datetimes first (ISO or similar)
                api_doc_date = doc.get("docDate") or doc.get("documentDate")
                api_rec_date = doc.get("docRecordedDateTime") or doc.get("recordedDateTime")

                if api_doc_date:
                    # try common formats
                    parsed = None
                    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                        try:
                            parsed = datetime.strptime(api_doc_date, fmt)
                            break
                        except Exception:
                            pass
                    if parsed:
                        new_doc["Doc Date"] = parsed.strftime("%m/%d/%Y")

                if api_rec_date:
                    parsed = None
                    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
                        try:
                            parsed = datetime.strptime(api_rec_date, fmt)
                            break
                        except Exception:
                            pass
                    if parsed:
                        new_doc["Recorded Date"] = parsed.strftime("%m/%d/%Y, %I:%M %p")

                # If any date is still empty, override from table map
                if doc_no in table_date_map:
                    if not new_doc["Doc Date"]:
                        new_doc["Doc Date"] = table_date_map[doc_no].get("Doc Date", "")
                    if not new_doc["Recorded Date"]:
                        new_doc["Recorded Date"] = table_date_map[doc_no].get("Recorded Date", "")

                new_docs_list.append(new_doc)
            except Exception as e:
                self.__write_log(f"Error in __clean_data(): {e}")
            doc_id += 1
        return new_docs_list

    def __write_json(self, data, filename):
        try:
            path = os.path.join(self.OUTPUT_DIRECTORY, f"{filename}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"JSON file '{path}' written.")
        except Exception as e:
            print("Error in __write_json():", e)
            self.__write_log(e)

    # ----------------- main flow -----------------

    def extract_data(self):
        try:
            if self.hard_timeout and (time.time() - self.flow_start_time) > self.hard_timeout:
                raise TimeoutError("Hard timeout reached before start")

            if not self.__login():
                return

            available_counties = self.__get_counties()
            total_counties = len(available_counties)
            print("Total Counties:", total_counties)

            # Build a list of (index, name) so we can filter by name if requested
            county_names = []
            for idx in range(total_counties):
                try:
                    name = self.driver.find_elements(
                        By.XPATH, '//span[contains(@class, "county-name")]'
                    )[idx].text.strip()
                except Exception:
                    name = f"county-{idx}"
                county_names.append((idx, name))

            current = 0
            scrape_count = {}

            while current < total_counties:
                idx, county_name = county_names[current]

                # If only specific counties requested, skip others
                if self.only_counties and county_name not in self.only_counties:
                    current += 1
                    continue

                if self.hard_timeout and (time.time() - self.flow_start_time) > self.hard_timeout:
                    raise TimeoutError("Hard timeout reached during scraping")

                try:
                    county_slug = slugify(county_name)
                    print(f"Scraping {county_name}")

                    count = scrape_count.get(idx, 0)
                    second_pass = count == 1 and idx in self.rescrape_indices

                    if self.__connect_county(county_name, idx):
                        self.__close_popup()
                        self.__fill_form(county_name, second_pass=second_pass)

                        # 1) scrape table dates (source of truth for dates)
                        table_date_map = self.__scrape_table_dates_map()

                        # 2) intercept API results + auth (for other fields and details)
                        intercept_data = self.__intercept()
                        docs_list = intercept_data.get("docs_list", [])
                        auth_token = intercept_data.get("auth_token", "")

                        # 3) clean + fill dates from table if API missing
                        cleaned_docs_list = self.__clean_data(
                            county_slug, docs_list, table_date_map
                        )

                        if cleaned_docs_list:
                            grouped_data = self.get_grouped_data(cleaned_docs_list)
                            doc_id_map = self.get_map(docs_list)
                            combined_records = self.get_combined_records(
                                auth_token, doc_id_map, grouped_data
                            )
                            if combined_records:
                                filename = f"{county_slug}_resolution" if second_pass else county_slug
                                self.__write_json(combined_records, filename)
                                self.flow_log.setdefault(county_name, {})["data"] = "saved"
                            else:
                                print(
                                    f"JSON not saved. Empty combined records for '{county_name}'"
                                )
                                self.flow_log.setdefault(county_name, {})["data"] = "not saved"

                        self.__disconnect_county(county_name)

                    scrape_count[idx] = count + 1
                    if idx in self.rescrape_indices and scrape_count[idx] == 1:
                        print(f"Re-scraping {county_name} for second pass")
                        continue  # Do the second pass

                    current += 1

                except Exception as e:
                    print(f"Error iterating counties: {e}")
                    self.__write_log(e)
                    current += 1

            self.__logout()
            self.driver.quit()
            self.flow_log["time_taken_seconds"] = round(time.time() - self.flow_start_time, 1)
            self.__write_flow_logs()

        except Exception as e:
            print("Error in extract_data():", e)
            self.__write_log(e)
            try:
                self.driver.quit()
            except Exception:
                pass


# ----------------- CLI -----------------

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--headless", action="store_true", help="Run Chrome headless")
    p.add_argument("--out", default="files", help="Output directory")
    p.add_argument("--wait", type=int, default=20, help="UI wait seconds")
    p.add_argument("--max-parties", type=int, default=6, help="Party1..PartyN")
    p.add_argument("--days-back", type=int, default=2, help="Start = today - N days")
    p.add_argument("--rescrape-indices", default="", help="Space-separated indices (e.g. '1 2')")
    p.add_argument("--only-counties", default="", help='Comma-separated county names (exact matches)')
    p.add_argument("--hard-timeout", type=int, default=0, help="Hard stop seconds for whole flow")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    scraper = Laredo(
        headless=args.headless,
        out_dir=args.out,
        wait_seconds=args.wait,
        max_parties=args.max_parties,
        days_back=args.days_back,
        rescrape_indices=args.rescrape_indices,
        only_counties=args.only_counties if args.only_counties else None,
        hard_timeout=args.hard_timeout if args.hard_timeout > 0 else None,
    )
    scraper.extract_data()
