
import argparse
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv
from slugify import slugify

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import StaleElementReferenceException


class LaredoScraper:
    """
    Scrapes laredoanywhere.com, intercepts the search API results, enriches with doc details,
    and writes JSON with the columns you specified.

    Base columns:
      id, Doc Number, Party, Book & Page, Doc Date, Recorded Date,
      Doc Type, Assoc Doc, Legal Summary, Consideration, Pages

    Also expanded:
      Party1..N, Address1..M, Parcel1..K
    """

    def __init__(self, out_dir: str = "files", headless: bool = True, wait_seconds: int = 10, max_parties: int = 6):
        load_dotenv()
        self.username = os.getenv("LAREDO_USERNAME", "")
        self.password = os.getenv("LAREDO_PASSWORD", "")
        if not self.username or not self.password:
            raise RuntimeError("Set LAREDO_USERNAME and LAREDO_PASSWORD (in .env for local OR GitHub Secrets for Actions).")

        self.flow_start_time = time.time()
        self.WAIT_DURATION = wait_seconds
        self.max_parties = max_parties

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

        self.wait = WebDriverWait(self.driver, self.WAIT_DURATION)
        self.actions = ActionChains(self.driver)

        self.OUT_DIR = out_dir
        Path(self.OUT_DIR).mkdir(parents=True, exist_ok=True)
        self.flow_log = {}
        self.combined_records_all = []

    # ------------------- Utilities -------------------
    def _write_flow_logs(self):
        try:
            with open('laredo-flow-logs.json', 'w', encoding='utf-8') as f:
                json.dump(self.flow_log, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print('Error in _write_flow_logs():', e)
            self._write_log(e)

    def _write_log(self, msg):
        try:
            with open('laredo.logs', 'a', encoding='utf-8') as f:
                f.write(str(msg) + "\n\n")
        except Exception as e:
            print("Error in _write_log():", e)

    def _wait_for(self, xpath: str, multiple: bool):
        for _ in range(5):
            try:
                if multiple:
                    return self.wait.until(EC.visibility_of_all_elements_located((By.XPATH, xpath)))
                return self.wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))
            except Exception:
                print(f"'{xpath}' not found yet; retrying...")
        return None

    def _get_week_start_mmddyyyy(self):
        dt = datetime.today() - timedelta(days=6)
        return dt.strftime("%m%d%Y")

    # ------------------- Network interception -------------------
    def _intercept_after_search(self):
        data = {"docs_list": [], "auth_token": ""}
        try:
            logs = self.driver.get_log("performance")
            for entry in logs:
                try:
                    message = json.loads(entry["message"])['message']
                    method = message.get("method")

                    if method == "Network.responseReceived":
                        params = message.get("params", {})
                        url = params.get("response", {}).get("url", "")
                        if url.endswith('api/advance/search'):
                            req_id = params.get("requestId")
                            resp = self.driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": req_id})
                            body = resp.get("body")
                            if body:
                                docs_data = json.loads(body)
                                if "documentList" in docs_data:
                                    data["docs_list"] = docs_data["documentList"]

                    elif method == "Network.requestWillBeSent":
                        params = message.get("params", {})
                        headers = params.get("request", {}).get("headers", {})
                        auth = headers.get("Authorization")
                        if auth and not data["auth_token"]:
                            data["auth_token"] = auth
                except Exception as ex:
                    self._write_log(f"Error parsing log entry: {ex}")
        except Exception as e:
            self._write_log(f"Error in _intercept_after_search(): {e}")
        return data

    # ------------------- Page flows -------------------
    def _login(self) -> bool:
        try:
            self.driver.get('https://www.laredoanywhere.com/')
            u = self._wait_for("//input[@id='username']", multiple=False)
            if not u:
                return False
            u.send_keys(self.username)
            p = self._wait_for("//input[@id='password']", multiple=False)
            p.send_keys(self.password)
            btn = self._wait_for("//button", multiple=False)
            btn.click()
            time.sleep(8)
            ok = self._wait_for("//div[contains(@class, 'button-wrapper')]", multiple=False)
            self.flow_log["login_status"] = "success" if ok else "failed"
            return ok is not None
        except Exception as e:
            self._write_log(f"Login error: {e}")
            self.flow_log["login_status"] = "failed"
            return False

    def _counties(self):
        try:
            els = self._wait_for("//div[contains(@class, 'button-wrapper')]", multiple=True)
            return list(els) if els else []
        except Exception as e:
            self._write_log(f"counties error: {e}")
            return []

    def _connect_county(self, county_name: str, county_index: int) -> bool:
        try:
            counties = self._counties()
            if county_index >= len(counties):
                raise IndexError(f"County index {county_index} out of range")
            county = counties[county_index]

            for _ in range(3):
                try:
                    time.sleep(2)
                    self.actions.move_to_element(county).perform()
                    county.click()
                    disconnect_button = self.wait.until(
                        EC.visibility_of_element_located((By.XPATH, "//button[@type='button']"))
                    )
                    if disconnect_button.text.strip() == "Disconnect":
                        self.flow_log[county_name] = {"connected": "success"}
                        return True
                except StaleElementReferenceException:
                    time.sleep(1)
                    counties = self._counties()
                    county = counties[county_index]
            raise RuntimeError("Failed to connect after retries")
        except Exception as e:
            self._write_log(f"connect_county({county_name}) error: {e}")
            self.flow_log.setdefault(county_name, {})["connected"] = "failed"
            return False

    def _close_popup(self):
        try:
            time.sleep(2)
            x = self._wait_for("//i[contains(@class, 'fa-xmark')]", multiple=False)
            if x:
                x.click()
        except Exception as e:
            self._write_log(f"close_popup error: {e}")

    def _disconnect(self, county_name: str):
        try:
            b = self._wait_for("//button[@type='button']", multiple=False)
            if b and b.text.strip() == 'Disconnect':
                b.click()
                self.flow_log.setdefault(county_name, {})["disconnected"] = "success"
            time.sleep(2)
        except Exception as e:
            self._write_log(f"disconnect error: {e}")
            self.flow_log.setdefault(county_name, {})["disconnected"] = "failed"

    def _logout(self):
        try:
            buttons = self._wait_for("//button[contains(@class,'nav-button')]", multiple=True)
            if buttons:
                logout_button = list(buttons)[-1]
                if 'Sign out' in logout_button.text:
                    logout_button.click()
                    time.sleep(1)
                    yes_button = self._wait_for("//button[contains(@class,'mobile-dialog-button')]", multiple=False)
                    if yes_button:
                        yes_button.click()
                        self.flow_log["logout"] = "success"
                        time.sleep(2)
        except Exception as e:
            self._write_log(f"logout error: {e}")
            self.flow_log["logout"] = "failed"

    def _fill_form(self, county_name: str, second_pass: bool = False):
        try:
            time.sleep(2)
            start_date = self._wait_for("//input[@placeholder='Enter a start date']", multiple=False)
            if start_date:
                try:
                    start_date.clear()
                except Exception:
                    pass
                start_date.send_keys(self._get_week_start_mmddyyyy())

            dd = self.wait.until(EC.visibility_of_element_located((By.XPATH, "//p-dropdown[@formcontrolname='selectedDocumentType']")))
            dd.click()
            time.sleep(1)
            search_input = self.wait.until(EC.visibility_of_element_located((By.XPATH, "//input[contains(@class, 'p-dropdown-filter')]")))

            if second_pass:
                search_input.send_keys('RESOLUTION')
            else:
                if county_name.strip() == "Jefferson County":
                    search_input.send_keys('APPOINTMENT')
                else:
                    search_input.send_keys('Successor')

            time.sleep(1)
            first_option = self.wait.until(EC.visibility_of_element_located((By.XPATH, "//li[@role='option']")))
            first_option.click()

            run_btn = self.wait.until(EC.visibility_of_element_located((By.XPATH, "//button[contains(@class,'run-btn')]")))
            run_btn.click()
            time.sleep(6)
        except Exception as e:
            self._write_log(f"fill_form error: {e}")

    # ------------------- Data shaping -------------------
    def _clean_results(self, county_slug: str, docs_list):
        doc_id = 1
        out = []
        for doc in docs_list or []:
            try:
                nd = {
                    "id": f"{county_slug}-{doc_id}",
                    "Doc Number": str(doc.get("userDocNo", "")),
                    "Party": f"{doc.get('partyOne','')}{f' ({doc.get('partyOneType')})' if doc.get('partyOneType') else ''}",
                    "Book & Page": doc.get("bookPage", ""),
                    "Doc Date": "",
                    "Recorded Date": "",
                    "Doc Type": doc.get("docType", ""),
                    "Assoc Doc": doc.get("assocDocSummary", ""),
                    "Legal Summary": doc.get("legalSummary", ""),
                    "Consideration": f"${doc['consideration']}" if doc.get("consideration") is not None else "",
                    "Pages": doc.get("pages", ""),
                }
                # Dates
                try:
                    if doc.get("docDate"):
                        dt = datetime.strptime(doc["docDate"], "%Y-%m-%dT%H:%M:%S")
                        nd["Doc Date"] = dt.strftime("%m/%d/%Y")
                except Exception:
                    pass
                try:
                    if doc.get("docRecordedDateTime"):
                        dt = datetime.strptime(doc["docRecordedDateTime"], "%Y-%m-%dT%H:%M:%S")
                        nd["Recorded Date"] = dt.strftime("%m/%d/%Y, %I:%M %p")
                except Exception:
                    pass
                out.append(nd)
            except Exception as e:
                self._write_log(f"clean doc error: {e}")
            doc_id += 1
        return out

    def _group_by_doc_number(self, clean_data):
        grouped = {}
        for r in clean_data:
            k = r.get("Doc Number", "")
            grouped.setdefault(k, []).append(r)
        return grouped

    def _doc_detail(self, auth_token: str, search_doc_id: str):
        details = {"addresses": [], "parcels": []}
        try:
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Authorization": auth_token,
                "Content-Type": "application/json",
                "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.7049.42 Safari/537.36',
            }
            data = {"searchDocId": search_doc_id, "searchResultId": None, "searchResultAuthCode": None}
            resp = requests.post(
                "https://www.laredoanywhere.com/LaredoAnywhere/LaredoAnywhere.WebService/api/docDetail",
                headers=headers,
                json=data,
                timeout=30,
            )
            jr = resp.json()
            if jr.get("document", {}).get("legalList"):
                addresses, parcels = [], []
                for legal in jr["document"]["legalList"]:
                    if legal.get("legalType") == "A":
                        addresses.append(legal.get("description", ""))
                    elif legal.get("legalType") == "P":
                        parcels.append(legal.get("description", ""))
                details["addresses"] = addresses
                details["parcels"] = parcels
        except Exception as e:
            self._write_log(f"doc_detail error: {e}")
        return details

    def _id_map(self, docs_list):
        m = {}
        try:
            for d in docs_list or []:
                u = d.get("userDocNo")
                if u and u not in m:
                    m[u] = d.get("searchDocId")
        except Exception as e:
            self._write_log(f"id_map error: {e}")
        return m

    def _combine_records(self, auth_token: str, doc_id_map: dict, grouped: dict):
        out = []
        records_list = []
        max_addresses = max_parcels = 0

        for doc_no, linked in grouped.items():
            base = linked[0]
            base.update(self._doc_detail(auth_token, doc_id_map.get(doc_no)))
            linked[0] = base
            max_addresses = max(max_addresses, len(base.get("addresses", [])))
            max_parcels = max(max_parcels, len(base.get("parcels", [])))
            records_list.append(linked)

        def add_series(max_n, key, linked, new_record):
            if key == "Party":
                values = [r.get("Party", "") for r in linked]
                values += [""] * (self.max_parties - len(values))
                for i, v in enumerate(values[: self.max_parties], start=1):
                    new_record[f"Party{i}"] = v
            else:
                seq = (linked[0].get(key, []) or [])
                seq += [""] * (max_n - len(seq))
                label = "Address" if key == "addresses" else "Parcel"
                for i, v in enumerate(seq[: max_n], start=1):
                    new_record[f"{label}{i}"] = v

        for linked in records_list:
            nr = {}
            base = linked[0]
            for k, v in base.items():
                if k == "Party":
                    add_series(self.max_parties, k, linked, nr)
                elif k == "addresses":
                    add_series(max_addresses, k, linked, nr)
                elif k == "parcels":
                    add_series(max_parcels, k, linked, nr)
                else:
                    nr[k] = v
            out.append(nr)
        return out

    # ------------------- JSON writer -------------------
    def _write_json(self, data, filename_stem: str):
        path = os.path.join(self.OUT_DIR, f"{filename_stem}.json")
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Saved {path} ({len(data)} records)")

    # ------------------- Orchestration -------------------
    def run(self, rescrape_indices):
        try:
            if not self._login():
                print("Login failed.")
                return

            counties = self._counties()
            total = len(counties)
            print("Total Counties:", total)

            idx = 0
            scrape_count = {}

            while idx < total:
                try:
                    county_name = self.driver.find_elements(By.XPATH, '//span[contains(@class, "county-name")]')[idx].text
                    county_slug = slugify(county_name)
                    print(f"Scraping {county_name}")

                    count = scrape_count.get(idx, 0)
                    second_pass = (count == 1) and (idx in rescrape_indices)

                    if self._connect_county(county_name, idx):
                        self._close_popup()
                        self._fill_form(county_name, second_pass=second_pass)
                        intercepted = self._intercept_after_search()
                        docs_list = intercepted.get("docs_list", [])
                        auth_token = intercepted.get("auth_token", "")

                        cleaned = self._clean_results(county_slug, docs_list)
                        if cleaned:
                            grouped = self._group_by_doc_number(cleaned)
                            id_map = self._id_map(docs_list)
                            combined = self._combine_records(auth_token, id_map, grouped)
                            if combined:
                                stem = f"{county_slug}_resolution" if second_pass else county_slug
                                self._write_json(combined, stem)
                                self.combined_records_all.extend(combined)
                                self.flow_log.setdefault(county_name, {})["data_json"] = "saved"
                            else:
                                print(f"No combined records for '{county_name}'")
                                self.flow_log.setdefault(county_name, {})["data_json"] = "empty"
                        self._disconnect(county_name)

                    scrape_count[idx] = count + 1
                    if idx in rescrape_indices and scrape_count[idx] == 1:
                        print(f"Re-scraping {county_name} for second pass")
                        continue  # same idx again
                    idx += 1

                except Exception as e:
                    self._write_log(f"iterate counties error: {e}")
                    idx += 1

                counties = self._counties()
                total = len(counties)

            if self.combined_records_all:
                self._write_json(self.combined_records_all, "all_counties")

            self._logout()
            self.driver.quit()
            self.flow_log["time_taken_sec"] = round(time.time() - self.flow_start_time, 2)
            self._write_flow_logs()
        except Exception as e:
            self._write_log(f"run() error: {e}")
            try:
                self.driver.quit()
            except Exception:
                pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Laredo scraper -> JSON")
    parser.add_argument("--out", default="files", help="Output directory")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    parser.add_argument("--wait", type=int, default=12, help="Wait seconds for UI elements")
    parser.add_argument("--max-parties", type=int, default=6, help="How many Party columns to expose (Party1..N)")
    parser.add_argument("--rescrape-indices", nargs='*', type=int, default=[1, 2], help="County indices to scrape twice")
    args = parser.parse_args()

    scraper = LaredoScraper(out_dir=args.out, headless=args.headless, wait_seconds=args.wait, max_parties=args.max_parties)
    scraper.run(rescrape_indices=set(args.rescrape_indices))
