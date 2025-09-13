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
    Scrapes laredoanywhere.com, intercepts search API results,
    enriches with doc details, and writes JSON files (always writes a per-county file).
    Also drops debug artifacts for troubleshooting in CI.
    """

    def __init__(self, out_dir="files", headless=True, wait_seconds=30, max_parties=6):
        load_dotenv()
        self.username = os.getenv("LAREDO_USERNAME", "")
        self.password = os.getenv("LAREDO_PASSWORD", "")
        if not self.username or not self.password:
            raise RuntimeError(
                "Set LAREDO_USERNAME and LAREDO_PASSWORD (in .env for local OR GitHub Secrets for Actions)."
            )

        self.flow_start_time = time.time()
        self.WAIT_DURATION = wait_seconds
        self.max_parties = max_parties

        # Chrome setup
        chrome_options = webdriver.ChromeOptions()
        if headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1600,1000")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                                    "Chrome/120.0.0.0 Safari/537.36")
        chrome_options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.execute_cdp_cmd("Network.enable", {})

        self.wait = WebDriverWait(self.driver, self.WAIT_DURATION)
        self.actions = ActionChains(self.driver)

        self.OUT_DIR = out_dir
        Path(self.OUT_DIR).mkdir(parents=True, exist_ok=True)
        self.flow_log = {}
        self.combined_records_all = []

    # ---------- Utility Methods ----------
    def _save_screenshot(self, name: str):
        try:
            path = os.path.join(self.OUT_DIR, f"{name}.png")
            self.driver.save_screenshot(path)
        except Exception:
            pass

    def _save_html(self, name: str):
        try:
            path = os.path.join(self.OUT_DIR, f"{name}.html")
            with open(path, "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
        except Exception:
            pass

    def _write_flow_logs(self):
        try:
            with open("laredo-flow-logs.json", "w", encoding="utf-8") as f:
                json.dump(self.flow_log, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self._write_log(f"Error writing flow logs: {e}")

    def _write_log(self, msg):
        try:
            with open("laredo.logs", "a", encoding="utf-8") as f:
                f.write(str(msg) + "\n")
        except Exception:
            pass

    def _wait_for(self, xpath, multiple=False):
        for _ in range(8):
            try:
                if multiple:
                    return self.wait.until(
                        EC.visibility_of_all_elements_located((By.XPATH, xpath))
                    )
                return self.wait.until(
                    EC.visibility_of_element_located((By.XPATH, xpath))
                )
            except Exception:
                time.sleep(1)
        return None

    def _get_week_start_mmddyyyy(self):
        dt = datetime.today() - timedelta(days=6)
        return dt.strftime("%m%d%Y")

    # ---------- Network interception ----------
    def _intercept_after_search(self):
        data = {"docs_list": [], "auth_token": ""}
        try:
            logs = self.driver.get_log("performance")
            for entry in logs:
                try:
                    message = json.loads(entry["message"])["message"]
                    method = message.get("method")

                    if method == "Network.responseReceived":
                        params = message.get("params", {})
                        url = params.get("response", {}).get("url", "")
                        if url.endswith("api/advance/search"):
                            req_id = params.get("requestId")
                            resp = self.driver.execute_cdp_cmd(
                                "Network.getResponseBody", {"requestId": req_id}
                            )
                            body = resp.get("body")
                            if body:
                                docs_data = json.loads(body)
                                if "documentList" in docs_data:
                                    data["docs_list"] = docs_data["documentList"]

                    elif method == "Network.requestWillBeSent":
                        headers = (
                            message.get("params", {})
                            .get("request", {})
                            .get("headers", {})
                        )
                        auth = headers.get("Authorization")
                        if auth and not data["auth_token"]:
                            data["auth_token"] = auth
                except Exception as ex:
                    self._write_log(f"Error parsing log entry: {ex}")
        except Exception as e:
            self._write_log(f"Error intercepting: {e}")

        # Always save a raw copy for debugging
        try:
            with open(os.path.join(self.OUT_DIR, "_debug_last_search.json"), "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

        return data

    # ---------- Page flows ----------
    def _login(self):
        try:
            self.driver.get("https://www.laredoanywhere.com/")
            u = self._wait_for("//input[@id='username']")
            if not u:
                self._save_screenshot("debug_login_missing_username")
                self._save_html("debug_login_missing_username")
                return False
            u.send_keys(self.username)
            p = self._wait_for("//input[@id='password']")
            p.send_keys(self.password)
            btn = self._wait_for("//button")
            btn.click()
            time.sleep(10)
            ok = self._wait_for("//div[contains(@class, 'button-wrapper')]")
            self._save_screenshot("debug_login")
            self._save_html("debug_login")
            self.flow_log["login_status"] = "success" if ok else "failed"
            return ok is not None
        except Exception as e:
            self._write_log(f"Login error: {e}")
            self._save_screenshot("debug_login_error")
            self._save_html("debug_login_error")
            self.flow_log["login_status"] = "failed"
            return False

    def _counties(self):
        els = self._wait_for("//div[contains(@class, 'button-wrapper')]", multiple=True)
        return list(els) if els else []

    def _connect_county(self, name, idx):
        try:
            counties = self._counties()
            county = counties[idx]
            self.actions.move_to_element(county).perform()
            self._save_screenshot(f"debug_county_{idx}_hover")

            for _ in range(3):
                try:
                    time.sleep(2)
                    county.click()
                    disconnect = self.wait.until(
                        EC.visibility_of_element_located((By.XPATH, "//button[@type='button']"))
                    )
                    if disconnect.text.strip() == "Disconnect":
                        self.flow_log[name] = {"connected": "success"}
                        self._save_screenshot(f"debug_county_{idx}_connected")
                        return True
                except StaleElementReferenceException:
                    counties = self._counties()
                    county = counties[idx]
            raise RuntimeError("Failed to connect after retries")
        except Exception as e:
            self._write_log(f"Connect county error: {e}")
            self.flow_log.setdefault(name, {})["connected"] = "failed"
            self._save_screenshot(f"debug_county_{idx}_connect_error")
            return False

    def _close_popup(self):
        try:
            x = self._wait_for("//i[contains(@class, 'fa-xmark')]")
            if x:
                x.click()
        except Exception as e:
            self._write_log(f"Close popup error: {e}")

    def _disconnect(self, name):
        try:
            b = self._wait_for("//button[@type='button']")
            if b and b.text.strip() == "Disconnect":
                b.click()
                self.flow_log.setdefault(name, {})["disconnected"] = "success"
            time.sleep(2)
        except Exception as e:
            self._write_log(f"Disconnect error: {e}")
            self.flow_log.setdefault(name, {})["disconnected"] = "failed"

    def _logout(self):
        try:
            buttons = self._wait_for("//button[contains(@class,'nav-button')]", multiple=True)
            if buttons:
                logout_btn = list(buttons)[-1]
                if "Sign out" in logout_btn.text:
                    logout_btn.click()
                    time.sleep(1)
                    yes = self._wait_for("//button[contains(@class,'mobile-dialog-button')]")
                    if yes:
                        yes.click()
                        self.flow_log["logout"] = "success"
                        time.sleep(2)
        except Exception as e:
            self._write_log(f"Logout error: {e}")
            self.flow_log["logout"] = "failed"

    def _fill_form(self, county_name, second_pass=False):
        try:
            time.sleep(2)
            start = self._wait_for("//input[@placeholder='Enter a start date']")
            if start:
                try:
                    start.clear()
                except Exception:
                    pass
                start.send_keys(self._get_week_start_mmddyyyy())

            dd = self.wait.until(
                EC.visibility_of_element_located(
                    (By.XPATH, "//p-dropdown[@formcontrolname='selectedDocumentType']")
                )
            )
            dd.click()
            search = self.wait.until(
                EC.visibility_of_element_located(
                    (By.XPATH, "//input[contains(@class, 'p-dropdown-filter')]")
                )
            )
            # clear any existing text
            try:
                search.clear()
            except Exception:
                pass

            if second_pass:
                search.send_keys("RESOLUTION")
            else:
                if county_name.strip() == "Jefferson County":
                    search.send_keys("APPOINTMENT")
                else:
                    search.send_keys("Successor")

            time.sleep(1)
            option = self.wait.until(
                EC.visibility_of_element_located((By.XPATH, "//li[@role='option']"))
            )
            option.click()
            run_btn = self.wait.until(
                EC.visibility_of_element_located((By.XPATH, "//button[contains(@class,'run-btn')]"))
            )
            run_btn.click()
            time.sleep(10)
            self._save_screenshot("debug_after_search")
            self._save_html("debug_after_search")
        except Exception as e:
            self._write_log(f"Fill form error: {e}")

    # ---------- Data shaping ----------
    def _clean_results(self, county_slug, docs_list):
        out = []
        doc_id = 1
        for doc in docs_list or []:
            try:
                nd = {
                    "id": f"{county_slug}-{doc_id}",
                    "Doc Number": str(doc.get("userDocNo", "")),
                    "Party": f"{doc.get('partyOne','')}" + (
                        f" ({doc.get('partyOneType')})" if doc.get("partyOneType") else ""
                    ),
                    "Book & Page": doc.get("bookPage", ""),
                    "Doc Date": "",
                    "Recorded Date": "",
                    "Doc Type": doc.get("docType", ""),
                    "Assoc Doc": doc.get("assocDocSummary", ""),
                    "Legal Summary": doc.get("legalSummary", ""),
                    "Consideration": f"${doc['consideration']}" if doc.get("consideration") is not None else "",
                    "Pages": doc.get("pages", ""),
                }
                # Parse dates
                if doc.get("docDate"):
                    try:
                        dt = datetime.strptime(doc["docDate"], "%Y-%m-%dT%H:%M:%S")
                        nd["Doc Date"] = dt.strftime("%m/%d/%Y")
                    except Exception:
                        pass
                if doc.get("docRecordedDateTime"):
                    try:
                        dt = datetime.strptime(doc["docRecordedDateTime"], "%Y-%m-%dT%H:%M:%S")
                        nd["Recorded Date"] = dt.strftime("%m/%d/%Y, %I:%M %p")
                    except Exception:
                        pass
                out.append(nd)
            except Exception as e:
                self._write_log(f"Clean doc error: {e}")
            doc_id += 1
        return out

    def _group_by_doc_number(self, clean):
        grouped = {}
        for r in clean:
            grouped.setdefault(r.get("Doc Number", ""), []).append(r)
        return grouped

    def _doc_detail(self, auth_token, doc_id):
        details = {"addresses": [], "parcels": []}
        if not doc_id:
            return details
        try:
            headers = {
                "Authorization": auth_token,
                "Content-Type": "application/json",
                "Accept": "application/json, text/plain, */*",
            }
            resp = requests.post(
                "https://www.laredoanywhere.com/LaredoAnywhere/LaredoAnywhere.WebService/api/docDetail",
                headers=headers,
                json={"searchDocId": doc_id, "searchResultId": None, "searchResultAuthCode": None},
                timeout=30,
            )
            jr = resp.json()
            if jr.get("document", {}).get("legalList"):
                for l in jr["document"]["legalList"]:
                    if l.get("legalType") == "A":
                        details["addresses"].append(l.get("description", ""))
                    elif l.get("legalType") == "P":
                        details["parcels"].append(l.get("description", ""))
        except Exception as e:
            self._write_log(f"Doc detail error: {e}")
        return details

    def _id_map(self, docs):
        m = {}
        for d in docs or []:
            u = d.get("userDocNo")
            if u and u not in m:
                m[u] = d.get("searchDocId")
        return m

    def _combine_records(self, auth_token, id_map, grouped):
        out = []
        max_addr = max_parc = 0
        records_list = []

        for doc_no, linked in grouped.items():
            base = linked[0]
            base.update(self._doc_detail(auth_token, id_map.get(doc_no)))
            linked[0] = base
            max_addr = max(max_addr, len(base.get("addresses", [])))
            max_parc = max(max_parc, len(base.get("parcels", [])))
            records_list.append(linked)

        def add_series(max_n, key, linked, rec):
            if key == "Party":
                vals = [r.get("Party", "") for r in linked] + [""] * (self.max_parties - len(linked))
                for i, v in enumerate(vals[: self.max_parties], 1):
                    rec[f"Party{i}"] = v
            else:
                seq0 = linked[0].get(key, []) or []
                seq = seq0 + [""] * (max_n - len(seq0))
                label = "Address" if key == "addresses" else "Parcel"
                for i, v in enumerate(seq[:max_n], 1):
                    rec[f"{label}{i}"] = v

        for linked in records_list:
            nr = {}
            base = linked[0]
            for k, v in base.items():
                if k == "Party":
                    add_series(self.max_parties, k, linked, nr)
                elif k == "addresses":
                    add_series(max_addr, k, linked, nr)
                elif k == "parcels":
                    add_series(max_parc, k, linked, nr)
                else:
                    nr[k] = v
            out.append(nr)
        return out

    def _write_json(self, data, name):
        path = os.path.join(self.OUT_DIR, f"{name}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Saved {path} ({len(data)} records)")

    # ---------- Main run ----------
    def run(self, rescrape_indices):
        try:
            if not self._login():
                print("Login failed.")
                return

            counties = self._counties()
            if not counties:
                self._write_log("No counties found on the page.")
                self._save_screenshot("debug_no_counties")
                self._save_html("debug_no_counties")
            total = len(counties)
            print("Total Counties:", total)

            idx = 0
            scrape_count = {}

            while idx < total:
                try:
                    county_name = self.driver.find_elements(
                        By.XPATH, '//span[contains(@class, "county-name")]'
                    )[idx].text
                    slug = slugify(county_name)
                    print(f"Scraping {county_name}")

                    second = scrape_count.get(idx, 0) == 1 and idx in rescrape_indices

                    if self._connect_county(county_name, idx):
                        self._close_popup()
                        self._fill_form(county_name, second_pass=second)
                        intercepted = self._intercept_after_search()
                        docs = intercepted.get("docs_list", []) or []
                        token = intercepted.get("auth_token", "")

                        print(f"Intercepted docs: {len(docs)}")
                        cleaned = self._clean_results(slug, docs)
                        grouped = self._group_by_doc_number(cleaned)
                        ids = self._id_map(docs)
                        combined = self._combine_records(token, ids, grouped) if cleaned else []

                        # Always write a per-county file (even if empty)
                        name = f"{slug}_resolution" if second else slug
                        self._write_json(combined, name)
                        if combined:
                            self.combined_records_all.extend(combined)
                            self.flow_log.setdefault(county_name, {})["data_json"] = "saved"
                        else:
                            self.flow_log.setdefault(county_name, {})["data_json"] = "empty"

                        self._disconnect(county_name)

                    scrape_count[idx] = scrape_count.get(idx, 0) + 1
                    if idx in rescrape_indices and scrape_count[idx] == 1:
                        print(f"Re-scraping {county_name} for second pass")
                        continue
                    idx += 1

                except Exception as e:
                    self._write_log(f"Iterate counties error: {e}")
                    idx += 1

                counties = self._counties()
                total = len(counties)

            # Combined file (even if empty)
            self._write_json(self.combined_records_all, "all_counties")

            self._logout()
            self.driver.quit()
            self.flow_log["time_taken_sec"] = round(time.time() - self.flow_start_time, 2)
            self._write_flow_logs()
        except Exception as e:
            self._write_log(f"Run error: {e}")
            try:
                self.driver.quit()
            except Exception:
                pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Laredo scraper -> JSON")
    parser.add_argument("--out", default="files", help="Output directory")
    parser.add_argument("--headless", action="store_true", help="Run headless")
    parser.add_argument("--wait", type=int, default=30, help="Wait seconds for UI")
    parser.add_argument("--max-parties", type=int, default=6)
    parser.add_argument(
        "--rescrape-indices", nargs="*", type=int, default=[1, 2], help="County indices to scrape twice"
    )
    args = parser.parse_args()

    scraper = LaredoScraper(
        out_dir=args.out, headless=args.headless, wait_seconds=args.wait, max_parties=args.max_parties
    )
    scraper.run(rescrape_indices=set(args.rescrape_indices))
