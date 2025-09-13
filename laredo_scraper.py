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
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException


class LaredoScraper:
    """
    Scrapes laredoanywhere.com search results and writes per-county JSON files.
    - Date window: start = today - N days, end = today
    - Fallback dates from the visible table when API misses them
    - Can limit to specific counties and enforce a hard runtime timeout
    """

    def __init__(self, out_dir="files", headless=True, wait_seconds=25,
                 max_parties=6, days_back=2, hard_timeout_sec=900,
                 only_counties=None):
        load_dotenv()
        self.username = os.getenv("LAREDO_USERNAME", "")
        self.password = os.getenv("LAREDO_PASSWORD", "")
        if not self.username or not self.password:
            raise RuntimeError("Missing LAREDO_USERNAME/LAREDO_PASSWORD")

        self.flow_start_time = time.time()
        self.WAIT_DURATION = wait_seconds
        self.max_parties = max_parties
        self.days_back = max(0, int(days_back))
        self.hard_timeout_sec = max(60, int(hard_timeout_sec))
        self.only_counties = set([c.strip() for c in (only_counties or []) if c.strip()])

        # Chrome setup (faster starts, eager page load)
        chrome_options = webdriver.ChromeOptions()
        if headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1600,1200")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                                    "Chrome/120.0.0.0 Safari/537.36")
        chrome_options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
        chrome_options.set_capability("pageLoadStrategy", "eager")  # important

        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.execute_cdp_cmd("Network.enable", {})
        self.wait = WebDriverWait(self.driver, self.WAIT_DURATION)
        self.actions = ActionChains(self.driver)

        self.OUT_DIR = out_dir
        Path(self.OUT_DIR).mkdir(parents=True, exist_ok=True)
        self.flow_log = {}
        self.combined_records_all = []

    # ---------- Utility ----------
    def _now_exceeded(self) -> bool:
        return (time.time() - self.flow_start_time) >= self.hard_timeout_sec

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

    def _wait_for(self, xpath, multiple=False, timeout=None):
        timeout = timeout or self.WAIT_DURATION
        try:
            if multiple:
                return WebDriverWait(self.driver, timeout).until(
                    EC.visibility_of_all_elements_located((By.XPATH, xpath))
                )
            return WebDriverWait(self.driver, timeout).until(
                EC.visibility_of_element_located((By.XPATH, xpath))
            )
        except TimeoutException:
            return None

    @staticmethod
    def _fmt_mmddyyyy(dt: datetime) -> str:
        return dt.strftime("%m%d%Y")

    def _start_date_str(self) -> str:
        return self._fmt_mmddyyyy(datetime.today() - timedelta(days=self.days_back))

    def _end_date_str(self) -> str:
        return self._fmt_mmddyyyy(datetime.today())

    # ---------- Network interception ----------
    def _intercept_after_search(self, poll_sec=8):
        """
        Poll performance logs up to poll_sec seconds for the advance/search response.
        """
        data = {"docs_list": [], "auth_token": ""}
        end_t = time.time() + max(4, poll_sec)
        seen_auth = False
        while time.time() < end_t:
            logs = self.driver.get_log("performance")
            for entry in logs:
                try:
                    message = json.loads(entry["message"])["message"]
                    m = message.get("method")

                    if m == "Network.responseReceived":
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

                    elif m == "Network.requestWillBeSent" and not seen_auth:
                        headers = message.get("params", {}).get("request", {}).get("headers", {})
                        auth = headers.get("Authorization")
                        if auth:
                            data["auth_token"] = auth
                            seen_auth = True
                except Exception as ex:
                    self._write_log(f"Perf log parse error: {ex}")

            if data["docs_list"]:
                break
            time.sleep(0.5)

        # Save debug
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
            u = self._wait_for("//input[@id='username']", timeout=20)
            if not u:
                self._save_screenshot("debug_login_missing_username")
                self._save_html("debug_login_missing_username")
                return False
            u.send_keys(self.username)
            p = self._wait_for("//input[@id='password']", timeout=10)
            p.send_keys(self.password)
            btn = self._wait_for("//button", timeout=10)
            btn.click()
            # wait until county tiles visible
            ok = self._wait_for("//div[contains(@class, 'button-wrapper')]", timeout=25)
            self._save_screenshot("debug_login")
            self._save_html("debug_login")
            self.flow_log["login_status"] = "success" if ok else "failed"
            return ok is not None
        except Exception as e:
            self._write_log(f"Login error: {e}")
            self.flow_log["login_status"] = "failed"
            return False

    def _all_county_names(self):
        names = []
        try:
            spans = self.driver.find_elements(By.XPATH, '//span[contains(@class, "county-name")]')
            for s in spans:
                names.append((s.text or "").strip())
        except Exception as e:
            self._write_log(f"_all_county_names error: {e}")
        return names

    def _counties(self):
        els = self._wait_for("//div[contains(@class, 'button-wrapper')]", multiple=True, timeout=10)
        return list(els) if els else []

    def _connect_county(self, idx):
        try:
            counties = self._counties()
            county = counties[idx]
            self.actions.move_to_element(county).perform()
            for _ in range(3):
                try:
                    county.click()
                    disconnect = self._wait_for("//button[@type='button']", timeout=10)
                    if disconnect and disconnect.text.strip() == "Disconnect":
                        return True
                except StaleElementReferenceException:
                    counties = self._counties()
                    county = counties[idx]
            return False
        except Exception as e:
            self._write_log(f"Connect county error: {e}")
            return False

    def _close_popup(self):
        try:
            x = self._wait_for("//i[contains(@class, 'fa-xmark')]", timeout=3)
            if x:
                x.click()
        except Exception:
            pass

    def _disconnect(self):
        try:
            b = self._wait_for("//button[@type='button']", timeout=5)
            if b and b.text.strip() == "Disconnect":
                b.click()
            time.sleep(0.5)
        except Exception:
            pass

    def _logout(self):
        try:
            buttons = self._wait_for("//button[contains(@class,'nav-button')]", multiple=True, timeout=8)
            if buttons:
                logout_btn = list(buttons)[-1]
                if "Sign out" in logout_btn.text:
                    logout_btn.click()
                    yes = self._wait_for("//button[contains(@class,'mobile-dialog-button')]", timeout=6)
                    if yes:
                        yes.click()
        except Exception:
            pass

    def _fill_form(self, county_name, second_pass=False):
        try:
            # Start / End
            start = self._wait_for("//input[@placeholder='Enter a start date']", timeout=10)
            if start:
                try:
                    start.clear()
                except Exception:
                    pass
                start.send_keys(self._start_date_str())

            end = self._wait_for("//input[@placeholder='Enter an end date']", timeout=10)
            if end:
                try:
                    end.clear()
                except Exception:
                    pass
                end.send_keys(self._end_date_str())

            # Doc type selector
            dd = self._wait_for("//p-dropdown[@formcontrolname='selectedDocumentType']", timeout=10)
            dd.click()
            search = self._wait_for("//input[contains(@class, 'p-dropdown-filter')]", timeout=10)
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

            option = self._wait_for("//li[@role='option']", timeout=10)
            option.click()
            run_btn = self._wait_for("//button[contains(@class,'run-btn')]", timeout=10)
            run_btn.click()

            # Wait for either table rows OR API payload, up to WAIT_DURATION seconds
            self._wait_for("//table[contains(@class,'p-datatable-table')]", timeout=self.WAIT_DURATION)
        except Exception as e:
            self._write_log(f"Fill form error: {e}")

    # ---------- Date parsing ----------
    @staticmethod
    def _try_parse_date(s: str, fmts):
        for fmt in fmts:
            try:
                return datetime.strptime(s.strip(), fmt)
            except Exception:
                continue
        return None

    def _extract_doc_date(self, doc) -> str:
        candidates = [
            doc.get("docDate"),
            doc.get("docDateDisplay"),
            doc.get("documentDate"),
            doc.get("documentDateDisplay"),
            doc.get("docDateStr"),
        ]
        for val in candidates:
            if not val:
                continue
            if isinstance(val, str) and "T" in val:
                dt = self._try_parse_date(val, ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"])
                if dt:
                    return dt.strftime("%m/%d/%Y")
            if isinstance(val, str):
                dt = self._try_parse_date(val, ["%b %d, %Y", "%m/%d/%Y", "%b %d, %Y, %I:%M %p", "%Y-%m-%d"])
                if dt:
                    return dt.strftime("%m/%d/%Y")
        return ""

    def _extract_recorded_date(self, doc) -> str:
        candidates = [
            doc.get("docRecordedDateTime"),
            doc.get("docRecordedDateTimeDisplay"),
            doc.get("recordedDate"),
            doc.get("recordedDateDisplay"),
            doc.get("recordedDateStr"),
        ]
        for val in candidates:
            if not val:
                continue
            if isinstance(val, str) and "T" in val:
                dt = self._try_parse_date(val, ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"])
                if dt:
                    return dt.strftime("%m/%d/%Y, %I:%M %p")
            if isinstance(val, str):
                dt = self._try_parse_date(val, ["%b %d, %Y, %I:%M %p", "%m/%d/%Y, %I:%M %p"])
                if dt:
                    return dt.strftime("%m/%d/%Y, %I:%M %p")
                dt = self._try_parse_date(val, ["%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d"])
                if dt:
                    return dt.strftime("%m/%d/%Y")
        return ""

    # ---------- Read dates from the visible table ----------
    def _collect_table_dates(self):
        result = {}
        try:
            header_ths = self.driver.find_elements(By.XPATH, "//table[contains(@class,'p-datatable-table')]//thead//th")
            col_map = {}
            for idx, th in enumerate(header_ths):
                label = (th.text or "").strip().lower()
                if "doc number" in label:
                    col_map["doc_number"] = idx
                elif label == "doc date":
                    col_map["doc_date"] = idx
                elif "recorded date" in label:
                    col_map["recorded_date"] = idx

            if not all(k in col_map for k in ["doc_number", "doc_date", "recorded_date"]):
                return result

            rows = self.driver.find_elements(By.XPATH, "//table[contains(@class,'p-datatable-table')]//tbody/tr")
            for r in rows:
                tds = r.find_elements(By.XPATH, "./td")
                try:
                    dn = (tds[col_map["doc_number"]].text or "").strip()
                    dd_raw = (tds[col_map["doc_date"]].text or "").strip()
                    rd_raw = (tds[col_map["recorded_date"]].text or "").strip()

                    dd_norm = ""
                    rd_norm = ""

                    if dd_raw:
                        dt = self._try_parse_date(dd_raw, ["%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d"])
                        if dt:
                            dd_norm = dt.strftime("%m/%d/%Y")

                    if rd_raw:
                        dt = self._try_parse_date(rd_raw, ["%b %d, %Y, %I:%M %p", "%m/%d/%Y, %I:%M %p"])
                        if dt:
                            rd_norm = dt.strftime("%m/%d/%Y, %I:%M %p")
                        else:
                            dt = self._try_parse_date(rd_raw, ["%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d"])
                            if dt:
                                rd_norm = dt.strftime("%m/%d/%Y")

                    if dn:
                        result[dn] = {"doc_date": dd_norm, "recorded_date": rd_norm}
                except Exception as er:
                    self._write_log(f"_collect_table_dates row error: {er}")
        except Exception as e:
            self._write_log(f"_collect_table_dates error: {e}")

        try:
            with open(os.path.join(self.OUT_DIR, "_debug_table_dates.json"), "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

        return result

    # ---------- Data shaping ----------
    def _clean_results(self, county_slug, docs_list, table_dates_map):
        out = []
        doc_id = 1
        for doc in docs_list or []:
            try:
                doc_number = str(doc.get("userDocNo", "") or "").strip()

                api_doc_date = self._extract_doc_date(doc)
                api_rec_date = self._extract_recorded_date(doc)

                # fallback from table
                if (not api_doc_date or not api_doc_date.strip()) and doc_number in table_dates_map:
                    api_doc_date = table_dates_map[doc_number].get("doc_date", "") or api_doc_date
                if (not api_rec_date or not api_rec_date.strip()) and doc_number in table_dates_map:
                    api_rec_date = table_dates_map[doc_number].get("recorded_date", "") or api_rec_date

                nd = {
                    "id": f"{county_slug}-{doc_id}",
                    "Doc Number": doc_number,
                    "Party": f"{doc.get('partyOne','')}" + (f" ({doc.get('partyOneType')})" if doc.get("partyOneType") else ""),
                    "Book & Page": doc.get("bookPage", ""),
                    "Doc Date": api_doc_date or "",
                    "Recorded Date": api_rec_date or "",
                    "Doc Type": doc.get("docType", ""),
                    "Assoc Doc": doc.get("assocDocSummary", ""),
                    "Legal Summary": doc.get("legalSummary", ""),
                    "Consideration": f"${doc['consideration']}" if doc.get("consideration") is not None else "",
                    "Pages": doc.get("pages", ""),
                }
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
                timeout=25,
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
                    add_series(max(max_addr, 0), k, linked, nr)
                elif k == "parcels":
                    add_series(max(max_parc, 0), k, linked, nr)
                else:
                    nr[k] = v
            out.append(nr)
        return out

    def _write_json(self, data, name):
        path = os.path.join(self.OUT_DIR, f"{name}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Saved {path} ({len(data)} records)")

    # ---------- Main ----------
    def run(self, rescrape_indices):
        try:
            if not self._login():
                print("Login failed.")
                return

            # Build mapping name->index and allow filtering
            names = self._all_county_names()
            buttons = self._counties()
            if not names or not buttons:
                self._write_log("No counties found")
                self._save_screenshot("debug_no_counties")
                self._save_html("debug_no_counties")
                return

            name_to_index = {names[i]: i for i in range(min(len(names), len(buttons)))}

            # If user specified only_counties, trim to those
            target_indices = []
            if self.only_counties:
                for want in self.only_counties:
                    if want in name_to_index:
                        target_indices.append(name_to_index[want])
                    else:
                        self._write_log(f"Requested county '{want}' not found in page list: {names}")
            else:
                # default: all
                target_indices = list(range(len(name_to_index)))

            print("Target county indices:", target_indices)

            scrape_count = {}
            i_ptr = 0
            while i_ptr < len(target_indices):
                if self._now_exceeded():
                    self._write_log("Hard timeout reached; stopping.")
                    break

                idx = target_indices[i_ptr]
                county_name = names[idx]
                slug = slugify(county_name)
                print(f"Scraping {county_name}")

                second = scrape_count.get(idx, 0) == 1 and idx in rescrape_indices

                if self._connect_county(idx):
                    self._close_popup()
                    self._fill_form(county_name, second_pass=second)

                    # Collect visible table dates quickly
                    table_dates = self._collect_table_dates()

                    # Intercept API payload (bounded polling)
                    intercepted = self._intercept_after_search(poll_sec=10)
                    docs = intercepted.get("docs_list", []) or []
                    token = intercepted.get("auth_token", "")

                    print(f"Intercepted docs: {len(docs)}")

                    cleaned = self._clean_results(slug, docs, table_dates)
                    grouped = self._group_by_doc_number(cleaned)
                    ids = self._id_map(docs)
                    combined = self._combine_records(token, ids, grouped) if cleaned else []

                    name = f"{slug}_resolution" if second else slug
                    self._write_json(combined, name)
                    if combined:
                        self.combined_records_all.extend(combined)
                        self.flow_log.setdefault(county_name, {})["data_json"] = "saved"
                    else:
                        self.flow_log.setdefault(county_name, {})["data_json"] = "empty"

                    self._disconnect()

                scrape_count[idx] = scrape_count.get(idx, 0) + 1
                if idx in rescrape_indices and scrape_count[idx] == 1:
                    print(f"Re-scraping {county_name} for second pass")
                    continue

                i_ptr += 1

            # Combined file
            self._write_json(self.combined_records_all, "all_counties")

            self._logout()
            try:
                self.driver.quit()
            except Exception:
                pass

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
    parser.add_argument("--wait", type=int, default=25, help="Wait seconds for UI")
    parser.add_argument("--max-parties", type=int, default=6)
    parser.add_argument("--days-back", type=int, default=2, help="Start = today-N, End = today")
    parser.add_argument("--hard-timeout", type=int, default=900, help="Hard timeout (seconds) for entire run")
    parser.add_argument("--only-counties", type=str, default="", help="Comma-separated county names to scrape")
    parser.add_argument("--rescrape-indices", nargs="*", type=int, default=[1, 2], help="County indices to scrape twice")
    args = parser.parse_args()

    only = [s for s in args.only_counties.split(",")] if args.only_counties else []

    scraper = LaredoScraper(
        out_dir=args.out,
        headless=args.headless,
        wait_seconds=args.wait,
        max_parties=args.max_parties,
        days_back=args.days_back,
        hard_timeout_sec=args.hard_timeout,
        only_counties=only,
    )
    scraper.run(rescrape_indices=set(args.rescrape_indices))
