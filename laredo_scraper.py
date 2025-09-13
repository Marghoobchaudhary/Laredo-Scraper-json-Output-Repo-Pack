#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Laredo scraper
- Scrapes the PrimeNG table you shared (Doc Number, Parties, Book & Page, Doc Date, Recorded Date, Doc Type,
  Assoc Doc, Legal Summary, Consideration, Additional Party, Pages)
- Aggregates duplicate Doc Numbers into one record and fills Party1..N
- Robust handling of rescrape indices and optional iframe/table selectors
- Dumps HTML and screenshot on failure for quick diagnosis
"""

import os
import re
import csv
import sys
import json
import time
import argparse
from datetime import datetime, timedelta
from collections import OrderedDict, defaultdict

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchFrameException, WebDriverException

LOG_FILE = "laredo.logs"
FLOW_LOG = "laredo-flow-logs.json"

# ---------- logging ----------
def log(msg: str):
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

def write_flow_log(data):
    try:
        with open(FLOW_LOG, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        log(f"Failed writing flow log: {e}")

# ---------- args ----------
def parse_indices(s: str):
    if not s:
        return []
    s = s.strip()
    if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
        s = s[1:-1]
    parts = re.split(r"[\s,]+", s.strip())
    out = []
    for p in parts:
        if not p:
            continue
        try:
            out.append(int(p))
        except ValueError:
            log(f"Warning: ignoring non-integer rescrape index token: {p!r}")
    return out

def parse_list(s: str):
    if not s:
        return []
    return [p for p in re.split(r"[\s,]+", s.strip()) if p]

def get_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--headless", action="store_true", help="Run headless")
    ap.add_argument("--out", default=os.environ.get("OUT_DIR", "files"), help="Output directory")
    ap.add_argument("--wait", type=int, default=30, help="UI wait seconds")
    ap.add_argument("--max-parties", type=int, default=6, help="Number of Party fields (Party1..N)")
    ap.add_argument("--days-back", type=int, default=2, help="Skip Doc Date older than N days (0: disable)")
    ap.add_argument("--rescrape-indices", default="", help="Space/comma-separated indices for a second pass")
    ap.add_argument("--only-counties", default="", help="Optional filter: only scrape these county slugs")
    ap.add_argument("--hard-timeout", type=int, default=0, help="Hard kill after N seconds (0=disabled)")
    ap.add_argument("--county-slug", default="st-charles-county", help="Slug used in output filenames/ids")
    ap.add_argument("--start-url", default=os.environ.get("LAREDO_URL", ""), help="Direct URL to results page")
    ap.add_argument("--iframe-css", default="", help="CSS selector for iframe containing the table")
    ap.add_argument("--table-css", default="", help="CSS selector for table (overrides auto detection)")
    return ap.parse_args()

# ---------- driver ----------
def build_driver(headless: bool):
    opts = ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1920,1480")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    prefs = {
        "profile.default_content_setting_values.notifications": 2,
        "download.prompt_for_download": False,
    }
    opts.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(180)
    return driver

# ---------- login (stub) ----------
def maybe_login(driver, wait_secs: int):
    if not (os.environ.get("LAREDO_USERNAME") and os.environ.get("LAREDO_PASSWORD")):
        log("No LAREDO_USERNAME/PASSWORD in env — skipping login.")
        return
    try:
        log("Login stub: implement if needed (skipped).")
    except Exception as e:
        log(f"Login skipped/failed: {e}")

# ---------- helpers ----------
def _dump_debug_artifacts(driver):
    try:
        with open("laredo_page.html", "w", encoding="utf-8", errors="ignore") as f:
            f.write(driver.page_source)
        log("Saved current page HTML -> laredo_page.html")
    except Exception as e:
        log(f"Failed to save HTML: {e}")
    try:
        driver.save_screenshot("laredo_page.png")
        log("Saved screenshot -> laredo_page.png")
    except Exception as e:
        log(f"Failed to save screenshot: {e}")

def _switch_into_iframe(driver, iframe_css: str):
    if not iframe_css:
        return
    try:
        frame = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, iframe_css))
        )
        driver.switch_to.frame(frame)
        log(f"Switched into iframe: {iframe_css}")
    except (TimeoutException, NoSuchFrameException) as e:
        log(f"WARNING: iframe not found ({iframe_css}). Continuing in main context. ({e})")

def _any_present(driver, selectors):
    for sel in selectors:
        if not sel:
            continue
        try:
            if driver.find_elements(By.CSS_SELECTOR, sel):
                return True
        except Exception:
            pass
    return False

def _scroll_breath(driver):
    try:
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.3)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
        time.sleep(0.3)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    except WebDriverException:
        pass

def _robust_wait_for_table(driver, table_css: str, total_wait_s: int):
    # Your table id seen in the dump: #pn_id_910-table
    fallbacks = [
        table_css or "",                            # explicit override takes priority
        "table[role='table'] tbody tr",             # generic
        "table.p-datatable-table tbody tr",         # PrimeNG class
        "#pn_id_910-table tbody tr",                # explicit id from sample
        "table[role='table']"                       # at least the table exists
    ]
    deadline = time.time() + total_wait_s
    last_err = None
    while time.time() < deadline:
        try:
            if _any_present(driver, fallbacks):
                return True
        except Exception as e:
            last_err = e
        time.sleep(0.8)
        _scroll_breath(driver)
    if last_err:
        log(f"Last wait error: {last_err!r}")
    return False

def navigate_to_results(driver, start_url: str, iframe_css: str, table_css: str, wait_secs: int):
    if not start_url:
        log("No --start-url provided; trying current page context.")
    else:
        log(f"Opening start URL: {start_url}")
        driver.get(start_url)

    time.sleep(2)
    _switch_into_iframe(driver, iframe_css)

    if not _robust_wait_for_table(driver, table_css, max(wait_secs, 15)):
        log("Table not found on first try; reloading once…")
        try:
            driver.refresh()
            time.sleep(2)
            _switch_into_iframe(driver, iframe_css)
        except Exception as e:
            log(f"Refresh failed: {e}")
        if not _robust_wait_for_table(driver, table_css, max(wait_secs, 15)):
            _dump_debug_artifacts(driver)
            raise TimeoutException("Results table not found after robust wait (+ reload).")

def safe_text(el):
    try:
        return el.text.strip()
    except Exception:
        return ""

def extract_party_and_role(td_elem):
    name = ""
    role = ""
    try:
        name = safe_text(td_elem.find_element(By.CSS_SELECTOR, "span"))
    except Exception:
        name = safe_text(td_elem)
    try:
        chip = td_elem.find_element(By.CSS_SELECTOR, ".party-chip")
        role_raw = safe_text(chip)
        m = re.search(r"\b(GRANTOR|GRANTEE)\b", role_raw, re.IGNORECASE)
        if m:
            role = m.group(1).upper()
    except Exception:
        role = ""
    return f"{name} ({role})" if name and role else name

def parse_date_mmmd(s: str):
    s = (s or "").strip()
    if not s:
        return None, ""
    for fmt in ("%b %d, %Y, %I:%M %p", "%b %d, %Y"):
        try:
            return datetime.strptime(s, fmt), s
        except Exception:
            continue
    return None, s

def _find_rows(driver, table_css: str):
    rows = []
    # Use either explicit table selector or known fallbacks
    if table_css:
        # If a table selector was provided, try to hit its rows
        if "tbody" in table_css:
            rows = driver.find_elements(By.CSS_SELECTOR, table_css)
        else:
            rows = driver.find_elements(By.CSS_SELECTOR, f"{table_css} tbody tr")
    if not rows:
        rows = driver.find_elements(By.CSS_SELECTOR, "table[role='table'] tbody tr")
    if not rows:
        rows = driver.find_elements(By.CSS_SELECTOR, "table.p-datatable-table tbody tr")
    if not rows:
        rows = driver.find_elements(By.CSS_SELECTOR, "#pn_id_910-table tbody tr")
    return rows

def rows_to_records(driver, county_slug: str, max_parties: int, wait_secs: int, days_back: int, table_css: str):
    _robust_wait_for_table(driver, table_css, max(wait_secs, 15))
    rows = _find_rows(driver, table_css)

    # In case of virtual scroll showing only a subset, try a little scroll to enforce rendering
    if not rows:
        _scroll_breath(driver)
        time.sleep(1)
        rows = _find_rows(driver, table_css)

    bucket = {}
    per_doc_parties = defaultdict(list)

    min_doc_date = None
    if days_back and days_back > 0:
        min_doc_date = datetime.utcnow().date() - timedelta(days=days_back)

    count = 0
    for row in rows:
        try:
            tds = row.find_elements(By.TAG_NAME, "td")
            if len(tds) < 14:
                continue

            doc_number = safe_text(tds[3])
            if not doc_number:
                continue

            party_cell = tds[4]
            addl_party_cell = tds[12]

            party_text = extract_party_and_role(party_cell)
            addl_party_text = extract_party_and_role(addl_party_cell)

            book_page = safe_text(tds[5]) or None
            doc_date_raw = safe_text(tds[6])          # Doc Date
            recorded_date_raw = safe_text(tds[7])     # Recorded Date
            doc_type = safe_text(tds[8])
            assoc_doc = safe_text(tds[9])
            legal_summary = safe_text(tds[10])
            consideration = safe_text(tds[11])
            pages = safe_text(tds[13])

            dt_doc, _ = parse_date_mmmd(doc_date_raw)
            if min_doc_date and dt_doc and (dt_doc.date() < min_doc_date):
                continue

            if doc_number not in bucket:
                count += 1
                rec = OrderedDict()
                rec["id"] = f"{county_slug}-{count}"
                rec["Doc Number"] = doc_number
                for i in range(1, max_parties + 1):
                    rec[f"Party{i}"] = ""
                rec["Book & Page"] = book_page
                rec["Doc Date"] = doc_date_raw
                rec["Recorded Date"] = recorded_date_raw
                rec["Doc Type"] = doc_type
                rec["Assoc Doc"] = assoc_doc
                rec["Legal Summary"] = legal_summary
                rec["Consideration"] = consideration
                try:
                    rec["Pages"] = int(pages)
                except Exception:
                    rec["Pages"] = pages
                bucket[doc_number] = rec

            rec = bucket[doc_number]
            if not rec.get("Book & Page") and book_page:
                rec["Book & Page"] = book_page
            if not rec.get("Doc Date") and doc_date_raw:
                rec["Doc Date"] = doc_date_raw
            if not rec.get("Recorded Date") and recorded_date_raw:
                rec["Recorded Date"] = recorded_date_raw
            if not rec.get("Doc Type") and doc_type:
                rec["Doc Type"] = doc_type
            if not rec.get("Assoc Doc") and assoc_doc:
                rec["Assoc Doc"] = assoc_doc
            if not rec.get("Legal Summary") and legal_summary:
                rec["Legal Summary"] = legal_summary
            if (isinstance(rec.get("Pages"), str) or not rec.get("Pages")) and pages:
                try:
                    rec["Pages"] = int(pages)
                except Exception:
                    rec["Pages"] = pages

            for p in [party_text, addl_party_text]:
                p_norm = p.strip()
                if p_norm and p_norm not in per_doc_parties[doc_number]:
                    per_doc_parties[doc_number].append(p_norm)

        except Exception as e:
            log(f"Row parse error: {e}")

    for doc_number, parties in per_doc_parties.items():
        rec = bucket.get(doc_number)
        if not rec:
            continue
        for i, p in enumerate(parties[:max_parties], start=1):
            rec[f"Party{i}"] = p

    return list(bucket.values())

# ---------- output ----------
def ensure_out(out_dir: str):
    os.makedirs(out_dir, exist_ok=True)

def save_json_csv(records, out_dir: str, county_slug: str):
    json_path = os.path.join(out_dir, f"{county_slug}.json")
    csv_path = os.path.join(out_dir, f"{county_slug}.csv")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    headers = OrderedDict()
    for r in records:
        for k in r.keys():
            headers[k] = True
    headers = list(headers.keys())

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in records:
            w.writerow(r)

    log(f"Wrote {json_path} and {csv_path}")
    return json_path, csv_path

# ---------- main ----------
def main():
    args = get_args()

    start_time = time.time()
    if args.hard_timeout and args.hard_timeout > 0:
        log(f"Hard timeout enabled: {args.hard_timeout}s")

    rescrape_list = parse_indices(args.rescrape_indices)
    only_counties = parse_list(args.only_counties)

    log(
        f"Params: headless={args.headless}, out={args.out}, wait={args.wait}, "
        f"max_parties={args.max_parties}, days_back={args.days_back}, "
        f"rescrape_indices={rescrape_list}, only_counties={only_counties}, "
        f"county_slug={args.county_slug}"
    )

    ensure_out(args.out)
    flow = {
        "started_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "county": args.county_slug,
        "rescrape_indices": rescrape_list,
        "only_counties": only_counties,
        "steps": []
    }

    driver = None
    try:
        driver = build_driver(args.headless)
        maybe_login(driver, args.wait)
        navigate_to_results(driver, args.start_url, args.iframe_css, args.table_css, args.wait)

        flow["steps"].append({"event": "first_pass_begin", "ts": datetime.utcnow().isoformat()})
        records = rows_to_records(
            driver=driver,
            county_slug=args.county_slug,
            max_parties=args.max_parties,
            wait_secs=args.wait,
            days_back=args.days_back,
            table_css=args.table_css,
        )
        flow["steps"].append({"event": "first_pass_records", "count": len(records)})

        if rescrape_list:
            for idx in rescrape_list:
                flow["steps"].append({"event": "rescrape_begin", "index": idx})
                # TODO: navigate/select different county/index if your UI requires.
                more = rows_to_records(
                    driver=driver,
                    county_slug=args.county_slug,
                    max_parties=args.max_parties,
                    wait_secs=args.wait,
                    days_back=args.days_back,
                    table_css=args.table_css,
                )
                by_doc = {r["Doc Number"]: r for r in records}
                for r in more:
                    by_doc[r["Doc Number"]] = r
                records = list(by_doc.values())
                flow["steps"].append({"event": "rescrape_records", "index": idx, "count": len(more)})

        json_path, csv_path = save_json_csv(records, args.out, args.county_slug)
        flow["finished_ok"] = True
        flow["records"] = len(records)
        flow["json_path"] = json_path
        flow["csv_path"] = csv_path

    except Exception as e:
        log(f"FATAL: {e}")
        if driver:
            _dump_debug_artifacts(driver)
        flow["finished_ok"] = False
        flow["error"] = repr(e)
        raise
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        write_flow_log(flow)

        if args.hard_timeout and args.hard_timeout > 0:
            elapsed = time.time() - start_time
            if elapsed > args.hard_timeout:
                log("Hard timeout reached; exiting.")
                sys.exit(124)

if __name__ == "__main__":
    main()
