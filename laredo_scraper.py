#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
St. Charles County — Laredo table scraper

- Reads the PrimeNG data table you shared.
- Dynamically maps columns from <thead><th> text so "Doc Date" and "Recorded Date"
  are captured reliably even if column order changes.
- Aggregates duplicate Doc Numbers and fills Party1..N (Party + Additional Party).
- Outputs <out>/<county_slug>.json and .csv.

USAGE (recommended):
  python laredo_scraper.py --headless \
    --start-url "https://<THE_ST_CHARLES_TABLE_URL>" \
    --out files --wait 30 --max-parties 6 --county-slug st-charles-county

If the table is inside an iframe, also pass: --iframe-css "iframe#resultsFrame"
You can optionally pass: --table-css "#pn_id_910-table"
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

# ---------------- logging ----------------
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

# ---------------- args ----------------
def get_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--headless", action="store_true", help="Run headless")
    ap.add_argument("--out", default=os.environ.get("OUT_DIR", "files"), help="Output directory")
    ap.add_argument("--wait", type=int, default=30, help="UI wait seconds")
    ap.add_argument("--max-parties", type=int, default=6, help="How many PartyN fields to output")
    ap.add_argument("--days-back", type=int, default=0, help="Skip rows with Doc Date older than N days (0=disable)")
    ap.add_argument("--county-slug", default="st-charles-county", help="Slug for output filenames/ids")
    ap.add_argument("--start-url", default=os.environ.get("LAREDO_URL", ""), help="Direct URL to St. Charles table")
    ap.add_argument("--iframe-css", default="", help="CSS for iframe containing the table (if any)")
    ap.add_argument("--table-css", default="", help="CSS for the table (optional override)")
    return ap.parse_args()

# ---------------- driver ----------------
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

# ---------------- debug helpers ----------------
def _dump_debug(driver):
    try:
        with open("laredo_page.html", "w", encoding="utf-8", errors="ignore") as f:
            f.write(driver.page_source)
        driver.save_screenshot("laredo_page.png")
        log("Saved laredo_page.html and laredo_page.png")
    except Exception as e:
        log(f"Debug dump failed: {e}")

def _switch_iframe(driver, iframe_css: str):
    if not iframe_css:
        return
    try:
        frame = WebDriverWait(driver, 12).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, iframe_css))
        )
        driver.switch_to.frame(frame)
        log(f"Switched into iframe: {iframe_css}")
    except (TimeoutException, NoSuchFrameException) as e:
        log(f"WARNING: iframe not found ({iframe_css}). Continuing in main context. {e}")

def _robust_wait_for_table(driver, table_css: str, wait_s: int):
    selectors = [
        table_css or "",
        "table[role='table']",
        "table.p-datatable-table",
        "#pn_id_910-table",
    ]
    end = time.time() + max(wait_s, 15)
    while time.time() < end:
        for sel in selectors:
            if not sel:
                continue
            try:
                if driver.find_elements(By.CSS_SELECTOR, f"{sel} thead th"):
                    if driver.find_elements(By.CSS_SELECTOR, f"{sel} tbody tr"):
                        return sel  # return the working base selector
            except Exception:
                pass
        time.sleep(0.8)
    return ""

def navigate(driver, start_url: str, iframe_css: str, table_css: str, wait_s: int):
    if start_url:
        log(f"Opening start URL: {start_url}")
        driver.get(start_url)
    else:
        log("No --start-url provided; using current page.")
    time.sleep(2)
    _switch_iframe(driver, iframe_css)

    base_sel = _robust_wait_for_table(driver, table_css, wait_s)
    if not base_sel:
        log("Table not found; refreshing once…")
        try:
            driver.refresh()
            time.sleep(2)
            _switch_iframe(driver, iframe_css)
            base_sel = _robust_wait_for_table(driver, table_css, wait_s)
        except Exception as e:
            log(f"Refresh failed: {e}")

    if not base_sel:
        _dump_debug(driver)
        raise TimeoutException("Results table not found after robust wait (+ reload).")
    return base_sel

# ---------------- scraping ----------------
def safe_text(el):
    try:
        return el.text.strip()
    except Exception:
        return ""

def normalize_header(text: str) -> str:
    t = (text or "").strip().lower()
    t = re.sub(r"\s+", " ", t)
    return t

def map_columns(table_elem):
    """
    Read header <th> text and return a mapping {normalized_header: column_index}.
    """
    headers = table_elem.find_elements(By.CSS_SELECTOR, "thead th")
    mapping = {}
    for idx, th in enumerate(headers):
        label = normalize_header(th.text)
        if not label:
            # some columns (index/images/shielded) may be blank; keep them mapped by index if needed
            continue
        mapping[label] = idx
    return mapping

def extract_party_and_role(td_elem):
    # Name in first span; role chip includes GRANTOR/GRANTEE
    name = ""
    role = ""
    try:
        name = safe_text(td_elem.find_element(By.CSS_SELECTOR, "span"))
    except Exception:
        name = safe_text(td_elem)
    try:
        chip = td_elem.find_element(By.CSS_SELECTOR, ".party-chip")
        m = re.search(r"\b(GRANTOR|GRANTEE)\b", chip.text, re.IGNORECASE)
        if m:
            role = m.group(1).upper()
    except Exception:
        role = ""
    return f"{name} ({role})" if name and role else name

def parse_date_raw(s: str):
    """
    We keep raw strings for JSON, but this helper can validate if needed.
    Accepts 'Sep 10, 2025' or 'Sep 12, 2025, 8:27 AM'
    """
    s = (s or "").strip()
    if not s:
        return None, ""
    for fmt in ("%b %d, %Y, %I:%M %p", "%b %d, %Y"):
        try:
            return datetime.strptime(s, fmt), s
        except Exception:
            continue
    return None, s  # return original string even if parse failed

def rows_to_records(driver, base_sel: str, county_slug: str, max_parties: int, wait_s: int, days_back: int):
    # Locate the concrete table element for consistent scoping
    table = driver.find_element(By.CSS_SELECTOR, base_sel)
    colmap = map_columns(table)

    # Figure out required columns by fuzzy header names
    # (normalize_header() is used, so match lowercase)
    want = {
        "doc number": None,
        "party": None,
        "book & page": None,
        "doc date": None,
        "recorded date": None,
        "doc type": None,
        "assoc doc": None,
        "legal summary": None,
        "consideration": None,
        "additional party": None,
        "pages": None,
    }
    for key in list(want.keys()):
        # find the first header containing the key (exact or startswith)
        exact = colmap.get(key)
        if exact is not None:
            want[key] = exact
            continue
        # fallback: look for any header that contains key as substring
        for htext, idx in colmap.items():
            if key in htext:
                want[key] = idx
                break

    missing = [k for k, v in want.items() if v is None and k in ("doc number", "doc date", "recorded date")]
    if missing:
        log(f"WARNING: missing critical header(s): {missing} — dates may not be captured.")

    # Collect all rows within this table
    rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
    if not rows:
        # try a gentle breathe scroll in case of virtual rendering
        try:
            driver.execute_script("arguments[0].scrollIntoView(true);", table)
            time.sleep(1)
        except Exception:
            pass
        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")

    bucket = {}
    per_doc_parties = defaultdict(list)

    min_doc_date = None
    if days_back and days_back > 0:
        min_doc_date = datetime.utcnow().date() - timedelta(days=days_back)

    count = 0
    for row in rows:
        try:
            tds = row.find_elements(By.TAG_NAME, "td")
            if not tds:
                continue

            def cell(idx):
                return safe_text(tds[idx]) if idx is not None and idx < len(tds) else ""

            doc_number = cell(want["doc number"])
            if not doc_number:
                continue

            party_main = ""
            if want["party"] is not None and want["party"] < len(tds):
                party_main = extract_party_and_role(tds[want["party"]])

            party_addl = ""
            if want["additional party"] is not None and want["additional party"] < len(tds):
                party_addl = extract_party_and_role(tds[want["additional party"]])

            book_page = cell(want["book & page"]) or None
            doc_date_raw = cell(want["doc date"])
            recorded_date_raw = cell(want["recorded date"])
            doc_type = cell(want["doc type"])
            assoc_doc = cell(want["assoc doc"])
            legal_summary = cell(want["legal summary"])
            consideration = cell(want["consideration"])
            pages_raw = cell(want["pages"])

            # Optional filter by days-back using Doc Date (if parseable)
            dt_doc, _ = parse_date_raw(doc_date_raw)
            if min_doc_date and dt_doc and dt_doc.date() < min_doc_date:
                continue

            if doc_number not in bucket:
                count += 1
                rec = OrderedDict()
                rec["id"] = f"{county_slug}-{count}"
                rec["Doc Number"] = doc_number
                for i in range(1, max_parties + 1):
                    rec[f"Party{i}"] = ""
                rec["Book & Page"] = book_page
                rec["Doc Date"] = doc_date_raw              # <-- FROM TABLE
                rec["Recorded Date"] = recorded_date_raw     # <-- FROM TABLE
                rec["Doc Type"] = doc_type
                rec["Assoc Doc"] = assoc_doc
                rec["Legal Summary"] = legal_summary
                rec["Consideration"] = consideration
                try:
                    rec["Pages"] = int(pages_raw)
                except Exception:
                    rec["Pages"] = pages_raw
                bucket[doc_number] = rec

            rec = bucket[doc_number]
            # Enrich/merge if blanks
            if (not rec.get("Book & Page")) and book_page:
                rec["Book & Page"] = book_page
            if (not rec.get("Doc Date")) and doc_date_raw:
                rec["Doc Date"] = doc_date_raw
            if (not rec.get("Recorded Date")) and recorded_date_raw:
                rec["Recorded Date"] = recorded_date_raw
            if (not rec.get("Doc Type")) and doc_type:
                rec["Doc Type"] = doc_type
            if (not rec.get("Assoc Doc")) and assoc_doc:
                rec["Assoc Doc"] = assoc_doc
            if (not rec.get("Legal Summary")) and legal_summary:
                rec["Legal Summary"] = legal_summary
            if (isinstance(rec.get("Pages"), str) or not rec.get("Pages")) and pages_raw:
                try:
                    rec["Pages"] = int(pages_raw)
                except Exception:
                    rec["Pages"] = pages_raw

            # Parties
            for p in (party_main, party_addl):
                p = (p or "").strip()
                if p and p not in per_doc_parties[doc_number]:
                    per_doc_parties[doc_number].append(p)

        except Exception as e:
            log(f"Row parse error: {e}")

    # Fill Party1..N
    for doc_number, parties in per_doc_parties.items():
        rec = bucket.get(doc_number)
        if not rec:
            continue
        for i, p in enumerate(parties[:max_parties], start=1):
            rec[f"Party{i}"] = p

    return list(bucket.values())

# ---------------- output ----------------
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

# ---------------- main ----------------
def main():
    args = get_args()

    log(
        f"Params: headless={args.headless}, out={args.out}, wait={args.wait}, "
        f"max_parties={args.max_parties}, days_back={args.days_back}, county_slug={args.county_slug}"
    )

    ensure_out(args.out)
    flow = {
        "started_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "county": args.county_slug,
        "steps": []
    }

    driver = None
    try:
        driver = build_driver(args.headless)
        base_sel = navigate(driver, args.start_url, args.iframe_css, args.table_css, args.wait)

        flow["steps"].append({"event": "scrape_begin", "ts": datetime.utcnow().isoformat()})
        records = rows_to_records(
            driver=driver,
            base_sel=base_sel,
            county_slug=args.county_slug,
            max_parties=args.max_parties,
            wait_s=args.wait,
            days_back=args.days_back,
        )
        flow["steps"].append({"event": "records", "count": len(records)})

        json_path, csv_path = save_json_csv(records, args.out, args.county_slug)
        flow["finished_ok"] = True
        flow["records"] = len(records)
        flow["json_path"] = json_path
        flow["csv_path"] = csv_path

    except Exception as e:
        log(f"FATAL: {e}")
        if driver:
            _dump_debug(driver)
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

if __name__ == "__main__":
    main()
