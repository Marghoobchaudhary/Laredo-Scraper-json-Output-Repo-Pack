#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Laredo scraper
- Scrapes the PrimeNG table (role="table") you shared
- Captures Doc Number, Party cells (with role chip), Book & Page, Doc Date, Recorded Date, Doc Type,
  Assoc Doc, Legal Summary, Consideration, Additional Party, Pages
- Aggregates duplicate Doc Numbers into a single record, filling Party1..PartyN
- Robust handling of --rescrape-indices ("1 2", "1,2", "'1 2'") and optional --only-counties
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

# --- Selenium imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --------------- Utility: logging ---------------

LOG_FILE = "laredo.logs"
FLOW_LOG = "laredo-flow-logs.json"

def log(msg: str):
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"[{timestamp}] {msg}"
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

# --------------- CLI args ---------------

def parse_indices(s: str):
    """
    Accepts:
      - '1 2'
      - "1,2"
      - "'1 2'"
      - " 1   2 , 3 "
    Returns [1, 2, 3] as ints
    """
    if not s:
        return []
    s = s.strip()
    # Remove wrapping quotes if present
    if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
        s = s[1:-1]
    # Replace commas with spaces, then split
    parts = re.split(r"[\s,]+", s.strip())
    out = []
    for p in parts:
        if not p:
            continue
        try:
            out.append(int(p))
        except ValueError:
            # if it's not an int, ignore (keeps parser resilient)
            log(f"Warning: ignoring non-integer rescrape index token: {p!r}")
    return out

def parse_list(s: str):
    if not s:
        return []
    # comma or space separated
    parts = re.split(r"[\s,]+", s.strip())
    return [p for p in parts if p]

def get_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--headless", action="store_true", help="Run headless")
    ap.add_argument("--out", default=os.environ.get("OUT_DIR", "files"), help="Output directory")
    ap.add_argument("--wait", type=int, default=15, help="UI wait seconds")
    ap.add_argument("--max-parties", type=int, default=6, help="Number of Party fields (Party1..N)")
    ap.add_argument("--days-back", type=int, default=2, help="Optional filter by Doc Date >= today - days-back")
    ap.add_argument("--rescrape-indices", default="", help="Space/comma-separated indices for a second pass")
    ap.add_argument("--only-counties", default="", help="Optional filter: only scrape these county slugs")
    ap.add_argument("--hard-timeout", type=int, default=0, help="Hard kill after N seconds (0=disabled)")
    ap.add_argument("--county-slug", default="st-charles-county", help="Slug used in output IDs and filenames")
    ap.add_argument("--start-url", default=os.environ.get("LAREDO_URL", ""), help="Optional start URL")
    return ap.parse_args()

# --------------- Selenium setup ---------------

def build_driver(headless: bool):
    chrome_opts = ChromeOptions()
    if headless:
        # new headless is more reliable in CI
        chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--disable-gpu")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--window-size=1920,1480")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--disable-blink-features=AutomationControlled")
    chrome_prefs = {
        "profile.default_content_setting_values.notifications": 2,
        "download.prompt_for_download": False,
    }
    chrome_opts.add_experimental_option("prefs", chrome_prefs)

    driver = webdriver.Chrome(options=chrome_opts)
    driver.set_page_load_timeout(180)
    return driver

# --------------- Login / Navigation (optional) ---------------

def maybe_login(driver, wait_secs: int):
    """
    If your site needs login, implement here.
    Leave as no-op if already authenticated by cookie or the site is open.
    """
    username = os.environ.get("LAREDO_USERNAME", "")
    password = os.environ.get("LAREDO_PASSWORD", "")
    if not username or not password:
        log("No LAREDO_USERNAME/PASSWORD in env — skipping login.")
        return

    try:
        # Example only — replace selectors/flow as needed.
        # driver.get("https://example.laredo.site/login")
        # WebDriverWait(driver, wait_secs).until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='username']"))).send_keys(username)
        # driver.find_element(By.CSS_SELECTOR, "input[name='password']").send_keys(password)
        # driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
        # WebDriverWait(driver, wait_secs).until(EC.presence_of_element_located((By.CSS_SELECTOR, "nav .user-badge")))
        log("Login stub: implement if needed (skipped).")
    except Exception as e:
        log(f"Login skipped/failed: {e}")

def navigate_to_results(driver, start_url: str, wait_secs: int):
    """
    If you have a direct URL to the results table, put it in --start-url or LAREDO_URL.
    Otherwise, navigate to it here (stub).
    """
    if start_url:
        log(f"Opening start URL: {start_url}")
        driver.get(start_url)
    else:
        log("No --start-url provided; assuming we are already at results table.")
    # Wait for the table to exist
    WebDriverWait(driver, wait_secs).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table[role='table'] tbody tr"))
    )

# --------------- Table parsing ---------------

def safe_text(el):
    try:
        return el.text.strip()
    except Exception:
        return ""

def extract_party_and_role(td_elem):
    """
    In each 'Party' cell, the name is a <span> and the role chip is another <span> containing 'GRANTOR'/'GRANTEE'
    We return "NAME (ROLE)" if role chip exists; else just NAME.
    """
    name = ""
    role = ""
    try:
        name = safe_text(td_elem.find_element(By.CSS_SELECTOR, "span"))
    except Exception:
        name = safe_text(td_elem)

    try:
        chip = td_elem.find_element(By.CSS_SELECTOR, ".party-chip")
        role_raw = safe_text(chip)
        # Normalise role text (strip arrows etc.)
        m = re.search(r"\b(GRANTOR|GRANTEE)\b", role_raw, re.IGNORECASE)
        if m:
            role = m.group(1).upper()
    except Exception:
        role = ""

    return f"{name} ({role})" if name and role else name

def parse_date_mmmd(s: str):
    """
    Parses 'Sep 10, 2025' or 'Sep 12, 2025, 8:27 AM' into a datetime.
    Returns (dt, normalized_string) where normalized_string is original input (kept).
    """
    s_clean = s.strip()
    if not s_clean:
        return None, ""
    # try with time
    for fmt in ("%b %d, %Y, %I:%M %p", "%b %d, %Y"):
        try:
            return datetime.strptime(s_clean, fmt), s_clean
        except Exception:
            continue
    # fallback: keep original
    return None, s_clean

def rows_to_records(driver, county_slug: str, max_parties: int, wait_secs: int, days_back: int):
    """
    Reads the table rows and aggregates by Doc Number.
    """
    # Column map based on the HTML you posted:
    # 0 #, 1 image, 2 shielded, 3 Doc Number, 4 Party, 5 Book & Page,
    # 6 Doc Date, 7 Recorded Date, 8 Doc Type, 9 Assoc Doc, 10 Legal Summary,
    # 11 Consideration, 12 Additional Party, 13 Pages
    rows = WebDriverWait(driver, wait_secs).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table[role='table'] tbody tr"))
    )

    bucket = {}  # key: Doc Number, value: dict record
    per_doc_parties = defaultdict(list)

    min_doc_date = None
    if days_back and days_back > 0:
        min_doc_date = datetime.utcnow().date() - timedelta(days=days_back)

    count = 0
    for row in rows:
        try:
            tds = row.find_elements(By.TAG_NAME, "td")
            if len(tds) < 14:
                # unexpected; skip
                continue

            doc_number = safe_text(tds[3])
            if not doc_number:
                continue

            party_cell = tds[4]
            addl_party_cell = tds[12]

            party_text = extract_party_and_role(party_cell)
            addl_party_text = extract_party_and_role(addl_party_cell)

            book_page = safe_text(tds[5]) or None
            doc_date_raw = safe_text(tds[6])
            recorded_date_raw = safe_text(tds[7])
            doc_type = safe_text(tds[8])
            assoc_doc = safe_text(tds[9])
            legal_summary = safe_text(tds[10])
            consideration = safe_text(tds[11])
            pages = safe_text(tds[13])

            # optional filter by days-back using Doc Date
            dt_doc, _doc_norm = parse_date_mmmd(doc_date_raw)
            if min_doc_date and dt_doc and (dt_doc.date() < min_doc_date):
                # skip if older
                continue

            # Initialize record if first time
            if doc_number not in bucket:
                count += 1
                rec = OrderedDict()
                rec["id"] = f"{county_slug}-{count}"
                rec["Doc Number"] = doc_number
                # Parties filled later after aggregation
                for i in range(1, max_parties + 1):
                    rec[f"Party{i}"] = ""
                rec["Book & Page"] = book_page
                rec["Doc Date"] = doc_date_raw
                rec["Recorded Date"] = recorded_date_raw
                rec["Doc Type"] = doc_type
                rec["Assoc Doc"] = assoc_doc
                rec["Legal Summary"] = legal_summary
                rec["Consideration"] = consideration
                # Pages numeric if possible
                try:
                    rec["Pages"] = int(pages)
                except Exception:
                    rec["Pages"] = pages
                bucket[doc_number] = rec

            # Merge fields that may be blank on first/other rows
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

            # aggregate parties (dedupe, keep order)
            for p in [party_text, addl_party_text]:
                p_norm = p.strip()
                if p_norm and p_norm not in per_doc_parties[doc_number]:
                    per_doc_parties[doc_number].append(p_norm)

        except Exception as e:
            log(f"Row parse error: {e}")

    # Fill Party1..PartyN
    for doc_number, parties in per_doc_parties.items():
        rec = bucket.get(doc_number)
        if not rec:
            continue
        # keep only unique, cap to max_parties
        parties = parties[:max_parties]
        for i, p in enumerate(parties, start=1):
            rec[f"Party{i}"] = p

    # stable order by id index
    records = list(bucket.values())
    # Already assigned ids in increasing count order
    return records

# --------------- Save ---------------

def ensure_out(out_dir: str):
    os.makedirs(out_dir, exist_ok=True)

def save_json_csv(records, out_dir: str, county_slug: str):
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    json_path = os.path.join(out_dir, f"{county_slug}.json")
    csv_path = os.path.join(out_dir, f"{county_slug}.csv")

    # JSON
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    # CSV: union headers
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

# --------------- Main flow ---------------

def main():
    args = get_args()

    start_time = time.time()
    if args.hard_timeout and args.hard_timeout > 0:
        log(f"Hard timeout enabled: {args.hard_timeout}s")

    rescrape_list = parse_indices(args.rescrape_indices)
    only_counties = parse_list(args.only_counties)

    log(f"Params: headless={args.headless}, out={args.out}, wait={args.wait}, "
        f"max_parties={args.max_parties}, days_back={args.days_back}, "
        f"rescrape_indices={rescrape_list}, only_counties={only_counties}, "
        f"county_slug={args.county_slug}")

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
        navigate_to_results(driver, args.start_url, args.wait)

        # First pass scrape
        flow["steps"].append({"event": "first_pass_begin", "ts": datetime.utcnow().isoformat()})
        records = rows_to_records(
            driver=driver,
            county_slug=args.county_slug,
            max_parties=args.max_parties,
            wait_secs=args.wait,
            days_back=args.days_back
        )
        flow["steps"].append({"event": "first_pass_records", "count": len(records)})

        # Optional: perform a "rescrape" run (e.g., change page/sort/filter in your app then scrape again)
        # This is a stub; if your UI requires moving through county indices 1..N, add that navigation here.
        if rescrape_list:
            for idx in rescrape_list:
                flow["steps"].append({"event": "rescrape_begin", "index": idx})
                # TODO: navigate to the selected county index here if applicable.
                # After navigation finishes loading, call rows_to_records again and merge/replace logic as needed.
                more = rows_to_records(
                    driver=driver,
                    county_slug=args.county_slug,
                    max_parties=args.max_parties,
                    wait_secs=args.wait,
                    days_back=args.days_back
                )
                # Merge: by Doc Number update
                by_doc = {r["Doc Number"]: r for r in records}
                for r in more:
                    by_doc[r["Doc Number"]] = r
                records = list(by_doc.values())
                flow["steps"].append({"event": "rescrape_records", "index": idx, "count": len(more)})

        # Save
        json_path, csv_path = save_json_csv(records, args.out, args.county_slug)
        flow["finished_ok"] = True
        flow["records"] = len(records)
        flow["json_path"] = json_path
        flow["csv_path"] = csv_path

    except Exception as e:
        log(f"FATAL: {e}")
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
