# Laredo Scraper → JSON (GitHub Actions)

Scrapes **LaredoAnywhere** using Selenium, intercepts the search API via Chrome DevTools Protocol, enriches each record with doc details, and writes **JSON** with your requested columns.

## Run from GitHub Actions
1. Add repo Secrets: `LAREDO_USERNAME`, `LAREDO_PASSWORD`
2. Actions → **Laredo Scraper** → **Run workflow**
3. Download the artifact **laredo-results** (contains JSON + logs), or set `push_results=true` to commit results back into the repo.

## Optional local run
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
cp .env.example .env  # then edit with your real creds for local only
python laredo_scraper.py --headless --out files --wait 12
