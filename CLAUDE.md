# Agency-Scrap

CEA (Council for Estate Agencies) salesperson registry scraper and dashboard for tracking real estate agent and agency changes in Singapore.

## Project Overview

This app scrapes the CEA public dataset of registered salespersons from data.gov.sg, compares it against a master database to detect changes (new/removed agents and agencies), stores run history, and sends notifications.

## Architecture

- **scraper.py** - Main scraper logic: initiates download from data.gov.sg API, polls for CSV, downloads and parses it, compares against master DB, records changes, sends notifications. Also has a `rollback()` function to reverse the last scrape run.
- **db.py** - PostgreSQL database layer using psycopg2. Tables: `agents_master` (current registry), `scrape_runs` (run history with metrics), `scrape_agent_changes` (per-agent add/remove records per run). Includes `get_agency_scorecards(days)` for per-agency net gains/losses over a time window.
- **app.py** - Flask web dashboard showing latest run metrics, 30-day run history, and agency scorecards.
- **templates/dashboard.html** - Dashboard UI template with nav link to scorecards.
- **templates/scorecards.html** - Agency scorecards page: top gainers/losers tables with 30/60/90 day tabs.
- **st_scraper.py** - ST Classifieds property scraper. Fetches Commercial/Industrial and Houses for Sale listings from stclassifieds.sg, parses with BeautifulSoup, sends formatted list to Telegram with section headers. Features: owner highlighting (red circle emoji), image links for image-only listings, repeat sighting history tracking via DB.
- **news_digest.py** - Google News headline digest. Fetches top 10 headlines from Google News RSS via feedparser, formats as numbered HTML list with source and relative timestamps, sends to Telegram every 6 hours.
- **config.py** - Environment variable config (DATABASE_URL, Telegram, Stripe, CEA API URLs, ST Classifieds URLs).
- **render.yaml** - Render deployment config: web service (gunicorn) + cron job (runs at 1am and 1pm SGT).

## Data Flow

1. Initiate download via data.gov.sg API (with 429 retry/backoff)
2. Poll for CSV readiness (or use direct URL if returned immediately)
3. Download and parse CSV
4. Compare new data against `agents_master` table
5. Record run metrics in `scrape_runs`, agent-level changes in `scrape_agent_changes`
6. Replace `agents_master` with fresh data
7. Send Telegram notification

## Notifications

- **Telegram**: Bot sends to configured chat ID with Markdown formatting
- Includes: totals, new/removed agencies (with license numbers), new/removed agents (with registration numbers and agency names), top 20 agencies by agent count
- Also includes top 5 agency gainers and top 5 losers (30-day net change) via `get_agency_scorecards()`
- Error notifications also sent via Telegram on scraper failure

## Environment Variables

- `DATABASE_URL` - PostgreSQL connection string (required)
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` - For Telegram notifications
- `TELEGRAM_CHANNEL_ID`, `BOT_USERNAME` - Additional Telegram config
- `ST_TELEGRAM_CHAT_ID` - Telegram chat ID for ST Classifieds notifications (defaults to TELEGRAM_CHAT_ID)
- `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`, `STRIPE_PRICE_ID` - Stripe integration

## Deployment

Hosted on Render (render.yaml):
- Web service: `gunicorn app:app` (starter plan)
- Cron job: `python scraper.py` at 01:00 and 13:00 UTC (starter plan)
- Cron job: `python st_scraper.py` at 00:00 UTC / 08:00 SGT (starter plan)
- Cron job: `python news_digest.py` every 6h at 05:30/11:30/17:30/23:30 UTC (7:30 AM/1:30 PM/7:30 PM/1:30 AM SGT) (starter plan)

## Key Decisions / History

- Started with free stack (Neon DB + GitHub Actions cron), later migrated to paid Render with cron job and Telegram support
- Rate limiting is a major concern with the data.gov.sg API - extensive 429 handling with backoff, re-initiation after consecutive failures, and retry logic
- Poll interval set to 30s (strict rate limit from API)
- Top agencies list shows 20 (was originally 10)
- Rollback feature added to reverse last scrape run for testing purposes
- Notifications include agent details (name, registration number, agency) alongside agency details (name, license number)
- Agency scorecards feature added: web dashboard page (`/scorecards`) + Telegram summary section showing per-agency net agent gains/losses
- CEA public register (eservices.cea.gov.sg) is JS-rendered SPA; contact numbers are not exposed in public data (API or website)

## Commands

```bash
# Run scraper
python scraper.py

# Rollback last run
python scraper.py rollback

# Run ST Classifieds scraper
python st_scraper.py

# Run news digest
python news_digest.py

# Run web dashboard locally
flask run
# or
gunicorn app:app
```
