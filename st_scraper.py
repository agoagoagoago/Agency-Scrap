import html
import logging
import re
import time
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup

from config import ST_CLASSIFIEDS_URL, ST_HOUSES_URL, TELEGRAM_BOT_TOKEN, ST_TELEGRAM_CHAT_ID, DATABASE_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SGT = timezone(timedelta(hours=8))


def fetch_page(url):
    """Fetch a ST Classifieds listing page with retries."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    for attempt in range(1, 4):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            log.info("Page fetched successfully (%d bytes)", len(resp.text))
            return resp.text
        except requests.RequestException as e:
            log.warning("Fetch attempt %d/3 failed: %s", attempt, e)
            if attempt < 3:
                time.sleep(5 * attempt)
    raise RuntimeError(f"Failed to fetch {url} after 3 attempts")


def parse_listings(page_html):
    """Parse listings from ST Classifieds HTML."""
    soup = BeautifulSoup(page_html, "html.parser")
    list_view = soup.find("div", class_="listView")
    if not list_view:
        log.warning("No div.listView found on page")
        return []

    listings = []
    rows = list_view.find_all("div", class_="row")
    for row in rows:
        # Classification type
        sub_section = row.find("div", class_="subSectionText2")
        classification = sub_section.get_text(strip=True) if sub_section else ""
        # Remove trailing " - NNNN" sub-section number
        classification = re.sub(r"\s*-\s*\d+$", "", classification).strip()

        # Description
        p_tag = row.find("p")
        description = p_tag.get_text(strip=True) if p_tag else ""

        # Phone and Ad ID from onclick handler
        phone = ""
        ad_id = ""
        onclick_el = row.find(attrs={"onclick": re.compile(r"openPopupDialog\('Call'")})
        if onclick_el:
            onclick = onclick_el.get("onclick", "")
            # openPopupDialog('Call','phone','param2','param3','adId',...)
            match = re.search(r"openPopupDialog\('Call','([^']*)'(?:,'[^']*'){2},'([^']*)'", onclick)
            if match:
                phone = match.group(1)
                ad_id = match.group(2)

        # Image URL (for image-only listings)
        img_tag = row.find("img", class_="imgCenterAlign")
        image_url = img_tag["src"] if img_tag and img_tag.get("src") else ""

        if description or classification:
            listings.append({
                "classification": classification,
                "description": description,
                "phone": phone,
                "ad_id": ad_id,
                "image_url": image_url,
            })

    log.info("Parsed %d listings", len(listings))
    return listings


def _shorten_type(classification):
    """Shorten classification for table display."""
    cl = classification.lower()
    if "factory" in cl or "warehouse" in cl:
        return "Factory/WH"
    if "office" in cl:
        return "Office"
    if "shop" in cl:
        return "Shop"
    if "land" in cl:
        return "Land"
    if "detached" in cl:
        return "Detached"
    if "semi" in cl:
        return "Semi-D"
    if "terrace" in cl:
        return "Terrace"
    if "bungalow" in cl:
        return "Bungalow"
    if "corner" in cl:
        return "Corner"
    if "cluster" in cl:
        return "Cluster"
    # Fallback: first 10 chars
    return classification[:10]


def _format_date(d):
    """Format date as d/m/yy without leading zeros, cross-platform."""
    return f"{d.day}/{d.month}/{d.strftime('%y')}"


def get_listing_history(listings):
    """Fetch sighting history for listings from DB."""
    if not DATABASE_URL:
        return {}
    try:
        from db import st_get_sighting_history
        ad_ids = [lst["ad_id"] for lst in listings if lst.get("ad_id")]
        if not ad_ids:
            return {}
        return st_get_sighting_history(ad_ids)
    except Exception:
        log.warning("Failed to fetch listing history", exc_info=True)
        return {}


def record_sightings(listings):
    """Record today's sightings in the DB."""
    if not DATABASE_URL:
        return
    try:
        from db import st_record_sightings
        ad_ids = [lst["ad_id"] for lst in listings if lst.get("ad_id")]
        today = datetime.now(SGT).date()
        st_record_sightings(ad_ids, today)
    except Exception:
        log.warning("Failed to record sightings", exc_info=True)


def format_telegram_message(sections, history=None):
    """Build HTML-formatted list messages for Telegram.

    sections: list of (title, listings) tuples
    """
    now = datetime.now(SGT)
    date_str = now.strftime("%d %b %Y").lstrip("0")
    total = sum(len(listings) for _, listings in sections)
    header = (
        f"<b>ST Classifieds</b>\n"
        f"<i>{date_str} — {total} listings</i>\n"
    )

    if total == 0:
        return [header + "\nNo listings found."]

    # Build listing blocks with section headers
    blocks = []
    for title, listings in sections:
        if not listings:
            continue
        blocks.append(f"\n<b>— {title} ({len(listings)}) —</b>\n")
        for i, lst in enumerate(listings, 1):
            short_type = _shorten_type(lst["classification"])
            phone = lst["phone"] or "—"
            desc = html.escape(lst["description"])
            if "Click on image" in lst["description"] and lst.get("image_url"):
                desc = f'<a href="{lst["image_url"]}">View listing image</a>'
            is_owner = "owner" in lst["description"].lower()
            prefix = "🔴 " if is_owner else ""
            block = f"\n<b>{prefix}{i}. {short_type}</b> | {phone}\n{desc}\n"
            if history and lst.get("ad_id") and lst["ad_id"] in history:
                dates = history[lst["ad_id"]]
                today = datetime.now(SGT).date()
                all_dates = sorted(set(dates + [today]))
                date_strs = [_format_date(d) for d in all_dates]
                count = len(date_strs)
                if count > 1:
                    block += f"<i>[Same listing advertised {count} times: {', '.join(date_strs)}]</i>\n"
            blocks.append(block)

    # Split into multiple messages if needed (Telegram limit 4096)
    messages = []
    current_blocks = []
    current_len = len(header)
    for block in blocks:
        block_len = len(block)
        if current_len + block_len > 3900 and current_blocks:
            messages.append(header + "".join(current_blocks))
            current_blocks = []
            current_len = len(header)
        current_blocks.append(block)
        current_len += block_len

    if current_blocks:
        messages.append(
            (header if not messages else "") + "".join(current_blocks)
        )

    return messages


def send_telegram(messages):
    """Send messages to Telegram."""
    if not TELEGRAM_BOT_TOKEN or not ST_TELEGRAM_CHAT_ID:
        log.info("Telegram not configured, skipping")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for i, msg in enumerate(messages):
        resp = requests.post(url, json={
            "chat_id": ST_TELEGRAM_CHAT_ID,
            "text": msg,
            "parse_mode": "HTML",
        }, timeout=30)
        if resp.ok:
            log.info("Telegram message %d/%d sent", i + 1, len(messages))
        else:
            log.warning("Telegram send failed: %s %s", resp.status_code, resp.text)


def send_telegram_error(error_msg):
    """Send error notification via Telegram."""
    if not TELEGRAM_BOT_TOKEN or not ST_TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={
            "chat_id": ST_TELEGRAM_CHAT_ID,
            "text": f"<b>ST Classifieds Scraper FAILED</b>\n\n{html.escape(error_msg)}",
            "parse_mode": "HTML",
        }, timeout=30)
    except Exception:
        log.exception("Telegram error notification failed")


def run():
    log.info("=== ST Classifieds Scraper starting ===")
    try:
        commercial_html = fetch_page(ST_CLASSIFIEDS_URL)
        commercial = parse_listings(commercial_html)

        houses_html = fetch_page(ST_HOUSES_URL)
        houses = parse_listings(houses_html)

        all_listings = commercial + houses
        history = get_listing_history(all_listings)

        sections = [
            ("Commercial/Industrial", commercial),
            ("Houses for Sale", houses),
        ]
        messages = format_telegram_message(sections, history)
        send_telegram(messages)
        record_sightings(all_listings)
        log.info("=== ST Classifieds Scrape complete ===")
    except Exception as e:
        log.exception("ST Classifieds scraper failed")
        send_telegram_error(str(e))
        raise


if __name__ == "__main__":
    run()
