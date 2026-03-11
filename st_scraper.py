import html
import logging
import re
import time
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup

from config import ST_CLASSIFIEDS_URL, TELEGRAM_BOT_TOKEN, ST_TELEGRAM_CHAT_ID

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SGT = timezone(timedelta(hours=8))


def fetch_page():
    """Fetch the ST Classifieds listing page with retries."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    for attempt in range(1, 4):
        try:
            resp = requests.get(ST_CLASSIFIEDS_URL, headers=headers, timeout=30)
            resp.raise_for_status()
            log.info("Page fetched successfully (%d bytes)", len(resp.text))
            return resp.text
        except requests.RequestException as e:
            log.warning("Fetch attempt %d/3 failed: %s", attempt, e)
            if attempt < 3:
                time.sleep(5 * attempt)
    raise RuntimeError("Failed to fetch ST Classifieds page after 3 attempts")


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
    # Fallback: first 10 chars
    return classification[:10]


def format_telegram_message(listings):
    """Build HTML-formatted list messages for Telegram."""
    now = datetime.now(SGT)
    date_str = now.strftime("%d %b %Y").lstrip("0")
    header = (
        f"<b>ST Classifieds - Commercial/Industrial</b>\n"
        f"<i>{date_str} — {len(listings)} listings</i>\n"
    )

    if not listings:
        return [header + "\nNo listings found."]

    # Build listing blocks
    blocks = []
    for i, lst in enumerate(listings, 1):
        short_type = _shorten_type(lst["classification"])
        phone = lst["phone"] or "—"
        desc = html.escape(lst["description"])
        if "Click on image" in lst["description"] and lst.get("image_url"):
            desc = f'<a href="{lst["image_url"]}">View listing image</a>'
        block = f"\n<b>{i}. {short_type}</b> | {phone}\n{desc}\n"
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
        page_html = fetch_page()
        listings = parse_listings(page_html)
        messages = format_telegram_message(listings)
        send_telegram(messages)
        log.info("=== ST Classifieds Scrape complete ===")
    except Exception as e:
        log.exception("ST Classifieds scraper failed")
        send_telegram_error(str(e))
        raise


if __name__ == "__main__":
    run()
