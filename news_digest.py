import html
import logging
import time
from datetime import datetime, timezone, timedelta

import feedparser
import requests

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SGT = timezone(timedelta(hours=8))

GOOGLE_NEWS_RSS = "https://news.google.com/rss?hl=en&gl=US&ceid=US:en"
GOOGLE_NEWS_BUSINESS_RSS = "https://news.google.com/rss/topics/CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx6TVdZU0FtVnVHZ0pWVXlnQVAB?hl=en&gl=US&ceid=US:en"


def fetch_feed(url):
    """Fetch an RSS feed with retries."""
    for attempt in range(1, 4):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            log.info("Feed fetched successfully (%d bytes)", len(resp.content))
            return feedparser.parse(resp.content)
        except requests.RequestException as e:
            log.warning("Fetch attempt %d/3 failed: %s", attempt, e)
            if attempt < 3:
                time.sleep(5 * attempt)
    raise RuntimeError(f"Failed to fetch RSS feed after 3 attempts: {url}")


def _relative_time(published_parsed):
    """Convert published time to relative string like '2h ago'."""
    if not published_parsed:
        return ""
    try:
        from calendar import timegm
        pub_ts = timegm(published_parsed)
        now_ts = time.time()
        diff = int(now_ts - pub_ts)
        if diff < 60:
            return "just now"
        if diff < 3600:
            m = diff // 60
            return f"{m}m ago"
        if diff < 86400:
            h = diff // 3600
            return f"{h}h ago"
        d = diff // 86400
        return f"{d}d ago"
    except Exception:
        return ""


def format_telegram_message(sections):
    """Build HTML-formatted news digest for Telegram.

    Args:
        sections: list of (section_title, entries) tuples
    """
    now = datetime.now(SGT)
    date_str = now.strftime("%d %b %Y").lstrip("0")
    time_str = now.strftime("%I:%M %p").lstrip("0")
    header = f"📰 <b>News Digest</b> — {date_str}, {time_str}\n"

    if not any(entries for _, entries in sections):
        return [header + "\nNo headlines found."]

    blocks = []
    for section_title, entries in sections:
        count = len(entries)
        blocks.append(f"\n<b>— {section_title} ({count}) —</b>\n")
        for i, entry in enumerate(entries, 1):
            title = html.escape(entry.get("title", "Untitled"))
            link = entry.get("link", "")
            source = html.escape(entry.get("source", {}).get("title", "")) if hasattr(entry.get("source", {}), "get") else ""
            time_ago = _relative_time(entry.get("published_parsed"))

            source_line = ""
            if source or time_ago:
                parts = [p for p in [source, time_ago] if p]
                source_line = f"   {' · '.join(parts)}\n"

            block = f"\n<b>{i}. {title}</b>\n{source_line}   🔗 <a href=\"{link}\">Read more</a>\n"
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
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.info("Telegram not configured, skipping")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for i, msg in enumerate(messages):
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=30)
        if resp.ok:
            log.info("Telegram message %d/%d sent", i + 1, len(messages))
        else:
            log.warning("Telegram send failed: %s %s", resp.status_code, resp.text)


def send_telegram_error(error_msg):
    """Send error notification via Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": f"<b>News Digest FAILED</b>\n\n{html.escape(error_msg)}",
            "parse_mode": "HTML",
        }, timeout=30)
    except Exception:
        log.exception("Telegram error notification failed")


def run():
    log.info("=== News Digest starting ===")
    try:
        general = fetch_feed(GOOGLE_NEWS_RSS)
        business = fetch_feed(GOOGLE_NEWS_BUSINESS_RSS)
        log.info("Found %d general, %d business entries",
                 len(general.get("entries", [])), len(business.get("entries", [])))
        sections = [
            ("Top Headlines", general.get("entries", [])[:10]),
            ("Financial News", business.get("entries", [])[:10]),
        ]
        messages = format_telegram_message(sections)
        send_telegram(messages)
        log.info("=== News Digest complete ===")
    except Exception as e:
        log.exception("News Digest failed")
        send_telegram_error(str(e))
        raise


if __name__ == "__main__":
    run()
