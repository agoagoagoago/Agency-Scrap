import csv
import io
import logging
import time
from datetime import datetime, timezone, timedelta

import requests

from config import (
    INITIATE_URL, POLL_URL, POLL_INTERVAL, POLL_MAX_ATTEMPTS, FORMSPREE_ENDPOINT,
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
)
import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def initiate_download():
    """Initiate download and return CSV URL if available, else None."""
    for attempt in range(1, 6):
        resp = requests.get(INITIATE_URL, timeout=30)
        if resp.status_code == 429:
            wait = 60 * attempt
            log.warning("Initiate rate limited (429), waiting %ds (attempt %d/5)...", wait, attempt)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        data = resp.json()
        url = data.get("data", {}).get("url")
        if url:
            log.info("Got download URL directly from initiate response")
            return url
        return None
    raise RuntimeError("initiate-download still rate limited after 5 retries")


def poll_download():
    wait = POLL_INTERVAL
    consecutive_429 = 0
    for attempt in range(1, POLL_MAX_ATTEMPTS + 1):
        log.info("Poll attempt %d/%d", attempt, POLL_MAX_ATTEMPTS)
        resp = requests.get(POLL_URL, timeout=30)
        if resp.status_code == 429:
            consecutive_429 += 1
            if consecutive_429 >= 3:
                log.warning("3 consecutive 429s, re-initiating download...")
                try:
                    initiate_download()
                except Exception:
                    log.warning("Re-initiate failed, continuing anyway")
                consecutive_429 = 0
                time.sleep(60)
            else:
                backoff = min(POLL_INTERVAL * (2 ** consecutive_429), 60)
                log.warning("Rate limited (429) x%d, backing off %ds...", consecutive_429, backoff)
                time.sleep(backoff)
            continue
        consecutive_429 = 0
        resp.raise_for_status()
        data = resp.json()
        code = data.get("data", {}).get("readyToDownload")
        if code is True:
            return data["data"]["url"]
        log.info("Not ready yet, waiting %ds...", wait)
        time.sleep(wait)
    raise TimeoutError("Download not ready after max poll attempts")


def download_csv(url):
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    text = resp.text
    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for r in reader:
        rows.append({
            "registration_no": r["registration_no"],
            "salesperson_name": r["salesperson_name"],
            "registration_start_date": r["registration_start_date"],
            "registration_end_date": r["registration_end_date"],
            "estate_agent_name": r["estate_agent_name"],
            "estate_agent_license_no": r["estate_agent_license_no"],
        })
    return rows


def compare(new_rows, old_master):
    new_reg_nos = {r["registration_no"] for r in new_rows}
    new_agencies = {r["estate_agent_name"] for r in new_rows}
    old_reg_nos = set(old_master.keys())
    old_agencies = {v["estate_agent_name"] for v in old_master.values()}

    new_lookup = {r["registration_no"]: r for r in new_rows}

    added_agent_ids = new_reg_nos - old_reg_nos
    removed_agent_ids = old_reg_nos - new_reg_nos
    added_agency_names = new_agencies - old_agencies
    removed_agency_names = old_agencies - new_agencies

    # Build agency name -> license_no mappings
    new_agency_license = {}
    for r in new_rows:
        new_agency_license.setdefault(r["estate_agent_name"], r["estate_agent_license_no"])
    old_agency_license = {}
    for v in old_master.values():
        old_agency_license.setdefault(v["estate_agent_name"], v.get("estate_agent_license_no", ""))

    changes = []
    for reg in added_agent_ids:
        r = new_lookup[reg]
        changes.append({
            "registration_no": reg,
            "salesperson_name": r["salesperson_name"],
            "estate_agent_name": r["estate_agent_name"],
            "change_type": "added",
        })
    for reg in removed_agent_ids:
        r = old_master[reg]
        changes.append({
            "registration_no": reg,
            "salesperson_name": r["salesperson_name"],
            "estate_agent_name": r["estate_agent_name"],
            "change_type": "removed",
        })

    # Top 10 agencies by agent count
    agency_counts = {}
    for r in new_rows:
        name = r["estate_agent_name"]
        agency_counts[name] = agency_counts.get(name, 0) + 1
    top_agencies = sorted(agency_counts.items(), key=lambda x: x[1], reverse=True)[:20]

    return {
        "total_agencies": len(new_agencies),
        "total_agents": len(new_reg_nos),
        "new_agencies": len(added_agency_names),
        "removed_agencies": len(removed_agency_names),
        "new_agents": len(added_agent_ids),
        "removed_agents": len(removed_agent_ids),
        "new_agency_names": sorted(added_agency_names),
        "removed_agency_names": sorted(removed_agency_names),
        "new_agency_details": sorted([(n, new_agency_license.get(n, "")) for n in added_agency_names]),
        "removed_agency_details": sorted([(n, old_agency_license.get(n, "")) for n in removed_agency_names]),
        "changes": changes,
        "top_agencies": top_agencies,
    }


def send_email(metrics):
    if not FORMSPREE_ENDPOINT:
        log.info("FORMSPREE_ENDPOINT not set, skipping email")
        return

    body_lines = [
        f"Total agencies: {metrics['total_agencies']}",
        f"Total agents: {metrics['total_agents']}",
        f"New agencies: {metrics['new_agencies']}",
        f"Removed agencies: {metrics['removed_agencies']}",
        f"New agents: {metrics['new_agents']}",
        f"Removed agents: {metrics['removed_agents']}",
    ]
    if metrics.get("new_agency_details"):
        body_lines.append("\nNewly added agencies:")
        for name, license_no in metrics["new_agency_details"]:
            body_lines.append(f"  - {name} ({license_no})")
    if metrics.get("removed_agency_details"):
        body_lines.append("\nRemoved agencies:")
        for name, license_no in metrics["removed_agency_details"]:
            body_lines.append(f"  - {name} ({license_no})")
    added_agents = [c for c in metrics.get("changes", []) if c["change_type"] == "added"]
    removed_agents = [c for c in metrics.get("changes", []) if c["change_type"] == "removed"]
    if added_agents:
        body_lines.append("\nNewly added agents:")
        for a in sorted(added_agents, key=lambda x: x["salesperson_name"]):
            body_lines.append(f"  - {a['salesperson_name']} ({a['registration_no']}) | {a['estate_agent_name']}")
    if removed_agents:
        body_lines.append("\nRemoved agents:")
        for a in sorted(removed_agents, key=lambda x: x["salesperson_name"]):
            body_lines.append(f"  - {a['salesperson_name']} ({a['registration_no']}) | {a['estate_agent_name']}")
    if metrics.get("top_agencies"):
        body_lines.append("\nTop 20 Agencies by Agent Count:")
        for i, (name, count) in enumerate(metrics["top_agencies"], 1):
            body_lines.append(f"  {i}. {name} ({count:,})")

    payload = {
        "subject": f"CEA Scrape: {metrics['new_agents']} added, {metrics['removed_agents']} removed",
        "message": "\n".join(body_lines),
    }
    resp = requests.post(FORMSPREE_ENDPOINT, json=payload, timeout=30)
    if resp.ok:
        log.info("Email sent successfully")
    else:
        log.warning("Email send failed: %s %s", resp.status_code, resp.text)


def send_telegram(metrics):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.info("Telegram not configured, skipping")
        return
    try:
        now = datetime.now(timezone(timedelta(hours=8)))
        timestamp = f"{now.strftime('%-d %b, %-I:%M')} {now.strftime('%p').lower()}"
        lines = [
            f"*CEA - Latest Update ({timestamp})*",
            "",
            f"Total agencies: {metrics['total_agencies']}",
            f"Total agents: {metrics['total_agents']}",
            f"New agencies: {metrics['new_agencies']}",
            f"Removed agencies: {metrics['removed_agencies']}",
            f"New agents: {metrics['new_agents']}",
            f"Removed agents: {metrics['removed_agents']}",
        ]
        if metrics.get("new_agency_details"):
            lines.append("\n*Newly added agencies:*")
            for name, license_no in metrics["new_agency_details"]:
                lines.append(f"  • {name} ({license_no})")
        if metrics.get("removed_agency_details"):
            lines.append("\n*Removed agencies:*")
            for name, license_no in metrics["removed_agency_details"]:
                lines.append(f"  • {name} ({license_no})")
        added_agents = [c for c in metrics.get("changes", []) if c["change_type"] == "added"]
        removed_agents = [c for c in metrics.get("changes", []) if c["change_type"] == "removed"]
        if added_agents:
            lines.append("\n*Newly added agents:*")
            for a in sorted(added_agents, key=lambda x: x["salesperson_name"]):
                lines.append(f"  • {a['salesperson_name']} ({a['registration_no']}) | {a['estate_agent_name']}")
        if removed_agents:
            lines.append("\n*Removed agents:*")
            for a in sorted(removed_agents, key=lambda x: x["salesperson_name"]):
                lines.append(f"  • {a['salesperson_name']} ({a['registration_no']}) | {a['estate_agent_name']}")
        if metrics.get("top_agencies"):
            lines.append("\n*Top 20 Agencies by Agent Count:*")
            for i, (name, count) in enumerate(metrics["top_agencies"], 1):
                lines.append(f"  {i}. {name} ({count:,})")

        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": "\n".join(lines),
            "parse_mode": "Markdown",
        }, timeout=30)
        if resp.ok:
            log.info("Telegram message sent")
        else:
            log.warning("Telegram send failed: %s %s", resp.status_code, resp.text)
    except Exception:
        log.exception("Telegram notification failed")


def send_telegram_error(error_msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": f"*CEA Scraper FAILED*\n\n{error_msg}",
            "parse_mode": "Markdown",
        }, timeout=30)
    except Exception:
        log.exception("Telegram error notification failed")


def run():
    log.info("=== CEA Scraper starting ===")
    db.init_db()

    try:
        log.info("Initiating download...")
        csv_url = initiate_download()

        if not csv_url:
            log.info("Polling for download URL...")
            csv_url = poll_download()

        log.info("Downloading CSV...")
        new_rows = download_csv(csv_url)
        log.info("Downloaded %d rows", len(new_rows))

        log.info("Loading current master data...")
        old_master = db.load_master_dict()
        log.info("Master has %d agents", len(old_master))

        log.info("Comparing...")
        metrics = compare(new_rows, old_master)
        log.info(
            "Results: %d agencies, %d agents, +%d/-%d agencies, +%d/-%d agents",
            metrics["total_agencies"], metrics["total_agents"],
            metrics["new_agencies"], metrics["removed_agencies"],
            metrics["new_agents"], metrics["removed_agents"],
        )

        run_id = db.insert_run(
            metrics["total_agencies"], metrics["total_agents"],
            metrics["new_agencies"], metrics["removed_agencies"],
            metrics["new_agents"], metrics["removed_agents"],
            metrics["new_agency_names"], metrics["removed_agency_names"],
        )
        db.insert_agent_changes(run_id, metrics["changes"])

        log.info("Replacing master table...")
        master_tuples = [
            (r["registration_no"], r["salesperson_name"],
             r["registration_start_date"], r["registration_end_date"],
             r["estate_agent_name"], r["estate_agent_license_no"])
            for r in new_rows
        ]
        db.replace_master(master_tuples)

        send_email(metrics)
        send_telegram(metrics)
        log.info("=== Scrape complete ===")

    except Exception as e:
        log.exception("Scraper failed")
        send_telegram_error(str(e))
        try:
            db.insert_run(0, 0, 0, 0, 0, 0, [], [], status="error", error_message=str(e))
        except Exception:
            log.exception("Failed to record error run")
        raise


def rollback():
    log.info("=== Rolling back last scrape run ===")
    db.init_db()
    result = db.rollback_last_run()
    if result is None:
        log.info("No scrape runs found to roll back")
    else:
        log.info(
            "Rolled back run %d: %d added agents deleted, %d removed agents re-inserted",
            result["run_id"], result["deleted"], result["reinserted"],
        )


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "rollback":
        rollback()
    else:
        run()
