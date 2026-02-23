import csv
import io
import logging
import time

import requests

from config import (
    INITIATE_URL, POLL_URL, POLL_INTERVAL, POLL_MAX_ATTEMPTS, FORMSPREE_ENDPOINT,
)
import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def initiate_download():
    resp = requests.get(INITIATE_URL, timeout=30)
    resp.raise_for_status()
    return resp.json()


def poll_download():
    wait = POLL_INTERVAL
    for attempt in range(1, POLL_MAX_ATTEMPTS + 1):
        log.info("Poll attempt %d/%d", attempt, POLL_MAX_ATTEMPTS)
        resp = requests.get(POLL_URL, timeout=30)
        if resp.status_code == 429:
            backoff = min(wait * 2, 120)
            log.warning("Rate limited (429), backing off %ds...", backoff)
            time.sleep(backoff)
            wait = backoff
            continue
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

    return {
        "total_agencies": len(new_agencies),
        "total_agents": len(new_reg_nos),
        "new_agencies": len(added_agency_names),
        "removed_agencies": len(removed_agency_names),
        "new_agents": len(added_agent_ids),
        "removed_agents": len(removed_agent_ids),
        "new_agency_names": sorted(added_agency_names),
        "removed_agency_names": sorted(removed_agency_names),
        "changes": changes,
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
    if metrics["new_agency_names"]:
        body_lines.append("\nNewly added agencies:")
        for name in metrics["new_agency_names"]:
            body_lines.append(f"  - {name}")
    if metrics["removed_agency_names"]:
        body_lines.append("\nRemoved agencies:")
        for name in metrics["removed_agency_names"]:
            body_lines.append(f"  - {name}")

    payload = {
        "subject": f"CEA Scrape: {metrics['new_agents']} added, {metrics['removed_agents']} removed",
        "message": "\n".join(body_lines),
    }
    resp = requests.post(FORMSPREE_ENDPOINT, json=payload, timeout=30)
    if resp.ok:
        log.info("Email sent successfully")
    else:
        log.warning("Email send failed: %s %s", resp.status_code, resp.text)


def run():
    log.info("=== CEA Scraper starting ===")
    db.init_db()

    try:
        log.info("Initiating download...")
        initiate_download()

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
        log.info("=== Scrape complete ===")

    except Exception as e:
        log.exception("Scraper failed")
        try:
            db.insert_run(0, 0, 0, 0, 0, 0, [], [], status="error", error_message=str(e))
        except Exception:
            log.exception("Failed to record error run")
        raise


if __name__ == "__main__":
    run()
