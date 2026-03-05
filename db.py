import psycopg2
from psycopg2.extras import execute_values
from config import DATABASE_URL


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS agents_master (
                    registration_no       TEXT PRIMARY KEY,
                    salesperson_name      TEXT,
                    registration_start_date TEXT,
                    registration_end_date   TEXT,
                    estate_agent_name     TEXT,
                    estate_agent_license_no TEXT,
                    updated_at            TIMESTAMP DEFAULT now()
                );

                CREATE TABLE IF NOT EXISTS scrape_runs (
                    id                  SERIAL PRIMARY KEY,
                    run_at              TIMESTAMP UNIQUE DEFAULT now(),
                    total_agencies      INTEGER,
                    total_agents        INTEGER,
                    new_agencies        INTEGER DEFAULT 0,
                    removed_agencies    INTEGER DEFAULT 0,
                    new_agents          INTEGER DEFAULT 0,
                    removed_agents      INTEGER DEFAULT 0,
                    new_agency_names    TEXT[] DEFAULT '{}',
                    removed_agency_names TEXT[] DEFAULT '{}',
                    status              TEXT DEFAULT 'success',
                    error_message       TEXT
                );

                CREATE TABLE IF NOT EXISTS scrape_agent_changes (
                    id              SERIAL PRIMARY KEY,
                    scrape_run_id   INTEGER REFERENCES scrape_runs(id),
                    registration_no TEXT,
                    salesperson_name TEXT,
                    estate_agent_name TEXT,
                    change_type     TEXT CHECK (change_type IN ('added', 'removed'))
                );
            """)


def load_master_sets():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT registration_no FROM agents_master")
            old_reg_nos = {row[0] for row in cur.fetchall()}
            cur.execute("SELECT DISTINCT estate_agent_name FROM agents_master")
            old_agencies = {row[0] for row in cur.fetchall()}
    return old_reg_nos, old_agencies


def load_master_dict():
    """Return {registration_no: row_dict} for the current master table."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT registration_no, salesperson_name, estate_agent_name, estate_agent_license_no
                FROM agents_master
            """)
            return {
                row[0]: {"salesperson_name": row[1], "estate_agent_name": row[2], "estate_agent_license_no": row[3]}
                for row in cur.fetchall()
            }


def replace_master(rows):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE agents_master")
            execute_values(
                cur,
                """INSERT INTO agents_master
                   (registration_no, salesperson_name, registration_start_date,
                    registration_end_date, estate_agent_name, estate_agent_license_no)
                   VALUES %s""",
                rows,
            )


def insert_run(total_agencies, total_agents, new_agencies, removed_agencies,
               new_agents, removed_agents, new_agency_names, removed_agency_names,
               status="success", error_message=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO scrape_runs
                    (total_agencies, total_agents, new_agencies, removed_agencies,
                     new_agents, removed_agents, new_agency_names, removed_agency_names,
                     status, error_message)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (total_agencies, total_agents, new_agencies, removed_agencies,
                  new_agents, removed_agents, list(new_agency_names),
                  list(removed_agency_names), status, error_message))
            return cur.fetchone()[0]


def insert_agent_changes(run_id, changes):
    if not changes:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """INSERT INTO scrape_agent_changes
                   (scrape_run_id, registration_no, salesperson_name,
                    estate_agent_name, change_type)
                   VALUES %s""",
                [(run_id, c["registration_no"], c["salesperson_name"],
                  c["estate_agent_name"], c["change_type"]) for c in changes],
            )


def get_latest_run():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT run_at, total_agencies, total_agents,
                       new_agencies, removed_agencies, new_agents, removed_agents,
                       new_agency_names, removed_agency_names, status
                FROM scrape_runs ORDER BY run_at DESC LIMIT 1
            """)
            return cur.fetchone()


def rollback_last_run():
    """Reverse the last scrape run: undo agent changes and delete run records."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM scrape_runs ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
            if not row:
                return None
            run_id = row[0]

            # Reverse 'added' agents: delete them from master
            cur.execute("""
                DELETE FROM agents_master
                WHERE registration_no IN (
                    SELECT registration_no FROM scrape_agent_changes
                    WHERE scrape_run_id = %s AND change_type = 'added'
                )
            """, (run_id,))
            deleted = cur.rowcount

            # Reverse 'removed' agents: re-insert them into master
            cur.execute("""
                INSERT INTO agents_master (registration_no, salesperson_name,
                    registration_start_date, registration_end_date,
                    estate_agent_name, estate_agent_license_no)
                SELECT registration_no, salesperson_name, '', '', estate_agent_name, ''
                FROM scrape_agent_changes
                WHERE scrape_run_id = %s AND change_type = 'removed'
                ON CONFLICT (registration_no) DO NOTHING
            """, (run_id,))
            reinserted = cur.rowcount

            # Delete run records
            cur.execute("DELETE FROM scrape_agent_changes WHERE scrape_run_id = %s", (run_id,))
            cur.execute("DELETE FROM scrape_runs WHERE id = %s", (run_id,))

    return {"run_id": run_id, "deleted": deleted, "reinserted": reinserted}


def get_run_history(limit=30):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT run_at, total_agencies, total_agents,
                       new_agencies, removed_agencies, new_agents, removed_agents,
                       status
                FROM scrape_runs ORDER BY run_at DESC LIMIT %s
            """, (limit,))
            return cur.fetchall()
