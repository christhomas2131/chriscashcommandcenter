"""
migrate_sqlite_to_postgres.py — One-time migration from jobs.db → Render Postgres.

Run once from your project root:
    python migrate_sqlite_to_postgres.py

Requires DATABASE_URL to be set in your .env file (pointing at Render's Postgres).
"""

import sqlite3
from pathlib import Path

from db.schema import migrate
from db.connection import cursor

SQLITE_PATH = Path(__file__).parent / "jobs.db"


def fetch_all(conn, query, params=()):
    conn.row_factory = sqlite3.Row
    rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def run():
    if not SQLITE_PATH.exists():
        print(f"ERROR: {SQLITE_PATH} not found. Run this from your project root.")
        return

    print("Connecting to SQLite...")
    sqlite = sqlite3.connect(SQLITE_PATH)

    print("Running Postgres migrations (creating tables if needed)...")
    migrate()

    # ── Jobs ──────────────────────────────────────────────────────────────
    jobs = fetch_all(sqlite, "SELECT * FROM jobs")
    print(f"Migrating {len(jobs)} jobs...")

    id_map = {}  # old SQLite id → new Postgres id

    with cursor() as cur:
        for job in jobs:
            cur.execute("""
                INSERT INTO jobs (
                    company_name, role_title, status, date_added, date_applied,
                    salary_min, salary_max, location, work_type, source,
                    job_url, notes, priority, first_response_date
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                RETURNING id
            """, (
                job["company_name"],
                job["role_title"],
                job.get("status", "Researching"),
                job.get("date_added"),
                job.get("date_applied"),
                job.get("salary_min"),
                job.get("salary_max"),
                job.get("location"),
                job.get("work_type", "Remote"),
                job.get("source", "Other"),
                job.get("job_url"),
                job.get("notes"),
                job.get("priority", "Medium"),
                job.get("first_response_date"),
            ))
            row = cur.fetchone()
            if row:
                id_map[job["id"]] = row["id"]
            else:
                # Row already existed (ON CONFLICT DO NOTHING) — look it up
                cur.execute(
                    "SELECT id FROM jobs WHERE company_name=%s AND role_title=%s LIMIT 1",
                    (job["company_name"], job["role_title"]),
                )
                existing = cur.fetchone()
                if existing:
                    id_map[job["id"]] = existing["id"]

    print(f"  ✓ {len(id_map)} jobs inserted/mapped")

    # ── Contacts ──────────────────────────────────────────────────────────
    contacts = fetch_all(sqlite, "SELECT * FROM contacts")
    print(f"Migrating {len(contacts)} contacts...")
    with cursor() as cur:
        for c in contacts:
            new_job_id = id_map.get(c["job_id"])
            if not new_job_id:
                continue
            cur.execute("""
                INSERT INTO contacts
                    (job_id, contact_name, contact_title, contact_email,
                     contact_phone, contact_linkedin, notes)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (
                new_job_id,
                c["contact_name"],
                c.get("contact_title"),
                c.get("contact_email"),
                c.get("contact_phone"),
                c.get("contact_linkedin"),
                c.get("notes"),
            ))
    print(f"  ✓ {len(contacts)} contacts inserted")

    # ── Follow-ups ────────────────────────────────────────────────────────
    follow_ups = fetch_all(sqlite, "SELECT * FROM follow_ups")
    print(f"Migrating {len(follow_ups)} follow-ups...")
    with cursor() as cur:
        for f in follow_ups:
            new_job_id = id_map.get(f["job_id"])
            if not new_job_id:
                continue
            cur.execute("""
                INSERT INTO follow_ups
                    (job_id, follow_up_date, follow_up_type, completed, notes)
                VALUES (%s,%s,%s,%s,%s)
            """, (
                new_job_id,
                f["follow_up_date"],
                f.get("follow_up_type", "Email"),
                bool(f.get("completed")),
                f.get("notes"),
            ))
    print(f"  ✓ {len(follow_ups)} follow-ups inserted")

    # ── Interview stages ──────────────────────────────────────────────────
    stages = fetch_all(sqlite, "SELECT * FROM interview_stages")
    print(f"Migrating {len(stages)} interview stages...")
    with cursor() as cur:
        for s in stages:
            new_job_id = id_map.get(s["job_id"])
            if not new_job_id:
                continue
            cur.execute("""
                INSERT INTO interview_stages
                    (job_id, stage_name, stage_date, interviewer_name, format, status, notes)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (
                new_job_id,
                s["stage_name"],
                s["stage_date"],
                s.get("interviewer_name"),
                s.get("format", "Video"),
                s.get("status", "Scheduled"),
                s.get("notes"),
            ))
    print(f"  ✓ {len(stages)} interview stages inserted")

    # ── Settings ──────────────────────────────────────────────────────────
    settings = fetch_all(sqlite, "SELECT * FROM settings")
    print(f"Migrating {len(settings)} settings...")
    with cursor() as cur:
        for s in settings:
            cur.execute("""
                INSERT INTO settings (key, value) VALUES (%s,%s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """, (s["key"], s["value"]))
    print(f"  ✓ {len(settings)} settings inserted")

    sqlite.close()
    print("\nMigration complete!")


if __name__ == "__main__":
    run()
