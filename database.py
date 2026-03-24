import sqlite3
from datetime import date, timedelta

DB_PATH = "jobs.db"


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_connection()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name TEXT NOT NULL,
            role_title TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'Researching',
            date_added DATE NOT NULL DEFAULT (date('now')),
            date_applied DATE,
            salary_min INTEGER,
            salary_max INTEGER,
            location TEXT,
            work_type TEXT DEFAULT 'Remote',
            source TEXT DEFAULT 'Other',
            job_url TEXT,
            notes TEXT,
            priority TEXT DEFAULT 'Medium'
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            contact_name TEXT NOT NULL,
            contact_title TEXT,
            contact_email TEXT,
            contact_phone TEXT,
            contact_linkedin TEXT,
            notes TEXT,
            FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS follow_ups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            follow_up_date DATE NOT NULL,
            follow_up_type TEXT NOT NULL DEFAULT 'Email',
            completed BOOLEAN NOT NULL DEFAULT 0,
            notes TEXT,
            FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS interview_stages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            stage_name TEXT NOT NULL,
            stage_date DATE NOT NULL,
            interviewer_name TEXT,
            format TEXT DEFAULT 'Video',
            status TEXT DEFAULT 'Scheduled',
            notes TEXT,
            FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    # Safe migration: add first_response_date if not already present
    try:
        c.execute("ALTER TABLE jobs ADD COLUMN first_response_date DATE")
    except Exception:
        pass

    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

def get_all_jobs():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM jobs ORDER BY date_added DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_job(job_id):
    conn = get_connection()
    row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def add_job(data):
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        INSERT INTO jobs (
            company_name, role_title, status, date_added, date_applied,
            salary_min, salary_max, location, work_type, source,
            job_url, notes, priority, first_response_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data["company_name"],
        data["role_title"],
        data.get("status", "Researching"),
        data.get("date_added", date.today().isoformat()),
        data.get("date_applied"),
        data.get("salary_min"),
        data.get("salary_max"),
        data.get("location"),
        data.get("work_type", "Remote"),
        data.get("source", "Other"),
        data.get("job_url"),
        data.get("notes"),
        data.get("priority", "Medium"),
        data.get("first_response_date"),
    ))
    job_id = c.lastrowid
    conn.commit()
    conn.close()
    return job_id


def update_job(job_id, data):
    conn = get_connection()
    conn.execute("""
        UPDATE jobs SET
            company_name = ?, role_title = ?, status = ?, date_applied = ?,
            salary_min = ?, salary_max = ?, location = ?, work_type = ?,
            source = ?, job_url = ?, notes = ?, priority = ?, first_response_date = ?
        WHERE id = ?
    """, (
        data["company_name"],
        data["role_title"],
        data["status"],
        data.get("date_applied"),
        data.get("salary_min"),
        data.get("salary_max"),
        data.get("location"),
        data.get("work_type"),
        data.get("source"),
        data.get("job_url"),
        data.get("notes"),
        data.get("priority"),
        data.get("first_response_date"),
        job_id,
    ))
    conn.commit()
    conn.close()


def update_job_status(job_id, status):
    conn = get_connection()
    conn.execute("UPDATE jobs SET status = ? WHERE id = ?", (status, job_id))
    conn.commit()
    conn.close()


def delete_job(job_id):
    conn = get_connection()
    conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Contacts
# ---------------------------------------------------------------------------

def get_contacts(job_id):
    conn = get_connection()
    rows = conn.execute("SELECT * FROM contacts WHERE job_id = ?", (job_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_contact(data):
    conn = get_connection()
    conn.execute("""
        INSERT INTO contacts (job_id, contact_name, contact_title, contact_email,
            contact_phone, contact_linkedin, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        data["job_id"],
        data["contact_name"],
        data.get("contact_title"),
        data.get("contact_email"),
        data.get("contact_phone"),
        data.get("contact_linkedin"),
        data.get("notes"),
    ))
    conn.commit()
    conn.close()


def update_contact(contact_id, data):
    conn = get_connection()
    conn.execute("""
        UPDATE contacts SET contact_name=?, contact_title=?, contact_email=?,
            contact_phone=?, contact_linkedin=?, notes=?
        WHERE id=?
    """, (
        data["contact_name"],
        data.get("contact_title"),
        data.get("contact_email"),
        data.get("contact_phone"),
        data.get("contact_linkedin"),
        data.get("notes"),
        contact_id,
    ))
    conn.commit()
    conn.close()


def delete_contact(contact_id):
    conn = get_connection()
    conn.execute("DELETE FROM contacts WHERE id = ?", (contact_id,))
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Follow-ups
# ---------------------------------------------------------------------------

def get_follow_ups(job_id=None, completed=None):
    conn = get_connection()
    query = """
        SELECT f.*, j.company_name, j.role_title
        FROM follow_ups f
        JOIN jobs j ON f.job_id = j.id
    """
    conditions, params = [], []
    if job_id is not None:
        conditions.append("f.job_id = ?")
        params.append(job_id)
    if completed is not None:
        conditions.append("f.completed = ?")
        params.append(1 if completed else 0)
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY f.follow_up_date ASC"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_follow_up(data):
    conn = get_connection()
    conn.execute("""
        INSERT INTO follow_ups (job_id, follow_up_date, follow_up_type, completed, notes)
        VALUES (?, ?, ?, ?, ?)
    """, (
        data["job_id"],
        data["follow_up_date"],
        data.get("follow_up_type", "Email"),
        1 if data.get("completed") else 0,
        data.get("notes"),
    ))
    conn.commit()
    conn.close()


def update_follow_up(follow_up_id, data):
    conn = get_connection()
    conn.execute("""
        UPDATE follow_ups SET follow_up_date=?, follow_up_type=?, completed=?, notes=?
        WHERE id=?
    """, (
        data["follow_up_date"],
        data["follow_up_type"],
        1 if data.get("completed") else 0,
        data.get("notes"),
        follow_up_id,
    ))
    conn.commit()
    conn.close()


def toggle_follow_up(follow_up_id):
    conn = get_connection()
    conn.execute(
        "UPDATE follow_ups SET completed = CASE WHEN completed = 1 THEN 0 ELSE 1 END WHERE id = ?",
        (follow_up_id,),
    )
    conn.commit()
    conn.close()


def delete_follow_up(follow_up_id):
    conn = get_connection()
    conn.execute("DELETE FROM follow_ups WHERE id = ?", (follow_up_id,))
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Interview stages
# ---------------------------------------------------------------------------

def get_interview_stages(job_id):
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM interview_stages WHERE job_id = ? ORDER BY stage_date ASC",
        (job_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_interview_stage(data):
    conn = get_connection()
    conn.execute("""
        INSERT INTO interview_stages
            (job_id, stage_name, stage_date, interviewer_name, format, status, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        data["job_id"],
        data["stage_name"],
        data["stage_date"],
        data.get("interviewer_name"),
        data.get("format", "Video"),
        data.get("status", "Scheduled"),
        data.get("notes"),
    ))
    conn.commit()
    conn.close()


def update_interview_stage(stage_id, data):
    conn = get_connection()
    conn.execute("""
        UPDATE interview_stages SET stage_name=?, stage_date=?, interviewer_name=?,
            format=?, status=?, notes=?
        WHERE id=?
    """, (
        data["stage_name"],
        data["stage_date"],
        data.get("interviewer_name"),
        data.get("format"),
        data.get("status"),
        data.get("notes"),
        stage_id,
    ))
    conn.commit()
    conn.close()


def delete_interview_stage(stage_id):
    conn = get_connection()
    conn.execute("DELETE FROM interview_stages WHERE id = ?", (stage_id,))
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Analytics queries
# ---------------------------------------------------------------------------

def get_applications_over_time():
    conn = get_connection()
    rows = conn.execute("""
        SELECT strftime('%Y-%W', date_added) AS week, COUNT(*) AS count
        FROM jobs
        GROUP BY week
        ORDER BY week
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_status_counts():
    conn = get_connection()
    rows = conn.execute("""
        SELECT status, COUNT(*) AS count
        FROM jobs
        GROUP BY status
        ORDER BY count DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_source_stats():
    conn = get_connection()
    rows = conn.execute("""
        SELECT
            source,
            COUNT(*) AS total,
            SUM(CASE WHEN status IN (
                'Interview','Technical Assessment','Final Round','Offer'
            ) THEN 1 ELSE 0 END) AS interviews,
            SUM(CASE WHEN status = 'Offer' THEN 1 ELSE 0 END) AS offers
        FROM jobs
        GROUP BY source
        ORDER BY total DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_upcoming_items(days=7):
    conn = get_connection()
    today = date.today().isoformat()
    end = (date.today() + timedelta(days=days)).isoformat()

    follow_ups = conn.execute("""
        SELECT f.*, j.company_name, j.role_title
        FROM follow_ups f
        JOIN jobs j ON f.job_id = j.id
        WHERE f.follow_up_date BETWEEN ? AND ? AND f.completed = 0
        ORDER BY f.follow_up_date ASC
    """, (today, end)).fetchall()

    interviews = conn.execute("""
        SELECT i.*, j.company_name, j.role_title
        FROM interview_stages i
        JOIN jobs j ON i.job_id = j.id
        WHERE i.stage_date BETWEEN ? AND ? AND i.status = 'Scheduled'
        ORDER BY i.stage_date ASC
    """, (today, end)).fetchall()

    conn.close()
    return [dict(r) for r in follow_ups], [dict(r) for r in interviews]


def get_avg_time_in_stages():
    conn = get_connection()
    rows = conn.execute("""
        SELECT
            status,
            AVG(julianday('now') - julianday(date_added)) AS avg_days,
            COUNT(*) AS count
        FROM jobs
        GROUP BY status
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# New Leads (triage inbox)
# ---------------------------------------------------------------------------

def get_new_leads(days=7):
    """Jobs added in last N days with status Researching and not snoozed (priority != Low)."""
    conn = get_connection()
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    rows = conn.execute("""
        SELECT * FROM jobs
        WHERE status = 'Researching'
          AND priority != 'Low'
          AND date_added >= ?
        ORDER BY date_added DESC
    """, (cutoff,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def count_new_leads(days=7):
    conn = get_connection()
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    row = conn.execute("""
        SELECT COUNT(*) FROM jobs
        WHERE status = 'Researching'
          AND priority != 'Low'
          AND date_added >= ?
    """, (cutoff,)).fetchone()
    conn.close()
    return row[0] if row else 0


def count_jobs_today():
    conn = get_connection()
    today = date.today().isoformat()
    row = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE date_added = ?", (today,)
    ).fetchone()
    conn.close()
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

def get_setting(key, default=None):
    conn = get_connection()
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row[0] if row else default


def set_setting(key, value):
    conn = get_connection()
    conn.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?)"
        " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, str(value)),
    )
    conn.commit()
    conn.close()


def get_response_times():
    """Avg days from date_applied to first_response_date, grouped by source."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT
            source,
            AVG(julianday(first_response_date) - julianday(date_applied)) AS avg_days,
            COUNT(*) AS count
        FROM jobs
        WHERE date_applied IS NOT NULL
          AND first_response_date IS NOT NULL
        GROUP BY source
        ORDER BY avg_days ASC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_weekly_applied_count():
    """Jobs with date_applied in the current calendar week (Mon-Sun)."""
    conn = get_connection()
    today = date.today()
    week_start = (today - timedelta(days=today.weekday())).isoformat()
    row = conn.execute("""
        SELECT COUNT(*) FROM jobs
        WHERE date_applied >= ?
          AND status NOT IN ('Researching', 'Ready to Apply', 'Withdrawn')
    """, (week_start,)).fetchone()
    conn.close()
    return row[0] if row else 0


def triage_job(job_id, status, priority=None):
    conn = get_connection()
    if priority is not None:
        conn.execute(
            "UPDATE jobs SET status = ?, priority = ? WHERE id = ?",
            (status, priority, job_id),
        )
    else:
        conn.execute(
            "UPDATE jobs SET status = ? WHERE id = ?",
            (status, job_id),
        )
    conn.commit()
    conn.close()
