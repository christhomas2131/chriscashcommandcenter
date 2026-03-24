"""Shared UI helpers: badges, formatters, and reusable form sections."""
import streamlit as st
from datetime import date, timedelta, datetime

import db.repository as db

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STATUS_COLORS = {
    "Researching": "#6B7280",
    "Ready to Apply": "#3B82F6",
    "Applied": "#60A5FA",
    "Phone Screen": "#F59E0B",
    "Interview": "#F97316",
    "Technical Assessment": "#FB923C",
    "Final Round": "#A855F7",
    "Offer": "#10B981",
    "Rejected": "#DC2626",
    "Withdrawn": "#9CA3AF",
    "Ghosted": "#4B5563",
}

PRIORITY_COLORS = {
    "High": "#EF4444",
    "Medium": "#F59E0B",
    "Low": "#6B7280",
}

ALL_STATUSES = list(STATUS_COLORS.keys())

ACTIVE_STATUSES = [
    "Researching", "Ready to Apply", "Applied",
    "Phone Screen", "Interview", "Technical Assessment", "Final Round",
]

WORK_TYPES = ["Remote", "Hybrid", "On-site"]
SOURCES = ["LinkedIn", "Indeed", "Company Site", "Referral", "Recruiter", "Conference/Networking", "Other"]
FOLLOW_UP_TYPES = ["Email", "Call", "LinkedIn Message", "Thank You Note", "Check-in"]
INTERVIEW_FORMATS = ["Phone", "Video", "In-Person", "Technical", "Panel", "Case Study"]
INTERVIEW_STATUSES = ["Scheduled", "Completed", "Cancelled", "No Show"]


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------

def status_badge(status: str) -> str:
    color = STATUS_COLORS.get(status, "#6B7280")
    return (
        f'<span style="display:inline-block;padding:2px 10px;border-radius:20px;'
        f'font-size:0.75rem;font-weight:600;color:white;background:{color};">'
        f'{status}</span>'
    )


def priority_badge(priority: str) -> str:
    color = PRIORITY_COLORS.get(priority, "#6B7280")
    return (
        f'<span style="display:inline-block;padding:2px 8px;border-radius:20px;'
        f'font-size:0.7rem;font-weight:600;color:white;background:{color};">'
        f'{priority}</span>'
    )


def format_salary(min_sal, max_sal) -> str:
    if min_sal and max_sal:
        return f"${int(min_sal):,} – ${int(max_sal):,}"
    if min_sal:
        return f"${int(min_sal):,}+"
    if max_sal:
        return f"Up to ${int(max_sal):,}"
    return "—"


def parse_date(val) -> date | None:
    if not val:
        return None
    if isinstance(val, date):
        return val
    try:
        return datetime.strptime(str(val), "%Y-%m-%d").date()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Reusable sub-sections
# ---------------------------------------------------------------------------

def contacts_section(job_id: int):
    st.markdown("#### Contacts")
    contacts = db.get_contacts(job_id)

    for c in contacts:
        with st.expander(f"{c['contact_name']}  —  {c.get('contact_title') or 'No title'}"):
            col1, col2 = st.columns([4, 1])
            with col1:
                if c.get("contact_email"):
                    st.write(f"Email: {c['contact_email']}")
                if c.get("contact_phone"):
                    st.write(f"Phone: {c['contact_phone']}")
                if c.get("contact_linkedin"):
                    st.write(f"LinkedIn: {c['contact_linkedin']}")
                if c.get("notes"):
                    st.caption(c["notes"])
            with col2:
                if st.button("Delete", key=f"del_c_{c['id']}"):
                    db.delete_contact(c["id"])
                    st.rerun()

    with st.form(f"add_contact_form_{job_id}", clear_on_submit=True):
        st.markdown("**Add Contact**")
        col1, col2 = st.columns(2)
        with col1:
            cn = st.text_input("Name *")
            ct = st.text_input("Title")
            ce = st.text_input("Email")
        with col2:
            cp = st.text_input("Phone")
            cl_url = st.text_input("LinkedIn URL")
            cn_notes = st.text_input("Notes")
        if st.form_submit_button("Add Contact", use_container_width=True):
            if cn.strip():
                db.add_contact({
                    "job_id": job_id, "contact_name": cn.strip(),
                    "contact_title": ct or None, "contact_email": ce or None,
                    "contact_phone": cp or None, "contact_linkedin": cl_url or None,
                    "notes": cn_notes or None,
                })
                st.rerun()
            else:
                st.error("Name is required.")


def follow_ups_section(job_id: int):
    st.markdown("#### Follow-Ups")
    today = date.today()
    follow_ups = db.get_follow_ups(job_id=job_id)

    for fu in follow_ups:
        fu_date = parse_date(fu["follow_up_date"])
        is_done = bool(fu["completed"])
        is_overdue = fu_date and fu_date < today and not is_done
        icon = "✅" if is_done else ("🔴" if is_overdue else "🔵")
        date_str = fu_date.strftime("%b %d, %Y") if fu_date else "—"

        with st.expander(f"{icon} {fu['follow_up_type']}  —  {date_str}"):
            col1, col2 = st.columns([4, 1])
            with col1:
                if fu.get("notes"):
                    st.caption(fu["notes"])
                if is_overdue:
                    st.markdown(
                        '<span style="color:#EF4444;font-size:0.75rem;">Overdue</span>',
                        unsafe_allow_html=True,
                    )
            with col2:
                btn_label = "Undo" if is_done else "Done ✓"
                if st.button(btn_label, key=f"tog_fu_{fu['id']}"):
                    db.toggle_follow_up(fu["id"])
                    st.rerun()
                if st.button("Delete", key=f"del_fu_{fu['id']}"):
                    db.delete_follow_up(fu["id"])
                    st.rerun()

    with st.form(f"add_fu_form_{job_id}", clear_on_submit=True):
        st.markdown("**Add Follow-Up**")
        col1, col2 = st.columns(2)
        with col1:
            fu_date_in = st.date_input("Date", value=today + timedelta(days=3), key=f"fu_date_{job_id}")
            fu_type = st.selectbox("Type", FOLLOW_UP_TYPES, key=f"fu_type_{job_id}")
        with col2:
            fu_notes = st.text_area("Notes", height=80, key=f"fu_notes_{job_id}")
        if st.form_submit_button("Add Follow-Up", use_container_width=True):
            db.add_follow_up({
                "job_id": job_id,
                "follow_up_date": fu_date_in.isoformat(),
                "follow_up_type": fu_type,
                "notes": fu_notes or None,
            })
            st.rerun()


def interview_stages_section(job_id: int):
    st.markdown("#### Interview Stages")
    stages = db.get_interview_stages(job_id)

    for s in stages:
        s_date = parse_date(s["stage_date"])
        status_icon = {"Completed": "✅", "Scheduled": "🗓️", "Cancelled": "❌", "No Show": "⚠️"}.get(
            s["status"], "🗓️"
        )
        date_str = s_date.strftime("%b %d, %Y") if s_date else "—"

        with st.expander(f"{status_icon} {s['stage_name']}  —  {date_str}"):
            col1, col2 = st.columns([4, 1])
            with col1:
                st.write(f"**Format:** {s.get('format', '—')}  |  **Status:** {s['status']}")
                if s.get("interviewer_name"):
                    st.write(f"**Interviewer:** {s['interviewer_name']}")
                if s.get("notes"):
                    st.caption(s["notes"])
            with col2:
                if st.button("Delete", key=f"del_stage_{s['id']}"):
                    db.delete_interview_stage(s["id"])
                    st.rerun()

    with st.form(f"add_stage_form_{job_id}", clear_on_submit=True):
        st.markdown("**Add Interview Stage**")
        col1, col2 = st.columns(2)
        with col1:
            sn = st.text_input("Stage Name *", key=f"sn_{job_id}")
            sd = st.date_input("Date", value=date.today() + timedelta(days=7), key=f"sd_{job_id}")
            sf = st.selectbox("Format", INTERVIEW_FORMATS, key=f"sf_{job_id}")
        with col2:
            ss = st.selectbox("Status", INTERVIEW_STATUSES, key=f"ss_{job_id}")
            si = st.text_input("Interviewer Name", key=f"si_{job_id}")
            sno = st.text_area("Notes", height=80, key=f"sno_{job_id}")
        if st.form_submit_button("Add Stage", use_container_width=True):
            if sn.strip():
                db.add_interview_stage({
                    "job_id": job_id, "stage_name": sn.strip(),
                    "stage_date": sd.isoformat(), "format": sf,
                    "status": ss, "interviewer_name": si or None,
                    "notes": sno or None,
                })
                st.rerun()
            else:
                st.error("Stage name is required.")
