import streamlit as st
from datetime import date, timedelta, datetime
import pandas as pd
import os
import glob

import db.repository as db
import charts as ch
import seed_data
import config
from components import (
    STATUS_COLORS, PRIORITY_COLORS, ALL_STATUSES, ACTIVE_STATUSES,
    WORK_TYPES, SOURCES, FOLLOW_UP_TYPES,
    status_badge, priority_badge, format_salary, parse_date,
    contacts_section, follow_ups_section, interview_stages_section,
)

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Job Tracker",
    page_icon="briefcase",
    layout="wide",
    initial_sidebar_state="collapsed",
)

db.init_db()
if config.SEED_ON_STARTUP:
    seed_data.seed()


# ---------------------------------------------------------------------------
# Global CSS
# ---------------------------------------------------------------------------

st.markdown("""
<style>
/* ---- Layout ---- */
.stApp { background-color: #111827; }
section[data-testid="stSidebar"] {
    background-color: #1F2937 !important;
    border-right: 1px solid #374151;
}
.block-container { padding-top: 1.5rem; }

/* ---- Typography ---- */
h1, h2, h3, h4, h5, h6 { color: #F9FAFB !important; }
p, li, label, .stMarkdown { color: #D1D5DB; }

/* ---- Metric cards ---- */
.metric-card {
    background: #1F2937;
    border: 1px solid #374151;
    border-radius: 12px;
    padding: 22px 16px;
    text-align: center;
}
.metric-value { font-size: 2.4rem; font-weight: 700; color: #F9FAFB; line-height: 1; }
.metric-label { font-size: 0.8rem; color: #9CA3AF; margin-top: 6px; letter-spacing: 0.03em; }

/* ---- Job cards ---- */
.job-card {
    background: #1F2937;
    border: 1px solid #374151;
    border-radius: 8px;
    padding: 12px 14px;
    margin-bottom: 8px;
}
.job-card-title { font-weight: 600; color: #F9FAFB; font-size: 0.875rem; }
.job-card-meta  { color: #9CA3AF; font-size: 0.775rem; margin-top: 2px; }

/* ---- Timeline ---- */
.timeline-wrap  { border-left: 2px solid #374151; margin-left: 8px; padding-left: 16px; }
.timeline-item  { position: relative; padding-bottom: 14px; }
.timeline-dot   {
    position: absolute; left: -22px; top: 3px;
    width: 10px; height: 10px; border-radius: 50%;
}
.timeline-title { color: #F9FAFB; font-size: 0.875rem; }
.timeline-date  { color: #6B7280; font-size: 0.75rem; }

/* ---- Top nav radio ---- */
div[data-testid="stHorizontalBlock"] div[data-testid="stRadio"] label {
    color: #D1D5DB !important;
}

/* ---- Main action buttons ---- */
.main-action .stButton button {
    background: #3B82F6 !important;
    color: white !important;
    border: none !important;
    border-radius: 6px !important;
}

/* ---- Inputs ---- */
.stTextInput input, .stTextArea textarea, .stNumberInput input {
    background-color: #1F2937 !important;
    color: #F9FAFB !important;
    border-color: #374151 !important;
}
.stSelectbox > div > div {
    background-color: #1F2937 !important;
    color: #F9FAFB !important;
    border-color: #374151 !important;
}

/* ---- Expanders ---- */
.streamlit-expanderHeader {
    background-color: #1F2937 !important;
    border-radius: 6px !important;
    color: #D1D5DB !important;
}

/* ---- Dataframe ---- */
.stDataFrame { border: 1px solid #374151; border-radius: 8px; }

/* ---- Kanban column header ---- */
.kanban-header {
    padding: 6px 0 10px 0;
    margin-bottom: 6px;
    border-top: 3px solid;
}

/* ---- Info / warning tweaks ---- */
.stAlert { border-radius: 8px !important; }

/* ---- Hide Streamlit chrome ---- */
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Navigation
# ---------------------------------------------------------------------------

NAV_KEYS = ["Overview", "Add / Edit Job", "Job Detail", "Follow-Up Tracker", "Analytics", "New Leads", "Import Job", "Ingestion"]


def _fetch_job_url(url):
    """Universal job importer. Handles Workable, LinkedIn, Greenhouse, Lever, and generic sites.
    Returns (data_dict, error_string). One of the two will be None."""
    import urllib.request
    import json
    import re
    import html as _html

    def _get(u, extra_headers=None):
        h = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
        if extra_headers:
            h.update(extra_headers)
        req = urllib.request.Request(u.strip(), headers=h)
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.read().decode("utf-8", errors="ignore")

    def _strip(s):
        s = re.sub(r"<[^>]+>", " ", str(s))
        return re.sub(r"\s+", " ", s).strip()

    url = url.strip()
    result = {"job_url": url, "source": "Company Site"}

    # ------------------------------------------------------------------ #
    # Workable: call v2 API directly                                       #
    # ------------------------------------------------------------------ #
    wm = re.search(r"apply\.workable\.com/([^/]+)/j/([^/]+)", url)
    if wm:
        slug, shortcode = wm.group(1), wm.group(2)
        try:
            api = json.loads(_get(
                f"https://apply.workable.com/api/v2/accounts/{slug}/jobs/{shortcode}",
                extra_headers={"Accept": "application/json"},
            ))
        except Exception as e:
            return None, f"Workable API error: {e}"

        result["role_title"] = _html.unescape(api.get("title", ""))
        loc = api.get("location") or {}
        result["location"] = ", ".join(p for p in [loc.get("city"), loc.get("region")] if p)
        wp = str(api.get("workplace", "")).lower()
        result["work_type"] = "Remote" if api.get("remote") else ("Hybrid" if "hybrid" in wp else "On-site")
        desc = _strip((api.get("description") or "") + " " + (api.get("requirements") or ""))
        result["notes"] = desc[:800]

        # Company name lives in the HTML title: "Job Title - Company Name"
        try:
            page = _get(url)
            tm = re.search(r"<title[^>]*>(.*?)</title>", page, re.DOTALL)
            if tm:
                raw = _html.unescape(re.sub(r"\s+", " ", tm.group(1)).strip())
                if " - " in raw:
                    result["company_name"] = raw.split(" - ", 1)[1].strip()
        except Exception:
            pass

        return (result, None) if result.get("role_title") else (None, "Could not extract job title from Workable.")

    # ------------------------------------------------------------------ #
    # All other sites: HTML scraping with fallback chain                   #
    # ------------------------------------------------------------------ #
    if "linkedin.com" in url:
        result["source"] = "LinkedIn"
    elif "indeed.com" in url:
        result["source"] = "Indeed"

    try:
        page = _get(url)
    except Exception as e:
        return None, f"Could not reach URL: {e}"

    # Strategy 1: JSON-LD (LinkedIn, Greenhouse, Lever, most modern job boards)
    ld_m = re.search(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', page, re.DOTALL)
    if ld_m:
        try:
            ld = json.loads(ld_m.group(1))
            if isinstance(ld, list): ld = ld[0]
            if ld.get("@type") == "JobPosting":
                result["role_title"] = _html.unescape(ld.get("title", "").strip())
                org = ld.get("hiringOrganization", {})
                if isinstance(org, dict):
                    result["company_name"] = org.get("name", "").strip()
                job_loc = ld.get("jobLocation")
                if isinstance(job_loc, list): job_loc = job_loc[0] if job_loc else {}
                if isinstance(job_loc, dict):
                    addr = job_loc.get("address", {})
                    result["location"] = ", ".join(p for p in [addr.get("addressLocality"), addr.get("addressRegion")] if p)
                loc_type = str(ld.get("jobLocationType", "")).upper()
                if "TELECOMMUTE" in loc_type or ld.get("applicantLocationRequirements"):
                    result["work_type"] = "Remote"
                elif "hybrid" in result.get("role_title", "").lower():
                    result["work_type"] = "Hybrid"
                else:
                    result["work_type"] = "On-site"
                sal = ld.get("baseSalary", {})
                if isinstance(sal, dict):
                    val = sal.get("value", {})
                    if isinstance(val, dict):
                        result["salary_min"] = val.get("minValue") or val.get("value")
                        result["salary_max"] = val.get("maxValue") or val.get("value")
                result["notes"] = _strip(ld.get("description", ""))[:800]
        except Exception:
            pass

    # Strategy 2: __NEXT_DATA__ blob (Greenhouse Next.js, etc.)
    if not result.get("role_title"):
        nd_m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', page, re.DOTALL)
        if nd_m:
            try:
                pp = json.loads(nd_m.group(1)).get("props", {}).get("pageProps", {})
                job = pp.get("job") or pp.get("jobPost") or {}
                if job:
                    result["role_title"] = _html.unescape(job.get("title", ""))
                    co = pp.get("company") or pp.get("account") or {}
                    if isinstance(co, dict): result["company_name"] = co.get("name", "")
                    loc = job.get("location") or {}
                    if isinstance(loc, dict):
                        result["location"] = ", ".join(p for p in [loc.get("city"), loc.get("region")] if p)
                    if job.get("remote") or job.get("telecommuting"):
                        result["work_type"] = "Remote"
                    result["notes"] = _strip(job.get("description") or job.get("content") or "")[:800]
            except Exception:
                pass

    # Strategy 3: Open Graph meta tags
    if not result.get("role_title"):
        og_t = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\'](.*?)["\']', page)
        og_d = re.search(r'<meta[^>]+(?:property=["\']og:description["\']|name=["\']description["\'])[^>]+content=["\'](.*?)["\']', page)
        if og_t:
            raw = _html.unescape(og_t.group(1).strip())
            parts = [p.strip() for p in raw.split("|")]
            result["role_title"] = parts[0]
            if len(parts) > 1 and not result.get("company_name"):
                result["company_name"] = parts[1]
        if og_d and not result.get("notes"):
            result["notes"] = _html.unescape(og_d.group(1))[:800]

    # Strategy 4: HTML <title> tag last resort
    if not result.get("role_title"):
        tm = re.search(r"<title[^>]*>(.*?)</title>", page, re.DOTALL)
        if tm:
            raw = _html.unescape(re.sub(r"\s+", " ", tm.group(1)).strip())
            for sep in (" - ", " | "):
                if sep in raw:
                    parts = raw.split(sep, 1)
                    result["role_title"] = parts[0].strip()
                    if not result.get("company_name"):
                        result["company_name"] = parts[1].strip()
                    break
            else:
                result["role_title"] = raw

    if not result.get("role_title") and not result.get("company_name"):
        return None, "Could not extract job details. The page may require a login or use heavy JavaScript rendering."

    if result.get("salary_min"): result["salary_min"] = int(float(result["salary_min"]))
    if result.get("salary_max"): result["salary_max"] = int(float(result["salary_max"]))

    return result, None


def get_last_scraper_run():
    """Return datetime of the most recent ingestion run (from DB), or None."""
    try:
        run = db.get_last_ingestion_run()
        if run and run.get("started_at"):
            ts = run["started_at"]
            if isinstance(ts, datetime):
                return ts
            return datetime.fromisoformat(str(ts))
    except Exception:
        pass
    # Fallback: check legacy log files
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "job-scraper", "scraper_logs")
    files = sorted(glob.glob(os.path.join(log_dir, "*.json")))
    if not files:
        return None
    basename = os.path.basename(files[-1]).replace(".json", "")
    try:
        return datetime.strptime(basename, "%Y-%m-%d_%H-%M")
    except ValueError:
        return None


def render_nav(col) -> str:
    """Render vertical nav into the given column. Returns selected page key."""
    if "page" not in st.session_state:
        st.session_state.page = "Overview"

    lead_count = db.count_new_leads()
    badge = f"  ({lead_count})" if lead_count else ""
    today_imports = db.count_jobs_today()
    import_badge = f"  ({today_imports})" if today_imports else ""
    nav_pages_display = [
        "🏠  Overview",
        "➕  Add / Edit Job",
        "📋  Job Detail",
        "📅  Follow-Up Tracker",
        "📊  Analytics",
        f"🆕  New Leads{badge}",
        f"📥  Import Job{import_badge}",
        "⚙️  Ingestion",
    ]

    with col:
        st.markdown(
            '<div style="font-size:1.1rem;font-weight:700;color:#F9FAFB;padding:4px 0 16px 0;">'
            '💼 Job Tracker</div>',
            unsafe_allow_html=True,
        )
        current_idx = NAV_KEYS.index(st.session_state.page) if st.session_state.page in NAV_KEYS else 0
        selected = st.radio("nav", nav_pages_display, index=current_idx, label_visibility="collapsed")
        page = NAV_KEYS[nav_pages_display.index(selected)]

        st.markdown("---")
        st.caption(date.today().strftime("%B %d, %Y"))

        pending = db.get_follow_ups(completed=False)
        overdue = [f for f in pending if parse_date(f["follow_up_date"]) and parse_date(f["follow_up_date"]) < date.today()]
        if overdue:
            st.markdown(
                f'<div style="background:#7F1D1D;border-radius:6px;padding:8px 10px;'
                f'font-size:0.8rem;color:#FCA5A5;margin-top:8px;">'
                f'⚠️ {len(overdue)} overdue follow-up{"s" if len(overdue) != 1 else ""}</div>',
                unsafe_allow_html=True,
            )

    if page != st.session_state.page:
        st.session_state.page = page
        if page not in ("Job Detail", "Add / Edit Job"):
            st.session_state.pop("selected_job_id", None)
            st.session_state.pop("edit_job_id", None)
        st.rerun()

    return page


# ---------------------------------------------------------------------------
# Page: Overview
# ---------------------------------------------------------------------------

def page_overview():
    st.markdown("# Overview")

    jobs = db.get_all_jobs()
    today = date.today()

    SUBMITTED_STATUSES = {"Applied", "Phone Screen", "Interview", "Technical Assessment",
                          "Final Round", "Offer", "Rejected", "Ghosted"}
    submitted_jobs = [j for j in jobs if j["status"] in SUBMITTED_STATUSES]
    upcoming_fu, upcoming_interviews = db.get_upcoming_items(7)
    offers = sum(1 for j in jobs if j["status"] == "Offer")
    applied = len(submitted_jobs)
    offer_rate = f"{offers/applied*100:.0f}%" if applied else "0%"

    # --- Stats row ---
    c1, c2, c3, c4 = st.columns(4)
    for col, val, label in [
        (c1, len(submitted_jobs), "Applications Submitted"),
        (c2, len(upcoming_interviews), "Interviews This Week"),
        (c3, len(upcoming_fu), "Pending Follow-Ups"),
        (c4, offer_rate, "Offer Rate"),
    ]:
        col.markdown(
            f'<div class="metric-card">'
            f'<div class="metric-value">{val}</div>'
            f'<div class="metric-label">{label}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Weekly goal tracker ---
    weekly_goal = int(db.get_setting("weekly_goal", "5"))
    weekly_done = db.get_weekly_applied_count()
    pct = min(weekly_done / weekly_goal, 1.0) if weekly_goal else 0
    bar_filled = int(pct * 20)
    bar_html = (
        f'<div style="background:#1F2937;border:1px solid #374151;border-radius:8px;'
        f'padding:12px 16px;margin-bottom:4px;display:flex;align-items:center;gap:16px;">'
        f'<span style="color:#9CA3AF;font-size:0.75rem;font-weight:600;white-space:nowrap;">WEEKLY GOAL</span>'
        f'<div style="flex:1;background:#374151;border-radius:4px;height:8px;">'
        f'<div style="width:{pct*100:.0f}%;background:{"#10B981" if pct >= 1 else "#3B82F6"};'
        f'border-radius:4px;height:8px;transition:width 0.3s;"></div></div>'
        f'<span style="color:#F9FAFB;font-size:0.85rem;font-weight:600;white-space:nowrap;">'
        f'{weekly_done} / {weekly_goal} this week</span>'
        f'</div>'
    )
    gc1, gc2 = st.columns([5, 1])
    with gc1:
        st.markdown(bar_html, unsafe_allow_html=True)
    with gc2:
        new_goal = st.number_input(
            "Weekly target", min_value=1, max_value=99, value=weekly_goal,
            label_visibility="collapsed", key="weekly_goal_input",
        )
        if new_goal != weekly_goal:
            db.set_setting("weekly_goal", str(new_goal))
            st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Charts row ---
    col_l, col_r = st.columns([3, 2])
    with col_l:
        st.markdown("### Applications Over Time")
        st.plotly_chart(ch.applications_over_time_chart(db.get_applications_over_time()), use_container_width=True, key="ov_apps_over_time")
    with col_r:
        st.markdown("### Status Breakdown")
        st.plotly_chart(ch.status_breakdown_chart(db.get_status_counts()), use_container_width=True, key="ov_status_breakdown")

    st.markdown("---")

    # --- Kanban ---
    st.markdown("### Pipeline")
    kanban_order = ["Researching", "Ready to Apply", "Applied", "Phone Screen",
                    "Interview", "Technical Assessment", "Final Round", "Offer"]
    by_status = {s: [] for s in kanban_order}
    for j in jobs:
        if j["status"] in by_status:
            by_status[j["status"]].append(j)

    visible = [(s, by_status[s]) for s in kanban_order if by_status[s]]
    if visible:
        cols = st.columns(len(visible))
        for col, (status, sjobs) in zip(cols, visible):
            color = STATUS_COLORS.get(status, "#6B7280")
            with col:
                st.markdown(
                    f'<div style="border-top:3px solid {color};padding-top:8px;margin-bottom:10px;">'
                    f'<span style="color:{color};font-size:0.75rem;font-weight:700;">{status.upper()}</span>'
                    f'<span style="color:#4B5563;font-size:0.75rem;"> ({len(sjobs)})</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
                for j in sjobs:
                    pc = PRIORITY_COLORS.get(j["priority"], "#6B7280")
                    st.markdown(
                        f'<div class="job-card">'
                        f'<div class="job-card-title">{j["role_title"]}</div>'
                        f'<div class="job-card-meta">{j["company_name"]}</div>'
                        f'<div class="job-card-meta" style="margin-top:4px;">'
                        f'{j.get("work_type","")}'
                        f'  <span style="color:{pc};">• {j.get("priority","")} Pri</span>'
                        f'</div></div>',
                        unsafe_allow_html=True,
                    )
                    btn_col1, btn_col2 = st.columns(2)
                    with btn_col1:
                        if st.button("View →", key=f"ov_view_{j['id']}", use_container_width=True):
                            st.session_state.selected_job_id = j["id"]
                            st.session_state.page = "Job Detail"
                            st.rerun()
                    with btn_col2:
                        if st.button("Delete", key=f"ov_del_{j['id']}", use_container_width=True):
                            st.session_state[f"ov_confirm_{j['id']}"] = True
                    if st.session_state.get(f"ov_confirm_{j['id']}"):
                        st.warning(f"Delete {j['company_name']}?")
                        yes, no = st.columns(2)
                        with yes:
                            if st.button("Yes", key=f"ov_yes_{j['id']}", use_container_width=True):
                                db.delete_job(j["id"])
                                st.session_state.pop(f"ov_confirm_{j['id']}", None)
                                st.rerun()
                        with no:
                            if st.button("No", key=f"ov_no_{j['id']}", use_container_width=True):
                                st.session_state.pop(f"ov_confirm_{j['id']}", None)
                                st.rerun()
    else:
        st.info("No active applications yet. Click **Add / Edit Job** to get started.")

    st.markdown("---")

    # --- Upcoming ---
    st.markdown("### Next 7 Days")
    col_fu, col_int = st.columns(2)

    with col_fu:
        st.markdown("**Follow-Ups**")
        if upcoming_fu:
            for fu in upcoming_fu:
                fu_date = parse_date(fu["follow_up_date"])
                days = (fu_date - today).days if fu_date else 0
                color = "#EF4444" if days < 0 else "#3B82F6"
                day_str = f"Today" if days == 0 else (f"in {days}d" if days > 0 else f"{-days}d overdue")
                st.markdown(
                    f'<div class="job-card" style="border-left:3px solid {color};">'
                    f'<div class="job-card-title">{fu["company_name"]} · {fu["follow_up_type"]}</div>'
                    f'<div class="job-card-meta">{fu["role_title"]} &nbsp;·&nbsp; '
                    f'{fu_date.strftime("%b %d") if fu_date else "—"} ({day_str})</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.caption("No upcoming follow-ups.")

    with col_int:
        st.markdown("**Interviews**")
        if upcoming_interviews:
            for iv in upcoming_interviews:
                iv_date = parse_date(iv["stage_date"])
                days = (iv_date - today).days if iv_date else 0
                day_str = f"Today" if days == 0 else f"in {days}d"
                st.markdown(
                    f'<div class="job-card" style="border-left:3px solid #A855F7;">'
                    f'<div class="job-card-title">{iv["company_name"]} · {iv["stage_name"]}</div>'
                    f'<div class="job-card-meta">{iv["role_title"]} &nbsp;·&nbsp; '
                    f'{iv_date.strftime("%b %d") if iv_date else "—"} ({day_str}) · {iv.get("format","")}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.caption("No upcoming interviews.")


# ---------------------------------------------------------------------------
# Page: Add / Edit Job
# ---------------------------------------------------------------------------

def _job_form(job=None, job_id=None, prefill=None):
    """Render add/edit form. Returns new job_id on add, 'updated' on edit, or None."""
    is_edit = job is not None
    pf = prefill or {}

    def _idx(lst, val, default=0):
        try:
            return lst.index(val)
        except ValueError:
            return default

    with st.form("job_form_main"):
        c1, c2 = st.columns(2)
        with c1:
            company_name = st.text_input("Company Name *", value=job["company_name"] if is_edit else pf.get("company_name", ""))
            role_title   = st.text_input("Role Title *",   value=job["role_title"]   if is_edit else pf.get("role_title", ""))
            status       = st.selectbox("Status", ALL_STATUSES, index=_idx(ALL_STATUSES, job["status"] if is_edit else "Researching"))
            priority     = st.selectbox("Priority", ["High","Medium","Low"], index=_idx(["High","Medium","Low"], job.get("priority","Medium") if is_edit else "Medium"))
            location     = st.text_input("Location", value=job.get("location","") if is_edit else pf.get("location", ""))
            work_type    = st.selectbox("Work Type", WORK_TYPES, index=_idx(WORK_TYPES, job.get("work_type","Remote") if is_edit else pf.get("work_type", "Remote")))

        with c2:
            source = st.selectbox("Source", SOURCES, index=_idx(SOURCES, job.get("source","Other") if is_edit else pf.get("source", "Other")))

            applied_val = parse_date(job.get("date_applied")) if is_edit else None
            date_applied = st.date_input(
                "Date Applied (leave at today if not yet applied)",
                value=applied_val,
                format="YYYY-MM-DD",
            )
            applied_checked = st.checkbox(
                "Mark date applied",
                value=bool(applied_val),
                help="Check to record the date applied above.",
            )

            response_val = parse_date(job.get("first_response_date")) if is_edit else None
            first_response_date_in = st.date_input(
                "First Response Date",
                value=response_val,
                format="YYYY-MM-DD",
            )
            response_checked = st.checkbox(
                "Mark first response date",
                value=bool(response_val),
                help="Date you got the first reply — interview invite, rejection, or any response.",
            )

            sc1, sc2 = st.columns(2)
            with sc1:
                salary_min = st.number_input("Salary Min ($)", min_value=0, step=5000,
                    value=int(job["salary_min"]) if is_edit and job.get("salary_min") else (pf.get("salary_min") or 0))
            with sc2:
                salary_max = st.number_input("Salary Max ($)", min_value=0, step=5000,
                    value=int(job["salary_max"]) if is_edit and job.get("salary_max") else (pf.get("salary_max") or 0))

            job_url = st.text_input("Job URL", value=job.get("job_url","") if is_edit else pf.get("job_url", ""))

        notes = st.text_area("Notes", value=job.get("notes","") if is_edit else pf.get("notes", ""), height=100)

        submitted = st.form_submit_button(
            "Save Changes" if is_edit else "Add Job",
            use_container_width=True,
        )

    if submitted:
        if not company_name.strip() or not role_title.strip():
            st.error("Company name and role title are required.")
            return None

        data = {
            "company_name": company_name.strip(),
            "role_title":   role_title.strip(),
            "status":       status,
            "priority":     priority,
            "location":     location.strip() or None,
            "work_type":    work_type,
            "source":       source,
            "date_applied":         date_applied.isoformat() if applied_checked and date_applied else None,
            "first_response_date":  first_response_date_in.isoformat() if response_checked and first_response_date_in else None,
            "salary_min":   salary_min if salary_min > 0 else None,
            "salary_max":   salary_max if salary_max > 0 else None,
            "job_url":      job_url.strip() or None,
            "notes":        notes.strip() or None,
        }

        if is_edit:
            db.update_job(job_id, data)
            st.success("Job updated!")
            return "updated"
        else:
            new_id = db.add_job(data)
            st.success(f"Job added!")
            return new_id

    return None


def page_add_edit_job():
    edit_id = st.session_state.get("edit_job_id")

    if edit_id:
        job = db.get_job(edit_id)
        if not job:
            st.error("Job not found.")
            return
        st.markdown(f"# Edit: {job['company_name']} — {job['role_title']}")
        result = _job_form(job=job, job_id=edit_id)
        if result == "updated":
            st.session_state.pop("edit_job_id", None)
            st.session_state.selected_job_id = edit_id
            st.session_state.page = "Job Detail"
            st.rerun()

        st.markdown("---")
        tab1, tab2, tab3 = st.tabs(["Contacts", "Follow-Ups", "Interview Stages"])
        with tab1: contacts_section(edit_id)
        with tab2: follow_ups_section(edit_id)
        with tab3: interview_stages_section(edit_id)

    else:
        st.markdown("# Add New Job")

        with st.expander("Import from job URL", expanded=bool(st.session_state.get("li_prefill"))):
            li_url = st.text_input("Paste job URL", placeholder="LinkedIn, Workable, Greenhouse, Lever, Indeed, or any job board…")
            if st.button("Fetch Job Details", use_container_width=True):
                if li_url.strip():
                    with st.spinner("Fetching…"):
                        data, err = _fetch_job_url(li_url)
                    if err:
                        st.error(err)
                    else:
                        st.session_state["li_prefill"] = data
                        st.success(f"Fetched: {data['role_title']} at {data['company_name']}")
                        st.rerun()
                else:
                    st.warning("Enter a URL first.")

        result = _job_form(prefill=st.session_state.get("li_prefill"))
        if result and result != "updated":
            st.session_state.pop("li_prefill", None)
            st.session_state.selected_job_id = result
            st.session_state.page = "Job Detail"
            st.rerun()


# ---------------------------------------------------------------------------
# Page: Job Detail
# ---------------------------------------------------------------------------

def page_job_detail():
    jobs = db.get_all_jobs()
    if not jobs:
        st.warning("No jobs yet. Add one first.")
        return

    job_options = {f"{j['company_name']}  —  {j['role_title']}": j["id"] for j in jobs}
    labels = list(job_options.keys())

    default_idx = 0
    sel_id = st.session_state.get("selected_job_id")
    if sel_id:
        for i, (lbl, jid) in enumerate(job_options.items()):
            if jid == sel_id:
                default_idx = i
                break

    selected_label = st.selectbox("Select Job", labels, index=default_idx)
    job_id = job_options[selected_label]
    job = db.get_job(job_id)

    if not job:
        st.error("Job not found.")
        return

    # --- Header ---
    hc1, hc2, hc3 = st.columns([5, 1, 1])
    with hc1:
        st.markdown(f"# {job['role_title']}")
        st.markdown(
            f"**{job['company_name']}** &nbsp;·&nbsp; {job.get('location') or '—'} &nbsp;·&nbsp; {job.get('work_type','')}"
        )
        st.markdown(
            status_badge(job["status"]) + "&nbsp;&nbsp;" + priority_badge(job.get("priority","Medium")),
            unsafe_allow_html=True,
        )
    with hc2:
        if st.button("Edit", use_container_width=True):
            st.session_state.edit_job_id = job_id
            st.session_state.page = "Add / Edit Job"
            st.rerun()
    with hc3:
        if st.button("Delete", use_container_width=True):
            st.session_state["confirm_delete"] = job_id

    if st.session_state.get("confirm_delete") == job_id:
        st.warning("Delete this job and all related data?")
        yes_col, no_col = st.columns(2)
        with yes_col:
            if st.button("Yes, delete"):
                db.delete_job(job_id)
                st.session_state.pop("confirm_delete", None)
                st.session_state.pop("selected_job_id", None)
                st.session_state.page = "Overview"
                st.rerun()
        with no_col:
            if st.button("Cancel"):
                st.session_state.pop("confirm_delete", None)
                st.rerun()

    st.markdown("---")

    # --- Quick status ---
    st.markdown("**Quick Status Update**")
    new_status = st.selectbox(
        "Status", ALL_STATUSES,
        index=ALL_STATUSES.index(job["status"]),
        key="quick_status_sel",
        label_visibility="collapsed",
    )
    if new_status != job["status"]:
        db.update_job_status(job_id, new_status)
        st.toast(f"Status updated → {new_status}", icon="✅")
        st.rerun()

    st.markdown("---")

    # --- Details + Timeline ---
    dc1, dc2 = st.columns(2)

    with dc1:
        st.markdown("### Details")
        rows = [
            ("Date Added",   job.get("date_added") or "—"),
            ("Date Applied", job.get("date_applied") or "—"),
            ("Salary Range", format_salary(job.get("salary_min"), job.get("salary_max"))),
            ("Source",       job.get("source") or "—"),
            ("Work Type",    job.get("work_type") or "—"),
        ]
        for k, v in rows:
            st.markdown(f"**{k}:** {v}")

        if job.get("job_url"):
            st.markdown(f"**Job URL:** {job['job_url']}")

        if job.get("notes"):
            st.markdown("**Notes:**")
            st.info(job["notes"])

    with dc2:
        st.markdown("### Timeline")
        stages = db.get_interview_stages(job_id)
        events = []
        if job.get("date_added"):
            events.append((job["date_added"], "Added to tracker", "#6B7280"))
        if job.get("date_applied"):
            events.append((job["date_applied"], "Applied", "#3B82F6"))
        for s in stages:
            c = {"Completed": "#10B981", "Scheduled": "#A855F7",
                 "Cancelled": "#EF4444", "No Show": "#DC2626"}.get(s["status"], "#6B7280")
            events.append((s["stage_date"], f"{s['stage_name']} ({s['status']})", c))

        events.sort(key=lambda x: x[0])

        if events:
            st.markdown('<div class="timeline-wrap">', unsafe_allow_html=True)
            for ev_date, ev_label, ev_color in events:
                d = parse_date(ev_date)
                formatted = d.strftime("%b %d, %Y") if d else ev_date
                st.markdown(
                    f'<div class="timeline-item">'
                    f'<div class="timeline-dot" style="background:{ev_color};"></div>'
                    f'<div><div class="timeline-title">{ev_label}</div>'
                    f'<div class="timeline-date">{formatted}</div></div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.caption("No timeline events yet.")

    st.markdown("---")

    # --- Related records ---
    tab1, tab2, tab3 = st.tabs(["Contacts", "Follow-Ups", "Interview Stages"])
    with tab1: contacts_section(job_id)
    with tab2: follow_ups_section(job_id)
    with tab3: interview_stages_section(job_id)


# ---------------------------------------------------------------------------
# Page: Follow-Up Tracker
# ---------------------------------------------------------------------------

def page_follow_up_tracker():
    st.markdown("# Follow-Up Tracker")

    jobs = db.get_all_jobs()
    today = date.today()

    # Filters
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        job_options = {"All Jobs": None}
        job_options.update({f"{j['company_name']} — {j['role_title']}": j["id"] for j in jobs})
        job_filter = st.selectbox("Job", list(job_options.keys()))
    with fc2:
        type_filter = st.selectbox("Type", ["All Types"] + FOLLOW_UP_TYPES)
    with fc3:
        show_filter = st.selectbox("Show", ["Pending Only", "All", "Completed Only"])

    job_id_f = job_options[job_filter]
    completed_f = None if show_filter == "All" else (show_filter == "Completed Only")

    follow_ups = db.get_follow_ups(job_id=job_id_f, completed=completed_f)
    if type_filter != "All Types":
        follow_ups = [f for f in follow_ups if f["follow_up_type"] == type_filter]

    if not follow_ups:
        st.info("No follow-ups match your filters.")
        return

    overdue  = [f for f in follow_ups if not f["completed"] and parse_date(f["follow_up_date"]) and parse_date(f["follow_up_date"]) < today]
    the_rest = [f for f in follow_ups if f not in overdue]

    def _render_fu(fu, border_color, extra_label=""):
        fu_date = parse_date(fu["follow_up_date"])
        done = bool(fu["completed"])
        date_str = fu_date.strftime("%b %d, %Y") if fu_date else "—"
        icon = "✅" if done else ("🔴" if border_color == "#EF4444" else "🔵")
        opacity = "opacity:0.55;" if done else ""

        col1, col2 = st.columns([6, 1])
        with col1:
            st.markdown(
                f'<div class="job-card" style="border-left:3px solid {border_color};{opacity}">'
                f'<div class="job-card-title">{icon} {fu["company_name"]} &nbsp;·&nbsp; {fu["follow_up_type"]}</div>'
                f'<div class="job-card-meta">{fu["role_title"]} &nbsp;·&nbsp; {date_str}{extra_label}</div>'
                + (f'<div class="job-card-meta" style="margin-top:4px;">{fu["notes"]}</div>' if fu.get("notes") else "")
                + '</div>',
                unsafe_allow_html=True,
            )
        with col2:
            btn = "Undo" if done else "Done ✓"
            if st.button(btn, key=f"tog_{fu['id']}"):
                db.toggle_follow_up(fu["id"])
                st.rerun()
            if st.button("Del", key=f"del_{fu['id']}"):
                db.delete_follow_up(fu["id"])
                st.rerun()

    if overdue:
        st.markdown(f"### Overdue  ({len(overdue)})")
        for fu in overdue:
            d = parse_date(fu["follow_up_date"])
            days_ago = (today - d).days if d else 0
            _render_fu(fu, "#EF4444", f"  —  {days_ago}d overdue")
        st.markdown("")

    if the_rest:
        label = "Upcoming / Completed" if show_filter != "Pending Only" else "Upcoming"
        st.markdown(f"### {label}  ({len(the_rest)})")
        for fu in the_rest:
            d = parse_date(fu["follow_up_date"])
            done = bool(fu["completed"])
            days = (d - today).days if d else 0
            border = "#10B981" if done else "#3B82F6"
            extra = "" if done else (f"  —  Today" if days == 0 else f"  —  in {days}d")
            _render_fu(fu, border, extra)


# ---------------------------------------------------------------------------
# Page: Analytics
# ---------------------------------------------------------------------------

def page_analytics():
    st.markdown("# Analytics")

    jobs = db.get_all_jobs()
    if not jobs:
        st.info("No data yet. Add some jobs to see analytics.")
        return

    status_counts = db.get_status_counts()
    count_map = {d["status"]: d["count"] for d in status_counts}

    # Funnel + conversion rates
    c1, c2 = st.columns([2, 1])
    with c1:
        st.markdown("### Application Funnel")
        st.plotly_chart(ch.funnel_chart(status_counts), use_container_width=True, key="an_funnel")
    with c2:
        st.markdown("### Conversion Rates")
        stages_interviewed = ["Interview", "Technical Assessment", "Final Round", "Offer"]
        total_applied      = sum(count_map.get(s, 0) for s in ALL_STATUSES if s not in ("Researching", "Ready to Apply", "Withdrawn"))
        total_interviewed  = sum(count_map.get(s, 0) for s in stages_interviewed)
        total_offers       = count_map.get("Offer", 0)
        total_rejected     = count_map.get("Rejected", 0)

        st.metric("Applications Submitted", total_applied)
        st.metric(
            "Applied → Interview",
            f"{total_interviewed/total_applied*100:.0f}%" if total_applied else "—",
        )
        st.metric(
            "Applied → Offer",
            f"{total_offers/total_applied*100:.0f}%" if total_applied else "—",
        )
        if total_interviewed:
            st.metric("Interview → Offer", f"{total_offers/total_interviewed*100:.0f}%")
        if total_rejected:
            st.metric("Rejection Rate", f"{total_rejected/total_applied*100:.0f}%" if total_applied else "—")

    st.markdown("---")

    # Source analysis
    st.markdown("### Applications by Source")
    st.plotly_chart(ch.source_chart(db.get_source_stats()), use_container_width=True, key="an_source")

    st.markdown("---")

    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("### Avg Time in Each Stage (Days Since Added)")
        st.plotly_chart(ch.avg_time_chart(db.get_avg_time_in_stages()), use_container_width=True, key="an_avg_time")

    with col_b:
        st.markdown("### Salary Range Distribution")
        sfig = ch.salary_distribution_chart(jobs)
        if sfig:
            st.plotly_chart(sfig, use_container_width=True, key="an_salary")
        else:
            st.info("Add salary data to jobs to see this chart.")

    st.markdown("---")

    st.markdown("### Days to First Response by Source")
    rt_data = db.get_response_times()
    rt_fig  = ch.response_time_chart(rt_data)
    if rt_fig:
        st.plotly_chart(rt_fig, use_container_width=True, key="an_response_time")
    else:
        st.info(
            "No response time data yet. Set the **First Response Date** on a job "
            "(Add/Edit Job form) once you hear back."
        )

    st.markdown("---")

    st.markdown("### Application Volume Over Time")
    st.plotly_chart(ch.applications_over_time_chart(db.get_applications_over_time()), use_container_width=True)

    st.markdown("---")

    # Summary table
    st.markdown("### All Applications")
    df = pd.DataFrame(jobs)
    display_cols = {
        "company_name": "Company",
        "role_title":   "Role",
        "status":       "Status",
        "priority":     "Priority",
        "work_type":    "Work Type",
        "source":       "Source",
        "date_added":   "Added",
        "date_applied": "Applied",
    }
    df_disp = df[[c for c in display_cols if c in df.columns]].rename(columns=display_cols)
    st.dataframe(df_disp, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Page: New Leads
# ---------------------------------------------------------------------------

_DR_KEYWORDS   = ["disaster", "fema", "emergency management", "hazard", "recovery",
                   "public assistance", "grant", "mitigation", "cdbg", "hud"]
_TECH_KEYWORDS = ["implementation", "customer success", "saas", "onboarding",
                  "solutions consultant", "crm", "software", "platform", "salesforce",
                  "servicenow", "zendesk", "hubspot"]


def _lead_matches(job, keywords):
    text = " ".join([
        job.get("role_title") or "",
        job.get("notes") or "",
        job.get("source") or "",
        job.get("company_name") or "",
    ]).lower()
    return any(k in text for k in keywords)


def page_new_leads():
    st.markdown("# New Leads")

    last_run = get_last_scraper_run()
    if last_run:
        st.caption(f"Last ingestion: {last_run.strftime('%B %d, %Y at %H:%M')}")
    else:
        st.caption("No ingestion runs yet. Use the Ingestion page to pull fresh listings.")

    st.markdown("---")

    # ── Controls ──────────────────────────────────────────────────────────
    ctrl1, ctrl2, ctrl3 = st.columns([3, 3, 1])
    with ctrl1:
        filter_opt = st.radio(
            "Role type",
            ["All", "DR/EM", "Tech Pivot"],
            horizontal=True,
        )
    with ctrl2:
        sort_opt = st.radio(
            "Sort by",
            ["Newest Import", "Oldest Import", "Priority", "Date Posted"],
            horizontal=True,
        )
    with ctrl3:
        days_opt = st.selectbox(
            "Lookback",
            [7, 14, 30, 60, 90],
            format_func=lambda d: f"{d}d",
        )

    sort_map = {
        "Newest Import": "imported_desc",
        "Oldest Import": "imported_asc",
        "Priority":      "priority",
        "Date Posted":   "date_added_desc",
    }

    leads = db.get_new_leads(days=days_opt, sort_by=sort_map[sort_opt])

    if not leads:
        st.info(f"No new leads in the last {days_opt} days.")
        return

    if filter_opt == "DR/EM":
        leads = [j for j in leads if _lead_matches(j, _DR_KEYWORDS)]
    elif filter_opt == "Tech Pivot":
        leads = [j for j in leads if _lead_matches(j, _TECH_KEYWORDS)]

    if not leads:
        st.info("No leads match this filter.")
        return

    st.markdown(f"**{len(leads)} lead{'s' if len(leads) != 1 else ''} to review**")
    st.markdown("")

    for job in leads:
        salary_str = format_salary(job.get("salary_min"), job.get("salary_max"))
        meta_parts = []
        if job.get("location"):  meta_parts.append(job["location"])
        if job.get("work_type"): meta_parts.append(job["work_type"])
        if salary_str != "—":    meta_parts.append(salary_str)
        if job.get("source"):    meta_parts.append(f"via {job['source']}")

        added = job.get("date_added", "")

        # Format imported-at timestamp
        imported_at = job.get("created_at")
        if imported_at:
            if not isinstance(imported_at, datetime):
                try:
                    imported_at = datetime.fromisoformat(str(imported_at))
                except Exception:
                    imported_at = None
        if imported_at:
            delta = datetime.now(imported_at.tzinfo) - imported_at if imported_at.tzinfo else datetime.now() - imported_at
            hours = int(delta.total_seconds() // 3600)
            if hours < 1:
                imported_str = "imported just now"
            elif hours < 24:
                imported_str = f"imported {hours}h ago"
            else:
                imported_str = f"imported {delta.days}d ago"
        else:
            imported_str = f"added {added}"

        with st.expander(f"{job['company_name']}  —  {job['role_title']}", expanded=True):
            col_info, col_actions = st.columns([3, 2])

            with col_info:
                if meta_parts:
                    st.markdown(
                        f'<div class="job-card-meta" style="margin-bottom:6px;">'
                        + "  ·  ".join(meta_parts) +
                        "</div>",
                        unsafe_allow_html=True,
                    )
                if job.get("notes"):
                    st.markdown(
                        f'<div class="job-card-meta" style="margin-bottom:6px;">'
                        f'{job["notes"][:300]}{"..." if len(job["notes"]) > 300 else ""}'
                        f"</div>",
                        unsafe_allow_html=True,
                    )
                st.markdown(
                    f'<div style="margin-top:4px;">'
                    + priority_badge(job.get("priority", "Medium"))
                    + f'<span style="color:#6B7280;font-size:0.75rem;margin-left:8px;">{imported_str}</span>'
                    + "</div>",
                    unsafe_allow_html=True,
                )
                if job.get("job_url"):
                    st.markdown(f"[View posting]({job['job_url']})")

            with col_actions:
                st.markdown("**Triage**")
                b1, b2, b3 = st.columns(3)
                with b1:
                    if st.button("Interested", key=f"nl_int_{job['id']}", use_container_width=True, type="primary"):
                        db.triage_job(job["id"], "Ready to Apply", "High")
                        st.toast("Moved to Ready to Apply", icon="✅")
                        st.rerun()
                with b2:
                    if st.button("Maybe\nLater", key=f"nl_maybe_{job['id']}", use_container_width=True):
                        db.triage_job(job["id"], "Researching", "Low")
                        st.toast("Snoozed", icon="💤")
                        st.rerun()
                with b3:
                    if st.button("Not a\nFit", key=f"nl_nope_{job['id']}", use_container_width=True):
                        db.triage_job(job["id"], "Withdrawn")
                        st.toast("Dismissed", icon="👋")
                        st.rerun()


# ---------------------------------------------------------------------------
# Page: Import Job
# ---------------------------------------------------------------------------

def _parse_job_text(text: str) -> dict:
    """Extract job fields from plain text using regex. Returns a partial dict."""
    import re
    result = {}
    lower = text.lower()

    # Work type
    if re.search(r'\bhybrid\b', lower):
        result["work_type"] = "Hybrid"
    elif re.search(r'\bremote\b', lower):
        result["work_type"] = "Remote"
    elif re.search(r'\b(on[- ]?site|in[- ]?office)\b', lower):
        result["work_type"] = "On-site"

    # Salary range: "$80,000 - $110,000", "$80K-$110K", "$40/hr"
    sal_m = re.search(
        r'\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(k)?\s*[-–—]\s*\$?\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(k)?',
        text, re.IGNORECASE,
    )
    if sal_m:
        mn = float(sal_m.group(1).replace(',', '')) * (1000 if sal_m.group(2) else 1)
        mx = float(sal_m.group(3).replace(',', '')) * (1000 if sal_m.group(4) else 1)
        if mn < 500:  # looks hourly → annualise
            mn, mx = mn * 2080, mx * 2080
        result["salary_min"] = int(mn)
        result["salary_max"] = int(mx)
    else:
        sal_s = re.search(r'\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(k)?', text, re.IGNORECASE)
        if sal_s:
            v = float(sal_s.group(1).replace(',', '')) * (1000 if sal_s.group(2) else 1)
            if v < 500:
                v *= 2080
            result["salary_min"] = int(v)

    # Location: explicit label first, then "City, ST" pattern
    loc_m = re.search(r'(?:location|based in|office)\s*[:\-]\s*([^\n\|]+)', text, re.IGNORECASE)
    if loc_m:
        result["location"] = loc_m.group(1).strip()
    else:
        city_m = re.search(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s+([A-Z]{2})\b', text)
        if city_m:
            result["location"] = city_m.group(0)

    # Company name: explicit label
    co_m = re.search(r'(?:company|employer|organization)\s*[:\-]\s*([^\n]+)', text, re.IGNORECASE)
    if co_m:
        result["company_name"] = co_m.group(1).strip()

    # Job title: explicit label, then first short non-empty line
    title_m = re.search(
        r'(?:job title|position title|title|role|position)\s*[:\-]\s*([^\n]+)', text, re.IGNORECASE
    )
    if title_m:
        result["role_title"] = title_m.group(1).strip()
    else:
        for line in text.strip().splitlines():
            line = line.strip()
            if 5 < len(line) < 100:
                result["role_title"] = line
                break

    # Notes: cleaned first 500 chars
    result["notes"] = re.sub(r'\s+', ' ', text.strip())[:500]

    return result


def _check_import_duplicate(company: str, role: str):
    """Return existing job dict if same company + similar role found, else None."""
    company_l = company.lower().strip()
    role_l = role.lower().strip()
    for j in db.get_all_jobs():
        if j["company_name"].lower().strip() == company_l:
            er = j["role_title"].lower().strip()
            if role_l in er or er in role_l:
                return j
    return None


def _execute_import_save(data: dict) -> int:
    """Insert job + optional contact/follow-up from import form data. Returns job_id."""
    job_id = db.add_job({
        "company_name": data["company_name"],
        "role_title":   data["role_title"],
        "status":       data["status"],
        "priority":     data["priority"],
        "source":       data["source"],
        "location":     data.get("location") or None,
        "work_type":    data["work_type"],
        "salary_min":   data.get("salary_min") or None,
        "salary_max":   data.get("salary_max") or None,
        "job_url":      data.get("job_url") or None,
        "notes":        data.get("notes") or None,
    })
    if data.get("contact_name", "").strip():
        db.add_contact({
            "job_id":        job_id,
            "contact_name":  data["contact_name"].strip(),
            "contact_email": data.get("contact_email") or None,
        })
    fu_date = data.get("follow_up_date")
    if fu_date:
        db.add_follow_up({
            "job_id":          job_id,
            "follow_up_date":  fu_date.isoformat() if hasattr(fu_date, "isoformat") else str(fu_date),
            "follow_up_type":  "Email",
        })
    return job_id


def page_import_job():
    st.markdown("# Import Job")
    st.caption("Paste a URL or job description to quickly add a posting to your tracker.")

    # ── Duplicate confirmation (shown when a dupe was detected on last submit) ──
    if st.session_state.get("import_confirm_pending"):
        pending = st.session_state["import_confirm_pending"]
        dupe = pending["_dupe"]
        st.warning(
            f"A similar job already exists: **{dupe['company_name']}** — {dupe['role_title']} "
            f"(Status: {dupe['status']}). Save anyway?"
        )
        c1, c2, _ = st.columns([1, 1, 3])
        with c1:
            if st.button("Yes, save", type="primary", key="import_save_anyway"):
                job_id = _execute_import_save(pending)
                st.session_state.pop("import_confirm_pending", None)
                st.session_state.pop("import_prefill", None)
                st.session_state["import_success"] = {
                    "job_id":  job_id,
                    "company": pending["company_name"],
                    "role":    pending["role_title"],
                }
                st.session_state["import_form_ver"] = st.session_state.get("import_form_ver", 0) + 1
                st.rerun()
        with c2:
            if st.button("Cancel", key="import_cancel"):
                st.session_state.pop("import_confirm_pending", None)
                st.rerun()
        st.markdown("---")

    # ── Success banner ─────────────────────────────────────────────────────────
    if st.session_state.get("import_success"):
        suc = st.session_state["import_success"]
        st.success(f"Saved **{suc['company']}** — {suc['role']}!")
        if st.button("View in tracker →", key="import_view_btn"):
            st.session_state["selected_job_id"] = suc["job_id"]
            st.session_state["page"] = "Job Detail"
            st.session_state.pop("import_success", None)
            st.rerun()
        st.markdown("---")

    # ── Extraction input tabs ──────────────────────────────────────────────────
    tab_url, tab_text = st.tabs(["Paste URL", "Paste Job Description"])

    with tab_url:
        url_val = st.text_input(
            "Job posting URL",
            placeholder="https://apply.workable.com/…, linkedin.com/jobs/…, greenhouse.io/…",
            key="import_url_input",
        )
        if st.button("Import from URL", use_container_width=True, key="import_url_btn"):
            if url_val.strip():
                with st.spinner("Fetching job details…"):
                    data, err = _fetch_job_url(url_val.strip())
                if err:
                    st.error(
                        f"Couldn't fetch that page — {err}.  "
                        "Try the **Paste Job Description** tab instead."
                    )
                else:
                    st.session_state["import_prefill"] = data
                    st.session_state.pop("import_success", None)
                    st.rerun()
            else:
                st.warning("Enter a URL first.")

    with tab_text:
        text_val = st.text_area(
            "Paste job description",
            placeholder="Paste the full job posting text here…",
            height=200,
            key="import_text_input",
        )
        if st.button("Extract Details", use_container_width=True, key="import_text_btn"):
            if text_val.strip():
                data = _parse_job_text(text_val)
                st.session_state["import_prefill"] = data
                st.session_state.pop("import_success", None)
                st.rerun()
            else:
                st.warning("Paste a job description first.")

    st.markdown("---")

    # ── Pre-filled editable form ───────────────────────────────────────────────
    pf = st.session_state.get("import_prefill", {})

    if pf:
        st.markdown("### Review & Save")
        st.caption("Fields were auto-populated — edit anything before saving.")
    else:
        st.markdown("### Job Details")

    def _idx(lst, val, default=0):
        try:
            return lst.index(val)
        except ValueError:
            return default

    form_ver = st.session_state.get("import_form_ver", 0)

    with st.form(f"import_job_form_{form_ver}", clear_on_submit=False):
        col1, col2 = st.columns(2)
        with col1:
            company_name = st.text_input("Company Name *", value=pf.get("company_name", ""))
            role_title   = st.text_input("Role Title *",   value=pf.get("role_title", ""))
            location     = st.text_input("Location",       value=pf.get("location", ""))
            work_type    = st.selectbox("Work Type", WORK_TYPES,
                                        index=_idx(WORK_TYPES, pf.get("work_type", "Remote")))
            job_url      = st.text_input("Job URL", value=pf.get("job_url", ""))
        with col2:
            status   = st.selectbox("Status", ALL_STATUSES, index=_idx(ALL_STATUSES, "Researching"))
            priority = st.selectbox("Priority", ["High", "Medium", "Low"], index=1)
            source_default = pf.get("source", "Other")
            if source_default not in SOURCES:
                source_default = "Other"
            source = st.selectbox("Source", SOURCES, index=SOURCES.index(source_default))
            sc1, sc2 = st.columns(2)
            with sc1:
                salary_min = st.number_input("Salary Min ($)", min_value=0, step=5000,
                                             value=int(pf.get("salary_min") or 0))
            with sc2:
                salary_max = st.number_input("Salary Max ($)", min_value=0, step=5000,
                                             value=int(pf.get("salary_max") or 0))

        st.markdown("#### Optional: Contact & Follow-Up")
        oc1, oc2, oc3 = st.columns(3)
        with oc1:
            contact_name  = st.text_input("Contact Name")
        with oc2:
            contact_email = st.text_input("Contact Email")
        with oc3:
            follow_up_date = st.date_input("Next Follow-Up Date", value=None, format="YYYY-MM-DD")

        notes = st.text_area("Notes", value=pf.get("notes", ""), height=120)

        submitted = st.form_submit_button("Save to Tracker", use_container_width=True, type="primary")

    if submitted:
        if not company_name.strip() or not role_title.strip():
            st.error("Company name and role title are required.")
            return

        form_data = {
            "company_name":   company_name.strip(),
            "role_title":     role_title.strip(),
            "status":         status,
            "priority":       priority,
            "source":         source,
            "location":       location.strip() or None,
            "work_type":      work_type,
            "salary_min":     salary_min if salary_min > 0 else None,
            "salary_max":     salary_max if salary_max > 0 else None,
            "job_url":        job_url.strip() or None,
            "notes":          notes.strip() or None,
            "contact_name":   contact_name,
            "contact_email":  contact_email,
            "follow_up_date": follow_up_date,
        }

        dupe = _check_import_duplicate(company_name.strip(), role_title.strip())
        if dupe:
            form_data["_dupe"] = dupe
            st.session_state["import_confirm_pending"] = form_data
            st.rerun()
        else:
            job_id = _execute_import_save(form_data)
            st.session_state.pop("import_prefill", None)
            st.session_state["import_success"] = {
                "job_id":  job_id,
                "company": company_name.strip(),
                "role":    role_title.strip(),
            }
            st.session_state["import_form_ver"] = form_ver + 1
            st.rerun()


# ---------------------------------------------------------------------------
# Page: Ingestion
# ---------------------------------------------------------------------------

def page_ingestion():
    st.markdown("# Ingestion")
    st.caption("Monitor and trigger automated job ingestion runs.")

    # ── Last run summary ──────────────────────────────────────────────────
    last_run = db.get_last_ingestion_run()
    if last_run:
        started = last_run.get("started_at")
        if started and not isinstance(started, datetime):
            started = datetime.fromisoformat(str(started))
        age_str = ""
        if started:
            delta = datetime.now(started.tzinfo) - started if started.tzinfo else datetime.now() - started
            hours = int(delta.total_seconds() // 3600)
            age_str = f"  —  {hours}h ago" if hours < 48 else ""

        status = last_run.get("status", "—")
        status_color = {"completed": "#10B981", "running": "#F59E0B", "error": "#EF4444"}.get(status, "#6B7280")

        m1, m2, m3, m4, m5 = st.columns(5)
        with m1:
            st.markdown(
                f'<div class="metric-card"><div class="metric-value" style="color:{status_color};font-size:1.4rem;">'
                f'{status}</div><div class="metric-label">Last Status</div></div>',
                unsafe_allow_html=True,
            )
        with m2:
            st.markdown(
                f'<div class="metric-card"><div class="metric-value">{last_run.get("jobs_created", 0)}</div>'
                f'<div class="metric-label">Jobs Created</div></div>',
                unsafe_allow_html=True,
            )
        with m3:
            st.markdown(
                f'<div class="metric-card"><div class="metric-value">{last_run.get("jobs_updated", 0)}</div>'
                f'<div class="metric-label">Jobs Updated</div></div>',
                unsafe_allow_html=True,
            )
        with m4:
            st.markdown(
                f'<div class="metric-card"><div class="metric-value">{last_run.get("jobs_skipped", 0)}</div>'
                f'<div class="metric-label">Dupes Skipped</div></div>',
                unsafe_allow_html=True,
            )
        with m5:
            ts_str = started.strftime("%b %d  %H:%M") if started else "—"
            st.markdown(
                f'<div class="metric-card"><div class="metric-value" style="font-size:1.2rem">'
                f'{ts_str}{age_str}</div><div class="metric-label">Last Run</div></div>',
                unsafe_allow_html=True,
            )
    else:
        st.info("No ingestion runs recorded yet.")

    st.markdown("---")

    # ── Manual trigger ────────────────────────────────────────────────────
    st.markdown("#### Manual Ingestion Run")
    st.caption(
        "Runs in-process (same Streamlit session) — suitable for testing. "
        "Production runs are triggered by the Render cron job."
    )

    col1, col2 = st.columns([3, 1])
    with col1:
        companies_only = st.checkbox("Company watcher only (skip API keyword search)")
        dry_run = st.checkbox("Dry run (fetch + dedup, don't write to DB)")
    with col2:
        trigger = st.button("Run Ingestion Now", type="primary", use_container_width=True)

    if trigger:
        from ingestion import orchestrator
        with st.spinner("Running ingestion pipeline…"):
            try:
                report = orchestrator.run(companies_only=companies_only, dry_run=dry_run)
                st.success(
                    f"Done — {report['jobs_created']} created, "
                    f"{report['jobs_updated']} updated, "
                    f"{report['jobs_skipped']} skipped."
                )
                if report.get("flagged_jobs"):
                    st.warning(f"{report['flagged']} near-duplicates flagged (saved but worth reviewing).")
            except Exception as exc:
                st.error(f"Ingestion error: {exc}")

    st.markdown("---")

    # ── Run history ───────────────────────────────────────────────────────
    st.markdown("#### Recent Runs")
    runs = db.get_ingestion_runs(limit=20)
    if not runs:
        st.caption("No runs yet.")
        return

    rows = []
    for r in runs:
        started = r.get("started_at")
        if started and not isinstance(started, datetime):
            started = datetime.fromisoformat(str(started))
        rows.append({
            "Started":      started.strftime("%Y-%m-%d  %H:%M") if started else "—",
            "Source":       r.get("source", "—"),
            "Status":       r.get("status", "—"),
            "Created":      r.get("jobs_created", 0),
            "Updated":      r.get("jobs_updated", 0),
            "Skipped":      r.get("jobs_skipped", 0),
            "Errors":       r.get("error_count", 0),
            "Notes":        r.get("run_notes") or "",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

import base64 as _b64
_face_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "chris_face.png")
_face_b64 = ""
if os.path.exists(_face_path):
    with open(_face_path, "rb") as _f:
        _face_b64 = _b64.b64encode(_f.read()).decode()
_img_tag = f'<img src="data:image/png;base64,{_face_b64}" style="height:96px;width:auto;object-fit:contain;opacity:0.92;display:block;margin:0;padding:0;vertical-align:bottom;">' if _face_b64 else ""

st.markdown(f"""
<style>
    div[data-testid="stMarkdownContainer"]:has(#header-banner) {{
        margin: 0 !important;
        padding: 0 !important;
        line-height: 0;
    }}
</style>
<div id="header-banner" style="
    background: linear-gradient(90deg, #1a0533, #0a1628, #1a0533);
    border-top: 2px solid #7C3AED;
    border-bottom: 2px solid #7C3AED;
    padding: 0;
    margin: 0 -1rem 8px -1rem;
    display: flex;
    align-items: stretch;
    justify-content: space-between;
    line-height: 0;
    overflow: hidden;
">
    {_img_tag}
    <div style="
        flex: 1;
        display: flex;
        align-items: center;
        justify-content: center;
        letter-spacing: 0.18em;
        font-size: 1.5rem;
        font-weight: 900;
        color: #F9FAFB;
        text-shadow: 0 0 20px #7C3AED, 0 0 40px #3B82F6;
        font-family: monospace;
        line-height: normal;
    ">CHRI$$$ CA$H FLOW COMMAND CENTER</div>
    {_img_tag}
</div>
""", unsafe_allow_html=True)

nav_col, content_col = st.columns([1, 4])
page = render_nav(nav_col)

with content_col:
    if page == "Overview":
        page_overview()
    elif page == "Add / Edit Job":
        page_add_edit_job()
    elif page == "Job Detail":
        page_job_detail()
    elif page == "Follow-Up Tracker":
        page_follow_up_tracker()
    elif page == "Analytics":
        page_analytics()
    elif page == "New Leads":
        page_new_leads()
    elif page == "Import Job":
        page_import_job()
    elif page == "Ingestion":
        page_ingestion()
