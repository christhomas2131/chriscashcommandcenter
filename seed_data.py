"""Populate the database with sample jobs, contacts, follow-ups, and interviews."""
from datetime import date, timedelta

import db.repository as db


def seed():
    if db.get_all_jobs():
        return  # already seeded

    today = date.today()

    jobs_raw = [
        {
            "company_name": "Hagerty Consulting",
            "role_title": "Disaster Recovery Consultant",
            "status": "Applied",
            "date_added": (today - timedelta(days=14)).isoformat(),
            "date_applied": (today - timedelta(days=10)).isoformat(),
            "salary_min": 90000,
            "salary_max": 115000,
            "location": "Remote",
            "work_type": "Remote",
            "source": "LinkedIn",
            "job_url": "https://www.hagertyinc.com/careers",
            "notes": "Strong match for FEMA DR programs background. Submitted tailored resume highlighting PA grants experience.",
            "priority": "High",
        },
        {
            "company_name": "ICF International",
            "role_title": "Emergency Management Specialist",
            "status": "Researching",
            "date_added": (today - timedelta(days=7)).isoformat(),
            "date_applied": None,
            "salary_min": 85000,
            "salary_max": 110000,
            "location": "Washington, DC",
            "work_type": "Hybrid",
            "source": "Company Site",
            "job_url": "https://www.icf.com/careers",
            "notes": "Need to tailor resume around mitigation planning. Check security clearance requirements before applying.",
            "priority": "High",
        },
        {
            "company_name": "Tetra Tech",
            "role_title": "FEMA PA Program Manager",
            "status": "Phone Screen",
            "date_added": (today - timedelta(days=21)).isoformat(),
            "date_applied": (today - timedelta(days=18)).isoformat(),
            "salary_min": 100000,
            "salary_max": 130000,
            "location": "Remote",
            "work_type": "Remote",
            "source": "LinkedIn",
            "job_url": "https://www.tetratech.com/careers",
            "notes": "HR screen went well. Hiring manager interview expected within 2 weeks.",
            "priority": "High",
        },
        {
            "company_name": "Tidal Basin",
            "role_title": "Grant Management Specialist",
            "status": "Ready to Apply",
            "date_added": (today - timedelta(days=5)).isoformat(),
            "date_applied": None,
            "salary_min": 80000,
            "salary_max": 100000,
            "location": "Remote",
            "work_type": "Remote",
            "source": "Indeed",
            "job_url": "https://www.tidalbasin.com/careers",
            "notes": "Good fit for grants background. Still need to write cover letter.",
            "priority": "Medium",
        },
        {
            "company_name": "Salesforce",
            "role_title": "Implementation Consultant",
            "status": "Researching",
            "date_added": (today - timedelta(days=3)).isoformat(),
            "date_applied": None,
            "salary_min": 120000,
            "salary_max": 160000,
            "location": "San Francisco, CA",
            "work_type": "Hybrid",
            "source": "Recruiter",
            "job_url": "https://www.salesforce.com/company/careers",
            "notes": "Inbound recruiter outreach via LinkedIn. Role would require relocation or heavy travel — low priority for now.",
            "priority": "Low",
        },
    ]

    ids = [db.add_job(j) for j in jobs_raw]
    hagerty_id, icf_id, tetra_id, tidal_id, sf_id = ids

    # Contacts
    db.add_contact({
        "job_id": hagerty_id,
        "contact_name": "Sarah Mitchell",
        "contact_title": "Recruiting Manager",
        "contact_email": "s.mitchell@hagertyinc.com",
        "contact_linkedin": "linkedin.com/in/sarahmitchell",
        "notes": "Initial point of contact. Very responsive — usually replies same day.",
    })

    db.add_contact({
        "job_id": tetra_id,
        "contact_name": "James Kowalski",
        "contact_title": "HR Coordinator",
        "contact_email": "j.kowalski@tetratech.com",
        "notes": "Conducted the phone screen. Said hiring manager (Dr. Tran) will reach out in 1–2 weeks.",
    })

    db.add_contact({
        "job_id": tetra_id,
        "contact_name": "Dr. Lisa Tran",
        "contact_title": "Director of Emergency Management",
        "contact_linkedin": "linkedin.com/in/lisatran-em",
        "notes": "Would be direct supervisor. Found on LinkedIn — strong FEMA PA background, 15 yrs at Tetra Tech.",
    })

    db.add_contact({
        "job_id": sf_id,
        "contact_name": "Marcus Webb",
        "contact_title": "Senior Recruiter",
        "contact_email": "m.webb@salesforce.com",
        "contact_linkedin": "linkedin.com/in/marcuswebb",
        "notes": "Reached out cold on LinkedIn. Very enthusiastic about my background.",
    })

    # Follow-ups
    db.add_follow_up({
        "job_id": hagerty_id,
        "follow_up_date": (today + timedelta(days=2)).isoformat(),
        "follow_up_type": "Email",
        "completed": False,
        "notes": "Follow up with Sarah if no update by end of week.",
    })

    db.add_follow_up({
        "job_id": hagerty_id,
        "follow_up_date": (today - timedelta(days=3)).isoformat(),
        "follow_up_type": "LinkedIn Message",
        "completed": True,
        "notes": "Sent connection request to hiring manager on LinkedIn.",
    })

    db.add_follow_up({
        "job_id": tetra_id,
        "follow_up_date": (today + timedelta(days=5)).isoformat(),
        "follow_up_type": "Email",
        "completed": False,
        "notes": "Follow up with James if no word on hiring manager interview.",
    })

    db.add_follow_up({
        "job_id": tidal_id,
        "follow_up_date": (today + timedelta(days=1)).isoformat(),
        "follow_up_type": "Check-in",
        "completed": False,
        "notes": "Finish and submit cover letter. Deadline approaching.",
    })

    db.add_follow_up({
        "job_id": icf_id,
        "follow_up_date": (today - timedelta(days=1)).isoformat(),
        "follow_up_type": "Email",
        "completed": False,
        "notes": "Research clearance requirements before applying.",
    })

    # Interview stages
    db.add_interview_stage({
        "job_id": tetra_id,
        "stage_name": "HR Phone Screen",
        "stage_date": (today - timedelta(days=5)).isoformat(),
        "interviewer_name": "James Kowalski",
        "format": "Phone",
        "status": "Completed",
        "notes": "30-min call. Standard screening questions. Good rapport with James. Discussed salary range — aligned.",
    })

    db.add_interview_stage({
        "job_id": tetra_id,
        "stage_name": "Hiring Manager Interview",
        "stage_date": (today + timedelta(days=3)).isoformat(),
        "interviewer_name": "Dr. Lisa Tran",
        "format": "Video",
        "status": "Scheduled",
        "notes": "45-min Zoom. Prep STAR stories: FEMA PA closeout, multi-state DR deployment, team leadership.",
    })
