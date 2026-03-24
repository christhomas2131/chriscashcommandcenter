"""
ingestion/dedup.py — Fuzzy deduplication against the existing jobs table.
"""

from __future__ import annotations

from rapidfuzz import fuzz, process

DEDUP_THRESHOLD  = 85   # score >= this → duplicate (skip)
REVIEW_THRESHOLD = 70   # score >= this → flag for review


def _job_key(company: str, title: str) -> str:
    return f"{company.lower().strip()} | {title.lower().strip()}"


def deduplicate(
    incoming: list[dict],
    existing: list[dict],
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Returns (new_jobs, duplicates, flagged_for_review).

    Deduplicates:
    1. Within the incoming batch (exact key match).
    2. Against existing DB rows using rapidfuzz token_sort_ratio.

    existing rows must have at minimum: company_name, role_title.
    """
    existing_keys = [_job_key(j["company_name"], j["role_title"]) for j in existing]
    seen: set[str] = set()
    new_jobs: list[dict] = []
    duplicates: list[dict] = []
    flagged: list[dict] = []

    for job in incoming:
        key = _job_key(job["company_name"], job["role_title"])

        # Within-batch dedup
        if key in seen:
            duplicates.append(job)
            continue
        seen.add(key)

        if not existing_keys:
            new_jobs.append(job)
            continue

        best  = process.extractOne(key, existing_keys, scorer=fuzz.token_sort_ratio)
        score = best[1] if best else 0

        if score >= DEDUP_THRESHOLD:
            duplicates.append(job)
        elif score >= REVIEW_THRESHOLD:
            job = dict(job)   # don't mutate the original
            job["_fuzzy_match"] = f"{best[0]}  (score: {score})"
            flagged.append(job)
        else:
            new_jobs.append(job)

    return new_jobs, duplicates, flagged
