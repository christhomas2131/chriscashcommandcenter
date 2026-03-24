# Job Search Scraper - Feed into SQLite Dashboard

Build me a Python script that searches for relevant job postings and imports them into my existing Streamlit job tracker's SQLite database (`jobs.db`).

## Approach: Use Free/Legitimate Job Search APIs

Use the **JSearch API** (available on RapidAPI, free tier gives 200 requests/month) as the primary source. If that doesn't work or you want a backup, also support **Adzuna API** (free tier available) and/or **The Muse API** (no key needed for basic access).

The script should be configurable to use whichever API the user has a key for.

## Configuration File: `search_config.json`

```json
{
  "apis": {
    "jsearch": {
      "enabled": true,
      "api_key": "YOUR_RAPIDAPI_KEY_HERE",
      "base_url": "https://jsearch.p.rapidapi.com"
    },
    "adzuna": {
      "enabled": false,
      "app_id": "YOUR_ADZUNA_APP_ID",
      "app_key": "YOUR_ADZUNA_KEY",
      "country": "us"
    }
  },
  "search_profiles": [
    {
      "name": "Disaster Recovery / Emergency Management",
      "queries": [
        "disaster recovery consultant remote",
        "FEMA public assistance specialist",
        "emergency management consultant remote",
        "disaster recovery project manager",
        "grant management specialist FEMA",
        "hazard mitigation project manager"
      ],
      "target_companies": [
        "Hagerty Consulting",
        "ICF",
        "Tetra Tech",
        "Tidal Basin",
        "AC Disaster Consulting",
        "Witt O'Brien's",
        "Guidehouse",
        "AECOM",
        "Stantec",
        "CDM Smith"
      ],
      "default_priority": "High"
    },
    {
      "name": "Tech Pivot - Implementation / Customer Success",
      "queries": [
        "implementation consultant remote",
        "customer success manager SaaS remote",
        "implementation specialist remote",
        "onboarding consultant remote",
        "solutions consultant remote"
      ],
      "target_companies": [
        "Salesforce",
        "ServiceTitan",
        "Plaid",
        "monday.com",
        "Notion",
        "PagerDuty",
        "Asana"
      ],
      "default_priority": "Medium"
    }
  ],
  "filters": {
    "work_types": ["remote", "hybrid"],
    "exclude_keywords": ["senior director", "VP", "vice president", "CTO", "intern", "internship"],
    "min_salary": 60000,
    "max_results_per_query": 20,
    "days_posted": 30
  }
}
```

## Core Script: `job_scraper.py`

### Features

1. **Search Execution**
   - Loop through each search profile and its queries
   - Hit the configured API(s) for each query
   - Collect and normalize results into a standard format

2. **Deduplication**
   - Check incoming results against existing entries in `jobs.db` by company_name + role_title (fuzzy match)
   - Also deduplicate within the current batch
   - Flag near-duplicates for review rather than auto-importing

3. **Normalization**
   - Map API fields to the dashboard's database schema
   - Auto-detect work_type from job description keywords (remote, hybrid, on-site)
   - Auto-detect source based on where the listing was found
   - Extract salary range if available in the API response
   - Extract location from the API response
   - Set status to "Researching" for all new imports
   - Set date_added to today

4. **Priority Scoring**
   - If the company matches a target_company in the search profile, boost priority to "High"
   - Otherwise, use the profile's default_priority
   - If salary info is available and above median for the role type, boost to "High"

5. **Import to SQLite**
   - Insert new jobs into the `jobs` table of `jobs.db`
   - Create a log entry for each import run

6. **Reporting**
   - After each run, print a summary:
     - Total results found
     - New jobs added
     - Duplicates skipped
     - Jobs flagged for review
   - Also save the report to `scraper_logs/YYYY-MM-DD_HH-MM.json`

### CLI Interface

```bash
# Run all search profiles
python job_scraper.py

# Run a specific profile
python job_scraper.py --profile "Disaster Recovery / Emergency Management"

# Dry run (show what would be imported, don't write to DB)
python job_scraper.py --dry-run

# Force re-import (skip dedup)
python job_scraper.py --force

# Specify a different database path
python job_scraper.py --db /path/to/jobs.db
```

## Bonus: Import from CSV

Also include a utility function that can import jobs from a CSV file (for the manual batch I already have):

```bash
python job_scraper.py --import-csv job-import-batch.csv
```

This should read the CSV, map columns to the database schema, skip duplicates, and import.

## File Structure
```
job-scraper/
├── job_scraper.py          # Main script
├── search_config.json      # API keys and search profiles
├── requirements.txt        # Dependencies
├── scraper_logs/           # Auto-created, stores run logs
└── README.md               # Setup instructions (API key registration, etc.)
```

## Technical Requirements
- Use `requests` for API calls
- Use `sqlite3` for database operations
- Use `rapidfuzz` for fuzzy string matching in dedup
- Include proper error handling and rate limiting
- Include a `requirements.txt`
- The script should work standalone (not require the Streamlit app to be running)
- Database path should default to `../jobs.db` (assuming scraper lives in a subfolder of the dashboard project) but be configurable

## Important
- Do NOT scrape LinkedIn, Indeed, or Glassdoor directly. Only use legitimate APIs.
- Include clear instructions in the README on how to get free API keys from RapidAPI/JSearch
- Make it easy to add new search queries and target companies by just editing the config file
