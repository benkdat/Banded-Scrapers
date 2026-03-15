"""
Banded Job Board Scraper
Scrapes Greenhouse, Lever, Ashby, and Workday job boards for salary data.
Runs via GitHub Actions. Saves to Supabase with upsert to prevent duplicates.
"""

import time
from datetime import datetime
from utils import (
    CompRecord, classify_job_family, parse_location, parse_salary,
    upload_to_supabase, log_scrape_run, fetch_with_retry, log,
)

# ─────────────────────────────────────────────────────────────────────────────
# COMPANY LISTS (deduplicated, verified slugs only)
# ─────────────────────────────────────────────────────────────────────────────

GREENHOUSE_COMPANIES = [
    # Fintech
    'stripe', 'coinbase', 'brex', 'robinhood', 'chime', 'sofi', 'affirm', 'plaid',
    'marqeta', 'checkout', 'wise', 'nubank',
    # Enterprise SaaS
    'datadog', 'databricks', 'mongodb', 'elastic', 'twilio', 'amplitude',
    'contentful', 'launchdarkly', 'braze', 'iterable', 'klaviyo', 'attentive',
    'gorgias', 'intercom',
    # Productivity
    'figma', 'asana', 'airtable', 'notion', 'dropbox', 'miro', 'canva',
    'loom', 'calendly', 'docusign', 'smartsheet',
    # AI / ML
    'anthropic', 'openai', 'cohere', 'scale', 'labelbox',
    # Consumer / Marketplace
    'airbnb', 'doordash', 'instacart', 'lyft', 'discord', 'reddit', 'pinterest',
    'nextdoor', 'yelp', 'zillow', 'redfin', 'opendoor', 'compass', 'thumbtack',
    # DevTools
    'cloudflare', 'vercel', 'netlify', 'supabase', 'planetscale',
    'grafana', 'newrelic', 'pagerduty', 'atlassian', 'gitlab',
    # HR Tech
    'rippling', 'gusto', 'deel', 'lattice', 'culture-amp', 'ashby', 'gem',
    # Security
    'crowdstrike', 'sentinelone', 'zscaler', 'okta', 'snyk', 'wiz',
    # E-commerce
    'shopify', 'squarespace', 'webflow', 'klarna',
    # Health Tech
    'tempus', 'flatiron', 'headway', 'alma',
    # Other
    'toast', 'procore', 'servicetitan', 'qualtrics',
]

LEVER_COMPANIES = [
    # Big Tech / Consumer
    'netflix', 'palantir', 'spotify', 'snap',
    # Fintech
    'wealthfront', 'betterment', 'carta', 'nerdwallet',
    # Enterprise / B2B
    'flexport', 'faire', 'anduril', 'weights-and-biases',
    'productboard', 'pendo', 'heap',
    # HR Tech
    '15five', 'betterworks', 'lever', 'greenhouse', 'hired',
    # Dev Tools
    'sourcegraph', 'linear', 'shortcut',
    # AI / ML
    'snorkel', 'hive',
    # Security
    'abnormal', 'vanta', 'drata',
    # Health Tech
    'headway', 'spring-health', 'lyra', 'modern-health',
    # Other
    'grammarly', 'duolingo', 'coursera',
]

ASHBY_COMPANIES = [
    # Fintech
    'ramp', 'mercury', 'pilot', 'modern-treasury', 'increase', 'unit', 'alloy',
    # Dev Tools
    'linear', 'vercel', 'railway', 'render', 'planetscale',
    'neon', 'resend', 'clerk', 'inngest', 'temporal',
    # AI / ML
    'anthropic', 'perplexity', 'writer', 'replicate', 'modal',
    # Open Source
    'posthog', 'airbyte', 'metabase',
    # Other
    'cal-com', 'n8n',
]

WORKDAY_COMPANIES = {
    'salesforce': 'https://salesforce.wd12.myworkdayjobs.com/wday/cxs/salesforce/External_Career_Site/jobs',
    'adobe': 'https://adobe.wd5.myworkdayjobs.com/wday/cxs/adobe/external_experienced/jobs',
    'visa': 'https://visa.wd5.myworkdayjobs.com/wday/cxs/visa/Visa_Careers/jobs',
    'nvidia': 'https://nvidia.wd5.myworkdayjobs.com/wday/cxs/nvidia/NVIDIAExternalCareerSite/jobs',
}


# ─────────────────────────────────────────────────────────────────────────────
# GREENHOUSE
# ─────────────────────────────────────────────────────────────────────────────

def scrape_greenhouse(company: str) -> list[CompRecord]:
    """Scrape jobs from a Greenhouse job board."""
    jobs = []
    url = f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs?content=true"

    response = fetch_with_retry(url)
    if not response or response.status_code != 200:
        return jobs

    try:
        data = response.json()
    except ValueError:
        return jobs

    for job in data.get('jobs', []):
        title = job.get('title', '')
        location = job.get('location', {}).get('name', '')
        job_url = job.get('absolute_url', '')
        content = job.get('content', '') or ''

        # Try salary from content (available with content=true param)
        salary_min, salary_max = parse_salary(content)

        if salary_min and salary_max:
            metro, state = parse_location(location)
            jobs.append(CompRecord(
                company=company.replace('-', ' ').title(),
                title=title,
                family=classify_job_family(title),
                metro=metro,
                state=state,
                salary_min=salary_min,
                salary_max=salary_max,
                midpoint=(salary_min + salary_max) // 2,
                source='Greenhouse',
                job_url=job_url,
                posted_date=datetime.now().strftime('%Y-%m-%d'),
            ))

    return jobs


# ─────────────────────────────────────────────────────────────────────────────
# LEVER
# ─────────────────────────────────────────────────────────────────────────────

def scrape_lever(company: str) -> list[CompRecord]:
    """Scrape jobs from a Lever job board."""
    jobs = []
    url = f"https://api.lever.co/v0/postings/{company}"

    response = fetch_with_retry(url)
    if not response or response.status_code != 200:
        return jobs

    try:
        data = response.json()
    except ValueError:
        return jobs

    for job in data:
        title = job.get('text', '')
        location = job.get('categories', {}).get('location', '')
        job_url = job.get('hostedUrl', '')
        description = job.get('descriptionPlain', '') or ''
        additional = job.get('additional', '') or ''

        full_text = f"{description} {additional}"
        salary_min, salary_max = parse_salary(full_text)

        if salary_min and salary_max:
            metro, state = parse_location(location)
            jobs.append(CompRecord(
                company=company.replace('-', ' ').title(),
                title=title,
                family=classify_job_family(title),
                metro=metro,
                state=state,
                salary_min=salary_min,
                salary_max=salary_max,
                midpoint=(salary_min + salary_max) // 2,
                source='Lever',
                job_url=job_url,
                posted_date=datetime.now().strftime('%Y-%m-%d'),
            ))

    return jobs


# ─────────────────────────────────────────────────────────────────────────────
# ASHBY
# ─────────────────────────────────────────────────────────────────────────────

def scrape_ashby(company: str) -> list[CompRecord]:
    """Scrape jobs from an Ashby job board."""
    jobs = []
    url = f"https://api.ashbyhq.com/posting-api/job-board/{company}"

    response = fetch_with_retry(url)
    if not response or response.status_code != 200:
        return jobs

    try:
        data = response.json()
    except ValueError:
        return jobs

    for job in data.get('jobs', []):
        title = job.get('title', '')
        location = job.get('location', '')
        job_url = job.get('jobUrl', '')

        # Check structured compensation field first
        comp_info = job.get('compensation', {}) or {}
        salary_min = comp_info.get('min')
        salary_max = comp_info.get('max')

        # Fall back to description parsing
        if not salary_min or not salary_max:
            description = job.get('description', '') or ''
            salary_min, salary_max = parse_salary(description)

        if salary_min and salary_max:
            metro, state = parse_location(location)
            jobs.append(CompRecord(
                company=company.replace('-', ' ').title(),
                title=title,
                family=classify_job_family(title),
                metro=metro,
                state=state,
                salary_min=int(salary_min),
                salary_max=int(salary_max),
                midpoint=(int(salary_min) + int(salary_max)) // 2,
                source='Ashby',
                job_url=job_url,
                posted_date=datetime.now().strftime('%Y-%m-%d'),
            ))

    return jobs


# ─────────────────────────────────────────────────────────────────────────────
# WORKDAY
# ─────────────────────────────────────────────────────────────────────────────

def scrape_workday(company: str, base_url: str) -> list[CompRecord]:
    """Scrape jobs from a Workday job board."""
    jobs = []

    payload = {"limit": 100, "offset": 0, "searchText": ""}
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

    response = fetch_with_retry(base_url, method='POST', json=payload, headers=headers)
    if not response or response.status_code != 200:
        return jobs

    try:
        data = response.json()
    except ValueError:
        return jobs

    for job in data.get('jobPostings', []):
        title = job.get('title', '')
        location = job.get('locationsText', '')
        job_url = job.get('externalPath', '')

        bullet_fields = job.get('bulletFields', [])
        full_text = ' '.join(str(f) for f in bullet_fields)
        salary_min, salary_max = parse_salary(full_text)

        if salary_min and salary_max:
            metro, state = parse_location(location)
            jobs.append(CompRecord(
                company=company.title(),
                title=title,
                family=classify_job_family(title),
                metro=metro,
                state=state,
                salary_min=salary_min,
                salary_max=salary_max,
                midpoint=(salary_min + salary_max) // 2,
                source='Workday',
                job_url=job_url,
                posted_date=datetime.now().strftime('%Y-%m-%d'),
            ))

    return jobs


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("BANDED JOB BOARD SCRAPER")
    log.info("=" * 60)

    all_jobs = []
    total_errors = 0

    # Greenhouse
    log.info("\n[Greenhouse] Scraping %d companies...", len(GREENHOUSE_COMPANIES))
    for company in GREENHOUSE_COMPANIES:
        try:
            jobs = scrape_greenhouse(company)
            if jobs:
                log.info("  %s: %d jobs with salary", company, len(jobs))
                all_jobs.extend(jobs)
        except Exception as e:
            log.error("  %s: %s", company, e)
            total_errors += 1
        time.sleep(0.5)

    # Lever
    log.info("\n[Lever] Scraping %d companies...", len(LEVER_COMPANIES))
    for company in LEVER_COMPANIES:
        try:
            jobs = scrape_lever(company)
            if jobs:
                log.info("  %s: %d jobs with salary", company, len(jobs))
                all_jobs.extend(jobs)
        except Exception as e:
            log.error("  %s: %s", company, e)
            total_errors += 1
        time.sleep(0.5)

    # Ashby
    log.info("\n[Ashby] Scraping %d companies...", len(ASHBY_COMPANIES))
    for company in ASHBY_COMPANIES:
        try:
            jobs = scrape_ashby(company)
            if jobs:
                log.info("  %s: %d jobs with salary", company, len(jobs))
                all_jobs.extend(jobs)
        except Exception as e:
            log.error("  %s: %s", company, e)
            total_errors += 1
        time.sleep(0.5)

    # Workday
    log.info("\n[Workday] Scraping %d companies...", len(WORKDAY_COMPANIES))
    for company, url in WORKDAY_COMPANIES.items():
        try:
            jobs = scrape_workday(company, url)
            if jobs:
                log.info("  %s: %d jobs with salary", company, len(jobs))
                all_jobs.extend(jobs)
        except Exception as e:
            log.error("  %s: %s", company, e)
            total_errors += 1
        time.sleep(0.5)

    # Summary and upload
    log.info("\n" + "=" * 60)
    log.info("Total jobs with salary data: %d", len(all_jobs))

    inserted = 0
    if all_jobs:
        log.info("Uploading to Supabase (upsert)...")
        records = [j.to_db_dict() for j in all_jobs]
        inserted = upload_to_supabase(records)

    log_scrape_run('Job Boards', inserted, len(all_jobs), total_errors)

    log.info("=" * 60)
    log.info("Complete. %d inserted, %d errors.", inserted, total_errors)


if __name__ == "__main__":
    main()
