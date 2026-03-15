"""
career_page_scraper.py
Scrapes salary and job data directly from company career pages.
Tier 2 SaaS/tech peers for DAT compensation benchmarking.

Runs via GitHub Actions (weekly for Tier 2) and saves to Supabase jobs table.
"""

import os
import re
import sys
import json
import time
import requests
from bs4 import BeautifulSoup

# Force UTF-8 output on Windows (avoids cp1252 encoding errors)
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from datetime import datetime
from typing import Optional, List
from dataclasses import dataclass, field

from salary_parser import parse_salary, format_salary_raw
from skills_extractor import extract_skills, extract_experience_years, extract_employment_type

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://xrcgtkkaapfmzzjvyphu.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/122.0.0.0 Safari/537.36'
    ),
    'Accept': 'application/json, text/html, */*',
    'Accept-Language': 'en-US,en;q=0.9',
}

REQUEST_TIMEOUT = 15
DELAY_BETWEEN_REQUESTS = 0.5  # seconds


@dataclass
class CareerJob:
    company: str
    title: str
    location: Optional[str] = None
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    salary_raw: Optional[str] = None
    job_url: Optional[str] = None
    jd_text: Optional[str] = None
    skills: List[str] = field(default_factory=list)
    employment_type: Optional[str] = "full-time"
    experience_years_min: Optional[int] = None
    family: Optional[str] = None
    source: str = "Career Page"
    posted_date: Optional[str] = None


# ─────────────────────────────────────────────────────────────────────────────
# TIER 2 — SaaS/Tech peers (weekly scrape)
# ─────────────────────────────────────────────────────────────────────────────

TIER2_COMPANIES = [
    # Greenhouse-based (use standard API)
    {"name": "Workday",          "slug": "workday",          "ats": "greenhouse"},
    {"name": "Coupa",            "slug": "coupa",            "ats": "greenhouse"},

    # Workday-based career sites
    {"name": "Manhattan Associates", "workday_tenant": "manassoc",     "workday_site": "ManAssocCareers", "ats": "workday"},
    {"name": "Blue Yonder",          "workday_tenant": "jda",          "workday_site": "Careers",          "ats": "workday"},

    # Custom career pages
    {"name": "Salesforce",  "careers_url": "https://careers.salesforce.com/en/jobs/?search=&country=United+States&pagesize=20", "ats": "custom"},
    {"name": "Oracle",      "careers_url": "https://careers.oracle.com/jobs/#en/sites/jobsearch/jobs?keyword=&location=United+States", "ats": "custom"},
    {"name": "SAP",         "careers_url": "https://jobs.sap.com/search/?q=&locationsearch=United+States&startrow=0", "ats": "custom"},
    {"name": "Trimble",     "careers_url": "https://careers.trimble.com/jobs/search?q=&l=United+States", "ats": "custom"},
    {"name": "Descartes",   "careers_url": "https://www.descartes.com/careers", "ats": "custom"},
    {"name": "MercuryGate", "careers_url": "https://mercurygate.com/about/careers/", "ats": "custom"},
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 1 — Freight-tech peers (daily scrape via existing scrapers)
# These companies use Greenhouse/Lever and are covered by scraper.py.
# Listed here for reference — add their Greenhouse/Lever slugs to scraper.py.
# ─────────────────────────────────────────────────────────────────────────────

TIER1_COMPANIES_REF = [
    {"name": "Project44",          "slug": "project44",    "ats": "greenhouse"},
    {"name": "FourKites",          "slug": "fourkites",    "ats": "greenhouse"},
    {"name": "Loadsmart",          "slug": "loadsmart",    "ats": "greenhouse"},
    {"name": "Flexport",           "slug": "flexport",     "ats": "greenhouse"},
    {"name": "Transplace",         "slug": "transplace",   "ats": "lever"},
]


# ─────────────────────────────────────────────────────────────────────────────
# JOB FAMILY CLASSIFICATION (shared with scraper.py logic)
# ─────────────────────────────────────────────────────────────────────────────

def classify_family(title: str) -> str:
    t = title.lower()
    if any(k in t for k in ['software', 'engineer', 'developer', 'sre', 'devops', 'backend', 'frontend', 'fullstack', 'full stack', 'platform', 'data engineer']):
        return 'Software Engineering'
    if any(k in t for k in ['product manager', 'product owner', 'product management', 'pm ']):
        return 'Product Management'
    if any(k in t for k in ['data scien', 'machine learning', 'ml ', 'ai ', 'analyst', 'analytics', 'business intelligence', 'bi ']):
        return 'Data Science'
    if any(k in t for k in ['design', 'ux', 'ui ', 'user experience', 'creative']):
        return 'Design'
    if any(k in t for k in ['marketing', 'growth', 'demand gen', 'content', 'brand', 'seo']):
        return 'Marketing'
    if any(k in t for k in ['sales', 'account executive', 'ae ', 'sdr', 'bdr', 'account manager', 'revenue']):
        return 'Sales'
    if any(k in t for k in ['hr', 'people', 'human resources', 'recruiting', 'talent', 'hrbp']):
        return 'People / HR'
    if any(k in t for k in ['finance', 'financial', 'accounting', 'accountant', 'controller', 'fp&a', 'treasury']):
        return 'Finance'
    if any(k in t for k in ['customer success', 'customer support', 'csm', 'implementation', 'solutions engineer']):
        return 'Customer Success'
    if any(k in t for k in ['legal', 'compliance', 'counsel', 'paralegal']):
        return 'Legal'
    return 'Operations'


def parse_location(loc_text: str) -> str:
    """Normalize location text."""
    if not loc_text:
        return ''
    loc = loc_text.strip()
    # Common remote indicators
    if any(r in loc.lower() for r in ['remote', 'anywhere', 'distributed', 'work from home']):
        return 'Remote'
    return loc


# ─────────────────────────────────────────────────────────────────────────────
# GREENHOUSE SCRAPER (standard API)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_greenhouse_company(company_name: str, slug: str) -> List[CareerJob]:
    """Scrape a Greenhouse-based company. Reuses same API as scraper.py."""
    jobs = []
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            print(f"    {company_name}: HTTP {resp.status_code}")
            return []
        data = resp.json()
    except Exception as e:
        print(f"    {company_name}: {e}")
        return []

    for job in data.get('jobs', []):
        content = job.get('content', '') or ''
        salary_min, salary_max = parse_salary(content)
        if not salary_min and not salary_max:
            # Also check metadata
            for q in job.get('questions', []):
                salary_min, salary_max = parse_salary(str(q))
                if salary_min or salary_max:
                    break

        job_id = job.get('id', '')
        job_url = f"https://boards.greenhouse.io/{slug}/jobs/{job_id}"

        title = job.get('title', '')
        location = parse_location(
            job.get('location', {}).get('name', '') or
            ', '.join(l.get('name', '') for l in job.get('offices', []))
        )

        skills = extract_skills(content)

        jobs.append(CareerJob(
            company=company_name,
            title=title,
            location=location,
            salary_min=salary_min,
            salary_max=salary_max,
            salary_raw=format_salary_raw(salary_min, salary_max),
            job_url=job_url,
            jd_text=content[:5000],
            skills=skills,
            employment_type=extract_employment_type(content),
            experience_years_min=extract_experience_years(content),
            family=classify_family(title),
            source='Greenhouse',
            posted_date=datetime.now().strftime('%Y-%m-%d'),
        ))

    return jobs


# ─────────────────────────────────────────────────────────────────────────────
# WORKDAY SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

def scrape_workday_company(company_name: str, tenant: str, site: str) -> List[CareerJob]:
    """
    Scrape a Workday-based career site.
    Workday exposes a semi-public search endpoint at:
    https://{tenant}.wd{n}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs
    """
    jobs = []

    data = None
    # Try common Workday API versions (wd1, wd5)
    for wd_ver in ('wd1', 'wd5'):
        api_url = f"https://{tenant}.{wd_ver}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs"
        payload = {"limit": 100, "offset": 0, "searchText": "", "locations": []}

        try:
            resp = requests.post(
                api_url,
                json=payload,
                headers={**HEADERS, 'Content-Type': 'application/json'},
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 200:
                data = resp.json()
                break
        except Exception:
            pass

    if not data:
        print(f"    {company_name}: Workday API unreachable")
        return []

    for job in data.get('jobPostings', []):
        title = job.get('title', '')
        external_path = job.get('externalPath', '')
        job_url = f"https://{tenant}.wd1.myworkdayjobs.com/{site}{external_path}" if external_path else ''

        # Salary often in bulletFields or jobRequisitionId
        content_parts = [
            job.get('jobDescription', {}).get('jobDescription', '') if isinstance(job.get('jobDescription'), dict) else '',
        ]
        # Also check bullet fields
        for field_name in ('briefDescription', 'description'):
            val = job.get(field_name, '')
            if isinstance(val, str):
                content_parts.append(val)

        content = ' '.join(filter(None, content_parts))
        salary_min, salary_max = parse_salary(content)

        location_parts = []
        for loc in job.get('locationsText', '').split(',') if isinstance(job.get('locationsText'), str) else []:
            location_parts.append(loc.strip())

        jobs.append(CareerJob(
            company=company_name,
            title=title,
            location=parse_location(', '.join(location_parts)),
            salary_min=salary_min,
            salary_max=salary_max,
            salary_raw=format_salary_raw(salary_min, salary_max),
            job_url=job_url,
            jd_text=content[:5000],
            skills=extract_skills(content),
            employment_type=extract_employment_type(content),
            experience_years_min=extract_experience_years(content),
            family=classify_family(title),
            source='Workday',
            posted_date=datetime.now().strftime('%Y-%m-%d'),
        ))

    return jobs


# ─────────────────────────────────────────────────────────────────────────────
# CUSTOM CAREER PAGE SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

def scrape_custom_page(company_name: str, url: str) -> List[CareerJob]:
    """
    Scrape a custom career page using BeautifulSoup.
    Extracts job listings and salary data where available.
    This is a best-effort scraper — works for many standard career pages.
    """
    jobs = []

    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            print(f"    {company_name}: HTTP {resp.status_code}")
            return []
        soup = BeautifulSoup(resp.text, 'html.parser')
    except Exception as e:
        print(f"    {company_name}: {e}")
        return []

    # Look for common job listing patterns
    # Most career pages use <li>, <div>, or <a> with job-related class names
    job_elements = []

    # Pattern 1: <a> or <div> with class containing "job" or "posting" or "opening"
    for tag in soup.find_all(['a', 'li', 'div', 'article']):
        class_str = ' '.join(tag.get('class', [])).lower()
        if any(k in class_str for k in ['job', 'posting', 'opening', 'position', 'role', 'career']):
            job_elements.append(tag)

    # Pattern 2: Look for structured data (JSON-LD)
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string or '')
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get('@type') in ('JobPosting', 'jobPosting'):
                    title = item.get('title', '')
                    location = item.get('jobLocation', {})
                    if isinstance(location, dict):
                        addr = location.get('address', {})
                        loc_text = f"{addr.get('addressLocality', '')}, {addr.get('addressRegion', '')}".strip(', ')
                    else:
                        loc_text = ''

                    # Salary from structured data
                    sal = item.get('baseSalary', {})
                    sal_min = sal.get('value', {}).get('minValue') if isinstance(sal.get('value'), dict) else None
                    sal_max = sal.get('value', {}).get('maxValue') if isinstance(sal.get('value'), dict) else None
                    sal_min = int(sal_min) if sal_min else None
                    sal_max = int(sal_max) if sal_max else None

                    description = item.get('description', '')
                    if not sal_min and not sal_max:
                        sal_min, sal_max = parse_salary(description)

                    job_url = item.get('url', item.get('identifier', ''))

                    jobs.append(CareerJob(
                        company=company_name,
                        title=title,
                        location=parse_location(loc_text),
                        salary_min=sal_min,
                        salary_max=sal_max,
                        salary_raw=format_salary_raw(sal_min, sal_max),
                        job_url=job_url,
                        jd_text=description[:5000],
                        skills=extract_skills(description),
                        employment_type=item.get('employmentType', 'full-time').lower(),
                        experience_years_min=extract_experience_years(description),
                        family=classify_family(title),
                        source='Career Page',
                        posted_date=datetime.now().strftime('%Y-%m-%d'),
                    ))
        except (json.JSONDecodeError, AttributeError):
            pass

    # If no structured data, parse visible job cards
    if not jobs and job_elements:
        seen_titles = set()
        for el in job_elements[:100]:
            title_el = el.find(['h1', 'h2', 'h3', 'h4', 'span', 'a'])
            title = title_el.get_text(strip=True) if title_el else ''
            if not title or len(title) < 5 or title in seen_titles:
                continue
            seen_titles.add(title)

            # Try to find salary in the element text
            el_text = el.get_text(' ')
            sal_min, sal_max = parse_salary(el_text)

            # Job URL
            href = el.get('href') or (el.find('a') or {}).get('href', '')
            if href and not href.startswith('http'):
                from urllib.parse import urljoin
                href = urljoin(url, href)

            # Location
            loc_text = ''
            for possible_loc in el.find_all(['span', 'div', 'p']):
                text = possible_loc.get_text(strip=True)
                if any(indicator in text.lower() for indicator in [', ', 'remote', 'hybrid', 'usa', 'united states']):
                    if len(text) < 80:
                        loc_text = text
                        break

            jobs.append(CareerJob(
                company=company_name,
                title=title,
                location=parse_location(loc_text),
                salary_min=sal_min,
                salary_max=sal_max,
                salary_raw=format_salary_raw(sal_min, sal_max),
                job_url=href,
                jd_text='',
                skills=[],
                employment_type='full-time',
                family=classify_family(title),
                source='Career Page',
                posted_date=datetime.now().strftime('%Y-%m-%d'),
            ))

    return jobs


# ─────────────────────────────────────────────────────────────────────────────
# SUPABASE UPLOAD (to 'jobs' table)
# ─────────────────────────────────────────────────────────────────────────────

def upload_jobs_to_supabase(jobs: List[CareerJob]) -> int:
    """Upload career jobs to the Supabase 'jobs' table. Returns inserted count."""
    if not jobs:
        return 0
    if not SUPABASE_KEY:
        print("Warning: SUPABASE_KEY not set. Skipping upload.")
        return 0

    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal',
    }

    records = []
    for job in jobs:
        records.append({
            'company':              job.company,
            'title':               job.title,
            'family':              job.family,
            'metro':               job.location,
            'salary_min':          job.salary_min,
            'salary_max':          job.salary_max,
            'salary_raw':          job.salary_raw,
            'job_url':             job.job_url,
            'skills':              job.skills,
            'employment_type':     job.employment_type,
            'experience_years_min': job.experience_years_min,
            'jd_text':             job.jd_text,
            'source':              job.source,
            'posted_date':         job.posted_date,
            'status':              'approved',
        })

    url = f"{SUPABASE_URL}/rest/v1/jobs"
    inserted = 0

    for i in range(0, len(records), 100):
        batch = records[i:i+100]
        try:
            resp = requests.post(url, json=batch, headers=headers, timeout=30)
            if resp.status_code in (200, 201):
                inserted += len(batch)
            else:
                print(f"  Upload error: {resp.status_code} — {resp.text[:200]}")
        except Exception as e:
            print(f"  Upload error: {e}")

    return inserted


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run_tier2():
    """Scrape all Tier 2 SaaS/tech peer companies."""
    print("=" * 60)
    print("CAREER PAGE SCRAPER — Tier 2 (SaaS Peers)")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)

    all_jobs: List[CareerJob] = []

    for company in TIER2_COMPANIES:
        name = company['name']
        ats = company.get('ats', 'custom')
        print(f"\n[{ats.upper()}] {name}...")

        try:
            if ats == 'greenhouse':
                jobs = scrape_greenhouse_company(name, company['slug'])
            elif ats == 'workday':
                jobs = scrape_workday_company(
                    name,
                    company['workday_tenant'],
                    company['workday_site'],
                )
            else:
                jobs = scrape_custom_page(name, company['careers_url'])

            print(f"  → {len(jobs)} jobs found ({sum(1 for j in jobs if j.salary_min) } with salary)")
            all_jobs.extend(jobs)
        except Exception as e:
            print(f"  Error: {e}")

        time.sleep(DELAY_BETWEEN_REQUESTS)

    print(f"\n{'='*60}")
    print(f"Total jobs scraped: {len(all_jobs)}")
    print(f"Jobs with salary: {sum(1 for j in all_jobs if j.salary_min)}")

    print("\nUploading to Supabase...")
    inserted = upload_jobs_to_supabase(all_jobs)
    print(f"Inserted: {inserted} records")
    print(f"Finished: {datetime.now().isoformat()}")


if __name__ == '__main__':
    run_tier2()
