"""
Banded Job Board Scraper
Scrapes Greenhouse, Lever, Ashby, and Workday job boards for salary data.
Runs via GitHub Actions and saves to Supabase.
"""

import os
import re
import json
import time
import requests
from datetime import datetime
from typing import Optional, Dict, List
from dataclasses import dataclass, asdict

# Supabase config (set via environment variables)
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://xrcgtkkaapfmzzjvyphu.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')

@dataclass
class JobPosting:
    company: str
    title: str
    family: Optional[str]
    metro: Optional[str]
    state: Optional[str]
    salary_min: Optional[int]
    salary_max: Optional[int]
    midpoint: Optional[int]
    source: str
    job_url: str
    posted_date: Optional[str]

# ─────────────────────────────────────────────────────────────────────────────
# SALARY PARSING
# ─────────────────────────────────────────────────────────────────────────────

def parse_salary(text: str) -> tuple[Optional[int], Optional[int]]:
    """Extract salary range from text. Returns (min, max) or (None, None)."""
    if not text:
        return None, None
    
    text = text.lower().replace(',', '').replace('$', '')
    
    # Patterns to match salary ranges
    patterns = [
        # "$120,000 - $150,000" or "120000-150000"
        r'(\d{2,3})[\s,]*(?:000|k)?\s*[-–to]+\s*(\d{2,3})[\s,]*(?:000|k)?',
        # "$120k - $150k"
        r'(\d{2,3})k\s*[-–to]+\s*(\d{2,3})k',
        # "120,000 to 150,000"
        r'(\d{3,})\s*(?:to|-)\s*(\d{3,})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            val1, val2 = int(match.group(1)), int(match.group(2))
            # Normalize to full dollar amounts
            if val1 < 1000:
                val1 *= 1000
            if val2 < 1000:
                val2 *= 1000
            # Sanity check: reasonable salary range
            if 20000 <= val1 <= 1500000 and 20000 <= val2 <= 1500000:
                return min(val1, val2), max(val1, val2)
    
    return None, None

def classify_job_family(title: str) -> Optional[str]:
    """Classify job title into a job family."""
    title_lower = title.lower()
    
    mappings = {
        'Software Engineering': ['engineer', 'developer', 'swe', 'frontend', 'backend', 'fullstack', 'devops', 'sre', 'platform'],
        'Product Management': ['product manager', 'product lead', 'pm', 'product owner'],
        'Data Science': ['data scientist', 'machine learning', 'ml engineer', 'ai ', 'data analyst', 'analytics'],
        'Design': ['designer', 'ux', 'ui', 'design lead', 'creative'],
        'Marketing': ['marketing', 'growth', 'brand', 'content', 'seo', 'demand gen'],
        'Sales': ['sales', 'account executive', 'ae ', 'sdr', 'bdr', 'revenue'],
        'People / HR': ['recruiter', 'people', 'hr ', 'human resources', 'talent', 'compensation'],
        'Finance': ['finance', 'accountant', 'controller', 'fp&a', 'financial'],
    }
    
    for family, keywords in mappings.items():
        if any(kw in title_lower for kw in keywords):
            return family
    
    return None

def parse_location(location: str) -> tuple[Optional[str], Optional[str]]:
    """Extract metro and state from location string."""
    if not location:
        return None, None
    
    # Common patterns
    us_states = {
        'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
        'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
        'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
        'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
        'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri',
        'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
        'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio',
        'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
        'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont',
        'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming',
        'DC': 'District of Columbia'
    }
    
    metro_mappings = {
        'san francisco': ('San Francisco', 'CA'),
        'sf': ('San Francisco', 'CA'),
        'new york': ('New York', 'NY'),
        'nyc': ('New York', 'NY'),
        'seattle': ('Seattle', 'WA'),
        'austin': ('Austin', 'TX'),
        'denver': ('Denver', 'CO'),
        'boston': ('Boston', 'MA'),
        'chicago': ('Chicago', 'IL'),
        'los angeles': ('Los Angeles', 'CA'),
        'la': ('Los Angeles', 'CA'),
        'miami': ('Miami', 'FL'),
        'atlanta': ('Atlanta', 'GA'),
        'portland': ('Portland', 'OR'),
        'raleigh': ('Raleigh', 'NC'),
        'remote': ('Remote', 'US'),
    }
    
    location_lower = location.lower()
    
    # Check metro mappings first
    for key, (metro, state) in metro_mappings.items():
        if key in location_lower:
            return metro, state
    
    # Check for state abbreviations
    for abbr, name in us_states.items():
        if abbr in location.upper() or name.lower() in location_lower:
            return None, abbr
    
    return None, None

# ─────────────────────────────────────────────────────────────────────────────
# GREENHOUSE SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

# Companies using Greenhouse (add more as needed)
GREENHOUSE_COMPANIES = [
    'stripe', 'figma', 'notion', 'airbnb', 'coinbase', 'plaid', 'ramp', 'brex',
    'datadog', 'snowflake', 'databricks', 'mongodb', 'elastic', 'hashicorp',
    'twilio', 'sendgrid', 'segment', 'amplitude', 'mixpanel',
    'dropbox', 'slack', 'zoom', 'asana', 'monday', 'clickup',
    'anthropic', 'openai', 'cohere', 'huggingface',
    'robinhood', 'chime', 'sofi', 'affirm', 'klarna',
    'doordash', 'instacart', 'uber', 'lyft',
    'netflix', 'spotify', 'discord', 'reddit', 'pinterest', 'snap',
    'cloudflare', 'vercel', 'netlify', 'supabase',
    'rippling', 'gusto', 'deel', 'remote', 'oyster',
    'linear', 'retool', 'airtable', 'coda',
]

def scrape_greenhouse(company: str) -> List[JobPosting]:
    """Scrape jobs from a Greenhouse job board."""
    jobs = []
    url = f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs"
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            print(f"  [Greenhouse] {company}: HTTP {response.status_code}")
            return jobs
        
        data = response.json()
        
        for job in data.get('jobs', []):
            title = job.get('title', '')
            location = job.get('location', {}).get('name', '')
            job_url = job.get('absolute_url', '')
            
            # Get job details for salary info
            job_id = job.get('id')
            if job_id:
                detail_url = f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs/{job_id}"
                try:
                    detail_resp = requests.get(detail_url, timeout=30)
                    if detail_resp.status_code == 200:
                        detail = detail_resp.json()
                        content = detail.get('content', '') or ''
                        salary_min, salary_max = parse_salary(content)
                        
                        if salary_min and salary_max:
                            metro, state = parse_location(location)
                            jobs.append(JobPosting(
                                company=company.title(),
                                title=title,
                                family=classify_job_family(title),
                                metro=metro,
                                state=state,
                                salary_min=salary_min,
                                salary_max=salary_max,
                                midpoint=(salary_min + salary_max) // 2,
                                source='Greenhouse',
                                job_url=job_url,
                                posted_date=datetime.now().strftime('%Y-%m-%d')
                            ))
                except Exception as e:
                    pass
            
            time.sleep(0.2)  # Rate limiting
            
    except Exception as e:
        print(f"  [Greenhouse] {company}: Error - {e}")
    
    return jobs

# ─────────────────────────────────────────────────────────────────────────────
# LEVER SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

LEVER_COMPANIES = [
    'netflix', 'twitch', 'palantir', 'flexport', 'faire', 'anduril',
    'scale', 'weights-and-biases', 'labelbox', 'snorkel',
    'nerdwallet', 'creditkarma', 'betterment', 'wealthfront',
    'carta', 'equity-methods', 'shareworks',
    'lattice', 'culture-amp', '15five', 'betterworks',
    'grammarly', 'notion', 'roam', 'mem',
]

def scrape_lever(company: str) -> List[JobPosting]:
    """Scrape jobs from a Lever job board."""
    jobs = []
    url = f"https://api.lever.co/v0/postings/{company}"
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            print(f"  [Lever] {company}: HTTP {response.status_code}")
            return jobs
        
        data = response.json()
        
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
                jobs.append(JobPosting(
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
                    posted_date=datetime.now().strftime('%Y-%m-%d')
                ))
            
            time.sleep(0.1)
            
    except Exception as e:
        print(f"  [Lever] {company}: Error - {e}")
    
    return jobs

# ─────────────────────────────────────────────────────────────────────────────
# ASHBY SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

ASHBY_COMPANIES = [
    'ramp', 'linear', 'vercel', 'railway', 'render', 'fly',
    'resend', 'clerk', 'inngest', 'trigger',
    'liveblocks', 'tiptap', 'plasmic',
    'cal', 'dub', 'papermark',
]

def scrape_ashby(company: str) -> List[JobPosting]:
    """Scrape jobs from an Ashby job board."""
    jobs = []
    url = f"https://api.ashbyhq.com/posting-api/job-board/{company}"
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            print(f"  [Ashby] {company}: HTTP {response.status_code}")
            return jobs
        
        data = response.json()
        
        for job in data.get('jobs', []):
            title = job.get('title', '')
            location = job.get('location', '')
            job_url = job.get('jobUrl', '')
            
            # Check for compensation in job info
            comp_info = job.get('compensation', {})
            salary_min = comp_info.get('min')
            salary_max = comp_info.get('max')
            
            # Also check description
            if not salary_min or not salary_max:
                description = job.get('description', '') or ''
                salary_min, salary_max = parse_salary(description)
            
            if salary_min and salary_max:
                metro, state = parse_location(location)
                jobs.append(JobPosting(
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
                    posted_date=datetime.now().strftime('%Y-%m-%d')
                ))
            
    except Exception as e:
        print(f"  [Ashby] {company}: Error - {e}")
    
    return jobs

# ─────────────────────────────────────────────────────────────────────────────
# WORKDAY SCRAPER (more complex, company-specific URLs)
# ─────────────────────────────────────────────────────────────────────────────

WORKDAY_COMPANIES = {
    'salesforce': 'https://salesforce.wd12.myworkdayjobs.com/wday/cxs/salesforce/External_Career_Site/jobs',
    'adobe': 'https://adobe.wd5.myworkdayjobs.com/wday/cxs/adobe/external_experienced/jobs',
    'visa': 'https://visa.wd5.myworkdayjobs.com/wday/cxs/visa/Visa_Careers/jobs',
    'nvidia': 'https://nvidia.wd5.myworkdayjobs.com/wday/cxs/nvidia/NVIDIAExternalCareerSite/jobs',
}

def scrape_workday(company: str, base_url: str) -> List[JobPosting]:
    """Scrape jobs from a Workday job board."""
    jobs = []
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }
    
    payload = {
        "limit": 100,
        "offset": 0,
        "searchText": "",
    }
    
    try:
        response = requests.post(base_url, json=payload, headers=headers, timeout=30)
        if response.status_code != 200:
            print(f"  [Workday] {company}: HTTP {response.status_code}")
            return jobs
        
        data = response.json()
        
        for job in data.get('jobPostings', []):
            title = job.get('title', '')
            location = job.get('locationsText', '')
            job_url = job.get('externalPath', '')
            
            # Workday sometimes includes salary in bulletFields
            bullet_fields = job.get('bulletFields', [])
            full_text = ' '.join(bullet_fields)
            salary_min, salary_max = parse_salary(full_text)
            
            if salary_min and salary_max:
                metro, state = parse_location(location)
                jobs.append(JobPosting(
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
                    posted_date=datetime.now().strftime('%Y-%m-%d')
                ))
            
    except Exception as e:
        print(f"  [Workday] {company}: Error - {e}")
    
    return jobs

# ─────────────────────────────────────────────────────────────────────────────
# SUPABASE UPLOAD
# ─────────────────────────────────────────────────────────────────────────────

def upload_to_supabase(jobs: List[JobPosting]) -> int:
    """Upload job postings to Supabase. Returns count of inserted records."""
    if not jobs:
        return 0
    
    if not SUPABASE_KEY:
        print("Warning: SUPABASE_KEY not set. Skipping upload.")
        return 0
    
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal'
    }
    
    # Convert to database format
    records = []
    for job in jobs:
        records.append({
            'company': job.company,
            'title': job.title,
            'family': job.family,
            'metro': job.metro,
            'state': job.state,
            'salary_min': job.salary_min,
            'salary_max': job.salary_max,
            'midpoint': job.midpoint,
            'source': job.source,
            'posted_date': job.posted_date,
            'status': 'approved',
        })
    
    url = f"{SUPABASE_URL}/rest/v1/comp_data"
    
    try:
        # Upload in batches of 100
        inserted = 0
        for i in range(0, len(records), 100):
            batch = records[i:i+100]
            response = requests.post(url, json=batch, headers=headers)
            if response.status_code in [200, 201]:
                inserted += len(batch)
            else:
                print(f"  Upload error: {response.status_code} - {response.text}")
        return inserted
    except Exception as e:
        print(f"  Upload error: {e}")
        return 0

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("BANDED JOB SCRAPER")
    print(f"Started at: {datetime.now().isoformat()}")
    print("=" * 60)
    
    all_jobs = []
    
    # Greenhouse
    print("\n[Greenhouse] Scraping...")
    for company in GREENHOUSE_COMPANIES:
        jobs = scrape_greenhouse(company)
        if jobs:
            print(f"  {company}: {len(jobs)} jobs with salary")
            all_jobs.extend(jobs)
        time.sleep(0.5)
    
    # Lever
    print("\n[Lever] Scraping...")
    for company in LEVER_COMPANIES:
        jobs = scrape_lever(company)
        if jobs:
            print(f"  {company}: {len(jobs)} jobs with salary")
            all_jobs.extend(jobs)
        time.sleep(0.5)
    
    # Ashby
    print("\n[Ashby] Scraping...")
    for company in ASHBY_COMPANIES:
        jobs = scrape_ashby(company)
        if jobs:
            print(f"  {company}: {len(jobs)} jobs with salary")
            all_jobs.extend(jobs)
        time.sleep(0.5)
    
    # Workday
    print("\n[Workday] Scraping...")
    for company, url in WORKDAY_COMPANIES.items():
        jobs = scrape_workday(company, url)
        if jobs:
            print(f"  {company}: {len(jobs)} jobs with salary")
            all_jobs.extend(jobs)
        time.sleep(0.5)
    
    # Summary
    print("\n" + "=" * 60)
    print(f"Total jobs with salary data: {len(all_jobs)}")
    
    # Upload to Supabase
    if all_jobs:
        print("\nUploading to Supabase...")
        inserted = upload_to_supabase(all_jobs)
        print(f"Inserted {inserted} records")
    
    print("\n" + "=" * 60)
    print(f"Completed at: {datetime.now().isoformat()}")
    print("=" * 60)

if __name__ == "__main__":
    main()
