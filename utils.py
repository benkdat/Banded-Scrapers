"""
Shared utilities for Banded scrapers.
Canonical versions of classification, location parsing, salary parsing, and Supabase upload.
"""

import os
import re
import time
import logging
import requests
from collections import defaultdict
from datetime import datetime
from typing import Optional, List, Tuple
from dataclasses import dataclass, asdict

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('banded')

# ─────────────────────────────────────────────────────────────────────────────
# DATA MODEL
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CompRecord:
    company: str
    title: str
    family: Optional[str]
    metro: Optional[str]
    state: Optional[str]
    salary_min: int
    salary_max: int
    midpoint: int
    source: str
    posted_date: Optional[str] = None
    job_url: Optional[str] = None
    data_year: Optional[int] = None

    def to_db_dict(self) -> dict:
        """Convert to Supabase insert format."""
        return {
            'company': self.company,
            'title': self.title,
            'family': self.family,
            'metro': self.metro,
            'state': self.state,
            'salary_min': self.salary_min,
            'salary_max': self.salary_max,
            'midpoint': self.midpoint,
            'source': self.source,
            'posted_date': self.posted_date or datetime.now().strftime('%Y-%m-%d'),
            'status': 'approved',
        }


# ─────────────────────────────────────────────────────────────────────────────
# JOB FAMILY CLASSIFICATION
# ─────────────────────────────────────────────────────────────────────────────

# Single canonical mapping used by ALL scrapers
JOB_FAMILY_KEYWORDS = {
    'Software Engineering': [
        'software engineer', 'developer', 'swe', 'frontend', 'backend', 'fullstack',
        'full-stack', 'full stack', 'devops', 'sre', 'site reliability', 'platform engineer',
        'infrastructure engineer', 'systems engineer', 'architect', 'programmer',
        'mobile engineer', 'ios engineer', 'android engineer', 'embedded engineer',
        'firmware', 'qa engineer', 'test engineer', 'security engineer',
    ],
    'Product Management': [
        'product manager', 'product lead', 'product owner', 'product director',
        'product vp', 'vp product', 'head of product', 'technical program manager',
        'program manager',
    ],
    'Data Science': [
        'data scientist', 'machine learning', 'ml engineer', 'ai engineer',
        'artificial intelligence', 'data analyst', 'analytics engineer',
        'research scientist', 'applied scientist', 'data engineer',
        'business intelligence', 'bi analyst', 'statistician',
    ],
    'Design': [
        'designer', 'ux ', 'ui ', 'ux/', 'ui/', 'design lead', 'design manager',
        'creative director', 'visual design', 'product design', 'interaction design',
        'brand design', 'design system',
    ],
    'Marketing': [
        'marketing', 'growth', 'brand manager', 'content', 'seo', 'sem',
        'demand gen', 'communications', 'public relations', 'pr manager',
        'social media', 'email marketing', 'lifecycle',
    ],
    'Sales': [
        'sales', 'account executive', 'account manager', ' ae ', 'sdr', 'bdr',
        'business development', 'revenue', 'solutions engineer', 'sales engineer',
        'enterprise rep', 'inside sales',
    ],
    'People / HR': [
        'recruiter', 'recruiting', 'people ops', 'people operations',
        'hr ', 'human resources', 'talent', 'compensation', 'hrbp',
        'benefits', 'dei ', 'diversity', 'learning & development',
        'total rewards', 'people partner', 'employee experience',
    ],
    'Finance': [
        'finance', 'accountant', 'accounting', 'controller', 'fp&a',
        'financial analyst', 'financial planning', 'tax ', 'audit',
        'treasury', 'investor relations', 'cfo ', 'bookkeeper',
    ],
    'Customer Success': [
        'customer success', 'csm', 'customer support', 'support engineer',
        'technical support', 'customer experience', 'client success',
        'implementation', 'onboarding specialist',
    ],
    'Operations': [
        'operations', 'ops manager', 'chief of staff', 'strategy',
        'business operations', 'revenue operations', 'revops',
        'supply chain', 'procurement', 'logistics',
    ],
    'Legal': [
        'legal', 'counsel', 'attorney', 'lawyer', 'compliance',
        'regulatory', 'paralegal', 'ip counsel', 'privacy',
    ],
    'Executive': [
        'chief executive', 'chief financial', 'chief technology',
        'chief operating', 'chief product', 'chief marketing',
        'chief people', 'chief revenue', 'president', 'ceo', 'cfo', 'cto', 'coo',
    ],
}


def classify_job_family(title: str) -> str:
    """Classify a job title into a job family. Returns 'Other' if no match."""
    if not title:
        return 'Other'
    title_lower = f' {title.lower()} '  # pad with spaces for word boundary matching

    for family, keywords in JOB_FAMILY_KEYWORDS.items():
        if any(kw in title_lower for kw in keywords):
            return family

    return 'Other'


# ─────────────────────────────────────────────────────────────────────────────
# LOCATION / METRO PARSING
# ─────────────────────────────────────────────────────────────────────────────

METRO_MAP = {
    # Bay Area
    'san francisco': ('San Francisco', 'CA'),
    'san jose': ('San Francisco', 'CA'),
    'mountain view': ('San Francisco', 'CA'),
    'palo alto': ('San Francisco', 'CA'),
    'sunnyvale': ('San Francisco', 'CA'),
    'menlo park': ('San Francisco', 'CA'),
    'redwood city': ('San Francisco', 'CA'),
    'cupertino': ('San Francisco', 'CA'),
    'santa clara': ('San Francisco', 'CA'),
    'oakland': ('San Francisco', 'CA'),
    'fremont': ('San Francisco', 'CA'),
    'sf': ('San Francisco', 'CA'),
    # NYC
    'new york': ('New York', 'NY'),
    'brooklyn': ('New York', 'NY'),
    'manhattan': ('New York', 'NY'),
    'nyc': ('New York', 'NY'),
    'jersey city': ('New York', 'NJ'),
    # Seattle
    'seattle': ('Seattle', 'WA'),
    'bellevue': ('Seattle', 'WA'),
    'redmond': ('Seattle', 'WA'),
    'kirkland': ('Seattle', 'WA'),
    # Austin
    'austin': ('Austin', 'TX'),
    # Denver
    'denver': ('Denver', 'CO'),
    'boulder': ('Denver', 'CO'),
    'broomfield': ('Denver', 'CO'),
    'littleton': ('Denver', 'CO'),
    # Boston
    'boston': ('Boston', 'MA'),
    'cambridge': ('Boston', 'MA'),
    'somerville': ('Boston', 'MA'),
    # Chicago
    'chicago': ('Chicago', 'IL'),
    'evanston': ('Chicago', 'IL'),
    # LA
    'los angeles': ('Los Angeles', 'CA'),
    'santa monica': ('Los Angeles', 'CA'),
    'culver city': ('Los Angeles', 'CA'),
    'la': ('Los Angeles', 'CA'),
    # Other major metros
    'miami': ('Miami', 'FL'),
    'atlanta': ('Atlanta', 'GA'),
    'portland': ('Portland', 'OR'),
    'beaverton': ('Portland', 'OR'),
    'raleigh': ('Raleigh', 'NC'),
    'durham': ('Raleigh', 'NC'),
    'washington': ('Washington DC', 'DC'),
    'arlington': ('Washington DC', 'VA'),
    'alexandria': ('Washington DC', 'VA'),
    'minneapolis': ('Minneapolis', 'MN'),
    'dallas': ('Dallas', 'TX'),
    'san diego': ('San Diego', 'CA'),
    'philadelphia': ('Philadelphia', 'PA'),
    'detroit': ('Detroit', 'MI'),
    'phoenix': ('Phoenix', 'AZ'),
    'salt lake': ('Salt Lake City', 'UT'),
    # Remote
    'remote': ('Remote', 'US'),
}

US_STATES = {
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
    'DC': 'District of Columbia',
}


def parse_location(location: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract (metro, state) from a location string."""
    if not location:
        return None, None

    location_lower = location.lower().strip()

    # Check metro mappings first (most specific)
    for key, (metro, state) in METRO_MAP.items():
        if key in location_lower:
            return metro, state

    # Check for state abbreviations or names
    for abbr, name in US_STATES.items():
        if f', {abbr.lower()}' in location_lower or f' {abbr.lower()}' == location_lower[-3:]:
            return None, abbr
        if name.lower() in location_lower:
            return None, abbr

    return None, None


def parse_metro_from_city(city: str, state: str = '') -> Optional[str]:
    """Map a city name to its metro area. Used for government data with separate city/state fields."""
    if not city:
        return None

    city_lower = city.lower().strip()

    for key, (metro, _) in METRO_MAP.items():
        if key in city_lower:
            return metro

    # Return the city itself if no metro match
    return city.strip().title() if city.strip() else None


# ─────────────────────────────────────────────────────────────────────────────
# SALARY PARSING
# ─────────────────────────────────────────────────────────────────────────────

def parse_salary(text: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Extract salary range from text. Returns (min, max) or (None, None).
    Handles formats like:
      $120,000 - $150,000
      $120K - $150K
      120k-150k
      $65/hr - $85/hr
      120000 to 150000
      $150,000 per year
    """
    if not text:
        return None, None

    text = text.lower()

    # ── Pattern 1: Explicit range with full dollar amounts ──
    # "$120,000 - $150,000" or "120,000 to 150,000"
    match = re.search(
        r'\$?\s*([\d,]+)\s*(?:[-–—to]+|through)\s*\$?\s*([\d,]+)',
        text
    )
    if match:
        val1 = int(match.group(1).replace(',', ''))
        val2 = int(match.group(2).replace(',', ''))

        # Both already full amounts
        if val1 >= 20000 and val2 >= 20000:
            if 20000 <= val1 <= 1500000 and 20000 <= val2 <= 1500000:
                return min(val1, val2), max(val1, val2)

    # ── Pattern 2: K notation range ──
    # "$120K - $150K" or "120k-150k"
    match = re.search(
        r'\$?\s*(\d{2,4})[\s]*k\s*[-–—to]+\s*\$?\s*(\d{2,4})[\s]*k',
        text
    )
    if match:
        val1 = int(match.group(1)) * 1000
        val2 = int(match.group(2)) * 1000
        if 20000 <= val1 <= 1500000 and 20000 <= val2 <= 1500000:
            return min(val1, val2), max(val1, val2)

    # ── Pattern 3: Hourly range ──
    # "$65/hr - $85/hr" or "$65 - $85 per hour"
    match = re.search(
        r'\$?\s*(\d{2,3}(?:\.\d{1,2})?)\s*(?:/\s*h(?:ou)?r|per\s+hour)?\s*[-–—to]+\s*\$?\s*(\d{2,3}(?:\.\d{1,2})?)\s*(?:/\s*h(?:ou)?r|per\s+hour)',
        text
    )
    if match:
        val1 = int(float(match.group(1)) * 2080)
        val2 = int(float(match.group(2)) * 2080)
        if 20000 <= val1 <= 500000 and 20000 <= val2 <= 500000:
            return min(val1, val2), max(val1, val2)

    # ── Pattern 4: Single hourly rate ──
    match = re.search(r'\$?\s*(\d{2,3}(?:\.\d{1,2})?)\s*(?:/\s*h(?:ou)?r|per\s+hour)', text)
    if match:
        annual = int(float(match.group(1)) * 2080)
        if 20000 <= annual <= 500000:
            return annual, annual

    # ── Pattern 5: Single salary with "per year" or "annually" ──
    match = re.search(r'\$?\s*([\d,]+)\s*(?:per\s+year|annually|annual|/\s*(?:yr|year))', text)
    if match:
        val = int(match.group(1).replace(',', ''))
        if 20000 <= val <= 1500000:
            return val, val

    return None, None


# ─────────────────────────────────────────────────────────────────────────────
# DATA QUALITY — Per-Family Salary Bounds & Outlier Detection
# ─────────────────────────────────────────────────────────────────────────────

FAMILY_SALARY_BOUNDS = {
    'Software Engineering': (45_000, 500_000),
    'Product Management':   (50_000, 450_000),
    'Data Science':         (45_000, 450_000),
    'Design':               (40_000, 350_000),
    'Marketing':            (35_000, 350_000),
    'Sales':                (30_000, 500_000),
    'People / HR':          (35_000, 350_000),
    'Finance':              (40_000, 400_000),
    'Customer Success':     (35_000, 250_000),
    'Operations':           (35_000, 300_000),
    'Legal':                (50_000, 450_000),
    'Executive':            (80_000, 1_000_000),
    'Other':                (25_000, 500_000),
}

DEFAULT_BOUNDS = (25_000, 500_000)


def _percentile(sorted_vals: list, pct: float) -> float:
    """Simple percentile calculation on a pre-sorted list."""
    if not sorted_vals:
        return 0
    idx = (len(sorted_vals) - 1) * pct
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def validate_and_filter(records: list) -> list:
    """
    Two-pass data quality gate:
      1. Per-family salary bounds — reject records outside realistic range
      2. IQR outlier detection — reject statistical outliers within each family
    Returns filtered list. Logs rejection counts.
    """
    if not records:
        return records

    original = len(records)

    # Pass 1: Per-family bounds
    bounded = []
    bounds_rejected = 0
    for r in records:
        fam = r.family or 'Other'
        lo, hi = FAMILY_SALARY_BOUNDS.get(fam, DEFAULT_BOUNDS)
        if lo <= r.midpoint <= hi:
            bounded.append(r)
        else:
            bounds_rejected += 1

    if bounds_rejected:
        log.info(f"  Bounds filter: rejected {bounds_rejected} records outside family salary ranges")

    # Pass 2: IQR outlier detection per family
    by_family = defaultdict(list)
    for r in bounded:
        by_family[r.family or 'Other'].append(r)

    clean = []
    iqr_rejected = 0
    for fam, fam_records in by_family.items():
        if len(fam_records) < 10:
            clean.extend(fam_records)
            continue

        mids = sorted(r.midpoint for r in fam_records)
        q1 = _percentile(mids, 0.25)
        q3 = _percentile(mids, 0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        for r in fam_records:
            if lower <= r.midpoint <= upper:
                clean.append(r)
            else:
                iqr_rejected += 1

    if iqr_rejected:
        log.info(f"  IQR filter: rejected {iqr_rejected} statistical outliers")

    total_rejected = original - len(clean)
    if total_rejected:
        log.info(f"  Data quality: {len(clean)}/{original} records passed ({total_rejected} filtered)")
    else:
        log.info(f"  Data quality: all {original} records passed")

    return clean


# ─────────────────────────────────────────────────────────────────────────────
# SUPABASE UPLOAD (with upsert / dedup)
# ─────────────────────────────────────────────────────────────────────────────

def upload_to_supabase(
    records: List[dict],
    batch_size: int = 200,
    upsert: bool = True,
    on_conflict: str = 'company,title,source,metro',
) -> int:
    """
    Upload records to Supabase comp_data table.
    Uses upsert by default to prevent duplicates.
    Returns count of inserted/updated records.
    """
    if not records:
        return 0

    if not SUPABASE_URL or not SUPABASE_KEY:
        log.warning("SUPABASE_URL or SUPABASE_KEY not set. Skipping upload.")
        return 0

    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal',
    }

    if upsert:
        headers['Prefer'] = f'resolution=merge-duplicates,return=minimal'

    url = f"{SUPABASE_URL}/rest/v1/comp_data"
    if upsert and on_conflict:
        url += f"?on_conflict={on_conflict}"

    inserted = 0
    errors = 0

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            response = requests.post(url, json=batch, headers=headers, timeout=60)
            if response.status_code in [200, 201]:
                inserted += len(batch)
            else:
                errors += 1
                log.error(f"Upload batch {i//batch_size + 1}: HTTP {response.status_code} - {response.text[:200]}")
        except requests.exceptions.RequestException as e:
            errors += 1
            log.error(f"Upload batch {i//batch_size + 1}: {e}")

        if inserted % 1000 == 0 and inserted > 0:
            log.info(f"  Uploaded {inserted}/{len(records)} records...")

    if errors:
        log.warning(f"Upload complete with {errors} batch errors. {inserted}/{len(records)} succeeded.")
    else:
        log.info(f"Upload complete: {inserted} records.")

    return inserted


def log_scrape_run(source: str, records_inserted: int, records_found: int, errors: int = 0):
    """Log a scrape run to the scrape_log table for freshness tracking."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return

    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal',
    }

    record = {
        'source': source,
        'records_found': records_found,
        'records_inserted': records_inserted,
        'errors': errors,
        'ran_at': datetime.now().isoformat(),
    }

    try:
        url = f"{SUPABASE_URL}/rest/v1/scrape_log"
        requests.post(url, json=[record], headers=headers, timeout=30)
    except Exception:
        pass  # non-critical, don't fail the scraper over logging


# ─────────────────────────────────────────────────────────────────────────────
# HTTP HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_with_retry(url: str, method: str = 'GET', max_retries: int = 3,
                     timeout: int = 30, **kwargs) -> Optional[requests.Response]:
    """Fetch a URL with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            if method.upper() == 'POST':
                response = requests.post(url, timeout=timeout, **kwargs)
            else:
                response = requests.get(url, timeout=timeout, **kwargs)

            if response.status_code == 429:
                wait = 2 ** attempt * 5
                log.warning(f"Rate limited. Waiting {wait}s...")
                time.sleep(wait)
                continue

            return response

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                log.warning(f"Request failed ({e}). Retrying in {wait}s...")
                time.sleep(wait)
            else:
                log.error(f"Request failed after {max_retries} attempts: {e}")

    return None
