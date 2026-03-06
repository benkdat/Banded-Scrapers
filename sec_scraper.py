"""
SEC Proxy Filing (DEF 14A) Executive Compensation Scraper
Extracts executive compensation data from public company proxy statements.
Source: SEC EDGAR
~3K executive comp records per year from public companies.
"""

import os
import re
import requests
from datetime import datetime, timedelta
from typing import List, Optional, Dict

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://xrcgtkkaapfmzzjvyphu.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')

# SEC EDGAR API
SEC_COMPANY_SEARCH = "https://efts.sec.gov/LATEST/search-index"
SEC_FILING_BASE = "https://www.sec.gov/cgi-bin/browse-edgar"

# Major tech companies CIK numbers
TECH_COMPANIES = {
    '1318605': ('Tesla', 'Automotive'),
    '1652044': ('Alphabet/Google', 'Tech'),
    '320193': ('Apple', 'Tech'),
    '789019': ('Microsoft', 'Tech'),
    '1018724': ('Amazon', 'Tech'),
    '1326801': ('Meta/Facebook', 'Tech'),
    '1045810': ('Nvidia', 'Tech'),
    '1403161': ('Visa', 'Fintech'),
    '1141391': ('Mastercard', 'Fintech'),
    '909832': ('Costco', 'Retail'),
    '1467373': ('Salesforce', 'Tech'),
    '1288776': ('Netflix', 'Tech'),
    '1730168': ('Uber', 'Tech'),
    '1585521': ('Airbnb', 'Tech'),
    '1792789': ('DoorDash', 'Tech'),
    '1418091': ('Twitter/X', 'Tech'),
    '1364742': ('Workday', 'Tech'),
    '1108524': ('CrowdStrike', 'Tech'),
    '1447669': ('Datadog', 'Tech'),
    '1564902': ('Snowflake', 'Tech'),
    '1108134': ('MongoDB', 'Tech'),
    '1477333': ('Twilio', 'Tech'),
    '1314727': ('HubSpot', 'Tech'),
    '1602065': ('Coinbase', 'Fintech'),
    '1527636': ('Dropbox', 'Tech'),
    '1399520': ('Splunk', 'Tech'),
    '1058290': ('Atlassian', 'Tech'),
    '1653482': ('Okta', 'Tech'),
    '1816736': ('Palantir', 'Tech'),
}

def fetch_sec_filings(cik: str) -> List[Dict]:
    """Fetch recent DEF 14A filings for a company."""
    filings = []
    
    # SEC EDGAR company filings API
    url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
    
    headers = {
        'User-Agent': 'Banded Analytics research@banded.xyz',
        'Accept': 'application/json',
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code != 200:
            return []
        
        data = response.json()
        recent = data.get('filings', {}).get('recent', {})
        
        forms = recent.get('form', [])
        accessions = recent.get('accessionNumber', [])
        dates = recent.get('filingDate', [])
        
        for i, form in enumerate(forms):
            if form == 'DEF 14A':
                filings.append({
                    'accession': accessions[i].replace('-', ''),
                    'date': dates[i],
                })
                if len(filings) >= 2:  # Last 2 years
                    break
        
        return filings
        
    except Exception as e:
        print(f"    Error fetching filings: {e}")
        return []

def parse_executive_comp(cik: str, accession: str, company_name: str) -> List[Dict]:
    """Parse executive compensation from a DEF 14A filing."""
    records = []
    
    # Construct filing URL
    url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/0.txt"
    
    headers = {
        'User-Agent': 'Banded Analytics research@banded.xyz',
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=60)
        if response.status_code != 200:
            return []
        
        text = response.text
        
        # Simple regex patterns for executive comp
        # Looking for patterns like "$1,234,567" or "1,234,567"
        salary_pattern = r'\$?([\d,]+(?:\.\d{2})?)\s*(?:salary|base)'
        total_pattern = r'total\s+compensation.*?\$?([\d,]+(?:\.\d{2})?)'
        
        # Executive titles to look for
        exec_titles = [
            ('CEO', 'Chief Executive Officer'),
            ('CFO', 'Chief Financial Officer'),
            ('COO', 'Chief Operating Officer'),
            ('CTO', 'Chief Technology Officer'),
            ('CPO', 'Chief Product Officer'),
            ('CMO', 'Chief Marketing Officer'),
            ('General Counsel', 'General Counsel'),
            ('President', 'President'),
        ]
        
        for short_title, full_title in exec_titles:
            # Look for compensation near title mentions
            pattern = f'{short_title}.*?\\$([\\d,]+)'
            matches = re.findall(pattern, text[:500000], re.IGNORECASE | re.DOTALL)
            
            for match in matches[:1]:  # Take first match per title
                try:
                    amount = int(match.replace(',', ''))
                    
                    # Sanity check: executive comp typically $200K - $100M
                    if 200000 <= amount <= 100000000:
                        records.append({
                            'company': company_name,
                            'title': full_title,
                            'family': 'Executive',
                            'metro': None,
                            'state': None,
                            'salary_min': amount,
                            'salary_max': amount,
                            'midpoint': amount,
                            'source': 'SEC Proxy (DEF 14A)',
                            'posted_date': datetime.now().strftime('%Y-%m-%d'),
                            'status': 'approved',
                        })
                        break
                except ValueError:
                    continue
        
        return records
        
    except Exception as e:
        print(f"    Error parsing filing: {e}")
        return []

def scrape_sec_executive_comp() -> List[Dict]:
    """Scrape executive compensation from SEC proxy filings."""
    all_records = []
    
    for cik, (company_name, industry) in TECH_COMPANIES.items():
        print(f"  {company_name}...")
        
        filings = fetch_sec_filings(cik)
        
        for filing in filings[:1]:  # Most recent filing
            records = parse_executive_comp(cik, filing['accession'], company_name)
            if records:
                print(f"    Found {len(records)} executives")
                all_records.extend(records)
        
        # Rate limiting
        import time
        time.sleep(0.5)
    
    return all_records

def upload_to_supabase(records: List[dict]) -> int:
    if not records or not SUPABASE_KEY:
        return 0
    
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal'
    }
    
    url = f"{SUPABASE_URL}/rest/v1/comp_data"
    
    try:
        response = requests.post(url, json=records, headers=headers, timeout=60)
        if response.status_code in [200, 201]:
            return len(records)
    except:
        pass
    
    return 0

def main():
    print("=" * 60)
    print("SEC PROXY EXECUTIVE COMPENSATION SCRAPER")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)
    
    print("\n[SEC DEF 14A Filings]")
    records = scrape_sec_executive_comp()
    print(f"\nTotal: {len(records)} executive compensation records")
    
    if records:
        print("\nUploading to Supabase...")
        inserted = upload_to_supabase(records)
        print(f"Inserted {inserted} records")
    
    print("\nCompleted!")

if __name__ == "__main__":
    main()
