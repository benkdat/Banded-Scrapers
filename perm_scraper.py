"""
PERM Labor Certification Scraper
Downloads prevailing wage data from permanent labor certification applications.
Source: U.S. Department of Labor
~150K records per year with wages for green card sponsors.
"""

import os
import csv
import requests
import io
from datetime import datetime
from typing import List, Optional

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://xrcgtkkaapfmzzjvyphu.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')

# PERM disclosure data
# https://www.dol.gov/agencies/eta/foreign-labor/performance
PERM_DATA_URLS = {
    '2024': 'https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/PERM_Disclosure_Data_FY2024_Q4.xlsx',
    '2023': 'https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/PERM_Disclosure_Data_FY2023.xlsx',
}

def classify_job_family(title: str) -> Optional[str]:
    if not title:
        return None
    title_lower = title.lower()
    
    mappings = {
        'Software Engineering': ['software', 'engineer', 'developer', 'programmer', 'devops', 'sre'],
        'Product Management': ['product manager', 'program manager'],
        'Data Science': ['data scientist', 'machine learning', 'ml engineer', 'data analyst', 'research scientist'],
        'Design': ['designer', 'ux', 'ui'],
        'Finance': ['finance', 'accountant', 'financial analyst'],
    }
    
    for family, keywords in mappings.items():
        if any(kw in title_lower for kw in keywords):
            return family
    return None

def parse_metro(city: str) -> Optional[str]:
    if not city:
        return None
    city_lower = city.lower()
    
    metro_map = {
        'san francisco': 'San Francisco', 'san jose': 'San Francisco', 'mountain view': 'San Francisco',
        'new york': 'New York', 'seattle': 'Seattle', 'bellevue': 'Seattle',
        'austin': 'Austin', 'denver': 'Denver', 'boston': 'Boston', 'chicago': 'Chicago',
        'los angeles': 'Los Angeles', 'miami': 'Miami', 'atlanta': 'Atlanta',
    }
    
    for key, metro in metro_map.items():
        if key in city_lower:
            return metro
    return city.title()

def download_perm_data(year: str) -> List[dict]:
    """Download and parse PERM disclosure data."""
    url = PERM_DATA_URLS.get(year)
    if not url:
        return []
    
    print(f"  Downloading {year} PERM data...")
    
    try:
        import openpyxl
        from io import BytesIO
        
        response = requests.get(url, timeout=120)
        if response.status_code != 200:
            print(f"  HTTP {response.status_code}")
            return []
        
        workbook = openpyxl.load_workbook(BytesIO(response.content), read_only=True)
        sheet = workbook.active
        
        records = []
        headers = None
        
        for i, row in enumerate(sheet.iter_rows(values_only=True)):
            if i == 0:
                headers = [str(h).upper() if h else '' for h in row]
                continue
            
            if not headers:
                continue
            
            row_dict = dict(zip(headers, row))
            
            company = row_dict.get('EMPLOYER_NAME', '')
            title = row_dict.get('JOB_INFO_JOB_TITLE') or row_dict.get('PW_JOB_TITLE', '')
            wage = row_dict.get('PW_WAGE_9089') or row_dict.get('WAGE_OFFER_FROM_9089', 0)
            wage_unit = row_dict.get('PW_UNIT_OF_PAY_9089', 'Year')
            city = row_dict.get('WORKSITE_CITY', '')
            state = row_dict.get('WORKSITE_STATE', '')
            case_status = row_dict.get('CASE_STATUS', '')
            
            if 'CERTIFIED' not in str(case_status).upper():
                continue
            
            try:
                wage = float(wage) if wage else 0
                wage_unit = str(wage_unit).lower()
                
                if 'hour' in wage_unit:
                    wage = wage * 2080
                elif 'week' in wage_unit:
                    wage = wage * 52
                elif 'month' in wage_unit:
                    wage = wage * 12
                
                if wage < 30000 or wage > 1000000:
                    continue
                    
            except (ValueError, TypeError):
                continue
            
            records.append({
                'company': company.title() if company else None,
                'title': title.title() if title else None,
                'family': classify_job_family(title),
                'metro': parse_metro(city),
                'state': state.upper() if state else None,
                'salary_min': int(wage),
                'salary_max': int(wage),
                'midpoint': int(wage),
                'source': f'PERM Disclosure {year}',
                'posted_date': f'{year}-01-01',
                'status': 'approved',
            })
            
            if len(records) >= 25000:
                break
        
        return records
        
    except ImportError:
        print("  openpyxl required for PERM data")
        return []
    except Exception as e:
        print(f"  Error: {e}")
        return []

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
    inserted = 0
    
    for i in range(0, len(records), 500):
        batch = records[i:i+500]
        try:
            response = requests.post(url, json=batch, headers=headers, timeout=60)
            if response.status_code in [200, 201]:
                inserted += len(batch)
        except:
            pass
    
    return inserted

def main():
    print("=" * 60)
    print("PERM LABOR CERTIFICATION SCRAPER")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)
    
    all_records = []
    
    for year in ['2024', '2023']:
        print(f"\n[PERM {year}]")
        records = download_perm_data(year)
        print(f"  Found {len(records)} certified records")
        all_records.extend(records)
    
    print(f"\nTotal: {len(all_records)} records")
    
    if all_records:
        print("\nUploading to Supabase...")
        inserted = upload_to_supabase(all_records)
        print(f"Inserted {inserted} records")
    
    print("\nCompleted!")

if __name__ == "__main__":
    main()
