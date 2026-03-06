"""
H-1B Disclosure Data Scraper
Downloads and processes H-1B LCA (Labor Condition Application) disclosure data.
Source: U.S. Department of Labor
~600K records per year with actual salaries paid to visa workers.
"""

import os
import csv
import requests
import zipfile
import io
from datetime import datetime
from typing import List, Optional

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://xrcgtkkaapfmzzjvyphu.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')

# H-1B LCA disclosure files by fiscal year
# Updated quarterly at: https://www.dol.gov/agencies/eta/foreign-labor/performance
H1B_DATA_URLS = {
    '2024': 'https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/LCA_Disclosure_Data_FY2024_Q4.xlsx',
    '2023': 'https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/LCA_Disclosure_Data_FY2023.xlsx',
}

def classify_job_family(title: str) -> Optional[str]:
    """Classify job title into a job family."""
    if not title:
        return None
    title_lower = title.lower()
    
    mappings = {
        'Software Engineering': ['software', 'engineer', 'developer', 'swe', 'frontend', 'backend', 'fullstack', 'devops', 'sre', 'platform', 'programmer'],
        'Product Management': ['product manager', 'product lead', 'program manager', 'technical program'],
        'Data Science': ['data scientist', 'machine learning', 'ml engineer', 'ai ', 'data analyst', 'analytics', 'research scientist'],
        'Design': ['designer', 'ux', 'ui', 'design lead', 'creative'],
        'Marketing': ['marketing', 'growth', 'brand', 'content', 'seo'],
        'Sales': ['sales', 'account executive', 'account manager', 'business development'],
        'People / HR': ['recruiter', 'people', 'hr ', 'human resources', 'talent'],
        'Finance': ['finance', 'accountant', 'controller', 'fp&a', 'financial analyst'],
    }
    
    for family, keywords in mappings.items():
        if any(kw in title_lower for kw in keywords):
            return family
    return None

def parse_metro(city: str, state: str) -> Optional[str]:
    """Map city/state to metro area."""
    if not city:
        return None
    
    city_lower = city.lower()
    metro_map = {
        'san francisco': 'San Francisco',
        'san jose': 'San Francisco',
        'mountain view': 'San Francisco',
        'palo alto': 'San Francisco',
        'sunnyvale': 'San Francisco',
        'menlo park': 'San Francisco',
        'redwood city': 'San Francisco',
        'new york': 'New York',
        'brooklyn': 'New York',
        'seattle': 'Seattle',
        'bellevue': 'Seattle',
        'redmond': 'Seattle',
        'austin': 'Austin',
        'denver': 'Denver',
        'boulder': 'Denver',
        'boston': 'Boston',
        'cambridge': 'Boston',
        'chicago': 'Chicago',
        'los angeles': 'Los Angeles',
        'santa monica': 'Los Angeles',
        'miami': 'Miami',
        'atlanta': 'Atlanta',
        'portland': 'Portland',
        'raleigh': 'Raleigh',
        'durham': 'Raleigh',
    }
    
    for key, metro in metro_map.items():
        if key in city_lower:
            return metro
    return city.title()

def download_h1b_data(year: str) -> List[dict]:
    """Download and parse H-1B disclosure data for a given year."""
    url = H1B_DATA_URLS.get(year)
    if not url:
        print(f"  No URL configured for year {year}")
        return []
    
    print(f"  Downloading {year} data...")
    
    try:
        # Need openpyxl for xlsx files
        import openpyxl
        from io import BytesIO
        
        response = requests.get(url, timeout=120)
        if response.status_code != 200:
            print(f"  HTTP {response.status_code} for {url}")
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
            
            # Extract relevant fields (column names vary by year)
            company = row_dict.get('EMPLOYER_NAME') or row_dict.get('EMPLOYER_BUSINESS_NAME', '')
            title = row_dict.get('JOB_TITLE') or row_dict.get('SOC_TITLE', '')
            wage = row_dict.get('WAGE_RATE_OF_PAY_FROM') or row_dict.get('PREVAILING_WAGE', 0)
            wage_to = row_dict.get('WAGE_RATE_OF_PAY_TO', wage)
            wage_unit = row_dict.get('WAGE_UNIT_OF_PAY', 'Year')
            city = row_dict.get('WORKSITE_CITY') or row_dict.get('EMPLOYER_CITY', '')
            state = row_dict.get('WORKSITE_STATE') or row_dict.get('EMPLOYER_STATE', '')
            case_status = row_dict.get('CASE_STATUS', '')
            
            # Only include certified cases
            if 'CERTIFIED' not in str(case_status).upper():
                continue
            
            # Convert wage to annual
            try:
                wage = float(wage) if wage else 0
                wage_to = float(wage_to) if wage_to else wage
                
                wage_unit = str(wage_unit).lower()
                if 'hour' in wage_unit:
                    wage = wage * 2080
                    wage_to = wage_to * 2080
                elif 'week' in wage_unit:
                    wage = wage * 52
                    wage_to = wage_to * 52
                elif 'month' in wage_unit:
                    wage = wage * 12
                    wage_to = wage_to * 12
                
                # Sanity check
                if wage < 30000 or wage > 1000000:
                    continue
                    
            except (ValueError, TypeError):
                continue
            
            salary_min = int(min(wage, wage_to))
            salary_max = int(max(wage, wage_to))
            
            records.append({
                'company': company.title() if company else None,
                'title': title.title() if title else None,
                'family': classify_job_family(title),
                'metro': parse_metro(city, state),
                'state': state.upper() if state else None,
                'salary_min': salary_min,
                'salary_max': salary_max,
                'midpoint': (salary_min + salary_max) // 2,
                'source': f'H-1B Disclosure {year}',
                'posted_date': f'{year}-01-01',
                'status': 'approved',
            })
            
            # Limit for testing
            if len(records) >= 50000:
                break
        
        return records
        
    except ImportError:
        print("  openpyxl not installed. Trying CSV fallback...")
        return download_h1b_csv_fallback(year)
    except Exception as e:
        print(f"  Error processing {year}: {e}")
        return []

def download_h1b_csv_fallback(year: str) -> List[dict]:
    """Fallback to CSV format if available."""
    # Some older data is available in CSV
    csv_urls = {
        '2023': 'https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/H-1B_Disclosure_Data_FY2023.csv',
    }
    
    url = csv_urls.get(year)
    if not url:
        return []
    
    try:
        response = requests.get(url, timeout=120)
        if response.status_code != 200:
            return []
        
        records = []
        reader = csv.DictReader(io.StringIO(response.text))
        
        for row in reader:
            # Similar processing as above
            company = row.get('EMPLOYER_NAME', '')
            title = row.get('JOB_TITLE', '')
            wage = row.get('WAGE_RATE_OF_PAY_FROM', 0)
            
            try:
                wage = float(wage)
                if wage < 30000 or wage > 1000000:
                    continue
            except:
                continue
            
            records.append({
                'company': company.title(),
                'title': title.title(),
                'family': classify_job_family(title),
                'metro': None,
                'state': row.get('WORKSITE_STATE', '').upper(),
                'salary_min': int(wage),
                'salary_max': int(wage),
                'midpoint': int(wage),
                'source': f'H-1B Disclosure {year}',
                'posted_date': f'{year}-01-01',
                'status': 'approved',
            })
            
            if len(records) >= 50000:
                break
        
        return records
    except Exception as e:
        print(f"  CSV fallback error: {e}")
        return []

def upload_to_supabase(records: List[dict]) -> int:
    """Upload records to Supabase."""
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
    batch_size = 500
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        try:
            response = requests.post(url, json=batch, headers=headers, timeout=60)
            if response.status_code in [200, 201]:
                inserted += len(batch)
                print(f"  Uploaded {inserted}/{len(records)} records...")
            else:
                print(f"  Upload error: {response.status_code}")
        except Exception as e:
            print(f"  Upload error: {e}")
    
    return inserted

def main():
    print("=" * 60)
    print("H-1B DISCLOSURE DATA SCRAPER")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)
    
    all_records = []
    
    for year in ['2024', '2023']:
        print(f"\n[H-1B {year}]")
        records = download_h1b_data(year)
        print(f"  Found {len(records)} certified records with salary data")
        all_records.extend(records)
    
    print(f"\nTotal records: {len(all_records)}")
    
    if all_records:
        print("\nUploading to Supabase...")
        inserted = upload_to_supabase(all_records)
        print(f"Inserted {inserted} records")
    
    print("\n" + "=" * 60)
    print(f"Completed: {datetime.now().isoformat()}")

if __name__ == "__main__":
    main()
