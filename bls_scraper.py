"""
BLS Occupational Employment and Wage Statistics (OEWS) Scraper
Downloads occupation wage data by metro area.
Source: Bureau of Labor Statistics
~800K records with median wages by occupation and geography.
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

# BLS OEWS data (updated annually in May)
# https://www.bls.gov/oes/tables.htm
BLS_OEWS_URL = "https://www.bls.gov/oes/special-requests/oesm23ma.zip"  # May 2023 Metro data

# Relevant SOC codes for tech/business roles
RELEVANT_SOC_CODES = {
    '15-1252': ('Software Developers', 'Software Engineering'),
    '15-1253': ('Software Quality Assurance Analysts and Testers', 'Software Engineering'),
    '15-1254': ('Web Developers', 'Software Engineering'),
    '15-1255': ('Web and Digital Interface Designers', 'Design'),
    '15-1211': ('Computer Systems Analysts', 'Software Engineering'),
    '15-1212': ('Information Security Analysts', 'Software Engineering'),
    '15-1221': ('Computer and Information Research Scientists', 'Data Science'),
    '15-1231': ('Computer Network Support Specialists', 'DevOps / SRE'),
    '15-1232': ('Computer User Support Specialists', 'Software Engineering'),
    '15-1241': ('Computer Network Architects', 'Software Engineering'),
    '15-1242': ('Database Administrators', 'Data Science'),
    '15-1243': ('Database Architects', 'Data Science'),
    '15-1244': ('Network and Computer Systems Administrators', 'DevOps / SRE'),
    '15-1245': ('Database and Network Administrators', 'DevOps / SRE'),
    '15-2031': ('Operations Research Analysts', 'Data Science'),
    '15-2041': ('Statisticians', 'Data Science'),
    '15-2051': ('Data Scientists', 'Data Science'),
    '11-2021': ('Marketing Managers', 'Marketing'),
    '11-2022': ('Sales Managers', 'Sales'),
    '11-3021': ('Computer and Information Systems Managers', 'Software Engineering'),
    '11-3031': ('Financial Managers', 'Finance'),
    '11-3111': ('Compensation and Benefits Managers', 'People / HR'),
    '11-3121': ('Human Resources Managers', 'People / HR'),
    '13-1071': ('Human Resources Specialists', 'People / HR'),
    '13-1075': ('Labor Relations Specialists', 'People / HR'),
    '13-1081': ('Logisticians', 'Operations'),
    '13-1111': ('Management Analysts', 'Operations'),
    '13-1161': ('Market Research Analysts', 'Marketing'),
    '13-2011': ('Accountants and Auditors', 'Finance'),
    '13-2051': ('Financial Analysts', 'Finance'),
    '13-2052': ('Personal Financial Advisors', 'Finance'),
    '27-1024': ('Graphic Designers', 'Design'),
}

# Major metro areas to include
METRO_AREAS = {
    '41860': ('San Francisco-Oakland-Hayward, CA', 'San Francisco', 'CA'),
    '41940': ('San Jose-Sunnyvale-Santa Clara, CA', 'San Francisco', 'CA'),
    '35620': ('New York-Newark-Jersey City, NY-NJ-PA', 'New York', 'NY'),
    '42660': ('Seattle-Tacoma-Bellevue, WA', 'Seattle', 'WA'),
    '12420': ('Austin-Round Rock, TX', 'Austin', 'TX'),
    '19740': ('Denver-Aurora-Lakewood, CO', 'Denver', 'CO'),
    '14460': ('Boston-Cambridge-Newton, MA-NH', 'Boston', 'MA'),
    '16980': ('Chicago-Naperville-Elgin, IL-IN-WI', 'Chicago', 'IL'),
    '31080': ('Los Angeles-Long Beach-Anaheim, CA', 'Los Angeles', 'CA'),
    '33100': ('Miami-Fort Lauderdale-West Palm Beach, FL', 'Miami', 'FL'),
    '12060': ('Atlanta-Sandy Springs-Roswell, GA', 'Atlanta', 'GA'),
    '38900': ('Portland-Vancouver-Hillsboro, OR-WA', 'Portland', 'OR'),
    '39580': ('Raleigh, NC', 'Raleigh', 'NC'),
    '47900': ('Washington-Arlington-Alexandria, DC-VA-MD-WV', 'Washington DC', 'DC'),
}

def download_bls_oews() -> List[dict]:
    """Download and parse BLS OEWS data."""
    print("  Downloading BLS OEWS data...")
    
    try:
        response = requests.get(BLS_OEWS_URL, timeout=120)
        if response.status_code != 200:
            print(f"  HTTP {response.status_code}")
            return []
        
        records = []
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            # Find the MSA (metro area) file
            msa_files = [f for f in z.namelist() if 'msa' in f.lower() and f.endswith('.xlsx')]
            
            if not msa_files:
                # Try CSV
                msa_files = [f for f in z.namelist() if 'msa' in f.lower() and f.endswith('.csv')]
            
            if not msa_files:
                print(f"  Available files: {z.namelist()}")
                return []
            
            filename = msa_files[0]
            print(f"  Processing {filename}...")
            
            if filename.endswith('.csv'):
                with z.open(filename) as f:
                    reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8'))
                    for row in reader:
                        record = process_bls_row(row)
                        if record:
                            records.append(record)
            else:
                # Excel file
                try:
                    import openpyxl
                    with z.open(filename) as f:
                        workbook = openpyxl.load_workbook(io.BytesIO(f.read()), read_only=True)
                        sheet = workbook.active
                        
                        headers = None
                        for i, row in enumerate(sheet.iter_rows(values_only=True)):
                            if i == 0:
                                headers = [str(h).upper() if h else '' for h in row]
                                continue
                            
                            if headers:
                                row_dict = dict(zip(headers, row))
                                record = process_bls_row(row_dict)
                                if record:
                                    records.append(record)
                except ImportError:
                    print("  openpyxl required")
                    return []
        
        return records
        
    except Exception as e:
        print(f"  Error: {e}")
        return []

def process_bls_row(row: dict) -> Optional[dict]:
    """Process a single BLS OEWS row."""
    # Column names may vary
    area_code = str(row.get('AREA', '') or row.get('AREA_CODE', '') or row.get('area', ''))
    occ_code = str(row.get('OCC_CODE', '') or row.get('occ_code', ''))
    occ_title = row.get('OCC_TITLE', '') or row.get('occ_title', '')
    
    # Check if this is a relevant occupation
    if occ_code not in RELEVANT_SOC_CODES:
        return None
    
    # Check if this is a relevant metro
    if area_code not in METRO_AREAS:
        return None
    
    title, family = RELEVANT_SOC_CODES[occ_code]
    area_name, metro, state = METRO_AREAS[area_code]
    
    # Get wage data
    annual_median = row.get('A_MEDIAN', '') or row.get('a_median', '')
    annual_pct25 = row.get('A_PCT25', '') or row.get('a_pct25', '')
    annual_pct75 = row.get('A_PCT75', '') or row.get('a_pct75', '')
    
    try:
        median = int(float(annual_median)) if annual_median and annual_median != '*' and annual_median != '#' else None
        pct25 = int(float(annual_pct25)) if annual_pct25 and annual_pct25 != '*' and annual_pct25 != '#' else None
        pct75 = int(float(annual_pct75)) if annual_pct75 and annual_pct75 != '*' and annual_pct75 != '#' else None
        
        if not median or median < 30000 or median > 500000:
            return None
        
        salary_min = pct25 or int(median * 0.8)
        salary_max = pct75 or int(median * 1.2)
        
    except (ValueError, TypeError):
        return None
    
    return {
        'company': 'BLS Aggregate',
        'title': title,
        'family': family,
        'metro': metro,
        'state': state,
        'salary_min': salary_min,
        'salary_max': salary_max,
        'midpoint': median,
        'source': 'BLS OEWS 2023',
        'posted_date': '2023-05-01',
        'status': 'approved',
    }

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
    print("BLS OEWS WAGE DATA SCRAPER")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)
    
    print("\n[BLS OEWS]")
    records = download_bls_oews()
    print(f"  Found {len(records)} occupation/metro records")
    
    if records:
        print("\nUploading to Supabase...")
        inserted = upload_to_supabase(records)
        print(f"Inserted {inserted} records")
    
    print("\nCompleted!")

if __name__ == "__main__":
    main()
