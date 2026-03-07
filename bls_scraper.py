"""
BLS Occupational Employment and Wage Statistics (OEWS) Scraper
Downloads occupation wage data by metro area from the Bureau of Labor Statistics.
"""

import csv
import io
import zipfile
from datetime import datetime
from typing import Optional
from utils import upload_to_supabase, log_scrape_run, fetch_with_retry, log

# BLS OEWS data (updated annually in May)
BLS_OEWS_URL = "https://www.bls.gov/oes/special-requests/oesm23ma.zip"

# SOC codes mapped to (display_title, job_family)
RELEVANT_SOC_CODES = {
    '15-1252': ('Software Developers', 'Software Engineering'),
    '15-1253': ('Software QA Analysts', 'Software Engineering'),
    '15-1254': ('Web Developers', 'Software Engineering'),
    '15-1255': ('Web and Digital Interface Designers', 'Design'),
    '15-1211': ('Computer Systems Analysts', 'Software Engineering'),
    '15-1212': ('Information Security Analysts', 'Software Engineering'),
    '15-1221': ('Computer and Information Research Scientists', 'Data Science'),
    '15-1241': ('Computer Network Architects', 'Software Engineering'),
    '15-1242': ('Database Administrators', 'Data Science'),
    '15-1243': ('Database Architects', 'Data Science'),
    '15-1244': ('Network and Systems Administrators', 'Operations'),
    '15-2031': ('Operations Research Analysts', 'Data Science'),
    '15-2041': ('Statisticians', 'Data Science'),
    '15-2051': ('Data Scientists', 'Data Science'),
    '11-2021': ('Marketing Managers', 'Marketing'),
    '11-2022': ('Sales Managers', 'Sales'),
    '11-3021': ('Computer and IS Managers', 'Software Engineering'),
    '11-3031': ('Financial Managers', 'Finance'),
    '11-3111': ('Compensation and Benefits Managers', 'People / HR'),
    '11-3121': ('Human Resources Managers', 'People / HR'),
    '13-1071': ('Human Resources Specialists', 'People / HR'),
    '13-1081': ('Logisticians', 'Operations'),
    '13-1111': ('Management Analysts', 'Operations'),
    '13-1161': ('Market Research Analysts', 'Marketing'),
    '13-2011': ('Accountants and Auditors', 'Finance'),
    '13-2051': ('Financial Analysts', 'Finance'),
    '27-1024': ('Graphic Designers', 'Design'),
}

# Metro area codes mapped to (full_name, metro_label, state)
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
    '33460': ('Minneapolis-St. Paul-Bloomington, MN-WI', 'Minneapolis', 'MN'),
    '19100': ('Dallas-Fort Worth-Arlington, TX', 'Dallas', 'TX'),
    '41740': ('San Diego-Carlsbad, CA', 'San Diego', 'CA'),
    '37980': ('Philadelphia-Camden-Wilmington, PA-NJ-DE-MD', 'Philadelphia', 'PA'),
    '40900': ('Sacramento-Roseville-Arden-Arcade, CA', 'Sacramento', 'CA'),
    '41180': ('St. Louis, MO-IL', 'St. Louis', 'MO'),
}


def process_bls_row(row: dict) -> Optional[dict]:
    """Process a single BLS OEWS row into a comp_data record."""
    area_code = str(row.get('AREA', '') or row.get('AREA_CODE', '') or row.get('area', ''))
    occ_code = str(row.get('OCC_CODE', '') or row.get('occ_code', ''))

    if occ_code not in RELEVANT_SOC_CODES:
        return None
    if area_code not in METRO_AREAS:
        return None

    title, family = RELEVANT_SOC_CODES[occ_code]
    _, metro, state = METRO_AREAS[area_code]

    annual_median = row.get('A_MEDIAN', '') or row.get('a_median', '')
    annual_pct25 = row.get('A_PCT25', '') or row.get('a_pct25', '')
    annual_pct75 = row.get('A_PCT75', '') or row.get('a_pct75', '')

    try:
        # BLS uses '*' and '#' for suppressed data
        suppressed = {'*', '#', '**', ''}
        median = int(float(annual_median)) if str(annual_median) not in suppressed else None
        pct25 = int(float(annual_pct25)) if str(annual_pct25) not in suppressed else None
        pct75 = int(float(annual_pct75)) if str(annual_pct75) not in suppressed else None

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


def download_bls_oews() -> list[dict]:
    """Download and parse BLS OEWS data."""
    log.info("  Downloading BLS OEWS data...")

    response = fetch_with_retry(BLS_OEWS_URL, timeout=180)
    if not response or response.status_code != 200:
        log.error("  Failed to download BLS data")
        return []

    records = []

    try:
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            # Find the MSA file (metro area data)
            msa_files = [f for f in z.namelist()
                         if 'msa' in f.lower() and (f.endswith('.xlsx') or f.endswith('.csv'))]

            if not msa_files:
                log.error("  No MSA file found. Available: %s", z.namelist())
                return []

            filename = msa_files[0]
            log.info("  Processing %s...", filename)

            if filename.endswith('.csv'):
                with z.open(filename) as f:
                    reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8'))
                    for row in reader:
                        record = process_bls_row(row)
                        if record:
                            records.append(record)
            else:
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
                    workbook.close()

    except Exception as e:
        log.error("  Error processing BLS data: %s", e)

    return records


def main():
    log.info("=" * 60)
    log.info("BLS OEWS WAGE DATA SCRAPER")
    log.info("=" * 60)

    log.info("\n[BLS OEWS]")
    records = download_bls_oews()
    log.info("  Found %d occupation/metro records", len(records))

    inserted = 0
    if records:
        log.info("Uploading to Supabase...")
        inserted = upload_to_supabase(records)

    log_scrape_run('BLS OEWS', inserted, len(records))
    log.info("Complete. %d inserted.", inserted)


if __name__ == "__main__":
    main()
