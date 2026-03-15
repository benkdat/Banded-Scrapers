"""
H-1B Disclosure Data Scraper
Downloads and processes H-1B LCA disclosure data from the U.S. Department of Labor.
~600K records per year with actual salaries paid to visa workers.
"""

import time
from datetime import datetime
from utils import (
    classify_job_family, parse_metro_from_city,
    upload_to_supabase, log_scrape_run, fetch_with_retry, log,
)

# H-1B LCA disclosure files by fiscal year
# Updated quarterly at: https://www.dol.gov/agencies/eta/foreign-labor/performance
H1B_DATA_URLS = {
    '2024': 'https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/LCA_Disclosure_Data_FY2024_Q4.xlsx',
    '2023': 'https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/LCA_Disclosure_Data_FY2023.xlsx',
}

MAX_RECORDS_PER_YEAR = 50000


def download_h1b_data(year: str) -> list[dict]:
    """Download and parse H-1B disclosure data for a given year."""
    url = H1B_DATA_URLS.get(year)
    if not url:
        log.warning("No URL configured for year %s", year)
        return []

    log.info("  Downloading %s data from DOL...", year)

    try:
        import openpyxl
        from io import BytesIO

        response = fetch_with_retry(url, timeout=180)
        if not response or response.status_code != 200:
            log.error("  Failed to download %s data: HTTP %s",
                      year, response.status_code if response else 'timeout')
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

            # Extract fields (column names vary by year)
            company = row_dict.get('EMPLOYER_NAME') or row_dict.get('EMPLOYER_BUSINESS_NAME', '')
            title = row_dict.get('JOB_TITLE') or row_dict.get('SOC_TITLE', '')
            wage = row_dict.get('WAGE_RATE_OF_PAY_FROM') or row_dict.get('PREVAILING_WAGE', 0)
            wage_to = row_dict.get('WAGE_RATE_OF_PAY_TO', wage)
            wage_unit = row_dict.get('WAGE_UNIT_OF_PAY', 'Year')
            city = row_dict.get('WORKSITE_CITY') or row_dict.get('EMPLOYER_CITY', '')
            state = row_dict.get('WORKSITE_STATE') or row_dict.get('EMPLOYER_STATE', '')
            case_status = row_dict.get('CASE_STATUS', '')

            if 'CERTIFIED' not in str(case_status).upper():
                continue

            # Convert wage to annual
            try:
                wage_val = float(wage) if wage else 0
                wage_to_val = float(wage_to) if wage_to else wage_val

                wage_unit_str = str(wage_unit).lower()
                if 'hour' in wage_unit_str:
                    wage_val *= 2080
                    wage_to_val *= 2080
                elif 'week' in wage_unit_str:
                    wage_val *= 52
                    wage_to_val *= 52
                elif 'month' in wage_unit_str:
                    wage_val *= 12
                    wage_to_val *= 12
                elif 'bi-week' in wage_unit_str:
                    wage_val *= 26
                    wage_to_val *= 26

                if wage_val < 30000 or wage_val > 1000000:
                    continue

            except (ValueError, TypeError):
                continue

            salary_min = int(min(wage_val, wage_to_val))
            salary_max = int(max(wage_val, wage_to_val))

            records.append({
                'company': company.strip().title() if company else None,
                'title': title.strip().title() if title else None,
                'family': classify_job_family(title),
                'metro': parse_metro_from_city(city, state),
                'state': state.upper().strip() if state else None,
                'salary_min': salary_min,
                'salary_max': salary_max,
                'midpoint': (salary_min + salary_max) // 2,
                'source': f'H-1B Disclosure {year}',
                'posted_date': f'{year}-01-01',
                'status': 'approved',
            })

            if len(records) >= MAX_RECORDS_PER_YEAR:
                break

        workbook.close()
        return records

    except ImportError:
        log.error("openpyxl not installed. Run: pip install openpyxl")
        return []
    except Exception as e:
        log.error("Error processing %s: %s", year, e)
        return []


def main():
    log.info("=" * 60)
    log.info("H-1B DISCLOSURE DATA SCRAPER")
    log.info("=" * 60)

    all_records = []
    total_errors = 0

    for year in ['2024', '2023']:
        log.info("\n[H-1B %s]", year)
        try:
            records = download_h1b_data(year)
            log.info("  Found %d certified records with salary data", len(records))
            all_records.extend(records)
        except Exception as e:
            log.error("  Year %s failed: %s", year, e)
            total_errors += 1

    log.info("\nTotal records: %d", len(all_records))

    inserted = 0
    if all_records:
        log.info("Uploading to Supabase (upsert)...")
        inserted = upload_to_supabase(
            all_records,
            batch_size=500,
            on_conflict='company,title,source,metro',
        )

    log_scrape_run('H-1B Disclosure', inserted, len(all_records), total_errors)
    log.info("Complete. %d inserted.", inserted)


if __name__ == "__main__":
    main()
