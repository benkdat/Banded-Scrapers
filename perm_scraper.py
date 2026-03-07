"""
PERM Labor Certification Scraper
Downloads prevailing wage data from permanent labor certification applications.
Source: U.S. Department of Labor. ~150K records per year.
"""

from datetime import datetime
from utils import (
    classify_job_family, parse_metro_from_city,
    upload_to_supabase, log_scrape_run, fetch_with_retry, log,
)

PERM_DATA_URLS = {
    '2024': 'https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/PERM_Disclosure_Data_FY2024_Q4.xlsx',
    '2023': 'https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/PERM_Disclosure_Data_FY2023.xlsx',
}

MAX_RECORDS_PER_YEAR = 25000


def download_perm_data(year: str) -> list[dict]:
    """Download and parse PERM disclosure data."""
    url = PERM_DATA_URLS.get(year)
    if not url:
        return []

    log.info("  Downloading %s PERM data...", year)

    try:
        import openpyxl
        from io import BytesIO

        response = fetch_with_retry(url, timeout=180)
        if not response or response.status_code != 200:
            log.error("  Failed to download: HTTP %s",
                      response.status_code if response else 'timeout')
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
                wage_val = float(wage) if wage else 0
                wage_unit_str = str(wage_unit).lower()

                if 'hour' in wage_unit_str:
                    wage_val *= 2080
                elif 'week' in wage_unit_str:
                    wage_val *= 52
                elif 'month' in wage_unit_str:
                    wage_val *= 12
                elif 'bi-week' in wage_unit_str:
                    wage_val *= 26

                if wage_val < 30000 or wage_val > 1000000:
                    continue
            except (ValueError, TypeError):
                continue

            records.append({
                'company': company.strip().title() if company else None,
                'title': title.strip().title() if title else None,
                'family': classify_job_family(title),
                'metro': parse_metro_from_city(city),
                'state': state.upper().strip() if state else None,
                'salary_min': int(wage_val),
                'salary_max': int(wage_val),
                'midpoint': int(wage_val),
                'source': f'PERM Disclosure {year}',
                'posted_date': f'{year}-01-01',
                'status': 'approved',
            })

            if len(records) >= MAX_RECORDS_PER_YEAR:
                break

        workbook.close()
        return records

    except ImportError:
        log.error("openpyxl required. Run: pip install openpyxl")
        return []
    except Exception as e:
        log.error("Error processing %s: %s", year, e)
        return []


def main():
    log.info("=" * 60)
    log.info("PERM LABOR CERTIFICATION SCRAPER")
    log.info("=" * 60)

    all_records = []
    total_errors = 0

    for year in ['2024', '2023']:
        log.info("\n[PERM %s]", year)
        try:
            records = download_perm_data(year)
            log.info("  Found %d certified records", len(records))
            all_records.extend(records)
        except Exception as e:
            log.error("  %s: %s", year, e)
            total_errors += 1

    log.info("\nTotal: %d records", len(all_records))

    inserted = 0
    if all_records:
        log.info("Uploading to Supabase...")
        inserted = upload_to_supabase(all_records, batch_size=500)

    log_scrape_run('PERM Disclosure', inserted, len(all_records), total_errors)
    log.info("Complete. %d inserted.", inserted)


if __name__ == "__main__":
    main()
