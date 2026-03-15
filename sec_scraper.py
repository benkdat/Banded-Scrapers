"""
SEC Proxy Filing (DEF 14A) Executive Compensation Scraper
Extracts executive compensation data from public company proxy statements.
Source: SEC EDGAR. ~3K executive comp records per year.

NOTE: This scraper uses regex on HTML proxy filings, which is inherently fragile.
It targets the Summary Compensation Table section to improve accuracy.
For production use, consider the SEC's structured XBRL data instead.
"""

import re
import time
from datetime import datetime
from typing import List, Dict
from utils import upload_to_supabase, log_scrape_run, fetch_with_retry, log

SEC_USER_AGENT = 'Banded Analytics ben@banded.xyz'

# Tech companies with CIK numbers
TECH_COMPANIES = {
    '1652044': 'Alphabet',
    '320193': 'Apple',
    '789019': 'Microsoft',
    '1018724': 'Amazon',
    '1326801': 'Meta',
    '1045810': 'Nvidia',
    '1318605': 'Tesla',
    '1403161': 'Visa',
    '1467373': 'Salesforce',
    '1288776': 'Netflix',
    '1730168': 'Uber',
    '1585521': 'Airbnb',
    '1792789': 'DoorDash',
    '1364742': 'Workday',
    '1447669': 'Datadog',
    '1564902': 'Snowflake',
    '1108134': 'MongoDB',
    '1477333': 'Twilio',
    '1314727': 'HubSpot',
    '1602065': 'Coinbase',
    '1527636': 'Dropbox',
    '1058290': 'Atlassian',
    '1653482': 'Okta',
    '1816736': 'Palantir',
}

EXEC_TITLES = [
    ('CEO', 'Chief Executive Officer'),
    ('CFO', 'Chief Financial Officer'),
    ('COO', 'Chief Operating Officer'),
    ('CTO', 'Chief Technology Officer'),
    ('CPO', 'Chief Product Officer'),
    ('CMO', 'Chief Marketing Officer'),
    ('CLO', 'General Counsel'),
    ('President', 'President'),
]


def fetch_sec_filings(cik: str) -> list[dict]:
    """Fetch recent DEF 14A filings for a company."""
    url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
    headers = {'User-Agent': SEC_USER_AGENT, 'Accept': 'application/json'}

    response = fetch_with_retry(url, headers=headers)
    if not response or response.status_code != 200:
        return []

    try:
        data = response.json()
        recent = data.get('filings', {}).get('recent', {})
        forms = recent.get('form', [])
        accessions = recent.get('accessionNumber', [])
        dates = recent.get('filingDate', [])

        filings = []
        for i, form in enumerate(forms):
            if form == 'DEF 14A':
                filings.append({
                    'accession': accessions[i].replace('-', ''),
                    'date': dates[i],
                })
                if len(filings) >= 1:  # Most recent only
                    break

        return filings
    except Exception as e:
        log.error("    Error fetching filings: %s", e)
        return []


def parse_executive_comp(cik: str, accession: str, company_name: str) -> list[dict]:
    """
    Parse executive compensation from a DEF 14A filing.
    Focuses on the Summary Compensation Table section for accuracy.
    """
    records = []

    # Try to find the main filing document
    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/"
    headers = {'User-Agent': SEC_USER_AGENT}

    response = fetch_with_retry(f"{index_url}index.json", headers=headers)
    if not response or response.status_code != 200:
        return []

    try:
        index_data = response.json()
        items = index_data.get('directory', {}).get('item', [])

        # Find the main HTML filing
        htm_file = None
        for item in items:
            name = item.get('name', '')
            if name.endswith('.htm') and 'def14a' in name.lower():
                htm_file = name
                break

        if not htm_file:
            # Fall back to first .htm file
            for item in items:
                name = item.get('name', '')
                if name.endswith('.htm'):
                    htm_file = name
                    break

        if not htm_file:
            return []

        # Fetch the filing
        filing_response = fetch_with_retry(f"{index_url}{htm_file}", headers=headers, timeout=60)
        if not filing_response or filing_response.status_code != 200:
            return []

        text = filing_response.text

        # Narrow search to Summary Compensation Table area
        sct_start = None
        for marker in ['summary compensation table', 'summary&nbsp;compensation&nbsp;table']:
            idx = text.lower().find(marker)
            if idx >= 0:
                sct_start = idx
                break

        if sct_start is None:
            return []

        # Look within ~50K chars after the table header
        search_text = text[sct_start:sct_start + 50000]

        # Look for dollar amounts near executive titles
        for short_title, full_title in EXEC_TITLES:
            # Find the title in the table area
            title_pattern = re.compile(
                rf'{re.escape(short_title)}.*?\$\s*([\d,]+)',
                re.IGNORECASE | re.DOTALL
            )
            matches = title_pattern.findall(search_text[:20000])

            for match in matches[:1]:
                try:
                    amount = int(match.replace(',', ''))

                    # Exec comp: $200K-$100M is reasonable
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

    except Exception as e:
        log.error("    Error parsing filing: %s", e)

    return records


def main():
    log.info("=" * 60)
    log.info("SEC PROXY EXECUTIVE COMPENSATION SCRAPER")
    log.info("=" * 60)

    all_records = []
    total_errors = 0

    for cik, company_name in TECH_COMPANIES.items():
        log.info("  %s...", company_name)
        try:
            filings = fetch_sec_filings(cik)
            for filing in filings[:1]:
                records = parse_executive_comp(cik, filing['accession'], company_name)
                if records:
                    log.info("    Found %d executives", len(records))
                    all_records.extend(records)
        except Exception as e:
            log.error("    %s: %s", company_name, e)
            total_errors += 1

        time.sleep(0.5)  # SEC rate limit

    log.info("\nTotal: %d executive compensation records", len(all_records))

    inserted = 0
    if all_records:
        log.info("Uploading to Supabase...")
        inserted = upload_to_supabase(all_records)

    log_scrape_run('SEC Proxy', inserted, len(all_records), total_errors)
    log.info("Complete. %d inserted.", inserted)


if __name__ == "__main__":
    main()
