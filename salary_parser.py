"""
salary_parser.py
Shared salary parsing utilities used by all scrapers.
"""

import re
from typing import Optional, Tuple

# Salary extraction patterns (order matters — most specific first)
SALARY_PATTERNS = [
    # $120,000 - $150,000 / yr or /year
    (r'\$\s*([\d,]+)\s*[-–—to]+\s*\$\s*([\d,]+)\s*(?:per\s*year|annually|/\s*yr|/\s*year)?', 'range_dollar'),
    # $120k - $150k
    (r'\$\s*([\d,]+)[kK]\s*[-–—to]+\s*\$\s*([\d,]+)[kK]', 'range_k'),
    # 120,000 - 150,000 USD / per year
    (r'([\d,]+)\s*[-–—to]+\s*([\d,]+)\s*(?:USD|usd)?\s*(?:per\s*year|annually|/\s*yr)?', 'range_bare'),
    # $120,000 (single value)
    (r'\$\s*([\d,]+)(?:\s*per\s*year|\s*annually|\s*/\s*yr)?', 'single_dollar'),
    # $120k (single k-value)
    (r'\$\s*([\d,]+)[kK]', 'single_k'),
]

# Sanity bounds
SALARY_MIN_SANE = 20_000
SALARY_MAX_SANE = 1_500_000


def _clean_num(s: str) -> Optional[int]:
    """Parse a raw number string, handle commas."""
    try:
        return int(s.replace(',', ''))
    except (ValueError, AttributeError):
        return None


def _to_annual(n: int, multiplier_hint: str = '') -> int:
    """If value looks like an hourly rate, convert to annual."""
    # Values under 1000 are likely hourly
    if n < 1000:
        return n * 2080
    # Values under 10000 might be monthly
    if n < 10000:
        return n * 12
    return n


def parse_salary(text: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Extract salary range from text.
    Returns (min, max) tuple. Both may be None if not found.
    If only one value found, returns (value, None).
    """
    if not text:
        return None, None

    text_clean = text.replace('\n', ' ').replace('\xa0', ' ')

    for pattern, kind in SALARY_PATTERNS:
        match = re.search(pattern, text_clean, re.IGNORECASE)
        if not match:
            continue

        if kind in ('range_dollar', 'range_bare'):
            a = _clean_num(match.group(1))
            b = _clean_num(match.group(2))
        elif kind == 'range_k':
            a_raw = _clean_num(match.group(1))
            b_raw = _clean_num(match.group(2))
            a = a_raw * 1000 if a_raw else None
            b = b_raw * 1000 if b_raw else None
        elif kind == 'single_dollar':
            a = _clean_num(match.group(1))
            b = None
        elif kind == 'single_k':
            raw = _clean_num(match.group(1))
            a = raw * 1000 if raw else None
            b = None
        else:
            continue

        if a:
            a = _to_annual(a)
        if b:
            b = _to_annual(b)

        # Swap if reversed
        if a and b and a > b:
            a, b = b, a

        # Sanity check
        if a and not (SALARY_MIN_SANE <= a <= SALARY_MAX_SANE):
            a = None
        if b and not (SALARY_MIN_SANE <= b <= SALARY_MAX_SANE):
            b = None

        if a or b:
            return a, b

    return None, None


def format_salary_raw(salary_min: Optional[int], salary_max: Optional[int]) -> str:
    """Format salary range as a human-readable string."""
    if salary_min and salary_max:
        return f"${salary_min:,} - ${salary_max:,}"
    if salary_min:
        return f"${salary_min:,}"
    if salary_max:
        return f"${salary_max:,}"
    return ""
