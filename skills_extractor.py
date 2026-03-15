"""
skills_extractor.py
Extracts skills from job description text by keyword matching.
"""

import re
from typing import List

COMMON_TECH_SKILLS = [
    "Python", "SQL", "Java", "JavaScript", "TypeScript", "React", "Node.js",
    "AWS", "Azure", "GCP", "Kubernetes", "Docker", "Terraform", "Helm",
    "Salesforce", "Workday", "SAP", "Oracle", "ServiceNow",
    "Tableau", "Power BI", "Looker", "dbt", "Databricks", "Snowflake",
    "Machine Learning", "AI", "LLM", "Data Science", "NLP",
    "REST API", "GraphQL", "Microservices", "Kafka", "RabbitMQ",
    "Agile", "Scrum", "Jira", "Confluence",
    "React Native", "iOS", "Android",
    "C#", "C++", "Go", "Rust", "Ruby", "PHP",
    "PostgreSQL", "MySQL", "MongoDB", "Redis", "Elasticsearch",
    "CI/CD", "GitHub Actions", "Jenkins", "CircleCI",
    "Product Management", "Roadmapping", "A/B Testing",
    "Excel", "Google Sheets",
]

COMMON_HR_SKILLS = [
    "HRIS", "ATS", "Greenhouse", "Workday HCM", "UKG", "Radford",
    "Compensation", "Total Rewards", "Benchmarking", "Salary Bands",
    "Talent Acquisition", "Recruiting", "Sourcing",
    "HRBP", "HR Business Partner", "Employee Relations",
    "Performance Management", "Workforce Planning", "Succession Planning",
    "L&D", "Learning and Development", "Onboarding",
    "FMLA", "ADA", "FLSA", "Employment Law",
    "People Analytics", "HR Analytics", "Organizational Design",
]

COMMON_FINANCE_SKILLS = [
    "FP&A", "Financial Modeling", "Budgeting", "Forecasting",
    "Variance Analysis", "DCF", "Valuation",
    "GAAP", "IFRS", "Revenue Recognition",
    "NetSuite", "Workday Finance", "SAP FI",
    "Tableau", "Power BI", "Excel",
    "CPA", "CFA", "MBA",
    "M&A", "Due Diligence", "Capital Markets",
    "Cost Accounting", "General Ledger",
]

ALL_SKILLS = COMMON_TECH_SKILLS + COMMON_HR_SKILLS + COMMON_FINANCE_SKILLS


def extract_skills(jd_text: str) -> List[str]:
    """
    Extract known skills from job description text.
    Returns a deduplicated list of matched skill strings.
    """
    if not jd_text:
        return []

    found = []
    text_lower = jd_text.lower()

    for skill in ALL_SKILLS:
        # Use word-boundary-aware matching for short/common terms
        pattern = r'\b' + re.escape(skill.lower()) + r'\b'
        if re.search(pattern, text_lower):
            found.append(skill)

    # Deduplicate while preserving order
    seen = set()
    result = []
    for s in found:
        if s.lower() not in seen:
            seen.add(s.lower())
            result.append(s)

    return result


def extract_experience_years(jd_text: str):
    """Extract minimum years of experience requirement from JD text."""
    if not jd_text:
        return None

    patterns = [
        r'(\d+)\+?\s*years?\s+of\s+(?:relevant\s+)?experience',
        r'(\d+)\+?\s*years?\s+(?:of\s+)?(?:professional\s+)?experience',
        r'minimum\s+of\s+(\d+)\s+years?',
        r'at\s+least\s+(\d+)\s+years?',
    ]

    for pattern in patterns:
        match = re.search(pattern, jd_text.lower())
        if match:
            try:
                return int(match.group(1))
            except (ValueError, IndexError):
                pass

    return None


def extract_employment_type(jd_text: str) -> str:
    """Detect employment type from job description."""
    if not jd_text:
        return "full-time"

    text_lower = jd_text.lower()
    if any(t in text_lower for t in ["contract", "contractor", "c2c", "1099"]):
        return "contract"
    if any(t in text_lower for t in ["part-time", "part time", "parttime"]):
        return "part-time"
    if "intern" in text_lower:
        return "internship"
    return "full-time"
