# Banded Job Scraper

Scrapes job boards for salary data and uploads to Supabase.

## Job Boards Supported

- **Greenhouse** - ~50 tech companies
- **Lever** - ~25 companies
- **Ashby** - ~15 companies  
- **Workday** - 4 enterprise companies

## How It Works

1. Scrapes public job board APIs for job postings
2. Extracts salary ranges using regex patterns
3. Classifies jobs into families (Engineering, Product, etc.)
4. Uploads to Supabase `comp_data` table

## Setup

### 1. Create a new GitHub repository

```bash
gh repo create banded-scrapers --private
```

### 2. Push this code

```bash
git init
git add .
git commit -m "Initial scraper setup"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/banded-scrapers.git
git push -u origin main
```

### 3. Add GitHub Secrets

Go to your repo → Settings → Secrets and variables → Actions → New repository secret

Add these two secrets:

| Name | Value |
|------|-------|
| `SUPABASE_URL` | `https://xrcgtkkaapfmzzjvyphu.supabase.co` |
| `SUPABASE_KEY` | Your Supabase anon key |

### 4. Run manually (first time)

Go to Actions → "Scrape Job Boards" → Run workflow

### 5. Automatic runs

The scraper runs automatically every 6 hours via GitHub Actions (free).

## Adding More Companies

Edit `scraper.py` and add company slugs to the appropriate list:

```python
GREENHOUSE_COMPANIES = [
    'stripe', 'figma', 'notion',  # Add more here
]
```

To find a company's slug:
- Greenhouse: Visit `boards.greenhouse.io/COMPANY_NAME`
- Lever: Visit `jobs.lever.co/COMPANY_NAME`
- Ashby: Visit `jobs.ashbyhq.com/COMPANY_NAME`

## Local Testing

```bash
# Install dependencies
pip install -r requirements.txt

# Run without uploading (no SUPABASE_KEY set)
python scraper.py

# Run with upload
export SUPABASE_URL="https://xrcgtkkaapfmzzjvyphu.supabase.co"
export SUPABASE_KEY="your-anon-key"
python scraper.py
```
