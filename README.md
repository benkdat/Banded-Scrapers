# Banded Job Scraper

Scrapes multiple data sources for salary data and uploads to Supabase.

## Data Sources

### Job Boards (runs every 6 hours)
- **Greenhouse** - ~50 tech companies
- **Lever** - ~25 companies
- **Ashby** - ~15 companies  
- **Workday** - 4 enterprise companies

### Government Data (runs every 6 hours)
- **H-1B Disclosure** - ~600K actual salaries per year (DOL)
- **PERM Labor Certs** - ~150K green card wages per year (DOL)
- **BLS OEWS** - Occupation wages by metro (~800K records)
- **SEC Proxy Filings** - Executive compensation (~3K records)

## How It Works

1. Scrapes public APIs and government data files
2. Extracts and normalizes salary data
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

You can choose to run:
- `all` - All scrapers
- `jobs` - Job board scraper only
- `h1b` - H-1B disclosure data only
- `perm` - PERM labor certs only
- `bls` - BLS OEWS data only
- `sec` - SEC proxy filings only

### 5. Automatic runs

All scrapers run automatically every 6 hours via GitHub Actions (free).

## Local Testing

```bash
# Install dependencies
pip install -r requirements.txt

# Run individual scrapers
python scraper.py      # Job boards
python h1b_scraper.py  # H-1B data
python perm_scraper.py # PERM data
python bls_scraper.py  # BLS OEWS
python sec_scraper.py  # SEC proxy

# With Supabase upload
export SUPABASE_URL="https://xrcgtkkaapfmzzjvyphu.supabase.co"
export SUPABASE_KEY="your-anon-key"
python scraper.py
```

## Data Volume Estimates

| Source | Records/Run | Annual Volume |
|--------|-------------|---------------|
| Job Boards | ~500-2,000 | ~100K |
| H-1B | ~50,000 | ~600K |
| PERM | ~25,000 | ~150K |
| BLS OEWS | ~500 | ~500 |
| SEC Proxy | ~100 | ~3K |

**Total potential:** ~850K+ records per year
