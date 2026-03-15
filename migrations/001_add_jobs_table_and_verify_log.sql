-- ============================================================================
-- Migration 001: Add jobs table + comp_verifications audit log
-- Run in Supabase SQL editor
-- ============================================================================

-- ── 1. Add missing columns to existing comp_data table ────────────────────
-- (job_url and skills for scraper.py enrichment)

ALTER TABLE comp_data
  ADD COLUMN IF NOT EXISTS job_url TEXT,
  ADD COLUMN IF NOT EXISTS skills TEXT[];

-- ── 2. Create the jobs table (for career_page_scraper.py output) ──────────

CREATE TABLE IF NOT EXISTS jobs (
  id                   BIGSERIAL PRIMARY KEY,
  company              TEXT NOT NULL,
  title                TEXT NOT NULL,
  family               TEXT,
  metro                TEXT,
  state                TEXT,
  salary_min           INTEGER,
  salary_max           INTEGER,
  salary_raw           TEXT,        -- original salary string before parsing
  job_url              TEXT,        -- direct link to the job posting
  skills               TEXT[],      -- array of extracted skills
  employment_type      TEXT DEFAULT 'full-time',
  experience_years_min INTEGER,
  jd_text              TEXT,        -- full job description text (truncated to 5000 chars)
  source               TEXT,        -- 'Greenhouse', 'Workday', 'Career Page', etc.
  posted_date          DATE,
  status               TEXT DEFAULT 'approved',
  created_at           TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS jobs_company_idx      ON jobs (company);
CREATE INDEX IF NOT EXISTS jobs_family_idx       ON jobs (family);
CREATE INDEX IF NOT EXISTS jobs_salary_min_idx   ON jobs (salary_min) WHERE salary_min IS NOT NULL;
CREATE INDEX IF NOT EXISTS jobs_title_trgm_idx   ON jobs USING gin (to_tsvector('english', title));
CREATE INDEX IF NOT EXISTS jobs_skills_idx       ON jobs USING gin (skills);

-- ── 3. Create comp_verifications audit log (for comp verify tool) ─────────

CREATE TABLE IF NOT EXISTS comp_verifications (
  id                   BIGSERIAL PRIMARY KEY,
  job_title            TEXT,
  department           TEXT,
  radford_code         TEXT,
  salary_min           INTEGER,
  salary_max           INTEGER,
  level                TEXT,
  rollup_department    TEXT,
  role_classification  TEXT,
  confidence           TEXT,        -- 'high', 'medium', 'low'
  input_text           TEXT,        -- first 2000 chars of input
  verified_at          TIMESTAMPTZ DEFAULT NOW(),
  verified_by          TEXT         -- future: user email/name
);

-- Index for recent lookups
CREATE INDEX IF NOT EXISTS comp_verifications_verified_at_idx ON comp_verifications (verified_at DESC);
CREATE INDEX IF NOT EXISTS comp_verifications_radford_code_idx ON comp_verifications (radford_code);

-- ── 4. Row Level Security (optional — enable if needed) ───────────────────
-- For internal tools, these tables can remain open to the anon key.
-- Uncomment to restrict:

-- ALTER TABLE comp_verifications ENABLE ROW LEVEL SECURITY;
-- CREATE POLICY "service_role_only" ON comp_verifications
--   USING (auth.role() = 'service_role');
