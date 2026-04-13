-- Run this in Supabase SQL Editor ONCE before running the pipeline.
-- Cardiovascular Health Score table

CREATE TABLE IF NOT EXISTS cardiovascular_scores (
  id SERIAL PRIMARY KEY,
  zipcode TEXT NOT NULL,
  metro TEXT NOT NULL,
  physical_inactivity_raw NUMERIC,
  physical_inactivity_normalized NUMERIC,
  chd_raw NUMERIC,
  chd_normalized NUMERIC,
  noise_raw NUMERIC,
  noise_normalized NUMERIC,
  impervious_raw NUMERIC,
  impervious_normalized NUMERIC,
  composite_score NUMERIC,
  letter_grade TEXT,
  interpretation TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(zipcode)
);

-- Auto-update timestamp trigger (uses shared function from respiratory setup)
CREATE TRIGGER set_updated_at_cardiovascular
BEFORE UPDATE ON cardiovascular_scores
FOR EACH ROW EXECUTE FUNCTION update_updated_at();
