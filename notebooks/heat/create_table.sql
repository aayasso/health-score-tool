-- Run this in Supabase SQL Editor ONCE before running the pipeline.
-- Heat & Climate Resilience Score table

CREATE TABLE IF NOT EXISTS heat_scores (
  id SERIAL PRIMARY KEY,
  zipcode TEXT NOT NULL,
  metro TEXT NOT NULL,
  impervious_raw NUMERIC,
  impervious_normalized NUMERIC,
  tree_canopy_raw NUMERIC,
  tree_canopy_normalized NUMERIC,
  health_outcome_raw NUMERIC,
  health_outcome_normalized NUMERIC,
  composite_score NUMERIC,
  letter_grade TEXT,
  interpretation TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(zipcode)
);

-- Auto-update timestamp trigger (uses shared function from respiratory setup)
CREATE TRIGGER set_updated_at_heat
BEFORE UPDATE ON heat_scores
FOR EACH ROW EXECUTE FUNCTION update_updated_at();
