-- Run this in Supabase SQL Editor ONCE before running the overall pipeline.
-- Overall Health Environment Score table — aggregates all 5 tool scores.

CREATE TABLE IF NOT EXISTS overall_scores (
  id SERIAL PRIMARY KEY,
  zipcode TEXT NOT NULL,
  metro TEXT NOT NULL,
  respiratory_score NUMERIC,
  cardiovascular_score NUMERIC,
  stress_score NUMERIC,
  food_access_score NUMERIC,
  heat_score NUMERIC,
  composite_score NUMERIC,
  letter_grade TEXT,
  interpretation TEXT,
  score_date DATE DEFAULT CURRENT_DATE,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(zipcode)
);

-- Auto-update timestamp trigger (uses shared function from respiratory setup)
CREATE TRIGGER set_updated_at_overall
BEFORE UPDATE ON overall_scores
FOR EACH ROW EXECUTE FUNCTION update_updated_at();
