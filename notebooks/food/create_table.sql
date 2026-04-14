-- Run this in Supabase SQL Editor ONCE before running the pipeline.
-- Food Access Score table

CREATE TABLE IF NOT EXISTS food_access_scores (
  id SERIAL PRIMARY KEY,
  zipcode TEXT NOT NULL,
  metro TEXT NOT NULL,
  low_access_raw NUMERIC,
  low_access_normalized NUMERIC,
  grocery_density_raw NUMERIC,
  grocery_density_normalized NUMERIC,
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
CREATE TRIGGER set_updated_at_food
BEFORE UPDATE ON food_access_scores
FOR EACH ROW EXECUTE FUNCTION update_updated_at();
