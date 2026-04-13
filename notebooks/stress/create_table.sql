-- Run this in Supabase SQL Editor ONCE before running the pipeline.
-- Stress / Sensory Environment Score table

CREATE TABLE IF NOT EXISTS stress_scores (
  id SERIAL PRIMARY KEY,
  zipcode TEXT NOT NULL,
  metro TEXT NOT NULL,
  noise_raw NUMERIC,
  noise_normalized NUMERIC,
  light_pollution_raw NUMERIC,
  light_pollution_normalized NUMERIC,
  depression_raw NUMERIC,
  depression_normalized NUMERIC,
  mental_health_raw NUMERIC,
  mental_health_normalized NUMERIC,
  composite_score NUMERIC,
  letter_grade TEXT,
  interpretation TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(zipcode)
);

-- Auto-update timestamp trigger (uses shared function from respiratory setup)
CREATE TRIGGER set_updated_at_stress
BEFORE UPDATE ON stress_scores
FOR EACH ROW EXECUTE FUNCTION update_updated_at();
