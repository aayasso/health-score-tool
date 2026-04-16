-- Respiratory Health Score — Supabase Schema
-- Run this SQL in the Supabase SQL Editor ONCE before running the pipeline.

CREATE TABLE IF NOT EXISTS respiratory_scores (
  id SERIAL PRIMARY KEY,
  zipcode TEXT NOT NULL,
  metro TEXT NOT NULL,
  air_quality_raw NUMERIC,
  air_quality_normalized NUMERIC,
  environmental_burden_raw NUMERIC,
  environmental_burden_normalized NUMERIC,
  green_cover_raw NUMERIC,
  green_cover_normalized NUMERIC,
  health_outcomes_raw NUMERIC,
  health_outcomes_normalized NUMERIC,
  composite_score NUMERIC,
  letter_grade TEXT,
  interpretation TEXT,
  score_date DATE,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(zipcode)
);

-- Auto-update timestamp trigger
CREATE TRIGGER set_updated_at_respiratory
BEFORE UPDATE ON respiratory_scores
FOR EACH ROW EXECUTE FUNCTION update_updated_at();
