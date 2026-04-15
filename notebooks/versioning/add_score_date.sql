-- Run this in Supabase SQL Editor ONCE.
-- Adds score_date to all 5 existing score tables for historical tracking.
-- Existing rows get today's date. Future pipeline runs populate score_date explicitly.

ALTER TABLE composite_scores
  ADD COLUMN IF NOT EXISTS score_date DATE DEFAULT CURRENT_DATE;

ALTER TABLE cardiovascular_scores
  ADD COLUMN IF NOT EXISTS score_date DATE DEFAULT CURRENT_DATE;

ALTER TABLE stress_scores
  ADD COLUMN IF NOT EXISTS score_date DATE DEFAULT CURRENT_DATE;

ALTER TABLE food_access_scores
  ADD COLUMN IF NOT EXISTS score_date DATE DEFAULT CURRENT_DATE;

ALTER TABLE heat_scores
  ADD COLUMN IF NOT EXISTS score_date DATE DEFAULT CURRENT_DATE;
