-- apply_percentile_grades.sql — PROPRIETARY (do not expose)
-- Re-runnable: computes POOLED percentile grades over ALL rows per dimension.
-- Distribution: A >= p90 (10%), B = p70-p90 (20%), C = p30-p70 (40%),
--               D = p10-p30 (20%), F < p10 (10%).
-- Scope: all 8 metros, no WHERE filter.
-- Precedent: CDC/ATSDR SVI, CalEnviroScreen (percentile/rank-based classification).
--
-- Run AFTER all 5 pipelines have written composite_score for all ZIPs.
-- Run each statement block in order in the Supabase SQL Editor.

-- ═══════════════════════════════════════════════════════════════
-- 1. Snapshot current grades (idempotent — column may already exist)
-- ═══════════════════════════════════════════════════════════════

ALTER TABLE respiratory_scores ADD COLUMN IF NOT EXISTS letter_grade_pre_gtm TEXT;
ALTER TABLE cardiovascular_scores ADD COLUMN IF NOT EXISTS letter_grade_pre_gtm TEXT;
ALTER TABLE stress_scores ADD COLUMN IF NOT EXISTS letter_grade_pre_gtm TEXT;
ALTER TABLE food_access_scores ADD COLUMN IF NOT EXISTS letter_grade_pre_gtm TEXT;
ALTER TABLE heat_scores ADD COLUMN IF NOT EXISTS letter_grade_pre_gtm TEXT;

UPDATE respiratory_scores SET letter_grade_pre_gtm = letter_grade;
UPDATE cardiovascular_scores SET letter_grade_pre_gtm = letter_grade;
UPDATE stress_scores SET letter_grade_pre_gtm = letter_grade;
UPDATE food_access_scores SET letter_grade_pre_gtm = letter_grade;
UPDATE heat_scores SET letter_grade_pre_gtm = letter_grade;

-- ═══════════════════════════════════════════════════════════════
-- 2. Recompute grades per dimension (POOLED, no WHERE filter)
-- ═══════════════════════════════════════════════════════════════

-- Respiratory
WITH pctls AS (
  SELECT
    PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY composite_score) AS p10,
    PERCENTILE_CONT(0.30) WITHIN GROUP (ORDER BY composite_score) AS p30,
    PERCENTILE_CONT(0.70) WITHIN GROUP (ORDER BY composite_score) AS p70,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY composite_score) AS p90
  FROM respiratory_scores
)
UPDATE respiratory_scores SET letter_grade = CASE
  WHEN composite_score >= (SELECT p90 FROM pctls) THEN 'A'
  WHEN composite_score >= (SELECT p70 FROM pctls) THEN 'B'
  WHEN composite_score >= (SELECT p30 FROM pctls) THEN 'C'
  WHEN composite_score >= (SELECT p10 FROM pctls) THEN 'D'
  ELSE 'F'
END;

-- Cardiovascular
WITH pctls AS (
  SELECT
    PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY composite_score) AS p10,
    PERCENTILE_CONT(0.30) WITHIN GROUP (ORDER BY composite_score) AS p30,
    PERCENTILE_CONT(0.70) WITHIN GROUP (ORDER BY composite_score) AS p70,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY composite_score) AS p90
  FROM cardiovascular_scores
)
UPDATE cardiovascular_scores SET letter_grade = CASE
  WHEN composite_score >= (SELECT p90 FROM pctls) THEN 'A'
  WHEN composite_score >= (SELECT p70 FROM pctls) THEN 'B'
  WHEN composite_score >= (SELECT p30 FROM pctls) THEN 'C'
  WHEN composite_score >= (SELECT p10 FROM pctls) THEN 'D'
  ELSE 'F'
END;

-- Stress
WITH pctls AS (
  SELECT
    PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY composite_score) AS p10,
    PERCENTILE_CONT(0.30) WITHIN GROUP (ORDER BY composite_score) AS p30,
    PERCENTILE_CONT(0.70) WITHIN GROUP (ORDER BY composite_score) AS p70,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY composite_score) AS p90
  FROM stress_scores
)
UPDATE stress_scores SET letter_grade = CASE
  WHEN composite_score >= (SELECT p90 FROM pctls) THEN 'A'
  WHEN composite_score >= (SELECT p70 FROM pctls) THEN 'B'
  WHEN composite_score >= (SELECT p30 FROM pctls) THEN 'C'
  WHEN composite_score >= (SELECT p10 FROM pctls) THEN 'D'
  ELSE 'F'
END;

-- Food Access
WITH pctls AS (
  SELECT
    PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY composite_score) AS p10,
    PERCENTILE_CONT(0.30) WITHIN GROUP (ORDER BY composite_score) AS p30,
    PERCENTILE_CONT(0.70) WITHIN GROUP (ORDER BY composite_score) AS p70,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY composite_score) AS p90
  FROM food_access_scores
)
UPDATE food_access_scores SET letter_grade = CASE
  WHEN composite_score >= (SELECT p90 FROM pctls) THEN 'A'
  WHEN composite_score >= (SELECT p70 FROM pctls) THEN 'B'
  WHEN composite_score >= (SELECT p30 FROM pctls) THEN 'C'
  WHEN composite_score >= (SELECT p10 FROM pctls) THEN 'D'
  ELSE 'F'
END;

-- Heat
WITH pctls AS (
  SELECT
    PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY composite_score) AS p10,
    PERCENTILE_CONT(0.30) WITHIN GROUP (ORDER BY composite_score) AS p30,
    PERCENTILE_CONT(0.70) WITHIN GROUP (ORDER BY composite_score) AS p70,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY composite_score) AS p90
  FROM heat_scores
)
UPDATE heat_scores SET letter_grade = CASE
  WHEN composite_score >= (SELECT p90 FROM pctls) THEN 'A'
  WHEN composite_score >= (SELECT p70 FROM pctls) THEN 'B'
  WHEN composite_score >= (SELECT p30 FROM pctls) THEN 'C'
  WHEN composite_score >= (SELECT p10 FROM pctls) THEN 'D'
  ELSE 'F'
END;

-- ═══════════════════════════════════════════════════════════════
-- 3. Verification queries (run after step 2)
-- ═══════════════════════════════════════════════════════════════

-- Grade distribution per dimension (expect ~10/20/40/20/10% split)
SELECT 'respiratory' AS dim, letter_grade, count(*) AS n
FROM respiratory_scores GROUP BY letter_grade
UNION ALL
SELECT 'cardiovascular', letter_grade, count(*) FROM cardiovascular_scores GROUP BY letter_grade
UNION ALL
SELECT 'stress', letter_grade, count(*) FROM stress_scores GROUP BY letter_grade
UNION ALL
SELECT 'food_access', letter_grade, count(*) FROM food_access_scores GROUP BY letter_grade
UNION ALL
SELECT 'heat', letter_grade, count(*) FROM heat_scores GROUP BY letter_grade
ORDER BY 1, 2;

-- Zero null grades
SELECT 'respiratory' AS dim, count(*) FILTER (WHERE letter_grade IS NULL) AS null_grades
FROM respiratory_scores
UNION ALL
SELECT 'cardiovascular', count(*) FILTER (WHERE letter_grade IS NULL) FROM cardiovascular_scores
UNION ALL
SELECT 'stress', count(*) FILTER (WHERE letter_grade IS NULL) FROM stress_scores
UNION ALL
SELECT 'food_access', count(*) FILTER (WHERE letter_grade IS NULL) FROM food_access_scores
UNION ALL
SELECT 'heat', count(*) FILTER (WHERE letter_grade IS NULL) FROM heat_scores;

-- ═══════════════════════════════════════════════════════════════
-- ROLLBACK (if needed)
-- ═══════════════════════════════════════════════════════════════
-- UPDATE respiratory_scores SET letter_grade = letter_grade_pre_gtm;
-- UPDATE cardiovascular_scores SET letter_grade = letter_grade_pre_gtm;
-- UPDATE stress_scores SET letter_grade = letter_grade_pre_gtm;
-- UPDATE food_access_scores SET letter_grade = letter_grade_pre_gtm;
-- UPDATE heat_scores SET letter_grade = letter_grade_pre_gtm;
