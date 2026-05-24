-- Task A2: Lock legacy tables from public access
-- Applied: 2026-05-24
-- Branch: gap-closure/a1-enable-rls
--
-- These tables are dead code (live frontend queries neither, confirmed Pass 2).
-- They will be dropped in Task F1 after CSV export. Locking them now prevents
-- public access to stale data (wrong grade scale, score-leaking interpretations).

ALTER TABLE composite_scores ENABLE ROW LEVEL SECURITY;
ALTER TABLE interpretations ENABLE ROW LEVEL SECURITY;

-- No anon policy = default deny = blocked from public access
-- service_role bypasses RLS (unaffected)
