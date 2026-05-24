-- Task A1: Enable Row Level Security on all score tables
-- Applied: 2026-05-24
-- Branch: gap-closure/a1-enable-rls
--
-- Stage 1 (heat_scores) applied first as low-risk probe to confirm
-- publishable key maps to anon role under RLS. Stage 2 applied after
-- Stage 1 verification passed.

-- Enable RLS (default deny for all operations)
ALTER TABLE respiratory_scores ENABLE ROW LEVEL SECURITY;
ALTER TABLE cardiovascular_scores ENABLE ROW LEVEL SECURITY;
ALTER TABLE stress_scores ENABLE ROW LEVEL SECURITY;
ALTER TABLE food_access_scores ENABLE ROW LEVEL SECURITY;
ALTER TABLE heat_scores ENABLE ROW LEVEL SECURITY;
ALTER TABLE overall_scores ENABLE ROW LEVEL SECURITY;
ALTER TABLE zip_codes ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw_signals ENABLE ROW LEVEL SECURITY;
ALTER TABLE score_config ENABLE ROW LEVEL SECURITY;

-- SELECT-only policies for anon role on frontend-facing tables
CREATE POLICY "anon_select" ON respiratory_scores FOR SELECT TO anon USING (true);
CREATE POLICY "anon_select" ON cardiovascular_scores FOR SELECT TO anon USING (true);
CREATE POLICY "anon_select" ON stress_scores FOR SELECT TO anon USING (true);
CREATE POLICY "anon_select" ON food_access_scores FOR SELECT TO anon USING (true);
CREATE POLICY "anon_select" ON heat_scores FOR SELECT TO anon USING (true);
CREATE POLICY "anon_select" ON overall_scores FOR SELECT TO anon USING (true);
CREATE POLICY "anon_select" ON zip_codes FOR SELECT TO anon USING (true);

-- score_config: NO anon policy = proprietary methodology blocked from public access
-- raw_signals: NO anon policy = intermediate pipeline data blocked from public access
-- service_role bypasses RLS, so pipeline writes and edge functions are unaffected
