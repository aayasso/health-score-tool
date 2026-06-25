# GAP_CLOSURE_LOG.md — Neighborhood Health Score

Execution audit trail for GAP_CLOSURE_PLAN.md. Each entry appended after a task is verified.

---

### 2026-05-24 — Task A1: Enable RLS on all score tables

**Branch:** `gap-closure/a1-enable-rls`

**What changed:** Enabled Row Level Security on 9 Supabase tables. Added SELECT-only policies for the `anon` role on 7 frontend-facing tables (`respiratory_scores`, `cardiovascular_scores`, `stress_scores`, `food_access_scores`, `heat_scores`, `overall_scores`, `zip_codes`). No policy on `score_config` or `raw_signals` (default deny). No INSERT/UPDATE/DELETE policies anywhere.

**Executed in two stages:**
- Stage 1: RLS + policy on `heat_scores` only (low-risk probe to confirm publishable key maps to `anon` role under RLS). Verified with both publishable and anon JWT keys. Passed.
- Stage 2: RLS + policies on remaining 8 tables. Full verification suite run.

**BEFORE → AFTER verification:**

| Test | BEFORE | AFTER | Verdict |
|---|---|---|---|
| Publishable key SELECT from 5 dimension tables | 200 + data | 200 + data | PASS |
| Publishable key SELECT from overall_scores | 200 + data | 200 + data | PASS |
| Publishable key SELECT from zip_codes | 200 + data | 200 + data | PASS |
| Publishable key SELECT from score_config | 200 + proprietary weights exposed | 200 + empty `[]` | PASS (blocked) |
| Publishable key SELECT from raw_signals | 200 + data | 200 + empty `[]` | PASS (blocked) |
| Publishable key INSERT into overall_scores | 201 (succeeded) | 401 RLS policy violation | PASS (writes blocked) |
| Badge endpoint ZIP 15213 | C, 54 | C, 54 | PASS (unchanged) |
| Live site render (operator manual check) | — | All 5 dimension cards + overall render for 15213 | PASS |

**SQL applied (via Supabase SQL Editor):**
- `ALTER TABLE [9 tables] ENABLE ROW LEVEL SECURITY;`
- `CREATE POLICY "anon_select" ON [7 tables] FOR SELECT TO anon USING (true);`

**Files committed:** `supabase/migrations/20260524_enable_rls.sql`

---

### 2026-05-24 — Task A2: Confirm score_config and dead tables locked

**Branch:** `gap-closure/a1-enable-rls`

**What changed:** Enabled RLS (default deny, no anon policy) on 2 legacy tables missed by A1: `composite_scores` (600 rows, stale respiratory data with wrong grade scale) and `interpretations` (600 rows, stale respiratory interpretations with score leaks). Neither is queried by the live frontend (confirmed Pass 2: zero bundle references). Also confirmed A1's locks on `score_config` and `raw_signals` remain in effect.

**BEFORE → AFTER verification:**

| Table | BEFORE | AFTER | Verdict |
|---|---|---|---|
| `score_config` | `[]` (locked by A1) | `[]` | PASS (still blocked) |
| `raw_signals` | `[]` (locked by A1) | `[]` | PASS (still blocked) |
| `composite_scores` | 200 + stale data exposed | `[]` | PASS (newly blocked) |
| `interpretations` | 200 + stale data exposed | `[]` | PASS (newly blocked) |
| `respiratory_scores` (sanity check) | 200 + data | 200 + data | PASS (no collateral damage) |

**SQL applied (via Supabase SQL Editor):**
- `ALTER TABLE composite_scores ENABLE ROW LEVEL SECURITY;`
- `ALTER TABLE interpretations ENABLE ROW LEVEL SECURITY;`

**Files committed:** `supabase/migrations/20260524_lock_legacy_tables.sql`

---

### 2026-05-24 — Task B1: Resolve health-outcomes weight inconsistency

**Branch:** `gap-closure/a1-enable-rls`

**Problem:** Health-outcomes components weighted inconsistently across dimensions — 35% in heat, 30% in food — with no documented rationale. Audit flagged this as the one real methodology inconsistency a reviewer would find.

**Resolution:** Reduced health-outcomes weight to a consistent 25% across both affected dimensions. Redistributed weight to physical/environmental components (the directly measurable signals).

**Weight changes (proprietary — do not expose exact values outside this log):**

| Dimension | Component | Old Weight | New Weight |
|---|---|---|---|
| Heat | impervious | 0.30 | 0.35 |
| Heat | tree_canopy | 0.35 | 0.40 |
| Heat | health_outcome | 0.35 | 0.25 |
| Food | low_access | 0.35 | 0.40 |
| Food | grocery_density | 0.35 | 0.35 |
| Food | health_outcome | 0.30 | 0.25 |

**Scope:** In-scope metros only (Pittsburgh, Los Angeles, Phoenix, Charlotte). Expansion metros (Chicago, Houston, Atlanta, Denver) were NOT re-scored.

**BEFORE snapshots:** `backups/heat_scores_before_b1.csv`, `backups/food_access_scores_before_b1.csv`

**Verification (all 4 queries pass):**

| Query | Test | Result |
|---|---|---|
| Q1 | Reproducibility trace — stored composite = weighted sum of stored normalized values (heat + food, 3 in-scope ZIPs) | PASS (matched to 5 decimals) |
| Q2 | Respiratory / cardiovascular / stress composites unchanged | PASS |
| Q3 | Grade reconciliation — every heat + food row's letter_grade matches grade implied by composite_score | PASS (0 mismatches) |
| Q4 | Expansion metros untouched — stored composites match old-weight recalculation | PASS |

**Lesson learned:** Supabase SQL Editor displays "Success. No rows returned" for UPDATE statements that succeed — this means no result set to display, NOT 0 rows affected. UPDATEs do not return rows unless you add `RETURNING *`.

**Files committed:** `notebooks/heat/heat_pipeline.py`, `notebooks/food/food_pipeline.py`, `backups/heat_scores_before_b1.csv`, `backups/food_access_scores_before_b1.csv`

---

### 2026-05-24 — Task B2: Store normalization anchors

**Branch:** `gap-closure/a1-enable-rls`

**Problem:** Audit finding #9 — min/max normalization anchors computed at runtime (`df[col].min()` / `df[col].max()`) in all 5 pipelines, stored nowhere. Scores not reproducible from stored data alone; any future re-run or new metro silently shifts every existing score.

**Resolution:** Back-computed min/max anchors from stored raw values in Supabase and persisted them in `config/normalization_anchors.yml` (version-controlled, marked proprietary). No scores changed.

**Storage choice:** Versioned file in repo (not a Supabase table). Rationale: reproducibility metadata belongs in git where changes are diffable; no DB schema or RLS overhead for data that changes only on pipeline re-runs.

**Scope determination (dual-scope check):** Computed respiratory anchors two ways — all 8 metros vs 4 in-scope metros only. Results differ:

| Component | All-metros min | In-scope min | Match? |
|---|---|---|---|
| air_quality | 33.58 | 36.34 | NO — expansion ZIP holds true min |
| health_outcomes | 4.60 | 4.95 | NO — expansion ZIP holds true min |
| environmental_burden | 0.0 | 0.0 | YES |
| green_cover | 0.0 | 0.0 | YES |

Conclusion: all 5 pipelines normalized over the full 8-metro dataset. All-metros anchors used.

**Verification (36/36 checks pass):** For each of the 18 components across 2 ZIPs (15213 Pittsburgh, 90012 Los Angeles), re-derived normalized values from stored raw values using captured anchors. All matched stored normalized values to 5 decimal places.

**18 components captured:** respiratory (4), cardiovascular (4), stress (4), food_access (3), heat (3).

**Files committed:** `config/normalization_anchors.yml`

---

### 2026-05-24 — Task C1: Percentile-based grade thresholds

**Branch:** `gap-closure/a1-enable-rls`

**Problem:** Fixed grade scale (A≥80/B≥65/C≥50/D≥35/F<35) produced badly skewed distributions — zero or near-zero A's in most dimensions, heavy F concentration in respiratory and heat. Scale was arbitrary and indefensible.

**Resolution:** Replaced with population-percentile thresholds (SVI/CalEnviroScreen precedent). Grades computed per-dimension over in-scope metros only. Distribution: A ≥ p90 (top 10%), B = p70–p90 (20%), C = p30–p70 (40%), D = p10–p30 (20%), F < p10 (bottom 10%).

**Scope:** 4 in-scope metros (Pittsburgh, Los Angeles, Phoenix, Charlotte), n=574 per dimension. Expansion metros excluded from percentile calculation and untouched.

**Cutoffs persisted in:** `config/grade_thresholds.yml` (proprietary, version-controlled).

**BEFORE snapshot approach:** Added `letter_grade_pre_c1` column to all 5 dimension tables, populated with pre-C1 letter_grade for in-scope rows only. Expansion rows have NULL in this column (proving they were never touched). Rollback is one statement per table: `UPDATE SET letter_grade = letter_grade_pre_c1 WHERE letter_grade_pre_c1 IS NOT NULL`. Columns kept as provenance until Phase E cleanup.

**BEFORE distribution (fixed scale):**

| Dimension | A | B | C | D | F |
|---|---|---|---|---|---|
| Cardiovascular | 2 | 227 | 283 | 60 | 2 |
| Food Access | 0 | 109 | 165 | 160 | 140 |
| Heat | 1 | 47 | 118 | 187 | 221 |
| Respiratory | 0 | 13 | 120 | 174 | 267 |
| Stress | 6 | 340 | 218 | 10 | 0 |

**AFTER distribution (percentile-based):**

| Dimension | A | B | C | D | F | % |
|---|---|---|---|---|---|---|
| All 5 (identical) | 58 | 114 | 230 | 114 | 58 | 10.1/19.9/40.1/19.9/10.1 |

**Verification:**

| Test | Result |
|---|---|
| V1 — Grade reconciliation (every in-scope grade matches percentile cutoffs) | PASS (0 mismatches) |
| V2A — Expansion `letter_grade_pre_c1` is NULL everywhere | PASS (0 rows, expansion never touched) |

**What changed:** `letter_grade` column in 5 dimension tables (in-scope rows only). No `composite_score` values changed. No expansion data changed. No pipeline code changed.

**Files committed:** `config/grade_thresholds.yml`

---

### 2026-05-24 — Task D1: Fix interpretation generation prompt (code only, not deployed)

**Branch:** `gap-closure/a1-enable-rls`

**Problem:** Pass 2 audit found the generate-interpretation edge function leaks exact scores (79% overall, 48% stress, 41% food) and emits markdown headers (100% of heat interpretations). Root cause: the prompt feeds `composite_score` and all 5 dimension scores as raw numbers to Claude, then instructs "do not mention exact scores" — the model ignores this ~80% of the time.

**Resolution:** Rewrote `supabase/functions/generate-interpretation/index.ts` with two structural changes:

1. **No numbers in input:** `composite_score` is never read from the DB and never enters the prompt. Component normalized values are converted to qualitative tercile descriptions (low/moderate/high) before reaching Claude. No numbers = no leaks.
2. **Per-dimension architecture:** Function now accepts `{ zipcode, dimension }` and generates one interpretation per dimension (not an overall interpretation). Aligns with Phase E (five independent dimensions, no overall composite).

**Tercile mapping:** Normalized values mapped to qualitative descriptions using boundaries 0–33.33 (low), 33.34–66.66 (moderate), 67–100 (high). Normalization already inverts bad-is-high components, so higher normalized = better for all. 18 component descriptors hardcoded in the function (3–4 per dimension × 3 terciles each).

**Prompt changes:**
- Grade framed as relative standing: "reflects how this ZIP code ranks relative to other neighborhoods in the covered metro areas for this specific dimension"
- Explicit rules: no markdown, no numbers, no cross-dimension comparison, no absolute/national health claims
- Component conditions described qualitatively, not numerically

**Verification (offline, via claude.ai — function not deployed):**

| Test ZIP | Dimension | Grade | Result |
|---|---|---|---|
| 28078 Charlotte | Heat | A | PASS — clean prose, zero numbers, zero markdown, relative framing |
| 15213 Pittsburgh | Heat | C | PASS — clean prose, zero numbers, zero markdown, relative framing |
| 85051 Phoenix | Heat | F | PASS — clean prose, zero numbers, zero markdown, relative framing, no absolute-health overclaim |

**What this task does NOT do:**
- Does NOT fix the 401 auth gate (function still unreachable via publishable key — that's D2)
- Does NOT deploy the function (that's D2/D3)
- Does NOT regenerate any stored interpretations (that's D3)
- Does NOT modify any database data

**Status:** Prompt and logic correct, offline-verified. Function is committed but not deployed. D2 fixes auth, D3 deploys and regenerates.

**Files committed:** `supabase/functions/generate-interpretation/index.ts`

---

### 2026-05-24 — Task D2: Auth decision for generate-interpretation (documentation only)

**Branch:** `gap-closure/a1-enable-rls`

**Problem:** The generate-interpretation edge function has `verify_jwt = true`. The Lovable frontend sends the publishable key (not a valid JWT) as the Bearer token, causing a 401 for any ZIP without a cached interpretation.

**Decision: The 401 is correct behavior, not a bug to fix.**

The function calls the Anthropic API (paid per call) and writes to the DB. Any public-facing endpoint that triggers paid API calls is an abuse vector. Three options were evaluated:

| Option | Abuse risk | Recommendation |
|---|---|---|
| A: verify_jwt=false + self-validate | HIGH — publishable key is public, anyone can curl | Rejected |
| B: Send anon JWT, keep verify_jwt=true | MEDIUM — anon key equally public | Rejected |
| C: Batch-only, service-role invocation | NONE — service role key is server-side only | **Adopted** |

**Option C rationale:** D3 pre-generates all in-scope interpretations in batch using the service role key. The frontend only reads cached interpretations from the DB. Live generation from the frontend is unnecessary attack surface with zero user benefit.

**Conditional-call finding:** The Lovable frontend checks `overall_scores.interpretation` before calling the function. If non-null (cached), it displays immediately — the edge function is never called. After D3 fills all in-scope caches, the 401 path is unreachable for shipping ZIPs. For expansion ZIPs (interpretation = null), the call fires but correctly 401s — those ZIPs are hidden in Phase E.

**Auth posture:** `verify_jwt = true` stays in `supabase/config.toml`. No code or config changes. The function is invocable only with `Authorization: Bearer <SERVICE_ROLE_KEY>` (D3 batch path).

**Phase E note:** The Lovable frontend currently reads interpretations from `overall_scores.interpretation`. D1 moved generation to per-dimension tables (`[dimension]_scores.interpretation`). Phase E's frontend rebuild must switch interpretation reads from `overall_scores` to the per-dimension tables.

**What changed:** Nothing. D2 is a security decision + documentation, not a code change.

**Files committed:** `GAP_CLOSURE_LOG.md` (this entry only)

---

### 2026-06-25 — Task D3: Batch-regenerate all in-scope interpretations

**Branch:** `gap-closure/a1-enable-rls`

**What this task does:** Regenerated fresh per-dimension interpretations for all 574
in-scope ZIPs × 5 dimensions (2,870 rows) using the D1 prompt logic (per-dimension,
qualitative terciles, no numbers), reflecting the new B1 scores and C1 percentile
grades, written via the service role key.

**Root cause of the prior silent failure (confirmed):** The Colab `SUPABASE_KEY` was the
publishable key (`sb_pub...`). Under the RLS enabled in A1 (SELECT-only for anon, no
UPDATE policy), every write was silently rejected — `supabase-py` does not raise on a
zero-row UPDATE. The script read fine, generated clean text, and reported success while
writing nothing. The DB retained the old, pre-C1 stale interpretations (score leaks,
markdown, mismatched grades). Fixed by swapping `SUPABASE_KEY` to the service role key
(`eyJ...`).

**Hardening added to `scripts/batch_generate_interpretations.py`:**
- **Write-verification guard:** after each UPDATE, re-read the row and confirm the stored
  text matches what was generated. On any mismatch/zero-write, raise
  `WriteVerificationError` and halt the batch immediately — no more silent success.
- **Model string:** `claude-sonnet-4-20250514` → `claude-sonnet-4-6`. The old model
  reached end-of-life 2026-06-15 and now 404s. (Model strings have hard EOL dates and
  fail mid-run; centralizing the model string is logged in TECH_DEBT.md.)
- **Prompt rule:** describe standing qualitatively; do NOT name the letter grade (A–F) or
  use the word "grade." Keeps interpretations from going stale when percentiles
  recompute, and removes redundancy with the UI badge.
- **Grade-leak detector** in `check_output_quality`, excluding "low-grade"/"high-grade"
  to avoid false positives on normal stress language.

**BEFORE snapshot (reversibility):** `interpretation_pre_d3` TEXT column added to all 5
dimension tables, populated with the current interpretation for in-scope rows only
(expansion rows left NULL, proving they're untouched).
Rollback: `UPDATE <table> SET interpretation = interpretation_pre_d3 WHERE
interpretation_pre_d3 IS NOT NULL`.

**Test-then-full approach:** Ran `--mode test-write` on 8 representative ZIPs (A/C/F
across all 4 metros) × 5 dims = 40 rows first; verified clean in the DB before the full
run.

**Full batch result:** 2,870 generated, 0 errors, all rows written + verified by the
guard. The detector flagged 9 rows; manual review confirmed all were false positives
("low-grade stress(or)" — the word "grade" inside "low-grade," no letter named). Detector
regex tightened accordingly. No genuine leaks.

**Verification (V1–V5):** completeness (574 non-null per dimension, 0 missing), zero digit
leaks, zero markdown, expansion metros untouched, and spot-checks across grades read
clean and grade-consistent.

**Still pending (not part of this commit):** Sync the `generate-interpretation` edge
function `index.ts` prompt with the same three rules (no numbers, no grade-naming, no ZIP
echo) — the batch script and edge-function prompts are currently drifted. Logged in
TECH_DEBT.md.

**Files committed:** `scripts/batch_generate_interpretations.py`, `TECH_DEBT.md`,
`GAP_CLOSURE_LOG.md` (this entry).
