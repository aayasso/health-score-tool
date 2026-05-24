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
