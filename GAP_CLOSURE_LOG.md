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
