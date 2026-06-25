# TECH_DEBT.md — Known Technical Debt & Risks

> Forward-looking register of shortcuts, gaps, and risks that are known but not yet
> resolved. Distinct from GAP_CLOSURE_LOG.md (a chronological audit of completed work)
> and CONTEXT.md (current session handoff). Add an item the moment a shortcut is taken;
> remove it (or move it to the log) when it's genuinely fixed. Stale debt is worse than
> none.

**Priority key:** 🔴 high (can silently break correctness or a shipped surface) ·
🟡 medium (real but contained) · 🟢 low (cleanup / hygiene)

---

## Operational / pipeline

These are the class of issue that caused the D3 silent-failure saga. The pipelines are
correct but *hand-operated*; production-grade means re-runnable without babysitting.

### 🔴 Write-verification exists only on the D3 path
`scripts/batch_generate_interpretations.py` now re-reads each row after UPDATE and halts
on a silent non-write (`WriteVerificationError`). **Every other write path** — score
pipelines, backfills — still does a bare `.update()/.upsert().execute()` with no check,
so under RLS or a wrong key they will silently no-op and report success.
**Fix:** extract the verify helper into a shared module and apply it to all write paths.

### 🔴 No pre-flight checks before a batch
Two separate footguns hit in one session: the Colab `SUPABASE_KEY` was the publishable
key (RLS blocks writes, no error), and the working branch was local-only so Colab
couldn't see the script.
**Fix:** a pre-flight that asserts, before any write batch runs: (a) the key is the
service-role key, (b) the model string resolves against the API, (c) the working branch
is pushed to origin. Fail loudly if any check fails.

### 🔴 Model string is hardcoded per-script, no central config
`claude-sonnet-4-20250514` was hardcoded and hit end-of-life on 2026-06-15, returning a
404 mid-run. Now updated to `claude-sonnet-4-6`, but still hardcoded in the one script.
**Fix:** single config constant for the model string (and key type), imported by every
script. Pair with the pre-flight model-resolves check above.

### 🟡 Commit/push discipline is manual
All A1–D3 commits were local-only until pushed late in a session; Colab can only pull
what's on the remote. Easy to forget, and the symptom (pathspec / file-not-found) is
confusing.
**Fix:** fold "branch is pushed" into the pre-flight; consider a session-end checklist.

### 🟡 Supabase success/no-op semantics are easy to misread
"Success. No rows returned" in the SQL Editor is the *normal* success message for
UPDATE/INSERT/DELETE — not "0 rows affected." Misreading it caused false-alarm
investigations; the inverse (a silent no-op read as success) caused the D3 failure.
**Mitigation:** the write-verification guard above; documented here so the next person
doesn't relearn it the hard way.

---

## Prompt / interpretation

### 🔴 Two copies of the interpretation prompt can drift
The strengthened rules (no numbers, no grade-naming, no ZIP echo, qualitative standing)
live in `scripts/batch_generate_interpretations.py`. The `generate-interpretation` edge
function (`supabase/functions/.../index.ts`) still has the **older** prompt and has not
been synced. Any ZIP generated on-demand via the edge function would produce the old,
leak-prone style.
**Fix:** sync `index.ts` with the same three rules; ideally one source of truth plus a
test that both reject a known bad output.

---

## Data quality

### 🟡 Green-cover imputation for expansion metros
The NLCD tree-canopy raster fallback only covers the original four metros (Pittsburgh,
LA, Phoenix, Charlotte). The four expansion metros (Chicago, Houston, Atlanta, Denver)
use **median imputation** for green cover, flagged in code comments. Acceptable while
expansion metros are hidden; must be resolved with a real data pass before they ship.

---

## Validation

### 🔴 Validation circularity against CDC PLACES
Several dimensions ingest CDC PLACES outcome measures **as scoring components**
(respiratory illness, CHD, depression, diabetes). Validating those dimensions by
correlating scores against the same PLACES outcomes is partly circular — a score
correlated against one of its own inputs.
**Fix before any white paper claiming external defensibility:** settle an independent
ground truth — held-out outcomes not used as inputs, hospitalization data, or mortality.

---

## Shipped state / frontend

### 🔴 Live frontend is out of sync with the corrected engine
The live Lovable site still shows the **old composite** and reads interpretations from
`overall_scores`, not the per-dimension tables. The corrected five-dimension engine is
not yet visible to users. (Tracked as Phase E — listed here because the *current shipped
state is wrong*, which is debt, not just planned work.)

### 🟡 Badge never verified in a live HTTPS embed
The `badge` edge function is curl-confirmed only. It has never been verified rendering in
a real third-party HTTPS embed context.

---

## Legacy cleanup (Phase F)

### 🟢 Legacy tables pending drop
`composite_scores` / `overall_scores` duplication and possibly `raw_signals` are slated
for removal once the per-dimension model is fully shipped and trusted.

### 🟢 `interpretation_pre_d3` snapshot columns
Temporary rollback columns added to all five dimension tables before the D3 overwrite.
Drop once the regenerated interpretations are confirmed trustworthy.
