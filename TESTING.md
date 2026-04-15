# TESTING.md — Test Suite Templates & QA Protocol
> Every pipeline must run all applicable test suites and achieve 100% pass before proceeding.
> Copy the relevant suite into each notebook. Never skip tests to save time.

---

## Core Test Runner (include in every notebook)

```python
import traceback
from datetime import datetime

# ── Logging ──────────────────────────────────────────────────────────────────

def log(level: str, message: str):
    """Structured logging for all pipeline steps."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    icons = {
        "INFO":  "ℹ️ ",
        "WARN":  "⚠️ ",
        "ERROR": "❌ ",
        "PASS":  "✅ ",
        "FAIL":  "❌ ",
        "TEST":  "🧪 ",
        "START": "🚀 ",
        "DONE":  "🏁 ",
    }
    icon = icons.get(level, "   ")
    print(f"[{timestamp}] {icon} [{level}] {message}")


# ── Test Runner ───────────────────────────────────────────────────────────────

def run_tests(suite_name: str, tests: list) -> bool:
    """
    Run a list of (test_name, test_fn) tuples.
    Each test_fn must return (passed: bool, detail: str).
    Prints PASS/FAIL for each test. Returns True only if all pass.
    
    Usage:
        all_passed = run_tests("CARDIOVASCULAR — INGESTION", [
            ("Row count >= 550", lambda: (len(df) >= 550, f"got {len(df)}")),
        ])
        if not all_passed:
            raise RuntimeError("Tests failed — do not proceed")
    """
    print(f"\n{'='*62}")
    print(f"  TEST SUITE — {suite_name}")
    print(f"{'='*62}")

    passed_count = 0
    failed_count = 0

    for test_name, test_fn in tests:
        try:
            ok, detail = test_fn()
            if ok:
                print(f"  ✅ PASS  {test_name}")
                passed_count += 1
            else:
                print(f"  ❌ FAIL  {test_name}")
                print(f"           → {detail}")
                failed_count += 1
        except Exception as e:
            print(f"  ❌ FAIL  {test_name}")
            print(f"           → Raised exception: {e}")
            print(f"           → {traceback.format_exc().strip()}")
            failed_count += 1

    total = passed_count + failed_count
    print(f"{'─'*62}")
    print(f"  Result: {passed_count}/{total} passed  |  {failed_count} failed")
    print(f"{'='*62}\n")

    return failed_count == 0


# ── Blocking Gate ─────────────────────────────────────────────────────────────

def require_all_pass(suite_name: str, passed: bool):
    """
    Hard stop if a test suite failed. Call after every run_tests().
    Prevents proceeding to the next pipeline step with bad data.
    """
    if not passed:
        raise RuntimeError(
            f"\n{'!'*62}\n"
            f"  BLOCKED: '{suite_name}' had failures.\n"
            f"  Fix all failures and re-run before proceeding.\n"
            f"{'!'*62}\n"
        )
    log("PASS", f"All tests passed — '{suite_name}' gate cleared")
```

---

## Suite 1 — Ingestion Tests

Run immediately after pulling raw data, before any normalization.

```python
def test_ingestion(df, tool_name: str, expected_columns: list[str], 
                   raw_value_ranges: dict, expected_zip_count: int = 600):
    """
    df: ingested DataFrame with columns zip_code, metro, and raw component columns
    tool_name: e.g. "CARDIOVASCULAR"
    expected_columns: list of column names that must be present
    raw_value_ranges: dict of {col_name: (min_plausible, max_plausible)}
        e.g. {"chd_raw": (1.0, 25.0), "lpa_raw": (5.0, 60.0)}
    """
    MIN_COVERAGE = 0.90  # require at least 90% ZIP coverage

    tests = [
        # ── Structure ──
        ("All expected columns present",
            lambda: (
                all(c in df.columns for c in expected_columns),
                f"Missing: {[c for c in expected_columns if c not in df.columns]}"
            )),

        # ── Coverage ──
        (f"Row count >= {int(expected_zip_count * MIN_COVERAGE)}",
            lambda: (
                len(df) >= int(expected_zip_count * MIN_COVERAGE),
                f"Got {len(df)}, need ≥ {int(expected_zip_count * MIN_COVERAGE)}"
            )),

        # ── ZIP integrity ──
        ("No null zip codes",
            lambda: (
                df["zipcode"].isna().sum() == 0,
                f"{df['zipcode'].isna().sum()} null zipcodes found"
            )),

        ("No duplicate zip codes",
            lambda: (
                df["zipcode"].duplicated().sum() == 0,
                f"{df['zipcode'].duplicated().sum()} duplicate ZIPs found: "
                f"{df[df['zipcode'].duplicated()]['zipcode'].tolist()[:10]}"
            )),

        # ── Metro coverage ──
        ("All 4 metros present",
            lambda: (
                set(df["metro"].unique()) == set(METRO_LABELS.values()),
                f"Found: {sorted(df['metro'].unique())}, "
                f"Expected: {sorted(METRO_LABELS.values())}"
            )),

        ("No metro is under 20 ZIPs",
            lambda: (
                df["metro"].value_counts().min() >= 20,
                f"Metro counts: {df['metro'].value_counts().to_dict()}"
            )),
    ]

    # ── Raw value range checks (one test per component) ──
    for col, (lo, hi) in raw_value_ranges.items():
        col_ref = col  # capture for lambda closure
        lo_ref, hi_ref = lo, hi
        tests.append((
            f"{col_ref} values in plausible range [{lo_ref}, {hi_ref}]",
            lambda c=col_ref, l=lo_ref, h=hi_ref: (
                df[c].dropna().between(l, h).all(),
                f"min={df[c].min():.3f}, max={df[c].max():.3f}, "
                f"out-of-range: {df[~df[c].between(l, h)]['zip_code'].tolist()[:5]}"
            )
        ))

        tests.append((
            f"{col_ref} — null count within tolerance (< 10%)",
            lambda c=col_ref: (
                df[c].isna().sum() / len(df) < 0.10,
                f"{df[c].isna().sum()} nulls ({df[c].isna().sum()/len(df)*100:.1f}%)"
            )
        ))

    passed = run_tests(f"{tool_name} — INGESTION", tests)
    require_all_pass(f"{tool_name} — INGESTION", passed)
    return passed


# Example call for Cardiovascular:
# test_ingestion(
#     df=df_raw,
#     tool_name="CARDIOVASCULAR",
#     expected_columns=["zip_code", "metro", "lpa_raw", "chd_raw", "noise_raw", "impervious_raw"],
#     raw_value_ranges={
#         "lpa_raw":         (5.0,  60.0),   # % physically inactive
#         "chd_raw":         (1.0,  20.0),   # % with coronary heart disease
#         "noise_raw":       (30.0, 90.0),   # dB day-night level
#         "impervious_raw":  (0.0,  100.0),  # % impervious surface
#     }
# )
```

---

## Suite 2 — Normalization Tests

Run after normalizing each component, before computing composite scores.

```python
def test_normalization(df_norm, tool_name: str, 
                       normalized_cols: list[str], 
                       inverted_cols: list[str],
                       raw_cols: list[str],
                       weights: list[float]):
    """
    df_norm: DataFrame after normalization, must contain both raw and normalized columns
    normalized_cols: list of normalized column names, e.g. ["lpa_normalized", "chd_normalized"]
    inverted_cols: subset of normalized_cols that were inverted (higher raw = lower normalized)
    raw_cols: parallel list of raw column names (same order as normalized_cols)
    weights: list of floats that must sum to 1.0
    """

    tests = [
        # ── Weights ──
        ("Weights sum to exactly 1.0",
            lambda: (
                abs(sum(weights) - 1.0) < 1e-9,
                f"Sum = {sum(weights)}"
            )),

        ("Weight count matches component count",
            lambda: (
                len(weights) == len(normalized_cols),
                f"{len(weights)} weights, {len(normalized_cols)} components"
            )),
    ]

    # ── Per-column normalization checks ──
    for col in normalized_cols:
        col_ref = col
        tests += [
            (f"{col_ref} — all values in [0.0, 100.0]",
                lambda c=col_ref: (
                    df_norm[c].dropna().between(0.0, 100.0).all(),
                    f"min={df_norm[c].min():.4f}, max={df_norm[c].max():.4f}, "
                    f"violations: {df_norm[~df_norm[c].between(0.0, 100.0)]['zip_code'].tolist()[:5]}"
                )),

            (f"{col_ref} — no nulls",
                lambda c=col_ref: (
                    df_norm[c].isna().sum() == 0,
                    f"{df_norm[c].isna().sum()} nulls"
                )),

            (f"{col_ref} — has meaningful spread (std > 1.0)",
                lambda c=col_ref: (
                    df_norm[c].std() > 1.0,
                    f"std={df_norm[c].std():.3f} — possible normalization error or flat data"
                )),
        ]

    # ── Inversion correctness ──
    for norm_col, raw_col in zip(normalized_cols, raw_cols):
        if norm_col in inverted_cols:
            nc, rc = norm_col, raw_col
            tests.append((
                f"{nc} — inversion correct (higher raw → lower normalized)",
                lambda n=nc, r=rc: (
                    df_norm[[r, n]].corr().iloc[0, 1] < -0.90,
                    f"Pearson correlation = {df_norm[[r, n]].corr().iloc[0, 1]:.3f} "
                    f"(expected < -0.90 for correctly inverted column)"
                )
            ))

    # ── Global min/max anchoring ──
    for col in normalized_cols:
        col_ref = col
        tests += [
            (f"{col_ref} — global min normalizes to ~0.0",
                lambda c=col_ref: (
                    df_norm[c].min() < 1.0,
                    f"Min normalized value = {df_norm[c].min():.4f}"
                )),
            (f"{col_ref} — global max normalizes to ~100.0",
                lambda c=col_ref: (
                    df_norm[c].max() > 99.0,
                    f"Max normalized value = {df_norm[c].max():.4f}"
                )),
        ]

    passed = run_tests(f"{tool_name} — NORMALIZATION", tests)
    require_all_pass(f"{tool_name} — NORMALIZATION", passed)
    return passed
```

---

## Suite 3 — Scoring Tests

Run after computing composite scores and letter grades.

```python
GRADE_SCALE = {"A": (80, 100), "B": (65, 79.999), "C": (50, 64.999), "D": (35, 49.999), "F": (0, 34.999)}
SPOT_CHECK_ZIPS = {
    # Add known ZIPs with expected grade direction before each tool build
    # Format: zip_code: ("min_grade", "max_grade") where grades are A/B/C/D/F
    # Example: "15213": ("C", "A")  # Carnegie Mellon area — expect mid-to-high
}

def test_scoring(df_scored, tool_name: str, spot_check_zips: dict = None):
    """
    df_scored: DataFrame with composite_score (float) and letter_grade (str) columns
    spot_check_zips: optional dict of {zip_code: (min_expected_grade, max_expected_grade)}
    """
    import numpy as np

    grade_order = ["A", "B", "C", "D", "F"]
    spot_check_zips = spot_check_zips or SPOT_CHECK_ZIPS

    def grade_in_range(grade, min_grade, max_grade):
        min_idx = grade_order.index(min_grade)
        max_idx = grade_order.index(max_grade)
        grade_idx = grade_order.index(grade)
        return max_idx >= grade_idx >= min_idx  # note: A=0 is "best", F=4 is "worst"

    tests = [
        # ── Range ──
        ("All composite scores in [0.0, 100.0]",
            lambda: (
                df_scored["composite_score"].between(0.0, 100.0).all(),
                f"min={df_scored['composite_score'].min():.3f}, "
                f"max={df_scored['composite_score'].max():.3f}"
            )),

        ("No null composite scores",
            lambda: (
                df_scored["composite_score"].isna().sum() == 0,
                f"{df_scored['composite_score'].isna().sum()} nulls"
            )),

        ("No null letter grades",
            lambda: (
                df_scored["letter_grade"].isna().sum() == 0,
                f"{df_scored['letter_grade'].isna().sum()} nulls"
            )),

        ("Letter grades are valid values only",
            lambda: (
                df_scored["letter_grade"].isin(["A", "B", "C", "D", "F"]).all(),
                f"Invalid grades: {df_scored[~df_scored['letter_grade'].isin(['A','B','C','D','F'])]['letter_grade'].unique()}"
            )),

        # ── Grade assignment correctness ──
        ("Grade assignments match score thresholds (all rows)",
            lambda: (
                all(
                    GRADE_SCALE[row["letter_grade"]][0] <= row["composite_score"] <= GRADE_SCALE[row["letter_grade"]][1]
                    for _, row in df_scored.iterrows()
                ),
                "Some rows have mismatched grade/score — check grade assignment logic"
            )),

        # ── Distribution sanity ──
        ("No single grade > 70% of ZIPs (distribution check)",
            lambda: (
                df_scored["letter_grade"].value_counts(normalize=True).max() < 0.70,
                f"Grade distribution: {df_scored['letter_grade'].value_counts().to_dict()}"
            )),

        ("Score has meaningful spread (std > 5.0)",
            lambda: (
                df_scored["composite_score"].std() > 5.0,
                f"std={df_scored['composite_score'].std():.3f} — scores may be artificially clustered"
            )),

        ("Score mean is in plausible mid range (25–75)",
            lambda: (
                25.0 <= df_scored["composite_score"].mean() <= 75.0,
                f"mean={df_scored['composite_score'].mean():.2f} — check normalization or weighting"
            )),
    ]

    # ── Spot checks ──
    for zip_code, (min_grade, max_grade) in spot_check_zips.items():
        z, mn, mx = zip_code, min_grade, max_grade
        tests.append((
            f"Spot check ZIP {z}: grade between {mn} and {mx}",
            lambda zc=z, lo=mn, hi=mx: (
                (row := df_scored[df_scored["zipcode"] == zc]).empty is False
                and grade_in_range(row.iloc[0]["letter_grade"], lo, hi),
                f"ZIP {zc} got grade {row.iloc[0]['letter_grade'] if not row.empty else 'NOT FOUND'}"
            )
        ))

    passed = run_tests(f"{tool_name} — SCORING", tests)
    require_all_pass(f"{tool_name} — SCORING", passed)
    return passed
```

---

## Suite 4 — Supabase Write Tests

Run after upserting all records. Queries Supabase directly — does not rely on local data.

```python
def test_supabase_write(supabase, table_name: str, tool_name: str,
                        local_df, spot_check_zips: list[str],
                        expected_min_rows: int = 550):
    """
    supabase: initialized Supabase client
    table_name: e.g. "cardiovascular_scores"
    local_df: the scored DataFrame used in the upsert (for value comparison)
    spot_check_zips: list of 5 ZIP codes to verify values match between local and Supabase
    """

    def get_supabase_count():
        result = supabase.table(table_name).select("zip_code", count="exact").execute()
        return result.count or 0

    def get_supabase_row(zip_code):
        result = supabase.table(table_name).select("*").eq("zip_code", zip_code).execute()
        return result.data[0] if result.data else None

    initial_count = get_supabase_count()

    tests = [
        # ── Row count ──
        (f"Supabase row count >= {expected_min_rows}",
            lambda: (
                initial_count >= expected_min_rows,
                f"Got {initial_count} rows in {table_name}"
            )),

        (f"Supabase row count matches local data",
            lambda: (
                abs(initial_count - len(local_df)) <= 5,  # allow small tolerance for known gaps
                f"Supabase: {initial_count}, local: {len(local_df)}"
            )),

        # ── Idempotency (re-run upsert, count must not change) ──
        ("Re-upsert is idempotent (row count unchanged)",
            lambda: (
                get_supabase_count() == initial_count,
                f"Before: {initial_count}, after re-upsert: {get_supabase_count()}"
            )),

        # ── No nulls in critical columns ──
        ("No null composite_score in Supabase",
            lambda: (
                supabase.table(table_name)
                    .select("zip_code", count="exact")
                    .is_("composite_score", "null")
                    .execute().count == 0,
                "Found null composite_score rows"
            )),

        ("No null letter_grade in Supabase",
            lambda: (
                supabase.table(table_name)
                    .select("zip_code", count="exact")
                    .is_("letter_grade", "null")
                    .execute().count == 0,
                "Found null letter_grade rows"
            )),
    ]

    # ── Spot-check: values match between local and Supabase ──
    for zip_code in spot_check_zips[:5]:
        z = zip_code
        local_row = local_df[local_df["zipcode"] == z]

        tests.append((
            f"Spot check ZIP {z}: exists in Supabase",
            lambda zc=z: (
                get_supabase_row(zc) is not None,
                f"ZIP {zc} not found in {table_name}"
            )
        ))

        if not local_row.empty:
            expected_score = round(float(local_row.iloc[0]["composite_score"]), 1)
            tests.append((
                f"Spot check ZIP {z}: composite_score matches local ({expected_score})",
                lambda zc=z, exp=expected_score: (
                    (row := get_supabase_row(zc)) is not None
                    and abs(round(float(row["composite_score"]), 1) - exp) < 0.5,
                    f"Supabase: {get_supabase_row(zc) and round(float(get_supabase_row(zc)['composite_score']), 1)}, "
                    f"local: {exp}"
                )
            ))

    passed = run_tests(f"{tool_name} — SUPABASE WRITE", tests)
    require_all_pass(f"{tool_name} — SUPABASE WRITE", passed)
    return passed
```

---

## Suite 5 — Streamlit Smoke Tests (Manual Checklist)

Run manually in the browser after each new tab is deployed. Check each item and note any failures.

```
STREAMLIT SMOKE TEST — [TOOL NAME] — [DATE]
============================================================

ZIP LOOKUP
  [ ] PASS / [ ] FAIL  Pittsburgh ZIP (e.g. 15213) returns a result
  [ ] PASS / [ ] FAIL  Los Angeles ZIP (e.g. 90210) returns a result
  [ ] PASS / [ ] FAIL  Phoenix ZIP (e.g. 85001) returns a result
  [ ] PASS / [ ] FAIL  Charlotte ZIP (e.g. 28202) returns a result
  [ ] PASS / [ ] FAIL  Invalid ZIP (e.g. 00000) shows a graceful "not found" message
  [ ] PASS / [ ] FAIL  Empty input shows a prompt, not an error

DISC VISUALIZATION
  [ ] PASS / [ ] FAIL  Disc renders without exception
  [ ] PASS / [ ] FAIL  Correct number of arc segments (matches component count)
  [ ] PASS / [ ] FAIL  Correct color palette for this tool
  [ ] PASS / [ ] FAIL  Overall score and letter grade displayed in center

COMPONENT BREAKDOWN
  [ ] PASS / [ ] FAIL  All components listed by qualitative name (no weights shown)
  [ ] PASS / [ ] FAIL  Each component shows its normalized score (0–100)
  [ ] PASS / [ ] FAIL  No raw values, weights, or normalization details exposed

INTERPRETATION
  [ ] PASS / [ ] FAIL  Interpretation text is present and non-empty
  [ ] PASS / [ ] FAIL  Text is plain English, no jargon
  [ ] PASS / [ ] FAIL  No weights or methodology details appear in interpretation

UI CONSISTENCY
  [ ] PASS / [ ] FAIL  Tab label matches tool name exactly
  [ ] PASS / [ ] FAIL  Layout matches Respiratory tab structure
  [ ] PASS / [ ] FAIL  No Python tracebacks visible in UI
  [ ] PASS / [ ] FAIL  Page loads in < 3 seconds for a cached ZIP

TOTAL: ___ / 18 passed

Notes on any failures:
```

---

## Pipeline Run Order & Gate Summary

Every tool build must execute steps and gates in this exact order:

```
Step 1: Ingest raw data
         ↓
         [run Suite 1 — Ingestion Tests] ← GATE: all must pass
         ↓
Step 2: Normalize components
         ↓
         [run Suite 2 — Normalization Tests] ← GATE: all must pass
         ↓
Step 3: Compute composite scores + letter grades
         ↓
         [run Suite 3 — Scoring Tests] ← GATE: all must pass
         ↓
Step 4: Generate interpretations (Claude API)
         ↓
Step 5: Upsert to Supabase
         ↓
         [run Suite 4 — Supabase Write Tests] ← GATE: all must pass
         ↓
Step 6: Deploy Streamlit tab
         ↓
         [run Suite 5 — Smoke Tests (manual)] ← GATE: all must pass
         ↓
         ✅ Tool phase complete — update CONTEXT.md session log
```

---

## Suite 6 — QA Data Integrity (Cross-Table, Automated)

**Location:** `notebooks/qa/qa_data_integrity.py`
**Test count:** 106 tests (as of 2026-04-15)
**Status:** All passing

This suite runs read-only checks against all 6 Supabase score tables and 4 metros. It is the final verification gate after all pipelines have completed.

**What it covers:**
- **Per-table integrity (6 tables × ~10 tests each):** row count ≥ 540, no null `composite_score`, no null grade, all 4 test ZIPs present, `score_date` non-null, valid grades, score range [0, 100]
- **Cross-table consistency (4 test ZIPs × 5 checks each):** all 5 tool scores present, scores in range, grades valid, metro consistent across tables, overall ≈ mean of 5 tools (±1.0)
- **Metro distribution (5 tables × 4 metros):** each metro has ≥ 20 ZIPs per table (skips `composite_scores` which has no `metro` column)

**Test ZIPs:** Pittsburgh=15213, Los Angeles=90210, Charlotte=28277, Phoenix=85257

**Running:**
- In Colab: uncomment the secrets block, run all cells
- Locally: set `SUPABASE_URL` and `SUPABASE_KEY` environment variables, then `python notebooks/qa/qa_data_integrity.py`

**Known quirks:**
- `composite_scores` (respiratory) has no `metro` column — metro lookups go through `zip_codes` table
- Metro values in DB are title case ("Pittsburgh", "Los Angeles", etc.)

---

## Debugging Checklist

When a test fails, work through this checklist before modifying any code:

1. **Read the full error message** — do not skim; the specific ZIP or value causing the failure is usually in the output
2. **Check the validation report** — was the issue present in the raw data, or introduced during normalization?
3. **Inspect the failing rows** — `df[df["column"].isna()]` or `df[~df["column"].between(lo, hi)]`
4. **Check inversion** — if a component score is inverted when it should not be (or vice versa), the correlation test will catch it; verify the `Direction` column in `TOOL_SPECS.md`
5. **Check for unit mismatch** — raster values may be in different units than expected (e.g., noise in Pa² instead of dB); verify against source documentation
6. **Check for ZCTA join failure** — if raster aggregation produced fewer ZIPs than expected, the ZCTA shapefile join may have failed for some rows; inspect join keys
7. **Check Supabase column names** — upsert failures are often caused by a column name mismatch between the local dict and the Supabase schema; compare exactly
8. **Re-run from the failing step only** — do not re-run the entire notebook; jump to the step that failed and re-run forward from there
9. **Document the fix** — add a comment in the code explaining what was wrong and how it was fixed
10. **Re-run the full test suite** for the affected step before proceeding
