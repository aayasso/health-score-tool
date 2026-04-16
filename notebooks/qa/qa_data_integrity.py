# %% [markdown]
# # QA Data Integrity Suite
# **LaSalle Technologies Health Environment Score**
#
# Read-only verification across all 6 score tables and 8 metros.
# Run in Colab after all pipelines have completed.
#
# **Test ZIPs:** Pittsburgh=15213, Los Angeles=90210, Charlotte=28277, Phoenix=85257

# %% [markdown]
# ## 0 · Setup

# %%
import os
import traceback
from datetime import datetime

# Colab secrets — uncomment in Colab
# from google.colab import userdata
# os.environ["SUPABASE_URL"] = userdata.get("SUPABASE_URL")
# os.environ["SUPABASE_KEY"] = userdata.get("SUPABASE_KEY")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

from supabase import create_client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# %%
# ── Logging & Test Runner (from TESTING.md) ───────────────────

def log(level: str, msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    icons = {
        "INFO": "ℹ️ ", "WARN": "⚠️ ", "ERROR": "❌ ", "PASS": "✅ ",
        "FAIL": "❌ ", "TEST": "🧪 ", "START": "🚀 ", "DONE": "🏁 ",
    }
    icon = icons.get(level, "   ")
    print(f"[{ts}] {icon} [{level}] {msg}")


def run_tests(suite_name: str, tests: list) -> int:
    print(f"\n{'='*62}")
    print(f"  TEST SUITE — {suite_name}")
    print(f"{'='*62}")
    passed = 0
    failed = 0
    for name, fn in tests:
        try:
            ok, detail = fn()
            if ok:
                print(f"  ✅ PASS  {name}")
                passed += 1
            else:
                print(f"  ❌ FAIL  {name}")
                print(f"           → {detail}")
                failed += 1
        except Exception as e:
            print(f"  ❌ FAIL  {name}")
            print(f"           → Exception: {e}")
            failed += 1
    total = passed + failed
    print(f"{'─'*62}")
    print(f"  Result: {passed}/{total} passed  |  {failed} failed")
    print(f"{'='*62}\n")
    return passed


# %%
# ── Constants ─────────────────────────────────────────────────

TEST_ZIPS = ["15213", "90210", "28277", "85257", "60614", "77002", "30309", "80202"]
TEST_ZIP_METROS = {
    "15213": "Pittsburgh",
    "90210": "Los Angeles",
    "28277": "Charlotte",
    "85257": "Phoenix",
    "60614": "Chicago",
    "77002": "Houston",
    "30309": "Atlanta",
    "80202": "Denver",
}
EXPECTED_METROS = {"Pittsburgh", "Los Angeles", "Phoenix", "Charlotte", "Chicago", "Houston", "Atlanta", "Denver"}
VALID_GRADES = {"A", "B", "C", "D", "F"}

# Table configurations
# Each entry: (table_name, grade_column, dimension_filter_or_None)
TABLE_CONFIGS = [
    ("respiratory_scores",     "letter_grade",  None),
    ("cardiovascular_scores",  "letter_grade",  None),
    ("stress_scores",          "letter_grade",  None),
    ("food_access_scores",     "letter_grade",  None),
    ("heat_scores",            "letter_grade",  None),
    ("overall_scores",         "letter_grade",  None),
]

TOOL_LABELS = {
    "respiratory_scores":     "Respiratory",
    "cardiovascular_scores":  "Cardiovascular",
    "stress_scores":          "Stress",
    "food_access_scores":     "Food Access",
    "heat_scores":            "Heat",
    "overall_scores":         "Overall",
}


# %%
# ── Helper: query with optional dimension filter ──────────────

def query_table(table, select_cols, dimension_filter=None, extra_filters=None):
    """Build a Supabase query with optional score_dimension filter."""
    q = supabase.table(table).select(select_cols)
    if dimension_filter:
        q = q.eq("score_dimension", dimension_filter)
    if extra_filters:
        for col, val in extra_filters.items():
            q = q.eq(col, val)
    return q


def get_count(table, dimension_filter=None, null_col=None):
    """Get row count, optionally filtering for null values in a column."""
    q = supabase.table(table).select("zipcode", count="exact")
    if dimension_filter:
        q = q.eq("score_dimension", dimension_filter)
    if null_col:
        q = q.is_(null_col, "null")
    result = q.execute()
    return result.count or 0


def get_row(table, zipcode, dimension_filter=None):
    """Fetch a single row by zipcode."""
    q = supabase.table(table).select("*").eq("zipcode", zipcode)
    if dimension_filter:
        q = q.eq("score_dimension", dimension_filter)
    result = q.limit(1).execute()
    return result.data[0] if result.data else None


# %% [markdown]
# ## 1 · Per-Table Integrity Tests

# %%
log("START", "Per-table integrity tests across all 6 score tables")

total_passed = 0
total_tests = 0

for table, grade_col, dim_filter in TABLE_CONFIGS:
    label = TOOL_LABELS[table]
    tests = []

    # 1. Row count >= 540
    tests.append((
        f"Row count >= 1100",
        lambda t=table, d=dim_filter: (
            (count := get_count(t, d)) >= 1100,
            f"Got {count}"
        )
    ))

    # 2. No null composite_score
    tests.append((
        f"No null composite_score",
        lambda t=table, d=dim_filter: (
            (nulls := get_count(t, d, null_col="composite_score")) == 0,
            f"Found {nulls} null composite_score rows"
        )
    ))

    # 3. No null grade column
    tests.append((
        f"No null {grade_col}",
        lambda t=table, d=dim_filter, gc=grade_col: (
            (nulls := get_count(t, d, null_col=gc)) == 0,
            f"Found {nulls} null {gc} rows"
        )
    ))

    # 4. All 4 test ZIPs present
    for zc in TEST_ZIPS:
        tests.append((
            f"Test ZIP {zc} present",
            lambda t=table, d=dim_filter, z=zc: (
                get_row(t, z, d) is not None,
                f"ZIP {z} not found in {t}"
            )
        ))

    # 5. score_date column exists and is non-null
    tests.append((
        f"score_date non-null (all rows)",
        lambda t=table, d=dim_filter: (
            (nulls := get_count(t, d, null_col="score_date")) == 0,
            f"Found {nulls} null score_date rows"
        )
    ))

    # 6. All grade values valid
    tests.append((
        f"All {grade_col} values in {{A,B,C,D,F}}",
        lambda t=table, d=dim_filter, gc=grade_col: (
            (lambda: (
                rows := query_table(t, gc, d).execute().data,
                grades := {r[gc] for r in rows if r.get(gc)},
                (grades.issubset(VALID_GRADES), f"Found invalid grades: {grades - VALID_GRADES}")
            )[-1])()
        )
    ))

    # 7. composite_score range [0, 100]
    for zc in TEST_ZIPS[:2]:  # Spot check 2 ZIPs for score range
        tests.append((
            f"ZIP {zc} composite_score in [0, 100]",
            lambda t=table, d=dim_filter, z=zc: (
                (row := get_row(t, z, d)) is not None
                and 0 <= float(row["composite_score"]) <= 100,
                f"ZIP {z}: score={get_row(t, z, d) and get_row(t, z, d).get('composite_score')}"
            )
        ))

    suite_passed = run_tests(f"{label} ({table}) — INTEGRITY", tests)
    total_passed += suite_passed
    total_tests += len(tests)


# %% [markdown]
# ## 2 · Cross-Table Consistency (4 Test ZIPs)

# %%
log("START", "Cross-table consistency checks for 4 test ZIPs")

# Tool tables (excluding overall)
TOOL_TABLES = [
    ("respiratory_scores",     "letter_grade",  None),
    ("cardiovascular_scores",  "letter_grade",  None),
    ("stress_scores",          "letter_grade",  None),
    ("food_access_scores",     "letter_grade",  None),
    ("heat_scores",            "letter_grade",  None),
]

cross_tests = []

for zc in TEST_ZIPS:
    expected_metro = TEST_ZIP_METROS[zc]

    # Fetch all 5 tool scores for this ZIP
    scores = {}
    grades = {}
    metros = {}
    for t_table, t_grade_col, t_dim in TOOL_TABLES:
        row = get_row(t_table, zc, t_dim)
        t_label = TOOL_LABELS[t_table]
        if row:
            scores[t_label] = float(row["composite_score"]) if row.get("composite_score") is not None else None
            grades[t_label] = row.get(t_grade_col)
            metros[t_label] = row.get("metro")

    # Check 1: All 5 tool scores exist
    cross_tests.append((
        f"ZIP {zc}: all 5 tool scores present",
        lambda s=scores: (
            len(s) == 5 and all(v is not None for v in s.values()),
            f"Found {len(s)} scores: {s}"
        )
    ))

    # Check 2: All 5 scores in [0, 100]
    cross_tests.append((
        f"ZIP {zc}: all scores in [0, 100]",
        lambda s=scores: (
            all(0 <= v <= 100 for v in s.values() if v is not None),
            f"Scores: {s}"
        )
    ))

    # Check 3: All 5 grades valid
    cross_tests.append((
        f"ZIP {zc}: all grades in {{A,B,C,D,F}}",
        lambda g=grades: (
            all(v in VALID_GRADES for v in g.values() if v is not None),
            f"Grades: {g}"
        )
    ))

    # Check 4: Metro consistent across tables
    cross_tests.append((
        f"ZIP {zc}: metro consistent across all tables",
        lambda m=metros, em=expected_metro: (
            all(v == em for v in m.values() if v is not None),
            f"Metros: {m}, expected: {em}"
        )
    ))

    # Check 5: If overall_scores exists, composite ≈ mean of 5 tools
    overall_row = get_row("overall_scores", zc)
    if overall_row and all(v is not None for v in scores.values()):
        expected_mean = sum(scores.values()) / 5
        actual_overall = float(overall_row["composite_score"])
        cross_tests.append((
            f"ZIP {zc}: overall ≈ mean of 5 tools (±1.0)",
            lambda a=actual_overall, e=expected_mean: (
                abs(a - e) <= 1.0,
                f"Overall: {a:.2f}, mean of 5: {e:.2f}, diff: {abs(a - e):.2f}"
            )
        ))
    else:
        cross_tests.append((
            f"ZIP {zc}: overall_scores row present (or table empty — deferred)",
            lambda r=overall_row: (
                r is not None,
                "overall_scores not yet populated for this ZIP (run overall pipeline first)"
            )
        ))

cross_passed = run_tests("CROSS-TABLE CONSISTENCY", cross_tests)
total_passed += cross_passed
total_tests += len(cross_tests)


# %% [markdown]
# ## 3 · Metro Distribution Check

# %%
log("START", "Metro distribution checks across all 6 tables")

metro_tests = []

for table, grade_col, dim_filter in TABLE_CONFIGS:
    label = TOOL_LABELS[table]

    # All tool tables now have a metro column — no special cases needed

    for metro in sorted(EXPECTED_METROS):
        metro_tests.append((
            f"{label}: {metro} >= 20 ZIPs",
            lambda t=table, d=dim_filter, m=metro: (
                (lambda: (
                    q := supabase.table(t).select("zipcode", count="exact").eq("metro", m),
                    q2 := q.eq("score_dimension", d) if d else q,
                    count := q2.execute().count or 0,
                    (count >= 20, f"Got {count}")
                )[-1])()
            )
        ))

metro_passed = run_tests("METRO DISTRIBUTION", metro_tests)
total_passed += metro_passed
total_tests += len(metro_tests)


# %% [markdown]
# ## 4 · Summary Report

# %%
print("\n" + "=" * 62)
print("  QA DATA INTEGRITY — FINAL SUMMARY")
print("=" * 62)
print(f"  Total tests: {total_tests}")
print(f"  Passed:      {total_passed}")
print(f"  Failed:      {total_tests - total_passed}")
print("=" * 62)

if total_passed == total_tests:
    log("DONE", "ALL TESTS PASSED — data integrity verified across all 6 tables and 8 metros")
else:
    log("WARN", f"{total_tests - total_passed} tests failed — review failures above before proceeding")


# %% [markdown]
# ## 5 · Streamlit UI Manual Checklist
#
# Run in browser against the live Streamlit app.
# Test with all 4 ZIPs: 15213, 90210, 28277, 85257
#
# ```
# ═══════════════════════════════════════════════════════════════
# STREAMLIT FULL QA — ALL TABS × ALL METROS
# ═══════════════════════════════════════════════════════════════
#
# CROSS-TAB CHECKS (do once)
#   [ ] All 5 tab labels visible and readable (not faint)
#   [ ] Switching tabs shows no stale data from previous tab
#   [ ] Same ZIP in different tabs returns different tool-specific scores
#   [ ] Footer: EPA, CDC PLACES, BTS, NLCD, NASA VIIRS, USDA all listed
#
# ───────────────────────────────────────────────────────────────
# TAB: 🌿 Respiratory
# Info box: bg=#EEF5F1, text=#2D6644
# Grade colors: A=#1A6B3C, B=#3A8C5C
# ───────────────────────────────────────────────────────────────
#   ZIP 15213  [ ] result shown  [ ] disc ok  [ ] breakdown ok  [ ] interp ok  [ ] metro peers ok
#   ZIP 90210  [ ] result shown  [ ] disc ok  [ ] breakdown ok  [ ] interp ok  [ ] metro peers ok
#   ZIP 28277  [ ] result shown  [ ] disc ok  [ ] breakdown ok  [ ] interp ok  [ ] metro peers ok
#   ZIP 85257  [ ] result shown  [ ] disc ok  [ ] breakdown ok  [ ] interp ok  [ ] metro peers ok
#   ZIP 00000  [ ] graceful "not found" message
#   Empty      [ ] info box prompt shown
#   Overall card: [ ] appears with score & grade (or gracefully absent)
#
# ───────────────────────────────────────────────────────────────
# TAB: ❤️ Cardiovascular
# Info box: bg=#FFF2EE, text=#8B3A2A
# Grade colors: A=#C1121F, B=#E63946
# ───────────────────────────────────────────────────────────────
#   ZIP 15213  [ ] result shown  [ ] disc ok  [ ] breakdown ok  [ ] interp ok  [ ] metro peers ok
#   ZIP 90210  [ ] result shown  [ ] disc ok  [ ] breakdown ok  [ ] interp ok  [ ] metro peers ok
#   ZIP 28277  [ ] result shown  [ ] disc ok  [ ] breakdown ok  [ ] interp ok  [ ] metro peers ok
#   ZIP 85257  [ ] result shown  [ ] disc ok  [ ] breakdown ok  [ ] interp ok  [ ] metro peers ok
#   ZIP 00000  [ ] graceful "not found" message
#   Empty      [ ] info box prompt shown
#   Overall card: [ ] appears with score & grade (or gracefully absent)
#
# ───────────────────────────────────────────────────────────────
# TAB: 🧠 Stress / Sensory
# Info box: bg=#EDE8F5, text=#3A0CA3
# Grade colors: A=#3A0CA3, B=#4361EE
# ───────────────────────────────────────────────────────────────
#   ZIP 15213  [ ] result shown  [ ] disc ok  [ ] breakdown ok  [ ] interp ok  [ ] metro peers ok
#   ZIP 90210  [ ] result shown  [ ] disc ok  [ ] breakdown ok  [ ] interp ok  [ ] metro peers ok
#   ZIP 28277  [ ] result shown  [ ] disc ok  [ ] breakdown ok  [ ] interp ok  [ ] metro peers ok
#   ZIP 85257  [ ] result shown  [ ] disc ok  [ ] breakdown ok  [ ] interp ok  [ ] metro peers ok
#   ZIP 00000  [ ] graceful "not found" message
#   Empty      [ ] info box prompt shown
#   Overall card: [ ] appears with score & grade (or gracefully absent)
#
# ───────────────────────────────────────────────────────────────
# TAB: 🥦 Food Access
# Info box: bg=#EEF5E6, text=#386641
# Grade colors: A=#386641, B=#6A994E
# ───────────────────────────────────────────────────────────────
#   ZIP 15213  [ ] result shown  [ ] disc ok  [ ] breakdown ok  [ ] interp ok  [ ] metro peers ok
#   ZIP 90210  [ ] result shown  [ ] disc ok  [ ] breakdown ok  [ ] interp ok  [ ] metro peers ok
#   ZIP 28277  [ ] result shown  [ ] disc ok  [ ] breakdown ok  [ ] interp ok  [ ] metro peers ok
#   ZIP 85257  [ ] result shown  [ ] disc ok  [ ] breakdown ok  [ ] interp ok  [ ] metro peers ok
#   ZIP 00000  [ ] graceful "not found" message
#   Empty      [ ] info box prompt shown
#   Overall card: [ ] appears with score & grade (or gracefully absent)
#
# ───────────────────────────────────────────────────────────────
# TAB: 🌡️ Heat & Climate
# Info box: bg=#FEF3E2, text=#7C3A00
# Grade colors: A=#E85D04, B=#F48C06
# ───────────────────────────────────────────────────────────────
#   ZIP 15213  [ ] result shown  [ ] disc ok  [ ] breakdown ok  [ ] interp ok  [ ] metro peers ok
#   ZIP 90210  [ ] result shown  [ ] disc ok  [ ] breakdown ok  [ ] interp ok  [ ] metro peers ok
#   ZIP 28277  [ ] result shown  [ ] disc ok  [ ] breakdown ok  [ ] interp ok  [ ] metro peers ok
#   ZIP 85257  [ ] result shown  [ ] disc ok  [ ] breakdown ok  [ ] interp ok  [ ] metro peers ok
#   ZIP 00000  [ ] graceful "not found" message
#   Empty      [ ] info box prompt shown
#   Overall card: [ ] appears with score & grade (or gracefully absent)
#
# ═══════════════════════════════════════════════════════════════
# TOTALS
#   Cross-tab checks:    ___/4
#   Per-tab checks:      ___/35 per tab × 5 tabs = ___/175
#   GRAND TOTAL:         ___/179
# ═══════════════════════════════════════════════════════════════
# ```
