# AGENTS.md — LaSalle Technologies Health Score Suite
> Read this file in full before beginning any task. It is the authoritative reference for architecture, methodology, conventions, and constraints for this project.

---

## 1. Project Overview

**LaSalle Technologies** has built a five-tool neighborhood-level health environment scoring platform called **The Health Environment Score**. It synthesizes federal data into scored, letter-graded dimensions across **574 ZIP codes** in four pilot metros: **Pittsburgh, Los Angeles, Phoenix, and Charlotte**. All 5 tools are complete and live.

**Target completion:** April 30, 2026  
**Repo:** `health-score-tool` (GitHub)  
**Stack:** Google Colab → Supabase PostgreSQL → Streamlit Community Cloud → Claude API

### The Five Tools + Overall
| # | Tool | Table | Status | Color Palette |
|---|---|---|---|---|
| 1 | 🫁 Respiratory Health Score | `composite_scores` | ✅ Complete | Green |
| 2 | ❤️ Cardiovascular Health Score | `cardiovascular_scores` | ✅ Complete | Red/Pink |
| 3 | 🧠 Stress / Sensory Environment Score | `stress_scores` | ✅ Complete | Blue/Purple |
| 4 | 🥦 Food Access Score | `food_access_scores` | ✅ Complete | Green/Yellow |
| 5 | 🌡️ Heat & Climate Resilience Score | `heat_scores` | ✅ Complete | Orange/Red |
| — | 📊 Overall Composite | `overall_scores` | ✅ Complete | — |

**All 5 tools scored across 574 ZIPs in 4 metros. Overall scores computed as equal-weighted average of all 5 tool composites. All tables include `score_date` for historical tracking.**

---

## 2. Planning Mode Protocol

**Always use planning mode before writing any code.** No exceptions.

Before executing any task, produce a written plan in this format:

```
## Plan: [Task Name]

### What I am about to do
[1–3 sentence summary of the goal]

### Files I will read first
- [list every existing file to inspect before writing anything new]

### Files I will create or modify
- [filename]: [purpose]

### Dependencies and risks
- [anything that could fail, any data assumptions being made, any environment setup needed]

### Ordered steps
1. [specific, concrete step]
2. [specific, concrete step]
...

### How I will verify success
- [specific checks — e.g., "query Supabase and confirm row count = 600", not just "run the script"]
- [spot-check criteria — which ZIPs, what expected ranges]
```

Present this plan and wait for explicit written approval ("looks good", "proceed", "approved", etc.) before executing any step. If approval is ambiguous, ask for clarification before proceeding.

---

## 3. Scoring Methodology

This is proprietary. Do not expose weights, normalization logic, or algorithmic decisions in any public-facing file, README, or UI copy.

### Pipeline (apply consistently across all tools)
1. **Ingest** — Pull from federal data source for all 600 ZIPs across all 4 metros
2. **Validate** — Check for nulls, outliers, coverage gaps by metro; log issues
3. **Normalize** — Min-max normalization per component, scaled 0–100
   - Formula: `normalized = (value - min) / (max - min) * 100`
   - Higher normalized score = better health environment (invert where needed)
   - Use global min/max across all 600 ZIPs, not per-metro
4. **Weight** — Apply component weights (specified per tool in `TOOL_SPECS.md`)
5. **Composite** — Weighted sum → composite score 0–100
6. **Grade** — Letter grade assignment:
   - A: ≥ 80
   - B: 65–79
   - C: 50–64
   - D: 35–49
   - F: < 35
7. **Interpret** — Generate plain-language interpretation via Claude API
8. **Write** — Upsert to Supabase

### Critical Methodology Rules
- Always invert components where higher raw value = worse health outcome (e.g., inactivity rate, disease prevalence, noise exposure, impervious surface)
- Null handling: log nulls explicitly; do not silently drop ZIPs; flag in validation report
- Re-runnability: all writes use upsert on UNIQUE constraint, never raw INSERT
- Global normalization prevents metro-specific score inflation

---

## 4. Supabase Schema

**Credentials:** Stored in Colab secrets manager as `SUPABASE_URL` and `SUPABASE_KEY`. Never hardcode. Never log.

### Established Tables
- `zip_codes` — master ZIP reference table
- `raw_signals` — raw ingested values before normalization
- `score_components` — normalized component scores per ZIP
- `composite_scores` — final weighted composite + letter grade
- `interpretations` — Claude API plain-language outputs
- `score_config` — weights and methodology config (internal only)

### Per-Tool Table Pattern
Each tool gets its own scores table following this naming convention:
- `respiratory_scores` (reference implementation — read this first)
- `cardiovascular_scores`
- `stress_scores`
- `food_access_scores`
- `heat_scores`

### Schema Pattern (replicate exactly)
```sql
CREATE TABLE [tool]_scores (
  id SERIAL PRIMARY KEY,
  zipcode TEXT NOT NULL,          -- NO underscore — matches zip_codes.zipcode exactly
  metro TEXT NOT NULL,            -- short form: "Pittsburgh", "Los Angeles", "Phoenix", "Charlotte"
  [component_1]_raw NUMERIC,
  [component_1]_normalized NUMERIC,
  [component_2]_raw NUMERIC,
  [component_2]_normalized NUMERIC,
  -- repeat for all components
  composite_score NUMERIC,
  letter_grade TEXT,
  interpretation TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(zipcode)                 -- raw_signals uses a compound UNIQUE — see TOOL_SPECS.md
);
```

### Upsert Pattern (always use this — never raw INSERT)
```python
# Column name is "zipcode" — NO underscore. This must match the Supabase schema exactly.
# Metro values are short form: "Pittsburgh", not "Pittsburgh, PA"
record = {
    "zipcode": "15213",                    # TEXT — no underscore
    "metro": "Pittsburgh",                 # TEXT — short form only
    "physical_inactivity_raw": 28.4,
    "physical_inactivity_normalized": 61.2,
    # ... all other components ...
    "composite_score": 58.7,
    "letter_grade": "C",
    "interpretation": "...",
}

supabase.table("[tool]_scores").upsert(
    record,
    on_conflict="zipcode"                  # no underscore — matches UNIQUE constraint
).execute()
```

**Batch write pattern (preferred for full 600-ZIP runs):**
```python
failed_zips = []
records = [...]  # list of dicts, one per ZIP
for record in records:
    try:
        supabase.table("[tool]_scores").upsert(record, on_conflict="zipcode").execute()
    except Exception as e:
        log("ERROR", f"Failed to write ZIP {record.get('zipcode')}: {e}")
        failed_zips.append(record.get("zipcode"))
        # Log and continue — never let one failed ZIP abort the entire run

if failed_zips:
    log("WARN", f"{len(failed_zips)} ZIPs failed: {failed_zips}")
```

**raw_signals uses a compound UNIQUE constraint** — see `TOOL_SPECS.md` for its upsert pattern. Never use single-column `on_conflict` for `raw_signals`.

---

## 5. Data Sources

All data is sourced from federal/public sources at no cost. This is a deliberate strategic choice — do not introduce paid data dependencies without explicit approval.

| Source | Used In | Access Method |
|---|---|---|
| EPA AQS | Respiratory | API |
| EPA EJScreen | Respiratory | API |
| CDC PLACES (`c7b2-4ecy`) | Respiratory, Cardiovascular, Stress, Food Access | Socrata API |
| BTS Transportation Noise | Cardiovascular, Stress | GeoTIFF raster download |
| NLCD Impervious Surface | Cardiovascular, Heat | GeoTIFF raster download |
| NLCD Green Space / Tree Canopy | Stress, Heat | GeoTIFF raster download |
| NASA VIIRS (light pollution) | Stress | GeoTIFF raster download |
| USDA FARA | Food Access | Tabular download |
| USDA Food Environment Atlas | Food Access | Tabular download |
| USGS Heat Island | Heat | Raster download |

### Raster Processing Rules
- Use `rasterio` and `geopandas` in Google Colab
- Aggregate raster values to ZIP polygons using ZCTA shapefiles
- Always use zonal statistics (mean) unless otherwise specified in `TOOL_SPECS.md`
- Reuse already-processed rasters across tools — never re-download if cached
- BTS noise raster: processed in Cardiovascular phase, reused in Stress phase

### ZIP Coverage
```python
# The 600 ZIPs span four metros — always filter to these
METROS = ["Pittsburgh", "Los Angeles", "Phoenix", "Charlotte"]
# Master ZIP list lives in the zip_codes Supabase table
```

---

## 6. Streamlit UI Conventions

Read the existing Respiratory tool implementation before building any new tab. All tabs must be consistent with it.

### Component Structure
- One tab per tool in the main app
- Each tab: ZIP lookup → disc visualization → component breakdown → plain-language interpretation
- Disc visualization: Apple Health-inspired single disc with weighted arc segments per component
- Component breakdown: shows each component's normalized score and contribution

### Color Palettes (per tool — use consistently)
```python
TOOL_COLORS = {
    "respiratory": ["#2D6A4F", "#40916C", "#52B788", "#74C69D"],   # Green hues
    "cardiovascular": ["#C1121F", "#E63946", "#FF6B6B", "#FF9999"], # Red/Pink
    "stress": ["#3A0CA3", "#4361EE", "#4CC9F0", "#7B2FBE"],        # Blue/Purple
    "food_access": ["#386641", "#6A994E", "#A7C957", "#F2E318"],    # Green/Yellow
    "heat": ["#D62828", "#F77F00", "#FCBF49", "#EAE2B7"],          # Orange/Red
}
```

### Claude API Interpretation Pattern
```python
import anthropic

def generate_interpretation(zip_code: str, tool_name: str, composite_score: float, 
                             letter_grade: str, components: dict) -> str:
    """
    Generate a plain-language health environment interpretation for a ZIP code.
    components: dict of {component_label: normalized_score} — qualitative labels only, no weights.
    Returns interpretation string to store in Supabase.
    """
    client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from environment
    
    prompt = f"""You are a public health analyst writing a plain-language summary for residents and 
real estate professionals. Write 2-3 sentences interpreting this neighborhood's {tool_name} health environment score.

ZIP Code: {zip_code}
Score: {composite_score:.1f}/100 (Grade: {letter_grade})
Component signals: {components}

Rules:
- Be specific, factual, and actionable
- Do not use jargon
- Do not mention scores, percentages, or numbers from the components
- Do not reveal how components are weighted or combined
- Do not say "based on our methodology" or any similar phrase"""

    response = client.messages.create(
        model="claude-opus-4-5-20251101",  # update model string if newer version available
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.content[0].text
```

### Weight Validation Rule
Before writing any composite score to Supabase, assert weights sum to exactly 1.0 (100%):
```python
weights = [w1, w2, w3, w4]  # floats, e.g. [0.30, 0.25, 0.25, 0.20]
assert abs(sum(weights) - 1.0) < 1e-9, f"Weights sum to {sum(weights)}, must equal 1.0"
```
This check must appear in every scoring script before the composite calculation loop.

---

## 7. What Is and Is Not Public

This is a **"public score, private engine"** platform. Understand the boundary before writing any code, copy, or documentation.

### Public (safe to expose in UI, README, any output)
- ZIP-level scores and letter grades
- The component factors for each tool and their general qualitative descriptions (e.g., "transportation noise levels in your area") — but never the weights
- The federal data sources used
- The full letter grade scale (A ≥ 80, B 65–79, C 50–64, D 35–49, F < 35)
- Plain-language interpretations generated by Claude API

### Proprietary (never expose in any file, UI, log, or output)
- Exact component weights
- Normalization formula details
- Min/max values used in normalization
- Cross-tool composite methodology (if built)
- The `score_config` table contents

If you are generating any public-facing file (README, Streamlit UI text, website copy), treat the methodology as a black box. Describe *what* is measured, never *how* it is calculated.

---

## 8. Reference Implementation

**The Respiratory tool is the gold standard.** Before building any new tool:
1. Read the Respiratory ingestion script
2. Read the Respiratory Supabase write script
3. Read the Respiratory Streamlit tab
4. Match the pattern exactly unless there is a documented reason to deviate

Deviations from the reference implementation must be noted in a comment explaining why.

---

## 9. Code Quality Standards

### Structure
- **Re-runnability first** — every script must be safely re-runnable without duplicating data or side effects
- **One function per logical step** — `ingest()`, `validate()`, `normalize()`, `score()`, `interpret()`, `write()` — never combine steps into one monolithic block
- **No hardcoded credentials** — always read from Colab secrets manager; raise a clear error if a secret is missing
- **No hardcoded paths** — use config constants at the top of each notebook, never inline strings
- **Comments explain why** — the code shows what; comments explain the intent, the decision, or the gotcha

### Logging Standard
Every script must use structured print-based logging (Colab has no log file system). Use this exact format so output is scannable:

```python
import traceback
from datetime import datetime

def log(level: str, message: str):
    """level: INFO | WARN | ERROR | PASS | FAIL"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    prefix = {
        "INFO":  "ℹ️ ",
        "WARN":  "⚠️ ",
        "ERROR": "❌ ",
        "PASS":  "✅ ",
        "FAIL":  "❌ FAIL",
        "TEST":  "🧪 ",
    }.get(level, "   ")
    print(f"[{timestamp}] {prefix} {message}")
```

Use `log("INFO", ...)` for progress, `log("WARN", ...)` for non-fatal issues, `log("ERROR", ...)` for failures, `log("PASS", ...)` and `log("FAIL", ...)` exclusively for test results.

### Error Handling Standard
Never use bare `except`. Always catch specific exceptions and log with context:

```python
# WRONG
try:
    result = fetch_data()
except:
    pass

# CORRECT
try:
    result = fetch_data()
except requests.HTTPError as e:
    log("ERROR", f"HTTP {e.response.status_code} fetching CDC PLACES for ZIP {zip_code}: {e}")
    raise  # re-raise after logging unless the failure is intentionally skippable
except Exception as e:
    log("ERROR", f"Unexpected error in fetch_data() for ZIP {zip_code}: {e}")
    log("ERROR", traceback.format_exc())
    raise
```

For batch loops (writing 600 ZIPs), catch per-record and continue — never let one record abort the run:
```python
failed_zips = []
for record in records:
    try:
        write_to_supabase(record)
    except Exception as e:
        log("ERROR", f"Failed to write ZIP {record.get('zip_code')}: {e}")
        failed_zips.append(record.get("zip_code"))

if failed_zips:
    log("WARN", f"{len(failed_zips)} ZIPs failed to write: {failed_zips}")
else:
    log("PASS", "All records written successfully")
```

### Validation Report Standard
Every ingestion step must print a validation report before proceeding to normalization. Use this format:

```python
def print_validation_report(tool_name: str, df, expected_zips: int = 600):
    print(f"\n{'='*60}")
    print(f"VALIDATION REPORT — {tool_name.upper()}")
    print(f"{'='*60}")
    print(f"  Total rows:        {len(df)}")
    print(f"  Expected ZIPs:     {expected_zips}")
    print(f"  Coverage:          {len(df)/expected_zips*100:.1f}%")
    print(f"  Null counts:")
    for col in df.columns:
        null_count = df[col].isna().sum()
        if null_count > 0:
            print(f"    {col}: {null_count} nulls ({null_count/len(df)*100:.1f}%)")
    print(f"  Numeric ranges:")
    for col in df.select_dtypes(include='number').columns:
        print(f"    {col}: min={df[col].min():.3f}, max={df[col].max():.3f}, mean={df[col].mean():.3f}")
    print(f"  Metro breakdown:")
    if 'metro' in df.columns:
        for metro, count in df['metro'].value_counts().items():
            print(f"    {metro}: {count} ZIPs")
    print(f"{'='*60}\n")
```

---

## 10. Testing Protocol

**Every pipeline function must have a corresponding test.** Tests run after each step and print PASS/FAIL for every assertion. A session is not complete until all tests pass.

### Test Output Format
All test results must be printed in this exact format so they are scannable in Colab output:

```
============================================================
TEST SUITE — [TOOL NAME] — [STEP NAME]
============================================================
  ✅ PASS  [test description]
  ✅ PASS  [test description]
  ❌ FAIL  [test description] — expected: [X], got: [Y]
  ⚠️ WARN  [test description] — [non-fatal issue]
------------------------------------------------------------
  Result: 3/4 passed | 1 failed | 0 warnings
============================================================
```

### Test Runner Pattern
Use this base pattern for all test suites:

```python
def run_tests(suite_name: str, tests: list[tuple]) -> bool:
    """
    Run a list of (test_name, test_fn) tuples and print PASS/FAIL for each.
    test_fn must return (passed: bool, detail: str)
    Returns True if all tests passed.
    """
    print(f"\n{'='*60}")
    print(f"TEST SUITE — {suite_name}")
    print(f"{'='*60}")
    
    passed = 0
    failed = 0
    results = []
    
    for test_name, test_fn in tests:
        try:
            ok, detail = test_fn()
            if ok:
                print(f"  ✅ PASS  {test_name}")
                passed += 1
            else:
                print(f"  ❌ FAIL  {test_name} — {detail}")
                failed += 1
            results.append((test_name, ok, detail))
        except Exception as e:
            print(f"  ❌ FAIL  {test_name} — raised exception: {e}")
            failed += 1
    
    print(f"{'-'*60}")
    print(f"  Result: {passed}/{passed+failed} passed | {failed} failed")
    print(f"{'='*60}\n")
    
    return failed == 0

# Usage example:
all_passed = run_tests("CARDIOVASCULAR — INGESTION", [
    ("Row count >= 550",         lambda: (len(df) >= 550, f"got {len(df)}")),
    ("No null zip_codes",        lambda: (df['zip_code'].isna().sum() == 0, f"{df['zip_code'].isna().sum()} nulls")),
    ("CHD values in range 0–50", lambda: (df['chd_raw'].between(0, 50).all(), f"min={df['chd_raw'].min()}, max={df['chd_raw'].max()}")),
    ("All 4 metros present",     lambda: (set(df['metro'].unique()) == set(METRO_LABELS.values()), f"found: {df['metro'].unique()}")),
])

if not all_passed:
    raise RuntimeError("Ingestion tests failed — do not proceed to normalization")
```

### Required Test Suites Per Tool
Build and run all of the following test suites in every tool pipeline:

**1. Ingestion Tests** — run immediately after pulling raw data
- Row count ≥ 550 (allow small gap for ZIPs with no federal data)
- No null ZIP codes
- All 4 metros represented
- All expected columns present
- Raw value ranges within plausible domain (e.g., inactivity rate 5–60%, not 0 or 100+)
- No duplicate ZIP codes in the ingested data

**2. Normalization Tests** — run after normalizing each component
- All normalized values in range [0.0, 100.0] — no exceptions
- No nulls in normalized columns
- Inversion correctness: highest raw value → lowest normalized score (for inverted components)
- Global min normalizes to 0.0, global max normalizes to 100.0
- Weights sum exactly to 1.0 (`abs(sum(weights) - 1.0) < 1e-9`)

**3. Scoring Tests** — run after computing composite scores
- All composite scores in range [0.0, 100.0]
- Letter grade assignment matches grade scale for every row (spot-check 20 ZIPs)
- No null composite scores or letter grades
- Score distribution sanity: no tool should give > 80% of ZIPs the same grade
- Known ZIP spot-checks: verify manually selected ZIPs score in expected direction (e.g., affluent suburban ZIP should not score F)

**4. Supabase Write Tests** — run after upsert
- Row count in target table matches expected (query Supabase, not just local data)
- Re-run upsert on same data — row count must not increase (idempotency check)
- Spot-query 5 specific ZIPs from Supabase and verify values match local computed values
- No null composite_score or letter_grade in the table

**5. Streamlit Smoke Tests** — run manually after deploying each new tab
- ZIP lookup returns a result for at least one ZIP from each metro
- Disc visualization renders without error
- Component breakdown displays correct number of components
- Interpretation text is present and non-empty
- No Streamlit exceptions in browser console

### Blocking Rule
**If any test in suites 1–4 fails, do not proceed to the next pipeline step.** Fix the failure, re-run the test suite, confirm all pass, then continue. Document the failure and fix in the session log in `CONTEXT.md`.

---

## 11. Session Startup Checklist

At the start of every Claude Code session, complete these steps in order before any task:

1. `CLAUDE.md` is auto-read — you have already read it
2. Read `AGENTS.md` in full (this file)
3. Read `TESTING.md` — internalize the test runner, all five suites, the gate rules, and the debugging checklist
4. Read `TOOL_SPECS.md` — identify the active tool and confirm components, weights, data sources
5. Read `CONTEXT.md` — identify current state, what is complete, what is blocked
6. Read `ARCHITECTURE.md` — confirm infrastructure constraints relevant to the session task
7. Read the Respiratory reference implementation: `notebooks/respiratory/` and `streamlit/tabs/respiratory.py`
8. Produce a written plan per Section 2 format and wait for explicit approval before writing any code

Do not proceed to step 8 until steps 2–7 are complete. Do not write code until the plan is approved.

---

## 12. Agent Roles

Four specialized agent roles for ongoing development and QA:

### Agent 1 — Data & Backend QA
**Scope:** Runs `notebooks/qa/qa_data_integrity.py`, validates pipeline outputs, checks `score_date` versioning across all 6 tables.
- Executes the 106-test QA suite and reports pass/fail
- Validates that all tables have consistent `score_date` values after pipeline runs
- Spot-checks row counts, null rates, and grade distributions
- Flags any cross-table inconsistencies (e.g., ZIP in one table but not another)

### Agent 2 — Frontend QA
**Scope:** Verifies Streamlit (current) and Lovable (next) UI across all tabs and test ZIPs.
- Tests all 5 tool tabs + overall with 4 test ZIPs: 15213, 90210, 28277, 85257
- Validates disc visualization, component breakdown, interpretation text, metro peers
- Checks graceful error handling (invalid ZIP, empty input)
- Runs Suite 5 manual Streamlit smoke tests from `TESTING.md`

### Agent 3 — Metro Expansion
**Scope:** Runs all 5 tool pipelines plus overall for new metros, updates `zip_codes` table.
- Adds new metro ZIPs to `zip_codes` table first
- Executes all 5 pipelines (Respiratory, Cardiovascular, Stress, Food Access, Heat) for new ZIPs
- Runs overall composite pipeline for new ZIPs
- Re-runs QA suite to confirm new metro data passes all checks
- Note: global normalization may need recalibration when new metros are added

### Agent 4 — Frontend Build (Lovable)
**Scope:** Builds Lovable React components consuming Supabase REST API directly.
- Supabase project ref: `hakiksjnpipgstomzzjy` — auto-exposes REST endpoints per table
- Lovable fetches directly using the Supabase anon key (no backend proxy needed)
- Replicates all Streamlit UI features: ZIP lookup, disc viz, component breakdown, interpretation, metro peers
- Maintains the public/proprietary boundary — no weights or methodology in frontend code
- Target: replace Streamlit with Lovable at `lasalletech.ai`
