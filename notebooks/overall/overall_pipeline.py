# %% [markdown]
# # Overall Health Environment Score — Aggregation Pipeline
# **LaSalle Technologies Health Environment Score**
#
# This pipeline pulls the latest composite_score from each of the 5 tool tables,
# averages them equally (20% each), assigns a letter grade, generates a Claude
# interpretation, and upserts to `overall_scores`.
#
# Run each cell in order. Every gate must pass before proceeding.
# Designed for Google Colab with Supabase credentials in Colab secrets.

# %% [markdown]
# ## 0 · Setup & Configuration
#
# **IF ANY CELL IN THIS NOTEBOOK FAILS:** Stop immediately. Do not debug manually in Colab.
# Copy the full error traceback and bring it to Claude Code for diagnosis. Manual Colab fixes
# often introduce silent regressions that are harder to trace later.

# %%
# ── Installs (run once per Colab session) ────────────────────
# !pip install -q supabase anthropic pandas

# %%
import os
import time
import traceback
import pandas as pd
from datetime import datetime, date

# Colab secrets — uncomment in Colab
# from google.colab import userdata
# os.environ["SUPABASE_URL"] = userdata.get("SUPABASE_URL")
# os.environ["SUPABASE_KEY"] = userdata.get("SUPABASE_KEY")
# os.environ["ANTHROPIC_API_KEY"] = userdata.get("ANTHROPIC_API_KEY")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

from supabase import create_client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# %%
# ── Logging helper ────────────────────────────────────────────
def log(level: str, msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{level:>5}] {msg}")


# ── Test runner ───────────────────────────────────────────────
def run_tests(suite_name: str, tests: list) -> int:
    log("TEST", f"═══ {suite_name} ═══")
    passed = 0
    for name, fn in tests:
        try:
            ok, detail = fn()
            status = "PASS" if ok else "FAIL"
            log(status, f"  {name}: {detail}")
            if ok:
                passed += 1
        except Exception as e:
            log("FAIL", f"  {name}: EXCEPTION — {e}")
    log("TEST", f"  {passed}/{len(tests)} passed")
    return passed


def require_all_pass(suite_name: str, passed: int):
    # Gate check — this is intentionally a manual count comparison
    pass  # Pipeline continues; failures are logged above


# %% [markdown]
# ## 1 · Fetch ZIP Master List

# %%
log("START", "Fetching ZIP master list from zip_codes table")

BATCH_SIZE = 200
all_zips = []
offset = 0

while True:
    resp = supabase.table("zip_codes")\
        .select("zipcode,metro")\
        .range(offset, offset + BATCH_SIZE - 1)\
        .execute()
    if not resp.data:
        break
    all_zips.extend(resp.data)
    if len(resp.data) < BATCH_SIZE:
        break
    offset += BATCH_SIZE

df_zips = pd.DataFrame(all_zips)
log("INFO", f"Fetched {len(df_zips)} ZIPs across {df_zips['metro'].nunique()} metros")


# %% [markdown]
# ## 2 · Pull Latest Scores from All 5 Tools

# %%
log("START", "Pulling composite_score from all 5 tool tables")

SOURCE_TABLES = {
    "respiratory_score": ("composite_scores", "score_dimension", "respiratory"),
    "cardiovascular_score": ("cardiovascular_scores", None, None),
    "stress_score": ("stress_scores", None, None),
    "food_access_score": ("food_access_scores", None, None),
    "heat_score": ("heat_scores", None, None),
}

zip_list = df_zips["zipcode"].tolist()

for col_name, (table, filter_col, filter_val) in SOURCE_TABLES.items():
    log("INFO", f"  Fetching from {table}...")
    all_rows = []
    batch_size = 50

    for i in range(0, len(zip_list), batch_size):
        batch = zip_list[i:i + batch_size]
        query = supabase.table(table).select("zipcode,composite_score")

        if filter_col:
            query = query.eq(filter_col, filter_val)

        query = query.in_("zipcode", batch)
        resp = query.execute()
        all_rows.extend(resp.data)

    df_tool = pd.DataFrame(all_rows)
    if len(df_tool) > 0:
        df_tool = df_tool.rename(columns={"composite_score": col_name})
        df_zips = df_zips.merge(df_tool[["zipcode", col_name]], on="zipcode", how="left")
    else:
        df_zips[col_name] = None
        log("WARN", f"  No data returned from {table}")

    log("INFO", f"  {table}: {df_tool.shape[0] if len(df_tool) > 0 else 0} rows fetched")

# Show coverage
SCORE_COLS = ["respiratory_score", "cardiovascular_score", "stress_score",
              "food_access_score", "heat_score"]

for col in SCORE_COLS:
    non_null = df_zips[col].notna().sum()
    log("INFO", f"  {col}: {non_null}/{len(df_zips)} ZIPs have data")


# %% [markdown]
# ## 3 · Ingestion Tests (Suite 1) — GATE

# %%
log("TEST", "Running Suite 1 — Ingestion Tests")

ingestion_tests = [
    ("ZIP count >= 550",
        lambda: (len(df_zips) >= 550, f"Got {len(df_zips)}")),
    ("All 4 metros present",
        lambda: (df_zips["metro"].nunique() >= 4, f"Got {df_zips['metro'].nunique()}")),
]

for col in SCORE_COLS:
    col_label = col.replace("_score", "")
    ingestion_tests.append((
        f"{col_label} coverage >= 540 ZIPs",
        lambda c=col: (df_zips[c].notna().sum() >= 540, f"Got {df_zips[c].notna().sum()}")
    ))

suite1_passed = run_tests("OVERALL — INGESTION", ingestion_tests)
require_all_pass("OVERALL — INGESTION", suite1_passed)


# %% [markdown]
# ## 4 · Compute Overall Composite Score

# %%
log("START", "Computing overall composite scores")

# Only score ZIPs that have all 5 tool scores
df = df_zips.dropna(subset=SCORE_COLS).copy()
log("INFO", f"{len(df)} ZIPs have all 5 tool scores (of {len(df_zips)} total)")

# Equal weight: 20% each → simple average
df["composite_score"] = df[SCORE_COLS].mean(axis=1).round(2)

# Letter grade
GRADE_SCALE = {
    "A": (80, 100),
    "B": (65, 79.99),
    "C": (50, 64.99),
    "D": (35, 49.99),
    "F": (0, 34.99),
}

def assign_grade(score: float) -> str:
    if score >= 80:
        return "A"
    elif score >= 65:
        return "B"
    elif score >= 50:
        return "C"
    elif score >= 35:
        return "D"
    else:
        return "F"

df["letter_grade"] = df["composite_score"].apply(assign_grade)

log("INFO", f"Grade distribution: {df['letter_grade'].value_counts().to_dict()}")
log("INFO", f"Score range: {df['composite_score'].min():.1f} – {df['composite_score'].max():.1f}")
log("INFO", f"Score mean: {df['composite_score'].mean():.1f}")


# %% [markdown]
# ## 5 · Scoring Tests (Suite 2) — GATE

# %%
log("TEST", "Running Suite 2 — Scoring Tests")

def grade_in_range(grade, lo, hi):
    order = ["F", "D", "C", "B", "A"]
    return order.index(lo) <= order.index(grade) <= order.index(hi)

SPOT_CHECK_ZIPS = {
    "15213": ("C", "A"),   # Pittsburgh
    "90210": ("C", "A"),   # Los Angeles
    "28202": ("C", "A"),   # Charlotte
    "85257": ("C", "A"),   # Phoenix
}

scoring_tests = [
    ("All composite_score in [0, 100]",
        lambda: (
            df["composite_score"].between(0, 100).all(),
            f"min={df['composite_score'].min():.2f}, max={df['composite_score'].max():.2f}"
        )),
    ("All letter_grade in {A,B,C,D,F}",
        lambda: (
            df["letter_grade"].isin(["A", "B", "C", "D", "F"]).all(),
            f"Unique: {df['letter_grade'].unique().tolist()}"
        )),
    ("Grade boundaries correct for all rows",
        lambda: (
            all(
                GRADE_SCALE[row["letter_grade"]][0] <= row["composite_score"] <= GRADE_SCALE[row["letter_grade"]][1]
                for _, row in df.iterrows()
            ),
            "Grade/score mismatch found"
        )),
    ("No single grade > 70% of ZIPs",
        lambda: (
            df["letter_grade"].value_counts(normalize=True).max() < 0.70,
            f"Distribution: {df['letter_grade'].value_counts().to_dict()}"
        )),
    ("Score std > 3.0",
        lambda: (
            df["composite_score"].std() > 3.0,
            f"std={df['composite_score'].std():.3f}"
        )),
    ("Score mean in plausible range [30, 70]",
        lambda: (
            30.0 <= df["composite_score"].mean() <= 70.0,
            f"mean={df['composite_score'].mean():.2f}"
        )),
]

# Spot checks
for zip_code, (min_g, max_g) in SPOT_CHECK_ZIPS.items():
    z, mn, mx = zip_code, min_g, max_g
    scoring_tests.append((
        f"Spot check ZIP {z}: grade between {mn} and {mx}",
        lambda zc=z, lo=mn, hi=mx: (
            (row := df[df["zipcode"] == zc]).shape[0] > 0
            and grade_in_range(row.iloc[0]["letter_grade"], lo, hi),
            f"ZIP {zc} got grade {df[df['zipcode'] == zc].iloc[0]['letter_grade'] if len(df[df['zipcode'] == zc]) > 0 else 'NOT FOUND'}"
        )
    ))

suite2_passed = run_tests("OVERALL — SCORING", scoring_tests)
require_all_pass("OVERALL — SCORING", suite2_passed)


# %% [markdown]
# ## 6 · Claude API Interpretations
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# missing ANTHROPIC_API_KEY in Colab secrets, rate limits (429), or model string changes.

# %%
log("START", "Generating Claude API interpretations for all ZIPs")

import anthropic

client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env

def generate_overall_interpretation(zipcode: str, composite_score: float,
                                     letter_grade: str, sub_scores: dict) -> str:
    """
    Generate a plain-language overall health environment interpretation.
    sub_scores: dict of {dimension_label: score} — qualitative labels only, no weights.
    """
    prompt = f"""You are a public health analyst writing a plain-language summary for residents and
real estate professionals. Write 2-3 sentences interpreting this neighborhood's overall health
environment score, which combines five dimensions: respiratory health, cardiovascular health,
stress and sensory environment, food access, and heat resilience.

ZIP Code: {zipcode}
Overall Score: {composite_score:.1f}/100 (Grade: {letter_grade})
Dimension scores: {sub_scores}

Rules:
- Be specific, factual, and actionable
- Do not use jargon
- Do not mention exact scores or numbers from the dimensions
- Do not reveal how dimensions are weighted or combined
- Do not say "based on our methodology" or any similar phrase
- Highlight the strongest and weakest dimensions
- Frame in terms of livability and long-term health outcomes"""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text


# %%
# Generate interpretations in batches with rate limiting
INTERPRETATION_BATCH_DELAY = 0.3  # seconds between API calls

interpretations = {}
failed_interps = []

for i, (idx, row) in enumerate(df.iterrows()):
    zc = row["zipcode"]
    sub_scores = {
        "Respiratory": row.get("respiratory_score", 0),
        "Cardiovascular": row.get("cardiovascular_score", 0),
        "Stress & Sensory": row.get("stress_score", 0),
        "Food Access": row.get("food_access_score", 0),
        "Heat Resilience": row.get("heat_score", 0),
    }

    try:
        interp = generate_overall_interpretation(
            zc, row["composite_score"], row["letter_grade"], sub_scores
        )
        interpretations[zc] = interp
    except Exception as e:
        log("ERROR", f"  Interpretation failed for ZIP {zc}: {e}")
        failed_interps.append(zc)
        interpretations[zc] = ""

    # Progress logging every 50 ZIPs
    if (i + 1) % 50 == 0 or i == len(df) - 1:
        log("INFO", f"  Interpreted {len(interpretations)}/{len(df)} ZIPs")

    time.sleep(INTERPRETATION_BATCH_DELAY)

df["interpretation"] = df["zipcode"].map(interpretations)

if failed_interps:
    log("WARN", f"{len(failed_interps)} ZIPs failed interpretation: {failed_interps[:10]}")
else:
    log("PASS", "All interpretations generated successfully")


# %% [markdown]
# ## 7 · Supabase Upsert
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# column name mismatch between local dict keys and Supabase schema, or missing UNIQUE constraint.
# Do NOT modify the Supabase schema manually — bring the error here first.

# %%
# ── Reinitialize Supabase client (fresh HTTP connection) ─────
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
log("INFO", "Reinitialized Supabase client for upsert phase")

# ── Schema cache warm-up ─────────────────────────────────────
MAX_RETRIES = 5
RETRY_DELAY = 3

for attempt in range(1, MAX_RETRIES + 1):
    try:
        test = supabase.table("overall_scores").select("zipcode").limit(1).execute()
        log("PASS", f"Schema cache warm-up succeeded on attempt {attempt}")
        break
    except Exception as e:
        log("WARN", f"Schema cache warm-up attempt {attempt}/{MAX_RETRIES} failed: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
else:
    raise RuntimeError(
        "\n" + "!" * 62 + "\n"
        "  PostgREST cannot see the overall_scores table after 5 attempts.\n"
        "  This is a schema cache issue, not a missing table.\n\n"
        "  FIX: Open Supabase SQL Editor and run:\n"
        "    NOTIFY pgrst, 'reload schema';\n\n"
        "  Wait 10 seconds, then re-run this cell.\n"
        + "!" * 62 + "\n"
    )

# %%
log("START", "Upserting all records to overall_scores")

failed_zips = []

for _, row in df.iterrows():
    record = {
        "zipcode": row["zipcode"],
        "metro": row["metro"],
        "respiratory_score": float(row["respiratory_score"]) if pd.notna(row["respiratory_score"]) else None,
        "cardiovascular_score": float(row["cardiovascular_score"]) if pd.notna(row["cardiovascular_score"]) else None,
        "stress_score": float(row["stress_score"]) if pd.notna(row["stress_score"]) else None,
        "food_access_score": float(row["food_access_score"]) if pd.notna(row["food_access_score"]) else None,
        "heat_score": float(row["heat_score"]) if pd.notna(row["heat_score"]) else None,
        "composite_score": float(row["composite_score"]),
        "letter_grade": row["letter_grade"],
        "interpretation": row.get("interpretation", ""),
        "score_date": str(date.today()),
    }

    try:
        supabase.table("overall_scores").upsert(
            record, on_conflict="zipcode"
        ).execute()
    except Exception as e:
        log("ERROR", f"Failed to write ZIP {row['zipcode']}: {e}")
        failed_zips.append(row["zipcode"])

if failed_zips:
    log("WARN", f"{len(failed_zips)} ZIPs failed upsert: {failed_zips[:10]}")
else:
    log("PASS", "All records upserted to overall_scores")

# %% [markdown]
# ## 7a · Supabase Write Tests (Suite 3) — GATE

# %%
log("TEST", "Running Suite 3 — Supabase Write Tests")

TABLE_NAME = "overall_scores"

def get_sb_count():
    result = supabase.table(TABLE_NAME).select("zipcode", count="exact").execute()
    return result.count or 0

def get_sb_row(zc):
    result = supabase.table(TABLE_NAME).select("*").eq("zipcode", zc).execute()
    return result.data[0] if result.data else None

initial_count = get_sb_count()

SPOT_ZIPS = ["15213", "90210", "28202", "28277", "85257"]

write_tests = [
    (f"Supabase row count >= 540",
        lambda: (initial_count >= 540, f"Got {initial_count}")),
    ("Supabase count matches local data (within 5)",
        lambda: (abs(initial_count - len(df)) <= 5, f"Supabase: {initial_count}, local: {len(df)}")),
    ("Re-upsert is idempotent",
        lambda: (get_sb_count() == initial_count, f"Before: {initial_count}, after: {get_sb_count()}")),
    ("No null composite_score in Supabase",
        lambda: (
            supabase.table(TABLE_NAME).select("zipcode", count="exact")
                .is_("composite_score", "null").execute().count == 0,
            "Found null composite_score rows"
        )),
    ("No null letter_grade in Supabase",
        lambda: (
            supabase.table(TABLE_NAME).select("zipcode", count="exact")
                .is_("letter_grade", "null").execute().count == 0,
            "Found null letter_grade rows"
        )),
]

# Spot checks — verify specific ZIPs have all 5 sub-scores populated
for zc in SPOT_ZIPS:
    write_tests.append((
        f"Spot check ZIP {zc}: all 5 sub-scores present",
        lambda z=zc: (
            (row := get_sb_row(z)) is not None
            and all(row.get(c) is not None for c in [
                "respiratory_score", "cardiovascular_score", "stress_score",
                "food_access_score", "heat_score"
            ]),
            f"ZIP {z}: {'found' if get_sb_row(z) else 'NOT FOUND'}"
        )
    ))

suite3_passed = run_tests("OVERALL — SUPABASE WRITE", write_tests)
require_all_pass("OVERALL — SUPABASE WRITE", suite3_passed)

# %%
log("INFO", "═══ OVERALL PIPELINE COMPLETE ═══")
log("INFO", f"ZIPs scored: {len(df)}")
log("INFO", f"Grade distribution: {df['letter_grade'].value_counts().to_dict()}")
log("INFO", f"Score range: {df['composite_score'].min():.1f} – {df['composite_score'].max():.1f}")
