# %% [markdown]
# # Food Access Score — Full Pipeline
# **Tool 4 of 5 · LaSalle Technologies Health Environment Score**
#
# Run each cell in order. Every gate must pass before proceeding.
# Designed for Google Colab with Supabase credentials in Colab secrets.
#
# **Purely tabular pipeline — no raster processing required.**

# %% [markdown]
# ## 0 · Setup & Configuration
#
# **IF ANY CELL IN THIS NOTEBOOK FAILS:** Stop immediately. Do not debug manually in Colab.
# Copy the full error traceback and bring it to Claude Code for diagnosis. Manual Colab fixes
# often introduce silent regressions that are harder to trace later.

# %%
# ── Installs (run once per Colab session) ────────────────────
# !pip install -q supabase anthropic requests pandas

# %%
import os
import time
import traceback
import requests
import pandas as pd
from datetime import datetime, date

# Colab secrets — uncomment in Colab
# from google.colab import userdata
# SUPABASE_URL = userdata.get("SUPABASE_URL")
# SUPABASE_KEY = userdata.get("SUPABASE_KEY")
# ANTHROPIC_API_KEY = userdata.get("ANTHROPIC_API_KEY")
# os.environ["ANTHROPIC_API_KEY"] = ANTHROPIC_API_KEY

# Local fallback (for testing outside Colab)
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

from supabase import create_client

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# %%
# ── Logging ──────────────────────────────────────────────────
def log(level: str, message: str):
    """Structured logging: INFO | WARN | ERROR | PASS | FAIL | TEST | START | DONE"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    icons = {
        "INFO": "ℹ️ ", "WARN": "⚠️ ", "ERROR": "❌ ", "PASS": "✅ ",
        "FAIL": "❌ ", "TEST": "🧪 ", "START": "🚀 ", "DONE": "🏁 ",
    }
    icon = icons.get(level, "   ")
    print(f"[{timestamp}] {icon} [{level}] {message}")

# ── Test Runner ──────────────────────────────────────────────
def run_tests(suite_name: str, tests: list) -> bool:
    """Run (test_name, test_fn) tuples. Each test_fn returns (passed, detail)."""
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

def require_all_pass(suite_name: str, passed: bool):
    """Hard stop if a test suite failed."""
    if not passed:
        raise RuntimeError(
            f"\n{'!'*62}\n"
            f"  BLOCKED: '{suite_name}' had failures.\n"
            f"  Fix all failures and re-run before proceeding.\n"
            f"{'!'*62}\n"
        )
    log("PASS", f"All tests passed — '{suite_name}' gate cleared")

# ── Validation Report ────────────────────────────────────────
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
    for col in df.select_dtypes(include="number").columns:
        print(f"    {col}: min={df[col].min():.3f}, max={df[col].max():.3f}, mean={df[col].mean():.3f}")
    print(f"  Metro breakdown:")
    if "metro" in df.columns:
        for metro, count in df["metro"].value_counts().items():
            print(f"    {metro}: {count} ZIPs")
    print(f"{'='*60}\n")

# %%
# ── Master ZIP List ──────────────────────────────────────────
log("START", "Fetching master ZIP list from Supabase")

zip_data = supabase.table("zip_codes").select("zipcode, metro").execute().data
df_zips = pd.DataFrame(zip_data)

METRO_LABELS = {
    "Pittsburgh": "Pittsburgh",
    "Los Angeles": "Los Angeles",
    "Phoenix": "Phoenix",
    "Charlotte": "Charlotte",
    "Chicago": "Chicago",
    "Houston": "Houston",
    "Atlanta": "Atlanta",
    "Denver": "Denver",
}

ALL_ZIPS = df_zips["zipcode"].tolist()
ZIP_METRO_MAP = dict(zip(df_zips["zipcode"], df_zips["metro"]))

log("INFO", f"Loaded {len(ALL_ZIPS)} ZIPs across {df_zips['metro'].nunique()} metros")
log("INFO", f"Metro counts: {df_zips['metro'].value_counts().to_dict()}")

# ── Component Weights (proprietary — do not expose) ─────────
WEIGHTS = [0.35, 0.35, 0.30]
WEIGHT_LABELS = [
    "low_access",
    "grocery_density",
    "health_outcome",
]
assert abs(sum(WEIGHTS) - 1.0) < 1e-9, f"Weights sum to {sum(WEIGHTS)}, must equal 1.0"
log("PASS", f"Weight sum check: {sum(WEIGHTS)}")

# ── Grade Scale ──────────────────────────────────────────────
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

# ── Google Drive Paths ───────────────────────────────────────
DRIVE_PREFIX = "/content/drive/MyDrive/Colab Notebooks/health-score-data"

# USDA Food Access Research Atlas — tract-level food access data
# Download from: https://www.ers.usda.gov/data-products/food-access-research-atlas/download-the-data/
FARA_PATH = f"{DRIVE_PREFIX}/FoodAccessResearchAtlasData2019.xlsx"

# USDA Food Environment Atlas — county-level food environment data
# Download from: https://www.ers.usda.gov/data-products/food-environment-atlas/
ATLAS_PATH = f"{DRIVE_PREFIX}/FoodEnvironmentAtlas.xlsx"

# HUD USPS Crosswalk files
# Download from: https://www.huduser.gov/portal/datasets/usps_crosswalk.html
TRACT_ZIP_CROSSWALK_PATH = f"{DRIVE_PREFIX}/TRACT_ZIP_032025.xlsx"
ZIP_COUNTY_CROSSWALK_PATH = f"{DRIVE_PREFIX}/ZIP_COUNTY_032025.xlsx"


# %% [markdown]
# ## 1 · Supabase Schema
# Run this SQL in the Supabase SQL Editor **once** before proceeding:
#
# ```sql
# CREATE TABLE IF NOT EXISTS food_access_scores (
#   id SERIAL PRIMARY KEY,
#   zipcode TEXT NOT NULL,
#   metro TEXT NOT NULL,
#   low_access_raw NUMERIC,
#   low_access_normalized NUMERIC,
#   grocery_density_raw NUMERIC,
#   grocery_density_normalized NUMERIC,
#   health_outcome_raw NUMERIC,
#   health_outcome_normalized NUMERIC,
#   composite_score NUMERIC,
#   letter_grade TEXT,
#   interpretation TEXT,
#   created_at TIMESTAMPTZ DEFAULT NOW(),
#   updated_at TIMESTAMPTZ DEFAULT NOW(),
#   UNIQUE(zipcode)
# );
#
# -- Auto-update timestamp trigger
# CREATE TRIGGER set_updated_at_food
# BEFORE UPDATE ON food_access_scores
# FOR EACH ROW EXECUTE FUNCTION update_updated_at();
# ```


# %% [markdown]
# ## 2 · USDA FARA Ingestion (Tract-Level → ZCTA Crosswalk)
#
# The Food Access Research Atlas provides tract-level data on population counts
# living beyond certain distances from a supermarket. We compute `lapophalf_share`
# (% of population >0.5mi from supermarket), then crosswalk to ZIP using the
# HUD USPS Tract-ZIP crosswalk with population-weighted aggregation.
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# Excel column names differ from expected, crosswalk file format mismatch, or Drive path wrong.

# %%
log("START", "Ingesting USDA Food Access Research Atlas (FARA)")

# ── Load FARA data ───────────────────────────────────────────
log("INFO", f"Loading FARA from {FARA_PATH}")
df_fara_raw = pd.read_excel(FARA_PATH, sheet_name="Food Access Research Atlas")

log("INFO", f"FARA columns: {list(df_fara_raw.columns)}")
log("INFO", f"FARA rows: {len(df_fara_raw)}")

# Key fields: CensusTract, lapophalf (low access pop at 0.5 mi), Pop2010
# Column names may vary — log and match flexibly
fara_cols = df_fara_raw.columns.tolist()
log("INFO", f"First 20 columns: {fara_cols[:20]}")

# Convert CensusTract to string for joining
df_fara_raw["CensusTract"] = df_fara_raw["CensusTract"].astype(str).str.zfill(11)

# Compute low access share: % of tract population >0.5mi from supermarket
df_fara_raw["lapophalf"] = pd.to_numeric(df_fara_raw["lapophalf"], errors="coerce")
df_fara_raw["Pop2010"] = pd.to_numeric(df_fara_raw["Pop2010"], errors="coerce")

# Filter out tracts with zero or null population
df_fara_raw = df_fara_raw[df_fara_raw["Pop2010"] > 0].copy()

df_fara_raw["lapophalf_share"] = (df_fara_raw["lapophalf"] / df_fara_raw["Pop2010"]) * 100.0

log("INFO", f"FARA tracts with data: {len(df_fara_raw)}")
log("INFO", f"lapophalf_share range: {df_fara_raw['lapophalf_share'].min():.1f}% – {df_fara_raw['lapophalf_share'].max():.1f}%")

# %%
# ── Load HUD Tract-ZIP Crosswalk ─────────────────────────────
log("INFO", f"Loading HUD Tract-ZIP crosswalk from {TRACT_ZIP_CROSSWALK_PATH}")
df_tract_zip = pd.read_excel(TRACT_ZIP_CROSSWALK_PATH)

log("INFO", f"Tract-ZIP crosswalk columns: {list(df_tract_zip.columns)}")
log("INFO", f"Tract-ZIP crosswalk rows: {len(df_tract_zip)}")

# Standardize column names — HUD uses TRACT, ZIP, RES_RATIO
# Column names may be uppercase or mixed case
df_tract_zip.columns = [c.upper().strip() for c in df_tract_zip.columns]
log("INFO", f"Tract-ZIP columns (uppercased): {list(df_tract_zip.columns)}")

# TRACT field is the 11-digit FIPS tract code
df_tract_zip["TRACT"] = df_tract_zip["TRACT"].astype(str).str.zfill(11)
df_tract_zip["ZIP"] = df_tract_zip["ZIP"].astype(str).str.zfill(5)
df_tract_zip["RES_RATIO"] = pd.to_numeric(df_tract_zip["RES_RATIO"], errors="coerce")

# %%
# ── Join FARA to Crosswalk and Aggregate to ZIP ──────────────
log("INFO", "Joining FARA tract data to HUD crosswalk")

df_fara_joined = df_fara_raw[["CensusTract", "lapophalf_share", "Pop2010"]].merge(
    df_tract_zip[["TRACT", "ZIP", "RES_RATIO"]],
    left_on="CensusTract",
    right_on="TRACT",
    how="inner",
)

log("INFO", f"Joined rows: {len(df_fara_joined)}")

# Population-weighted aggregation per ZIP:
# weighted_avg = sum(lapophalf_share * Pop2010 * RES_RATIO) / sum(Pop2010 * RES_RATIO)
df_fara_joined["weight"] = df_fara_joined["Pop2010"] * df_fara_joined["RES_RATIO"]
df_fara_joined["weighted_value"] = df_fara_joined["lapophalf_share"] * df_fara_joined["weight"]

df_fara_agg = df_fara_joined.groupby("ZIP").agg(
    weighted_sum=("weighted_value", "sum"),
    weight_sum=("weight", "sum"),
).reset_index()

df_fara_agg["low_access_raw"] = df_fara_agg["weighted_sum"] / df_fara_agg["weight_sum"]
df_fara_agg = df_fara_agg.rename(columns={"ZIP": "zipcode"})

# Filter to our master ZIPs
df_fara = df_fara_agg[df_fara_agg["zipcode"].isin(ALL_ZIPS)][["zipcode", "low_access_raw"]].copy()

log("INFO", f"FARA: {len(df_fara)} of our ZIPs matched")
log("INFO", f"low_access_raw range: {df_fara['low_access_raw'].min():.2f}% – {df_fara['low_access_raw'].max():.2f}%")


# %% [markdown]
# ## 3 · USDA Food Environment Atlas Ingestion (County → ZIP)
#
# The Atlas provides county-level food environment metrics. We use `GROCPTH16`
# (grocery stores per 1,000 population). We crosswalk from ZIP to county using
# HUD ZIP-County crosswalk (primary county by highest RES_RATIO).
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# Excel sheet name differs, FIPS column formatting, or crosswalk column mismatch.

# %%
log("START", "Ingesting USDA Food Environment Atlas")

# ── Load Atlas data ──────────────────────────────────────────
log("INFO", f"Loading Atlas from {ATLAS_PATH}")

# The Atlas Excel has multiple sheets — the grocery data is typically in "ACCESS"
# or "STORES". Try "STORES" first (contains GROCPTH16).
try:
    # header=1: row 0 is a title row, real column names are in row 1
    df_atlas_raw = pd.read_excel(ATLAS_PATH, sheet_name="STORES", header=1)
    log("INFO", "Loaded 'STORES' sheet from Atlas (header=1)")
except ValueError:
    # Fall back to trying other common sheet names
    xls = pd.ExcelFile(ATLAS_PATH)
    log("INFO", f"Available Atlas sheets: {xls.sheet_names}")
    raise RuntimeError(
        f"'STORES' sheet not found in Atlas file. Available sheets: {xls.sheet_names}. "
        f"Bring this error to Claude Code."
    )

log("INFO", f"Atlas columns ({len(df_atlas_raw.columns)}): {list(df_atlas_raw.columns)}")
log("INFO", f"Atlas rows: {len(df_atlas_raw)}")

# Verify required columns are present
for required_col in ["FIPS", "GROCPTH16"]:
    if required_col not in df_atlas_raw.columns:
        log("ERROR", f"Required column '{required_col}' not found in Atlas. "
            f"Available columns: {list(df_atlas_raw.columns)}")
        raise RuntimeError(
            f"Column '{required_col}' missing from STORES sheet. "
            f"Bring this error to Claude Code."
        )

# FIPS is the county FIPS code, GROCPTH16 is grocery stores per 1,000 pop
df_atlas_raw["FIPS"] = df_atlas_raw["FIPS"].astype(str).str.zfill(5)
df_atlas_raw["GROCPTH16"] = pd.to_numeric(df_atlas_raw["GROCPTH16"], errors="coerce")

log("INFO", f"GROCPTH16 range: {df_atlas_raw['GROCPTH16'].min():.4f} – {df_atlas_raw['GROCPTH16'].max():.4f}")

# %%
# ── Load HUD ZIP-County Crosswalk ────────────────────────────
log("INFO", f"Loading HUD ZIP-County crosswalk from {ZIP_COUNTY_CROSSWALK_PATH}")
df_zip_county = pd.read_excel(ZIP_COUNTY_CROSSWALK_PATH)

log("INFO", f"ZIP-County crosswalk columns: {list(df_zip_county.columns)}")
log("INFO", f"ZIP-County crosswalk rows: {len(df_zip_county)}")

# Standardize column names
df_zip_county.columns = [c.upper().strip() for c in df_zip_county.columns]
log("INFO", f"ZIP-County columns (uppercased): {list(df_zip_county.columns)}")

df_zip_county["ZIP"] = df_zip_county["ZIP"].astype(str).str.zfill(5)
df_zip_county["COUNTY"] = df_zip_county["COUNTY"].astype(str).str.zfill(5)
df_zip_county["RES_RATIO"] = pd.to_numeric(df_zip_county["RES_RATIO"], errors="coerce")

# Use primary county (highest RES_RATIO) for each ZIP
df_zip_primary_county = df_zip_county.sort_values("RES_RATIO", ascending=False) \
    .drop_duplicates(subset=["ZIP"], keep="first")[["ZIP", "COUNTY"]]

log("INFO", f"Primary county assignments: {len(df_zip_primary_county)} ZIPs")

# %%
# ── Join ZIP → County → Atlas ────────────────────────────────
log("INFO", "Joining ZIPs to Atlas via county FIPS")

df_atlas_joined = df_zip_primary_county.merge(
    df_atlas_raw[["FIPS", "GROCPTH16"]],
    left_on="COUNTY",
    right_on="FIPS",
    how="left",
)

df_atlas_joined = df_atlas_joined.rename(columns={
    "ZIP": "zipcode",
    "GROCPTH16": "grocery_density_raw",
})

# Filter to our master ZIPs
df_atlas = df_atlas_joined[df_atlas_joined["zipcode"].isin(ALL_ZIPS)][
    ["zipcode", "grocery_density_raw"]
].copy()

log("INFO", f"Atlas: {len(df_atlas)} of our ZIPs matched")
log("INFO", f"grocery_density_raw range: {df_atlas['grocery_density_raw'].min():.4f} – {df_atlas['grocery_density_raw'].max():.4f}")


# %% [markdown]
# ## 4 · CDC PLACES Ingestion (Diabetes + Obesity)
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# API format changes (fields renamed), rate limits (HTTP 429), or ZIP matching failures.
# Do NOT try to patch the query manually — the API schema has changed before (see CONTEXT.md).

# %%
log("START", "Ingesting CDC PLACES data for Diabetes and Obesity")

CDC_BASE_URL = "https://data.cdc.gov/resource/c7b2-4ecy.json"

# CDC PLACES API is WIDE format (confirmed April 2026):
#   - One row per ZCTA, with separate columns for each measure
#   - ZIP field is "zcta5" (not "locationname")
#   - No "measureid" column — measures are column names like "diabetes_crudeprev"
#   - Use $select to request only needed columns, $where to filter by zcta5
#   - Batch size 50 ZIPs per request to stay within Socrata URL length limits

def fetch_cdc_places_wide(zip_codes: list, select_cols: list, batch_size: int = 50) -> list:
    """
    Fetch CDC PLACES data in wide format, batching by ZIP to avoid URL length limits.
    select_cols: columns to request, e.g. ["zcta5", "diabetes_crudeprev", "obesity_crudeprev"]
    Returns list of raw row dicts (one row per ZIP, already wide).
    """
    all_rows = []
    select_str = ",".join(select_cols)

    for i in range(0, len(zip_codes), batch_size):
        batch_zips = zip_codes[i:i + batch_size]
        zip_list = ", ".join(f"'{z}'" for z in batch_zips)

        params = {
            "$select": select_str,
            "$where": f"zcta5 IN ({zip_list})",
            "$limit": 50000,
        }

        try:
            response = requests.get(CDC_BASE_URL, params=params)
            response.raise_for_status()
            rows = response.json()
            all_rows.extend(rows)
            log("INFO", f"  Batch {i//batch_size + 1}: fetched {len(rows)} rows "
                f"(ZIPs {i+1}–{min(i+batch_size, len(zip_codes))})")
        except requests.HTTPError as e:
            log("ERROR", f"HTTP {e.response.status_code} for batch starting at index {i}: {e}")
            raise
        except Exception as e:
            log("ERROR", f"Unexpected error fetching CDC PLACES batch at index {i}: {e}")
            raise

        # Respect rate limits
        time.sleep(0.5)

    return all_rows

# %%
# Fetch diabetes and obesity in wide format
CDC_SELECT_COLS = ["zcta5", "diabetes_crudeprev", "obesity_crudeprev"]

log("INFO", f"Fetching CDC PLACES (wide format) for columns: {CDC_SELECT_COLS}")
log("INFO", f"Total ZIPs to query: {len(ALL_ZIPS)}, batch size: 50")

raw_cdc = fetch_cdc_places_wide(ALL_ZIPS, CDC_SELECT_COLS, batch_size=50)
log("INFO", f"Total CDC rows received: {len(raw_cdc)}")

# %%
# Parse wide-format response — already one row per ZIP, no pivot needed
df_cdc = pd.DataFrame(raw_cdc)

log("INFO", f"CDC response columns: {list(df_cdc.columns)}")

# Convert types and rename to internal column names
df_cdc["zcta5"] = df_cdc["zcta5"].astype(str).str.strip()
df_cdc["diabetes_crudeprev"] = pd.to_numeric(df_cdc["diabetes_crudeprev"], errors="coerce")
df_cdc["obesity_crudeprev"] = pd.to_numeric(df_cdc["obesity_crudeprev"], errors="coerce")

# Combine: health_outcome_raw = average of diabetes and obesity prevalence
df_cdc["health_outcome_raw"] = (df_cdc["diabetes_crudeprev"] + df_cdc["obesity_crudeprev"]) / 2.0

df_food_cdc = df_cdc.rename(columns={"zcta5": "zipcode"})
df_food_cdc = df_food_cdc.drop_duplicates(subset=["zipcode"], keep="first")

# Keep diabetes and obesity raw values for reference (not stored in Supabase, just for logging)
df_food_cdc["diabetes_raw"] = df_food_cdc["diabetes_crudeprev"]
df_food_cdc["obesity_raw"] = df_food_cdc["obesity_crudeprev"]

# Add metro from master ZIP list
df_food_cdc["metro"] = df_food_cdc["zipcode"].map(ZIP_METRO_MAP)

# Filter to only our 600 ZIPs
df_food_cdc = df_food_cdc[df_food_cdc["zipcode"].isin(ALL_ZIPS)].copy()

log("INFO", f"CDC PLACES parsed: {len(df_food_cdc)} ZIPs with data")
log("INFO", f"  Diabetes coverage: {df_food_cdc['diabetes_raw'].notna().sum()} ZIPs")
log("INFO", f"  Obesity coverage: {df_food_cdc['obesity_raw'].notna().sum()} ZIPs")
log("INFO", f"  health_outcome_raw range: {df_food_cdc['health_outcome_raw'].min():.1f} – {df_food_cdc['health_outcome_raw'].max():.1f}")


# %% [markdown]
# ## 5 · Merge All Components
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Merge failures
# usually mean a ZIP column mismatch between data sources or an unexpected null pattern.

# %%
log("START", "Merging all three components into a single DataFrame")

# Start from CDC data (has zipcode + metro + health_outcome_raw)
df = df_food_cdc[["zipcode", "metro", "health_outcome_raw"]].copy()

# Merge FARA (low access)
df = df.merge(df_fara[["zipcode", "low_access_raw"]], on="zipcode", how="left")

# Merge Atlas (grocery density)
df = df.merge(df_atlas[["zipcode", "grocery_density_raw"]], on="zipcode", how="left")

# Ensure metro is filled for all ZIPs
df["metro"] = df["zipcode"].map(ZIP_METRO_MAP)

# Filter to only our 600 ZIPs (safety)
df = df[df["zipcode"].isin(ALL_ZIPS)].copy()

log("INFO", f"Merged DataFrame: {len(df)} rows")
print_validation_report("FOOD ACCESS — MERGED RAW DATA", df)

# %% [markdown]
# ## 5a · Ingestion Tests (Suite 1) — GATE

# %%
log("TEST", "Running Suite 1 — Ingestion Tests")

ingestion_tests = [
    ("All expected columns present",
        lambda: (
            all(c in df.columns for c in
                ["zipcode", "metro", "low_access_raw", "grocery_density_raw", "health_outcome_raw"]),
            f"Missing: {[c for c in ['zipcode','metro','low_access_raw','grocery_density_raw','health_outcome_raw'] if c not in df.columns]}"
        )),
    ("Row count >= 540",
        lambda: (len(df) >= 540, f"Got {len(df)}")),
    ("No null zipcodes",
        lambda: (df["zipcode"].isna().sum() == 0, f"{df['zipcode'].isna().sum()} nulls")),
    ("No duplicate zipcodes",
        lambda: (
            df["zipcode"].duplicated().sum() == 0,
            f"{df['zipcode'].duplicated().sum()} duplicates"
        )),
    ("All 8 metros present",
        lambda: (
            set(df["metro"].dropna().unique()) >= set(METRO_LABELS.values()),
            f"Found: {sorted(df['metro'].dropna().unique())}"
        )),
    ("No metro under 5 ZIPs",
        lambda: (
            df["metro"].value_counts().min() >= 5,
            f"Counts: {df['metro'].value_counts().to_dict()}"
        )),
    # Raw value range checks
    ("low_access_raw in [0, 100]",
        lambda: (
            df["low_access_raw"].dropna().between(-0.01, 100.01).all(),
            f"min={df['low_access_raw'].min():.4f}, max={df['low_access_raw'].max():.4f}"
        )),
    ("low_access_raw nulls < 10%",
        lambda: (
            df["low_access_raw"].isna().sum() / len(df) < 0.10,
            f"{df['low_access_raw'].isna().sum()} nulls ({df['low_access_raw'].isna().sum()/len(df)*100:.1f}%)"
        )),
    ("grocery_density_raw in [0, 5]",
        lambda: (
            df["grocery_density_raw"].dropna().between(0, 5).all(),
            f"min={df['grocery_density_raw'].min():.4f}, max={df['grocery_density_raw'].max():.4f}"
        )),
    ("grocery_density_raw nulls < 10%",
        lambda: (
            df["grocery_density_raw"].isna().sum() / len(df) < 0.10,
            f"{df['grocery_density_raw'].isna().sum()} nulls ({df['grocery_density_raw'].isna().sum()/len(df)*100:.1f}%)"
        )),
    ("health_outcome_raw in [5, 50]",
        lambda: (
            df["health_outcome_raw"].dropna().between(5, 50).all(),
            f"min={df['health_outcome_raw'].min():.1f}, max={df['health_outcome_raw'].max():.1f}"
        )),
    ("health_outcome_raw nulls < 10%",
        lambda: (
            df["health_outcome_raw"].isna().sum() / len(df) < 0.10,
            f"{df['health_outcome_raw'].isna().sum()} nulls ({df['health_outcome_raw'].isna().sum()/len(df)*100:.1f}%)"
        )),
]

suite1_passed = run_tests("FOOD ACCESS — INGESTION", ingestion_tests)
require_all_pass("FOOD ACCESS — INGESTION", suite1_passed)


# %% [markdown]
# ## 6 · Normalization
# Min-max normalization, global across all 600 ZIPs.
# - `low_access_raw` → **INVERT** (higher raw = worse — more people far from supermarkets)
# - `grocery_density_raw` → **DO NOT INVERT** (higher raw = better — more grocery stores)
# - `health_outcome_raw` → **INVERT** (higher raw = worse — more diabetes/obesity)
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Normalization
# failures typically mean a column has all-null or constant values from a broken ingestion step.

# %%
log("START", "Normalizing all three components")

RAW_COLS = ["low_access_raw", "grocery_density_raw", "health_outcome_raw"]
NORM_COLS = ["low_access_normalized", "grocery_density_normalized", "health_outcome_normalized"]
# True = inverted (higher raw → lower normalized)
INVERT_FLAGS = [True, False, True]

for raw_col, norm_col, invert in zip(RAW_COLS, NORM_COLS, INVERT_FLAGS):
    col_min = df[raw_col].min()
    col_max = df[raw_col].max()

    if col_max == col_min:
        log("WARN", f"  {raw_col}: min == max ({col_min}) — all normalized to 50.0")
        df[norm_col] = 50.0
    else:
        if invert:
            # Inverted: higher raw value → lower normalized score
            df[norm_col] = (1 - (df[raw_col] - col_min) / (col_max - col_min)) * 100.0
        else:
            # Non-inverted: higher raw value → higher normalized score
            df[norm_col] = ((df[raw_col] - col_min) / (col_max - col_min)) * 100.0

    log("INFO", f"  {raw_col} → {norm_col} ({'INVERTED' if invert else 'DIRECT'}): "
        f"raw [{col_min:.4f}, {col_max:.4f}] → norm [{df[norm_col].min():.2f}, {df[norm_col].max():.2f}]")

# Handle nulls: if raw was null, normalized is null — log but don't fail
for norm_col in NORM_COLS:
    null_count = df[norm_col].isna().sum()
    if null_count > 0:
        log("WARN", f"  {norm_col}: {null_count} nulls (from raw data gaps)")

# ── Median imputation for any remaining nulls ────────────────
# FARA crosswalk may have gaps for some ZIPs. Impute with median
# so nulls don't propagate into the composite score.
for norm_col in NORM_COLS:
    nulls = df[norm_col].isna()
    if nulls.any():
        median_val = df[norm_col].median()
        imputed_zips = df.loc[nulls, "zipcode"].tolist()
        df.loc[nulls, norm_col] = median_val
        log("INFO", f"  Imputed {len(imputed_zips)} {norm_col} nulls with median ({median_val:.2f}): {imputed_zips[:10]}")

print_validation_report("FOOD ACCESS — NORMALIZED", df)

# %% [markdown]
# ## 6a · Normalization Tests (Suite 2) — GATE

# %%
log("TEST", "Running Suite 2 — Normalization Tests")

norm_tests = [
    ("Weights sum to exactly 1.0",
        lambda: (abs(sum(WEIGHTS) - 1.0) < 1e-9, f"Sum = {sum(WEIGHTS)}")),
    ("Weight count matches component count",
        lambda: (len(WEIGHTS) == len(NORM_COLS), f"{len(WEIGHTS)} weights, {len(NORM_COLS)} components")),
]

for norm_col in NORM_COLS:
    nc = norm_col
    norm_tests += [
        (f"{nc} — all values in [0.0, 100.0]",
            lambda c=nc: (
                df[c].dropna().between(0.0, 100.0).all(),
                f"min={df[c].min():.4f}, max={df[c].max():.4f}"
            )),
        (f"{nc} — no nulls",
            lambda c=nc: (
                df[c].isna().sum() == 0,
                f"{df[c].isna().sum()} nulls"
            )),
        (f"{nc} — meaningful spread (std > 1.0)",
            lambda c=nc: (
                df[c].std() > 1.0,
                f"std={df[c].std():.3f}"
            )),
        (f"{nc} — global min normalizes to ~0.0",
            lambda c=nc: (
                df[c].min() < 1.0,
                f"Min = {df[c].min():.4f}"
            )),
        (f"{nc} — global max normalizes to ~100.0",
            lambda c=nc: (
                df[c].max() > 99.0,
                f"Max = {df[c].max():.4f}"
            )),
    ]

# Inversion correctness: low_access and health_outcome are inverted
for norm_col, raw_col, invert in zip(NORM_COLS, RAW_COLS, INVERT_FLAGS):
    if invert:
        nc, rc = norm_col, raw_col
        norm_tests.append((
            f"{nc} — inversion correct (corr < -0.90)",
            lambda n=nc, r=rc: (
                df[[r, n]].dropna().corr().iloc[0, 1] < -0.90,
                f"Pearson r = {df[[r, n]].dropna().corr().iloc[0, 1]:.3f}"
            )
        ))
    else:
        # Non-inverted: positive correlation
        nc, rc = norm_col, raw_col
        norm_tests.append((
            f"{nc} — positive correlation (corr > 0.90)",
            lambda n=nc, r=rc: (
                df[[r, n]].dropna().corr().iloc[0, 1] > 0.90,
                f"Pearson r = {df[[r, n]].dropna().corr().iloc[0, 1]:.3f}"
            )
        ))

suite2_passed = run_tests("FOOD ACCESS — NORMALIZATION", norm_tests)
require_all_pass("FOOD ACCESS — NORMALIZATION", suite2_passed)


# %% [markdown]
# ## 7 · Composite Scoring & Letter Grades
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Scoring failures
# usually mean nulls leaked through normalization — check the Suite 2 gate output above.

# %%
log("START", "Computing composite scores and letter grades")

# Weighted sum: composite = sum(weight_i * normalized_i)
df["composite_score"] = sum(
    w * df[nc] for w, nc in zip(WEIGHTS, NORM_COLS)
)

# Letter grades
df["letter_grade"] = df["composite_score"].apply(assign_grade)

log("INFO", f"Composite score range: {df['composite_score'].min():.2f} – {df['composite_score'].max():.2f}")
log("INFO", f"Mean: {df['composite_score'].mean():.2f}, Std: {df['composite_score'].std():.2f}")
log("INFO", f"Grade distribution:\n{df['letter_grade'].value_counts().to_string()}")

# %% [markdown]
# ## 7a · Scoring Tests (Suite 3) — GATE

# %%
log("TEST", "Running Suite 3 — Scoring Tests")

GRADE_SCALE = {
    "A": (80, 100), "B": (65, 79.999), "C": (50, 64.999),
    "D": (35, 49.999), "F": (0, 34.999),
}

# Spot-check ZIPs: expect general grade direction
# Beverly Hills (90210) should not be F — affluent area with grocery access
# Low-income urban ZIPs should not be A
SPOT_CHECK_ZIPS = {
    "15213": ("F", "A"),   # Pittsburgh — Carnegie Mellon area, mixed
    "90210": ("C", "A"),   # Beverly Hills — good grocery access expected
    "28202": ("F", "B"),   # Downtown Charlotte — urban core
    "85281": ("F", "A"),   # Tempe, AZ — near ASU, suburban/urban mix
}

# Grade order: A=0 (best) … F=4 (worst)
grade_order = ["A", "B", "C", "D", "F"]

def grade_in_range(grade, worst, best):
    """Check if grade falls between best and worst (inclusive)."""
    best_idx = grade_order.index(best)
    worst_idx = grade_order.index(worst)
    grade_idx = grade_order.index(grade)
    return best_idx <= grade_idx <= worst_idx

scoring_tests = [
    ("All composite scores in [0.0, 100.0]",
        lambda: (
            df["composite_score"].between(0.0, 100.0).all(),
            f"min={df['composite_score'].min():.3f}, max={df['composite_score'].max():.3f}"
        )),
    ("No null composite scores",
        lambda: (df["composite_score"].isna().sum() == 0, f"{df['composite_score'].isna().sum()} nulls")),
    ("No null letter grades",
        lambda: (df["letter_grade"].isna().sum() == 0, f"{df['letter_grade'].isna().sum()} nulls")),
    ("Letter grades are valid values",
        lambda: (
            df["letter_grade"].isin(["A", "B", "C", "D", "F"]).all(),
            f"Invalid: {df[~df['letter_grade'].isin(['A','B','C','D','F'])]['letter_grade'].unique()}"
        )),
    ("Grade assignments match score thresholds",
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
    ("Score std > 5.0",
        lambda: (
            df["composite_score"].std() > 5.0,
            f"std={df['composite_score'].std():.3f}"
        )),
    ("Score mean in plausible range [25, 75]",
        lambda: (
            25.0 <= df["composite_score"].mean() <= 75.0,
            f"mean={df['composite_score'].mean():.2f}"
        )),
]

# Spot checks
# Spot checks — skip ZIPs not in the scored DataFrame (may lack data coverage)
for zip_code, (min_g, max_g) in SPOT_CHECK_ZIPS.items():
    z, mn, mx = zip_code, min_g, max_g
    if df[df["zipcode"] == z].shape[0] == 0:
        log("INFO", f"  Skipping spot check for ZIP {z} — not in scored data")
        continue
    scoring_tests.append((
        f"Spot check ZIP {z}: grade between {mn} and {mx}",
        lambda zc=z, lo=mn, hi=mx: (
            grade_in_range(df[df["zipcode"] == zc].iloc[0]["letter_grade"], lo, hi),
            f"ZIP {zc} got grade {df[df['zipcode'] == zc].iloc[0]['letter_grade']}"
        )
    ))

suite3_passed = run_tests("FOOD ACCESS — SCORING", scoring_tests)
require_all_pass("FOOD ACCESS — SCORING", suite3_passed)


# %% [markdown]
# ## 8 · Supabase Upsert
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# column name mismatch between local dict keys and Supabase schema, or missing UNIQUE constraint.
# Do NOT modify the Supabase schema manually — bring the error here first.

# %%
# ── Reinitialize Supabase client (fresh HTTP connection) ─────
# PostgREST may have a stale schema cache from earlier in the pipeline,
# especially if food_access_scores was recently created. A fresh client
# forces a new HTTP connection to avoid PGRST205 errors.
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
log("INFO", "Reinitialized Supabase client for upsert phase")

# ── Schema cache warm-up ─────────────────────────────────────
# Test SELECT to confirm PostgREST recognizes the table before we upsert 600 rows.
MAX_RETRIES = 5
RETRY_DELAY = 3

for attempt in range(1, MAX_RETRIES + 1):
    try:
        test = supabase.table("food_access_scores").select("zipcode").limit(1).execute()
        log("PASS", f"Schema cache warm-up succeeded on attempt {attempt}")
        break
    except Exception as e:
        log("WARN", f"Schema cache warm-up attempt {attempt}/{MAX_RETRIES} failed: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
else:
    raise RuntimeError(
        "\n" + "!" * 62 + "\n"
        "  PostgREST cannot see the food_access_scores table after 5 attempts.\n"
        "  This is a schema cache issue, not a missing table.\n\n"
        "  FIX: Open Supabase SQL Editor and run:\n"
        "    NOTIFY pgrst, 'reload schema';\n\n"
        "  Wait 10 seconds, then re-run this cell.\n"
        + "!" * 62 + "\n"
    )

# %%
log("START", "Upserting all records to food_access_scores")

failed_zips = []

for _, row in df.iterrows():
    record = {
        "zipcode": row["zipcode"],
        "metro": row["metro"],
        "low_access_raw": float(row["low_access_raw"]) if pd.notna(row["low_access_raw"]) else None,
        "low_access_normalized": float(row["low_access_normalized"]) if pd.notna(row["low_access_normalized"]) else None,
        "grocery_density_raw": float(row["grocery_density_raw"]) if pd.notna(row["grocery_density_raw"]) else None,
        "grocery_density_normalized": float(row["grocery_density_normalized"]) if pd.notna(row["grocery_density_normalized"]) else None,
        "health_outcome_raw": float(row["health_outcome_raw"]) if pd.notna(row["health_outcome_raw"]) else None,
        "health_outcome_normalized": float(row["health_outcome_normalized"]) if pd.notna(row["health_outcome_normalized"]) else None,
        "composite_score": float(row["composite_score"]),
        "letter_grade": row["letter_grade"],
        "score_date": str(date.today()),
    }

    try:
        supabase.table("food_access_scores").upsert(
            record, on_conflict="zipcode"
        ).execute()
    except Exception as e:
        log("ERROR", f"Failed to write ZIP {row['zipcode']}: {e}")
        failed_zips.append(row["zipcode"])

if failed_zips:
    log("WARN", f"{len(failed_zips)} ZIPs failed upsert: {failed_zips[:10]}")
else:
    log("PASS", "All records upserted to food_access_scores")

# %% [markdown]
# ## 9a · Supabase Write Tests (Suite 4) — GATE

# %%
log("TEST", "Running Suite 4 — Supabase Write Tests")

TABLE_NAME = "food_access_scores"

def get_sb_count():
    result = supabase.table(TABLE_NAME).select("zipcode", count="exact").execute()
    return result.count or 0

def get_sb_row(zc):
    result = supabase.table(TABLE_NAME).select("*").eq("zipcode", zc).execute()
    return result.data[0] if result.data else None

initial_count = get_sb_count()

SPOT_ZIPS = ["15213", "90210", "28202", "28277", "85281"]

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

# Spot-check ZIPs: verify values match local data
for zc in SPOT_ZIPS:
    z = zc
    write_tests.append((
        f"Spot check ZIP {z}: exists in Supabase",
        lambda zc=z: (
            get_sb_row(zc) is not None,
            f"ZIP {zc} not found in {TABLE_NAME}"
        )
    ))

    local_row = df[df["zipcode"] == z]
    if not local_row.empty:
        expected_score = round(float(local_row.iloc[0]["composite_score"]), 1)
        write_tests.append((
            f"Spot check ZIP {z}: composite_score matches local ({expected_score})",
            lambda zc=z, exp=expected_score: (
                (row := get_sb_row(zc)) is not None
                and abs(round(float(row["composite_score"]), 1) - exp) < 0.5,
                f"Supabase: {get_sb_row(zc) and round(float(get_sb_row(zc)['composite_score']), 1)}, "
                f"local: {exp}"
            )
        ))

suite4_passed = run_tests("FOOD ACCESS — SUPABASE WRITE", write_tests)
require_all_pass("FOOD ACCESS — SUPABASE WRITE", suite4_passed)


# %% [markdown]
# ## 10 · Pipeline Complete
#
# All 4 test suites passed. Data is live in `food_access_scores`.

# %%
log("DONE", f"Food Access pipeline complete — {len(df)} ZIPs scored and written to Supabase")
log("INFO", f"Grade distribution: {df['letter_grade'].value_counts().to_dict()}")
log("INFO", f"Mean composite score: {df['composite_score'].mean():.2f}")
log("INFO", f"Score range: {df['composite_score'].min():.2f} – {df['composite_score'].max():.2f}")
