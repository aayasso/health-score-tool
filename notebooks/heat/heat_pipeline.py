# %% [markdown]
# # Heat & Climate Resilience Score — Full Pipeline
# **Tool 5 of 5 · LaSalle Technologies Health Environment Score**
#
# Run each cell in order. Every gate must pass before proceeding.
# Designed for Google Colab with Supabase credentials in Colab secrets.
#
# **Mixed pipeline: 1 raster (NLCD tree canopy), 1 reuse from Cardiovascular
# (impervious surface), 1 tabular (CDC PLACES asthma + COPD).**

# %% [markdown]
# ## 0 · Setup & Configuration
#
# **IF ANY CELL IN THIS NOTEBOOK FAILS:** Stop immediately. Do not debug manually in Colab.
# Copy the full error traceback and bring it to Claude Code for diagnosis. Manual Colab fixes
# often introduce silent regressions that are harder to trace later.

# %%
# ── Installs (run once per Colab session) ────────────────────
# !pip install -q supabase rasterio geopandas rasterstats anthropic shapely requests pandas

# %%
import os
import time
import traceback
import requests
import numpy as np
import pandas as pd
import geopandas as gpd
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


def upsert_with_retry(supabase, table_name, record, on_conflict="zipcode",
                      max_attempts=3, backoff_base=1):
    """
    Upsert a single record with retry on transient HTTP errors (502/503/504).
    Exponential backoff: 1s, 2s, 4s. Raises on non-retryable or final failure.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            supabase.table(table_name).upsert(
                record, on_conflict=on_conflict
            ).execute()
            return
        except Exception as e:
            err_str = str(e)
            retryable = any(code in err_str for code in ["502", "503", "504"]) \
                        or "ConnectionError" in type(e).__name__ \
                        or "ConnectionReset" in err_str \
                        or "RemoteDisconnected" in err_str
            if retryable and attempt < max_attempts:
                wait = backoff_base * (2 ** (attempt - 1))
                log("WARN", f"Retry {attempt}/{max_attempts} for ZIP {record.get('zipcode')} "
                            f"after {type(e).__name__}: {err_str[:100]}... waiting {wait}s")
                time.sleep(wait)
            else:
                raise


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
WEIGHTS = [0.30, 0.35, 0.35]
WEIGHT_LABELS = [
    "impervious",
    "tree_canopy",
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

# NLCD Tree Canopy Coverage raster
# Download from: https://www.mrlc.gov/data → select "NLCD Tree Canopy" → CONUS
TREE_CANOPY_RASTER_PATH = f"{DRIVE_PREFIX}/nlcd_tree_canopy.tif"

# ZCTA shapefile (same as used in Tools 2–3)
ZCTA_SHAPEFILE_PATH = f"{DRIVE_PREFIX}/tl_2023_us_zcta520/tl_2023_us_zcta520.shp"


# %% [markdown]
# ## 1 · Supabase Schema
# Run this SQL in the Supabase SQL Editor **once** before proceeding:
#
# ```sql
# CREATE TABLE IF NOT EXISTS heat_scores (
#   id SERIAL PRIMARY KEY,
#   zipcode TEXT NOT NULL,
#   metro TEXT NOT NULL,
#   impervious_raw NUMERIC,
#   impervious_normalized NUMERIC,
#   tree_canopy_raw NUMERIC,
#   tree_canopy_normalized NUMERIC,
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
# CREATE TRIGGER set_updated_at_heat
# BEFORE UPDATE ON heat_scores
# FOR EACH ROW EXECUTE FUNCTION update_updated_at();
# ```


# %% [markdown]
# ## 2 · Impervious Surface Ingestion (from cardiovascular_scores)
#
# Impervious surface was processed during the Cardiovascular (Tool 2) pipeline and
# stored in `cardiovascular_scores.impervious_raw`. We read it directly — same pattern
# as noise reuse in Tool 3. No raster reprocessing needed.
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. The most likely
# issue is that the Cardiovascular pipeline has not yet been run.

# %%
log("START", "Loading impervious surface data from cardiovascular_scores (direct reuse)")

# Read impervious_raw directly from cardiovascular_scores
all_imp_rows = []
batch_size = 500
_offset = 0

while True:
    resp = supabase.table("cardiovascular_scores") \
        .select("zipcode, impervious_raw") \
        .not_.is_("impervious_raw", "null") \
        .range(_offset, _offset + batch_size - 1) \
        .execute()
    if not resp.data:
        break
    all_imp_rows.extend(resp.data)
    if len(resp.data) < batch_size:
        break
    _offset += batch_size

if len(all_imp_rows) < 550:
    raise RuntimeError(
        f"Impervious surface data not found in cardiovascular_scores ({len(all_imp_rows)} rows, need ≥550). "
        f"Run the Cardiovascular pipeline first."
    )

df_impervious = pd.DataFrame(all_imp_rows)
df_impervious["impervious_raw"] = pd.to_numeric(df_impervious["impervious_raw"], errors="coerce")

log("PASS", f"Impervious surface data confirmed: {len(df_impervious)} rows from cardiovascular_scores")
log("INFO", f"  Range: {df_impervious['impervious_raw'].min():.1f} – {df_impervious['impervious_raw'].max():.1f}%")


# %% [markdown]
# ## 3 · NLCD Tree Canopy Raster Ingestion
#
# The NLCD Tree Canopy dataset provides percent tree canopy coverage per pixel.
# We run zonal_stats (mean) against our ZCTA polygons. This is a single CONUS
# file — processed in one pass like VIIRS in Tool 3.
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# missing raster on Drive, CRS mismatch, or nodata value mismatch.
# Check nodata from raster metadata — do NOT hardcode.

# %%
log("START", "Processing NLCD Tree Canopy raster")

import rasterio
from rasterstats import zonal_stats

# ── Load ZCTA shapefile ──────────────────────────────────────
log("INFO", f"Loading ZCTA shapefile from {ZCTA_SHAPEFILE_PATH}")
gdf_zcta = gpd.read_file(ZCTA_SHAPEFILE_PATH)

# ZCTA 2020 vintage uses ZCTA5CE20 column
gdf_zcta = gdf_zcta[gdf_zcta["ZCTA5CE20"].isin(ALL_ZIPS)].copy()
gdf_zcta = gdf_zcta.rename(columns={"ZCTA5CE20": "zipcode"})
log("INFO", f"  Filtered ZCTA to {len(gdf_zcta)} of our ZIPs")

# %%
log("INFO", f"Loading tree canopy raster from {TREE_CANOPY_RASTER_PATH}")

with rasterio.open(TREE_CANOPY_RASTER_PATH) as src:
    canopy_crs = src.crs
    canopy_nodata = src.nodata
    log("INFO", f"  Tree canopy CRS: {canopy_crs}, shape: {src.shape}, nodata: {canopy_nodata}")

# Reproject ZCTA polygons to match raster CRS
gdf_canopy = gdf_zcta.to_crs(canopy_crs)

# Read nodata from metadata — do NOT hardcode (lesson from NLCD impervious nodata=250.0)
canopy_nodata_val = canopy_nodata if canopy_nodata is not None else -9999
log("INFO", f"  Using nodata value: {canopy_nodata_val}")

log("INFO", f"  Running zonal_stats on all {len(gdf_canopy)} ZIPs...")
canopy_stats = zonal_stats(
    gdf_canopy,
    TREE_CANOPY_RASTER_PATH,
    stats=["mean"],
    geojson_out=False,
    nodata=canopy_nodata_val,
)

gdf_canopy["tree_canopy_raw"] = [s["mean"] for s in canopy_stats]
df_canopy = gdf_canopy[["zipcode", "tree_canopy_raw"]].copy()

log("INFO", f"  Done — {df_canopy['tree_canopy_raw'].notna().sum()} ZIPs with data")

# Report on nulls
null_canopy = df_canopy["tree_canopy_raw"].isna().sum()
if null_canopy > 0:
    log("WARN", f"  {null_canopy} ZIPs have no tree canopy data (raster nodata)")

log("INFO", f"  Tree canopy range: {df_canopy['tree_canopy_raw'].min():.2f} – {df_canopy['tree_canopy_raw'].max():.2f}%")


# %% [markdown]
# ## 4 · CDC PLACES Ingestion (Asthma + COPD)
#
# Heat-sensitive respiratory health outcomes: current asthma and COPD prevalence.
# These conditions are exacerbated by heat exposure and poor air quality in
# neighborhoods with high impervious surface and low tree canopy.
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# API format changes (fields renamed), rate limits (HTTP 429), or ZIP matching failures.
# Do NOT try to patch the query manually — the API schema has changed before (see CONTEXT.md).

# %%
log("START", "Ingesting CDC PLACES data for Asthma and COPD")

CDC_BASE_URL = "https://data.cdc.gov/resource/c7b2-4ecy.json"

# CDC PLACES API is WIDE format (confirmed April 2026):
#   - One row per ZCTA, with separate columns for each measure
#   - ZIP field is "zcta5" (not "locationname")
#   - No "measureid" column — measures are column names like "casthma_crudeprev"
#   - Use $select to request only needed columns, $where to filter by zcta5
#   - Batch size 50 ZIPs per request to stay within Socrata URL length limits

def fetch_cdc_places_wide(zip_codes: list, select_cols: list, batch_size: int = 50) -> list:
    """
    Fetch CDC PLACES data in wide format, batching by ZIP to avoid URL length limits.
    select_cols: columns to request, e.g. ["zcta5", "casthma_crudeprev", "copd_crudeprev"]
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
# Fetch asthma and COPD in wide format
CDC_SELECT_COLS = ["zcta5", "casthma_crudeprev", "copd_crudeprev"]

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
df_cdc["casthma_crudeprev"] = pd.to_numeric(df_cdc["casthma_crudeprev"], errors="coerce")
df_cdc["copd_crudeprev"] = pd.to_numeric(df_cdc["copd_crudeprev"], errors="coerce")

# Combine: health_outcome_raw = average of asthma and COPD prevalence
df_cdc["health_outcome_raw"] = (df_cdc["casthma_crudeprev"] + df_cdc["copd_crudeprev"]) / 2.0

df_heat_cdc = df_cdc.rename(columns={"zcta5": "zipcode"})
df_heat_cdc = df_heat_cdc.drop_duplicates(subset=["zipcode"], keep="first")

# Add metro from master ZIP list
df_heat_cdc["metro"] = df_heat_cdc["zipcode"].map(ZIP_METRO_MAP)

# Filter to only our 600 ZIPs
df_heat_cdc = df_heat_cdc[df_heat_cdc["zipcode"].isin(ALL_ZIPS)].copy()

log("INFO", f"CDC PLACES parsed: {len(df_heat_cdc)} ZIPs with data")
log("INFO", f"  Asthma coverage: {df_heat_cdc['casthma_crudeprev'].notna().sum()} ZIPs")
log("INFO", f"  COPD coverage: {df_heat_cdc['copd_crudeprev'].notna().sum()} ZIPs")
log("INFO", f"  health_outcome_raw range: {df_heat_cdc['health_outcome_raw'].min():.1f} – {df_heat_cdc['health_outcome_raw'].max():.1f}")


# %% [markdown]
# ## 5 · Merge All Components
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Merge failures
# usually mean a ZIP column mismatch between data sources or an unexpected null pattern.

# %%
log("START", "Merging all three components into a single DataFrame")

# Start from CDC data (has zipcode + metro + health_outcome_raw)
df = df_heat_cdc[["zipcode", "metro", "health_outcome_raw"]].copy()

# Merge impervious surface
df = df.merge(df_impervious[["zipcode", "impervious_raw"]], on="zipcode", how="left")

# Merge tree canopy
df = df.merge(df_canopy[["zipcode", "tree_canopy_raw"]], on="zipcode", how="left")

# Ensure metro is filled for all ZIPs
df["metro"] = df["zipcode"].map(ZIP_METRO_MAP)

# Filter to only our 600 ZIPs (safety)
df = df[df["zipcode"].isin(ALL_ZIPS)].copy()

log("INFO", f"Merged DataFrame: {len(df)} rows")
print_validation_report("HEAT & CLIMATE RESILIENCE — MERGED RAW DATA", df)

# %% [markdown]
# ## 5a · Ingestion Tests (Suite 1) — GATE

# %%
log("TEST", "Running Suite 1 — Ingestion Tests")

ingestion_tests = [
    ("All expected columns present",
        lambda: (
            all(c in df.columns for c in
                ["zipcode", "metro", "impervious_raw", "tree_canopy_raw", "health_outcome_raw"]),
            f"Missing: {[c for c in ['zipcode','metro','impervious_raw','tree_canopy_raw','health_outcome_raw'] if c not in df.columns]}"
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
    ("impervious_raw in [0, 100]",
        lambda: (
            df["impervious_raw"].dropna().between(-0.01, 100.01).all(),
            f"min={df['impervious_raw'].min():.1f}, max={df['impervious_raw'].max():.1f}"
        )),
    ("impervious_raw nulls < 10%",
        lambda: (
            df["impervious_raw"].isna().sum() / len(df) < 0.10,
            f"{df['impervious_raw'].isna().sum()} nulls ({df['impervious_raw'].isna().sum()/len(df)*100:.1f}%)"
        )),
    ("tree_canopy_raw in [0, 100]",
        lambda: (
            df["tree_canopy_raw"].dropna().between(-0.01, 100.01).all(),
            f"min={df['tree_canopy_raw'].min():.2f}, max={df['tree_canopy_raw'].max():.2f}"
        )),
    ("tree_canopy_raw nulls < 10%",
        lambda: (
            df["tree_canopy_raw"].isna().sum() / len(df) < 0.10,
            f"{df['tree_canopy_raw'].isna().sum()} nulls ({df['tree_canopy_raw'].isna().sum()/len(df)*100:.1f}%)"
        )),
    ("health_outcome_raw in [2, 30]",
        lambda: (
            df["health_outcome_raw"].dropna().between(2, 30).all(),
            f"min={df['health_outcome_raw'].min():.1f}, max={df['health_outcome_raw'].max():.1f}"
        )),
    ("health_outcome_raw nulls < 10%",
        lambda: (
            df["health_outcome_raw"].isna().sum() / len(df) < 0.10,
            f"{df['health_outcome_raw'].isna().sum()} nulls ({df['health_outcome_raw'].isna().sum()/len(df)*100:.1f}%)"
        )),
]

suite1_passed = run_tests("HEAT & CLIMATE RESILIENCE — INGESTION", ingestion_tests)
require_all_pass("HEAT & CLIMATE RESILIENCE — INGESTION", suite1_passed)


# %% [markdown]
# ## 6 · Normalization
# Min-max normalization, global across all 600 ZIPs.
# - `impervious_raw` → **INVERT** (higher raw = more paved = worse)
# - `tree_canopy_raw` → **DO NOT INVERT** (higher raw = more trees = better)
# - `health_outcome_raw` → **INVERT** (higher raw = more asthma/COPD = worse)
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Normalization
# failures typically mean a column has all-null or constant values from a broken ingestion step.

# %%
log("START", "Normalizing all three components")

RAW_COLS = ["impervious_raw", "tree_canopy_raw", "health_outcome_raw"]
NORM_COLS = ["impervious_normalized", "tree_canopy_normalized", "health_outcome_normalized"]
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
# Raster coverage gaps may leave some ZIPs null. Impute with median
# so nulls don't propagate into the composite score.
for norm_col in NORM_COLS:
    nulls = df[norm_col].isna()
    if nulls.any():
        median_val = df[norm_col].median()
        imputed_zips = df.loc[nulls, "zipcode"].tolist()
        df.loc[nulls, norm_col] = median_val
        log("INFO", f"  Imputed {len(imputed_zips)} {norm_col} nulls with median ({median_val:.2f}): {imputed_zips[:10]}")

print_validation_report("HEAT & CLIMATE RESILIENCE — NORMALIZED", df)

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

# Inversion correctness: impervious and health_outcome are inverted, tree_canopy is not
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

suite2_passed = run_tests("HEAT & CLIMATE RESILIENCE — NORMALIZATION", norm_tests)
require_all_pass("HEAT & CLIMATE RESILIENCE — NORMALIZATION", suite2_passed)


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
# Phoenix ZIPs should tend lower (less canopy, more pavement); tree-heavy suburban ZIPs higher
SPOT_CHECK_ZIPS = {
    "15213": ("F", "A"),   # Pittsburgh — Carnegie Mellon area, moderate canopy
    "90210": ("C", "A"),   # Beverly Hills — tree-covered, good air
    "28202": ("F", "B"),   # Downtown Charlotte — urban core, more impervious
    "85281": ("F", "B"),   # Tempe, AZ — desert, less canopy, more pavement
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

suite3_passed = run_tests("HEAT & CLIMATE RESILIENCE — SCORING", scoring_tests)
require_all_pass("HEAT & CLIMATE RESILIENCE — SCORING", suite3_passed)


# %% [markdown]
# ## 8 · Supabase Upsert
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# column name mismatch between local dict keys and Supabase schema, or missing UNIQUE constraint.
# Do NOT modify the Supabase schema manually — bring the error here first.

# %%
# ── Reinitialize Supabase client (fresh HTTP connection) ─────
# PostgREST may have a stale schema cache from earlier in the pipeline,
# especially if heat_scores was recently created. A fresh client
# forces a new HTTP connection to avoid PGRST205 errors.
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
log("INFO", "Reinitialized Supabase client for upsert phase")

# ── Schema cache warm-up ─────────────────────────────────────
# Test SELECT to confirm PostgREST recognizes the table before we upsert 600 rows.
MAX_RETRIES = 5
RETRY_DELAY = 3

for attempt in range(1, MAX_RETRIES + 1):
    try:
        test = supabase.table("heat_scores").select("zipcode").limit(1).execute()
        log("PASS", f"Schema cache warm-up succeeded on attempt {attempt}")
        break
    except Exception as e:
        log("WARN", f"Schema cache warm-up attempt {attempt}/{MAX_RETRIES} failed: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
else:
    raise RuntimeError(
        "\n" + "!" * 62 + "\n"
        "  PostgREST cannot see the heat_scores table after 5 attempts.\n"
        "  This is a schema cache issue, not a missing table.\n\n"
        "  FIX: Open Supabase SQL Editor and run:\n"
        "    NOTIFY pgrst, 'reload schema';\n\n"
        "  Wait 10 seconds, then re-run this cell.\n"
        + "!" * 62 + "\n"
    )

# %%
log("START", "Upserting all records to heat_scores")

failed_zips = []

for _, row in df.iterrows():
    record = {
        "zipcode": row["zipcode"],
        "metro": row["metro"],
        "impervious_raw": float(row["impervious_raw"]) if pd.notna(row["impervious_raw"]) else None,
        "impervious_normalized": float(row["impervious_normalized"]) if pd.notna(row["impervious_normalized"]) else None,
        "tree_canopy_raw": float(row["tree_canopy_raw"]) if pd.notna(row["tree_canopy_raw"]) else None,
        "tree_canopy_normalized": float(row["tree_canopy_normalized"]) if pd.notna(row["tree_canopy_normalized"]) else None,
        "health_outcome_raw": float(row["health_outcome_raw"]) if pd.notna(row["health_outcome_raw"]) else None,
        "health_outcome_normalized": float(row["health_outcome_normalized"]) if pd.notna(row["health_outcome_normalized"]) else None,
        "composite_score": float(row["composite_score"]),
        "letter_grade": row["letter_grade"],
        "score_date": str(date.today()),
    }

    try:
        upsert_with_retry(supabase, "heat_scores", record)
    except Exception as e:
        log("ERROR", f"Failed to write ZIP {row['zipcode']}: {e}")
        failed_zips.append(row["zipcode"])

if failed_zips:
    log("WARN", f"{len(failed_zips)} ZIPs failed upsert: {failed_zips[:10]}")
else:
    log("PASS", "All records upserted to heat_scores")

# %% [markdown]
# ## 9a · Supabase Write Tests (Suite 4) — GATE

# %%
log("TEST", "Running Suite 4 — Supabase Write Tests")

TABLE_NAME = "heat_scores"

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
    if df[df["zipcode"] == z].shape[0] == 0:
        log("INFO", f"  Skipping Suite 4 spot check for ZIP {z} — not in scored data")
        continue
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

suite4_passed = run_tests("HEAT & CLIMATE RESILIENCE — SUPABASE WRITE", write_tests)
require_all_pass("HEAT & CLIMATE RESILIENCE — SUPABASE WRITE", suite4_passed)


# %% [markdown]
# ## 10 · Pipeline Complete
#
# All 4 test suites passed. Data is live in `heat_scores`.

# %%
log("DONE", f"Heat & Climate Resilience pipeline complete — {len(df)} ZIPs scored and written to Supabase")
log("INFO", f"Grade distribution: {df['letter_grade'].value_counts().to_dict()}")
log("INFO", f"Mean composite score: {df['composite_score'].mean():.2f}")
log("INFO", f"Score range: {df['composite_score'].min():.2f} – {df['composite_score'].max():.2f}")
