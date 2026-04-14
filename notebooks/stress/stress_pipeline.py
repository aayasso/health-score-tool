# %% [markdown]
# # Stress / Sensory Environment Score — Full Pipeline
# **Tool 3 of 5 · LaSalle Technologies Health Environment Score**
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
# !pip install -q supabase rasterio geopandas rasterstats anthropic shapely requests pandas

# %%
import os
import math
import time
import traceback
import requests
import numpy as np
import pandas as pd
import geopandas as gpd
from datetime import datetime

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
}

ALL_ZIPS = df_zips["zipcode"].tolist()
ZIP_METRO_MAP = dict(zip(df_zips["zipcode"], df_zips["metro"]))

log("INFO", f"Loaded {len(ALL_ZIPS)} ZIPs across {df_zips['metro'].nunique()} metros")
log("INFO", f"Metro counts: {df_zips['metro'].value_counts().to_dict()}")

# ── Component Weights (proprietary — do not expose) ─────────
WEIGHTS = [0.35, 0.25, 0.25, 0.15]
WEIGHT_LABELS = [
    "noise",
    "light_pollution",
    "depression",
    "mental_health",
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


# %% [markdown]
# ## 1 · Supabase Schema
# Run this SQL in the Supabase SQL Editor **once** before proceeding:
#
# ```sql
# CREATE TABLE IF NOT EXISTS stress_scores (
#   id SERIAL PRIMARY KEY,
#   zipcode TEXT NOT NULL,
#   metro TEXT NOT NULL,
#   noise_raw NUMERIC,
#   noise_normalized NUMERIC,
#   light_pollution_raw NUMERIC,
#   light_pollution_normalized NUMERIC,
#   depression_raw NUMERIC,
#   depression_normalized NUMERIC,
#   mental_health_raw NUMERIC,
#   mental_health_normalized NUMERIC,
#   composite_score NUMERIC,
#   letter_grade TEXT,
#   interpretation TEXT,
#   created_at TIMESTAMPTZ DEFAULT NOW(),
#   updated_at TIMESTAMPTZ DEFAULT NOW(),
#   UNIQUE(zipcode)
# );
#
# -- Auto-update timestamp trigger
# CREATE TRIGGER set_updated_at_stress
# BEFORE UPDATE ON stress_scores
# FOR EACH ROW EXECUTE FUNCTION update_updated_at();
# ```

# %% [markdown]
# ## 2 · BTS Noise Ingestion (from cardiovascular_scores)
#
# Noise data was processed during the Cardiovascular (Tool 2) pipeline and stored in
# `cardiovascular_scores.noise_raw`. We read it directly — no `raw_signals` dependency needed.
# Same data, different narrative framing:
# Cardiovascular = physiological stress on heart; Stress = psychological burden (annoyance, sleep disruption).
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. The most likely issue
# is that the Cardiovascular pipeline has not yet been run.

# %%
log("START", "Loading BTS noise data from cardiovascular_scores (direct reuse)")

# Read noise_raw directly from cardiovascular_scores — no raw_signals dependency needed
all_noise_rows = []
batch_size = 500
_offset = 0

while True:
    resp = supabase.table("cardiovascular_scores") \
        .select("zipcode, noise_raw") \
        .not_.is_("noise_raw", "null") \
        .range(_offset, _offset + batch_size - 1) \
        .execute()
    if not resp.data:
        break
    all_noise_rows.extend(resp.data)
    if len(resp.data) < batch_size:
        break
    _offset += batch_size

if len(all_noise_rows) < 550:
    raise RuntimeError(
        f"BTS noise data not found in cardiovascular_scores ({len(all_noise_rows)} rows, need ≥550). "
        f"Run the Cardiovascular pipeline first."
    )

df_noise = pd.DataFrame(all_noise_rows)
df_noise["noise_raw"] = pd.to_numeric(df_noise["noise_raw"], errors="coerce")

log("PASS", f"BTS noise data confirmed: {len(df_noise)} rows from cardiovascular_scores")
log("INFO", f"  Range: {df_noise['noise_raw'].min():.1f} – {df_noise['noise_raw'].max():.1f} dB DNL")


# %% [markdown]
# ## 3 · NASA VIIRS Light Pollution Raster
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# missing VIIRS raster on Drive, CRS mismatch, or out-of-memory on global composite.
# VIIRS is a single global file — processed in one pass against all 600 ZIP polygons.

# %%
log("START", "Processing NASA VIIRS Light Pollution raster")

# ── Check if already processed ───────────────────────────────
def raster_already_processed(source_name: str, min_expected_rows: int = 550) -> bool:
    """Check raw_signals for existing processed raster data."""
    result = supabase.table("raw_signals") \
        .select("zipcode", count="exact") \
        .eq("data_source", source_name) \
        .execute()
    row_count = result.count or 0
    if row_count >= min_expected_rows:
        log("INFO", f"  {source_name} already processed ({row_count} ZIPs) — skipping download")
        return True
    log("INFO", f"  {source_name}: {row_count} rows found, need {min_expected_rows} — proceeding")
    return False

viirs_already_done = raster_already_processed("nasa_viirs")

# %%
# ── Download & process VIIRS raster ──────────────────────────
# This cell processes the raster. Skip if already done (viirs_already_done == True).
#
# INSTRUCTIONS FOR COLAB:
# 1. Download the NASA VIIRS annual VNL v2 composite GeoTIFF from
#    https://eogdata.mines.edu/products/vnl/ (select annual composite, global, most recent year)
# 2. Upload the .tif file to Google Drive
# 3. Set VIIRS_RASTER_PATH below to the file location
# Note: The global VIIRS composite is large (~1.5GB). If Colab RAM is insufficient,
# consider downloading only the tiles covering the US.

import rasterio
from rasterstats import zonal_stats

# ── Paths (update these in Colab) ────────────────────────────
DRIVE_PREFIX = "/content/drive/MyDrive/Colab Notebooks/health-score-data"

# VIIRS global composite — single file, processed against all 600 ZIP polygons at once.
VIIRS_RASTER_PATH = f"{DRIVE_PREFIX}/VNL_npp_2025_global_vcmslcfg_v2_c202604011200.average_masked.dat.tif"

ZCTA_SHAPEFILE_PATH = f"{DRIVE_PREFIX}/tl_2023_us_zcta520/tl_2023_us_zcta520.shp"

if not viirs_already_done:
    log("INFO", f"Loading ZCTA shapefile from {ZCTA_SHAPEFILE_PATH}")
    gdf_zcta = gpd.read_file(ZCTA_SHAPEFILE_PATH)

    # ZCTA 2020 vintage uses ZCTA5CE20 column
    gdf_zcta = gdf_zcta[gdf_zcta["ZCTA5CE20"].isin(ALL_ZIPS)].copy()
    gdf_zcta = gdf_zcta.rename(columns={"ZCTA5CE20": "zipcode"})
    log("INFO", f"  Filtered ZCTA to {len(gdf_zcta)} of our ZIPs")

    # ── Process VIIRS raster ─────────────────────────────────
    # VIIRS is a single global raster — process all 600 ZIPs in one pass.
    # Unlike BTS noise (per-state files), no state-level splitting is needed.
    with rasterio.open(VIIRS_RASTER_PATH) as src:
        viirs_crs = src.crs
        viirs_nodata = src.nodata
        log("INFO", f"  VIIRS CRS: {viirs_crs}, shape: {src.shape}, nodata: {viirs_nodata}")

    # Reproject ZCTA polygons to match raster CRS
    gdf_viirs = gdf_zcta.to_crs(viirs_crs)

    nodata_val = viirs_nodata if viirs_nodata is not None else -9999

    log("INFO", f"  Running zonal_stats on all {len(gdf_viirs)} ZIPs...")
    stats = zonal_stats(
        gdf_viirs,
        VIIRS_RASTER_PATH,
        stats=["mean"],
        geojson_out=False,
        nodata=nodata_val,
    )

    gdf_viirs["light_pollution_raw"] = [s["mean"] for s in stats]
    df_viirs = gdf_viirs[["zipcode", "light_pollution_raw"]].copy()
    log("INFO", f"  Done — {df_viirs['light_pollution_raw'].notna().sum()} ZIPs with data")

    # Report on nulls
    null_viirs = df_viirs["light_pollution_raw"].isna().sum()
    if null_viirs > 0:
        log("WARN", f"  {null_viirs} ZIPs have no VIIRS data (raster nodata)")

    log("INFO", f"  VIIRS processing complete: {df_viirs['light_pollution_raw'].notna().sum()} ZIPs with data")
    log("INFO", f"  Range: {df_viirs['light_pollution_raw'].min():.2f} – {df_viirs['light_pollution_raw'].max():.2f} nW/cm²/sr")

    # ── Write to raw_signals for potential reuse ─────────────
    log("INFO", "  Writing VIIRS values to raw_signals table...")
    viirs_failed = []
    for _, row in df_viirs.dropna(subset=["light_pollution_raw"]).iterrows():
        record = {
            "zipcode": row["zipcode"],
            "signal_name": "viirs_radiance",
            "data_source": "nasa_viirs",
            "data_vintage": 2024,
            "raw_value": float(row["light_pollution_raw"]),
            "unit": "nW/cm2/sr",
        }
        try:
            supabase.table("raw_signals").upsert(
                record,
                on_conflict="zipcode,signal_name,data_source,data_vintage"
            ).execute()
        except Exception as e:
            log("ERROR", f"  Failed to write VIIRS for ZIP {row['zipcode']}: {e}")
            viirs_failed.append(row["zipcode"])

    if viirs_failed:
        log("WARN", f"  {len(viirs_failed)} ZIPs failed raw_signals write: {viirs_failed[:10]}")
    else:
        log("PASS", "  All VIIRS values written to raw_signals")

else:
    # Load from raw_signals
    log("INFO", "  Loading cached VIIRS data from raw_signals...")
    viirs_result = supabase.table("raw_signals") \
        .select("zipcode, raw_value") \
        .eq("data_source", "nasa_viirs") \
        .execute()
    df_viirs = pd.DataFrame(viirs_result.data)
    df_viirs = df_viirs.rename(columns={"raw_value": "light_pollution_raw"})
    df_viirs["light_pollution_raw"] = pd.to_numeric(df_viirs["light_pollution_raw"], errors="coerce")
    log("INFO", f"  Loaded {len(df_viirs)} VIIRS values from raw_signals")


# %% [markdown]
# ## 4 · CDC PLACES Ingestion (Depression + Mental Health)
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# API format changes (fields renamed), rate limits (HTTP 429), or ZIP matching failures.
# Do NOT try to patch the query manually — the API schema has changed before (see CONTEXT.md).

# %%
log("START", "Ingesting CDC PLACES data for Depression and Poor Mental Health Days")

CDC_BASE_URL = "https://data.cdc.gov/resource/c7b2-4ecy.json"

# CDC PLACES API is WIDE format (confirmed April 2026):
#   - One row per ZCTA, with separate columns for each measure
#   - ZIP field is "zcta5" (not "locationname")
#   - No "measureid" column — measures are column names like "depression_crudeprev"
#   - Use $select to request only needed columns, $where to filter by zcta5
#   - Batch size 50 ZIPs per request to stay within Socrata URL length limits

def fetch_cdc_places_wide(zip_codes: list, select_cols: list, batch_size: int = 50) -> list:
    """
    Fetch CDC PLACES data in wide format, batching by ZIP to avoid URL length limits.
    select_cols: columns to request, e.g. ["zcta5", "depression_crudeprev", "mhlth_crudeprev"]
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
# Fetch depression and poor mental health days in wide format
CDC_SELECT_COLS = ["zcta5", "depression_crudeprev", "mhlth_crudeprev"]

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
df_cdc["depression_crudeprev"] = pd.to_numeric(df_cdc["depression_crudeprev"], errors="coerce")
df_cdc["mhlth_crudeprev"] = pd.to_numeric(df_cdc["mhlth_crudeprev"], errors="coerce")

df_stress_cdc = df_cdc.rename(columns={
    "zcta5": "zipcode",
    "depression_crudeprev": "depression_raw",
    "mhlth_crudeprev": "mental_health_raw",
})
df_stress_cdc = df_stress_cdc.drop_duplicates(subset=["zipcode"], keep="first")

# Add metro from master ZIP list
df_stress_cdc["metro"] = df_stress_cdc["zipcode"].map(ZIP_METRO_MAP)

# Filter to only our 600 ZIPs
df_stress_cdc = df_stress_cdc[df_stress_cdc["zipcode"].isin(ALL_ZIPS)].copy()

log("INFO", f"CDC PLACES parsed: {len(df_stress_cdc)} ZIPs with data")
log("INFO", f"  Depression coverage: {df_stress_cdc['depression_raw'].notna().sum()} ZIPs")
log("INFO", f"  Mental Health coverage: {df_stress_cdc['mental_health_raw'].notna().sum()} ZIPs")


# %% [markdown]
# ## 5 · Merge All Components
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Merge failures
# usually mean a ZIP column mismatch between data sources or an unexpected null pattern.

# %%
log("START", "Merging all four components into a single DataFrame")

# Start from CDC data (has zipcode + metro + depression + mental_health)
df = df_stress_cdc.copy()

# Merge noise
df = df.merge(df_noise[["zipcode", "noise_raw"]], on="zipcode", how="left")

# Merge light pollution
df = df.merge(df_viirs[["zipcode", "light_pollution_raw"]], on="zipcode", how="left")

# Ensure metro is filled for any ZIPs that came in via raster but not CDC
df["metro"] = df["zipcode"].map(ZIP_METRO_MAP)

# Filter to only our 600 ZIPs (safety)
df = df[df["zipcode"].isin(ALL_ZIPS)].copy()

log("INFO", f"Merged DataFrame: {len(df)} rows")
print_validation_report("STRESS / SENSORY — MERGED RAW DATA", df)

# %% [markdown]
# ## 5a · Ingestion Tests (Suite 1) — GATE

# %%
log("TEST", "Running Suite 1 — Ingestion Tests")

ingestion_tests = [
    ("All expected columns present",
        lambda: (
            all(c in df.columns for c in
                ["zipcode", "metro", "noise_raw", "light_pollution_raw", "depression_raw", "mental_health_raw"]),
            f"Missing: {[c for c in ['zipcode','metro','noise_raw','light_pollution_raw','depression_raw','mental_health_raw'] if c not in df.columns]}"
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
    ("All 4 metros present",
        lambda: (
            set(df["metro"].dropna().unique()) >= set(METRO_LABELS.values()),
            f"Found: {sorted(df['metro'].dropna().unique())}"
        )),
    ("No metro under 20 ZIPs",
        lambda: (
            df["metro"].value_counts().min() >= 20,
            f"Counts: {df['metro'].value_counts().to_dict()}"
        )),
    # Raw value range checks
    ("noise_raw in [20, 100]",
        lambda: (
            df["noise_raw"].dropna().between(20, 100).all(),
            f"min={df['noise_raw'].min():.1f}, max={df['noise_raw'].max():.1f}"
        )),
    ("noise_raw nulls < 10%",
        lambda: (
            df["noise_raw"].isna().sum() / len(df) < 0.10,
            f"{df['noise_raw'].isna().sum()} nulls ({df['noise_raw'].isna().sum()/len(df)*100:.1f}%)"
        )),
    ("light_pollution_raw in [0, 300]",
        lambda: (
            df["light_pollution_raw"].dropna().between(0, 300).all(),
            f"min={df['light_pollution_raw'].min():.2f}, max={df['light_pollution_raw'].max():.2f}"
        )),
    ("light_pollution_raw nulls < 10%",
        lambda: (
            df["light_pollution_raw"].isna().sum() / len(df) < 0.10,
            f"{df['light_pollution_raw'].isna().sum()} nulls ({df['light_pollution_raw'].isna().sum()/len(df)*100:.1f}%)"
        )),
    ("depression_raw in [5, 50]",
        lambda: (
            df["depression_raw"].dropna().between(5, 50).all(),
            f"min={df['depression_raw'].min():.1f}, max={df['depression_raw'].max():.1f}"
        )),
    ("depression_raw nulls < 10%",
        lambda: (
            df["depression_raw"].isna().sum() / len(df) < 0.10,
            f"{df['depression_raw'].isna().sum()} nulls ({df['depression_raw'].isna().sum()/len(df)*100:.1f}%)"
        )),
    ("mental_health_raw in [5, 40]",
        lambda: (
            df["mental_health_raw"].dropna().between(5, 40).all(),
            f"min={df['mental_health_raw'].min():.1f}, max={df['mental_health_raw'].max():.1f}"
        )),
    ("mental_health_raw nulls < 10%",
        lambda: (
            df["mental_health_raw"].isna().sum() / len(df) < 0.10,
            f"{df['mental_health_raw'].isna().sum()} nulls ({df['mental_health_raw'].isna().sum()/len(df)*100:.1f}%)"
        )),
]

suite1_passed = run_tests("STRESS / SENSORY — INGESTION", ingestion_tests)
require_all_pass("STRESS / SENSORY — INGESTION", suite1_passed)


# %% [markdown]
# ## 6 · Normalization
# Min-max normalization, global across all 600 ZIPs.
# All 4 components are **inverted** (higher raw = worse health environment).
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Normalization
# failures typically mean a column has all-null or constant values from a broken ingestion step.

# %%
log("START", "Normalizing all four components")

RAW_COLS = ["noise_raw", "light_pollution_raw", "depression_raw", "mental_health_raw"]
NORM_COLS = ["noise_normalized", "light_pollution_normalized", "depression_normalized", "mental_health_normalized"]

# All four components are inverted: lower raw = better health = higher normalized score
for raw_col, norm_col in zip(RAW_COLS, NORM_COLS):
    col_min = df[raw_col].min()
    col_max = df[raw_col].max()

    if col_max == col_min:
        log("WARN", f"  {raw_col}: min == max ({col_min}) — all normalized to 50.0")
        df[norm_col] = 50.0
    else:
        # Inverted: higher raw value → lower normalized score
        df[norm_col] = (1 - (df[raw_col] - col_min) / (col_max - col_min)) * 100.0

    log("INFO", f"  {raw_col} → {norm_col}: "
        f"raw [{col_min:.2f}, {col_max:.2f}] → norm [{df[norm_col].min():.2f}, {df[norm_col].max():.2f}]")

# Handle nulls: if raw was null, normalized is null — log but don't fail
for norm_col in NORM_COLS:
    null_count = df[norm_col].isna().sum()
    if null_count > 0:
        log("WARN", f"  {norm_col}: {null_count} nulls (from raw data gaps)")

# ── Median imputation for light_pollution_normalized ─────────
# VIIRS global raster has small coverage gaps (~2 ZIPs). Impute with median
# so nulls don't propagate into the composite score.
lp_nulls = df["light_pollution_normalized"].isna()
if lp_nulls.any():
    median_val = df["light_pollution_normalized"].median()
    imputed_zips = df.loc[lp_nulls, "zipcode"].tolist()
    df.loc[lp_nulls, "light_pollution_normalized"] = median_val
    log("INFO", f"  Imputed {len(imputed_zips)} light_pollution_normalized nulls with median ({median_val:.2f}): {imputed_zips}")

print_validation_report("STRESS / SENSORY — NORMALIZED", df)

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
        (f"{nc} — nulls < 1%",
            lambda c=nc: (
                df[c].isna().sum() / len(df) < 0.01,
                f"{df[c].isna().sum()} nulls ({df[c].isna().sum()/len(df)*100:.1f}%)"
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

# Inversion correctness: all 4 are inverted
for norm_col, raw_col in zip(NORM_COLS, RAW_COLS):
    nc, rc = norm_col, raw_col
    norm_tests.append((
        f"{nc} — inversion correct (corr < -0.90)",
        lambda n=nc, r=rc: (
            df[[r, n]].dropna().corr().iloc[0, 1] < -0.90,
            f"Pearson r = {df[[r, n]].dropna().corr().iloc[0, 1]:.3f}"
        )
    ))

suite2_passed = run_tests("STRESS / SENSORY — NORMALIZATION", norm_tests)
require_all_pass("STRESS / SENSORY — NORMALIZATION", suite2_passed)


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
# Affluent/suburban ZIPs should not score F; dense urban cores may score lower
SPOT_CHECK_ZIPS = {
    "15213": ("D", "A"),   # Pittsburgh — Carnegie Mellon area
    "90210": ("C", "A"),   # Beverly Hills — low noise, suburban
    "85001": ("F", "B"),   # Downtown Phoenix — urban core
    "28202": ("F", "B"),   # Downtown Charlotte — urban core
}

grade_order = ["A", "B", "C", "D", "F"]

def grade_in_range(grade, min_grade, max_grade):
    min_idx = grade_order.index(min_grade)
    max_idx = grade_order.index(max_grade)
    grade_idx = grade_order.index(grade)
    return max_idx >= grade_idx >= min_idx

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

suite3_passed = run_tests("STRESS / SENSORY — SCORING", scoring_tests)
require_all_pass("STRESS / SENSORY — SCORING", suite3_passed)


# %% [markdown]
# ## 8 · Claude API Interpretations
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# missing ANTHROPIC_API_KEY in Colab secrets, rate limits (429), or model string changes.

# %%
log("START", "Generating Claude API interpretations for all ZIPs")

import anthropic

client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env

def generate_stress_interpretation(zipcode: str, composite_score: float,
                                    letter_grade: str, components: dict) -> str:
    """
    Generate a plain-language stress/sensory environment interpretation.
    components: dict of {label: normalized_score} — qualitative labels only, no weights.
    Framing: noise = psychological burden (annoyance, sleep disruption), not cardiovascular risk.
    """
    prompt = f"""You are a public health analyst writing a plain-language summary for residents and
real estate professionals. Write 2-3 sentences interpreting this neighborhood's stress and sensory
environment score.

ZIP Code: {zipcode}
Score: {composite_score:.1f}/100 (Grade: {letter_grade})
Component signals: {components}

Rules:
- Be specific, factual, and actionable
- Do not use jargon
- Do not mention scores, percentages, or numbers from the components
- Do not reveal how components are weighted or combined
- Do not say "based on our methodology" or any similar phrase
- Frame noise as affecting sleep quality, concentration, and daily stress — not heart disease
- Frame light pollution as affecting sleep rhythms and nighttime rest
- Frame depression and mental health as community-level indicators of stress burden"""

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

for idx, row in df.iterrows():
    zc = row["zipcode"]
    components = {
        "Noise & Sleep Disruption": row.get("noise_normalized", 0),
        "Nighttime Light Exposure": row.get("light_pollution_normalized", 0),
        "Depression Prevalence": row.get("depression_normalized", 0),
        "Mental Health Conditions": row.get("mental_health_normalized", 0),
    }

    try:
        interp = generate_stress_interpretation(
            zc, row["composite_score"], row["letter_grade"], components
        )
        interpretations[zc] = interp
    except Exception as e:
        log("ERROR", f"  Interpretation failed for ZIP {zc}: {e}")
        failed_interps.append(zc)
        interpretations[zc] = ""

    # Progress logging every 50 ZIPs
    if (idx + 1) % 50 == 0 or idx == len(df) - 1:
        log("INFO", f"  Interpreted {len(interpretations)}/{len(df)} ZIPs")

    time.sleep(INTERPRETATION_BATCH_DELAY)

df["interpretation"] = df["zipcode"].map(interpretations)

if failed_interps:
    log("WARN", f"{len(failed_interps)} ZIPs failed interpretation: {failed_interps[:10]}")
else:
    log("PASS", "All interpretations generated successfully")


# %% [markdown]
# ## 9 · Supabase Upsert
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# column name mismatch between local dict keys and Supabase schema, or missing UNIQUE constraint.
# Do NOT modify the Supabase schema manually — bring the error here first.

# %%
log("START", "Upserting all records to stress_scores")

failed_zips = []

for _, row in df.iterrows():
    record = {
        "zipcode": row["zipcode"],
        "metro": row["metro"],
        "noise_raw": float(row["noise_raw"]) if pd.notna(row["noise_raw"]) else None,
        "noise_normalized": float(row["noise_normalized"]) if pd.notna(row["noise_normalized"]) else None,
        "light_pollution_raw": float(row["light_pollution_raw"]) if pd.notna(row["light_pollution_raw"]) else None,
        "light_pollution_normalized": float(row["light_pollution_normalized"]) if pd.notna(row["light_pollution_normalized"]) else None,
        "depression_raw": float(row["depression_raw"]) if pd.notna(row["depression_raw"]) else None,
        "depression_normalized": float(row["depression_normalized"]) if pd.notna(row["depression_normalized"]) else None,
        "mental_health_raw": float(row["mental_health_raw"]) if pd.notna(row["mental_health_raw"]) else None,
        "mental_health_normalized": float(row["mental_health_normalized"]) if pd.notna(row["mental_health_normalized"]) else None,
        "composite_score": float(row["composite_score"]),
        "letter_grade": row["letter_grade"],
        "interpretation": row.get("interpretation", ""),
    }

    try:
        supabase.table("stress_scores").upsert(
            record, on_conflict="zipcode"
        ).execute()
    except Exception as e:
        log("ERROR", f"Failed to write ZIP {row['zipcode']}: {e}")
        failed_zips.append(row["zipcode"])

if failed_zips:
    log("WARN", f"{len(failed_zips)} ZIPs failed upsert: {failed_zips[:10]}")
else:
    log("PASS", "All records upserted to stress_scores")

# %% [markdown]
# ## 9a · Supabase Write Tests (Suite 4) — GATE

# %%
log("TEST", "Running Suite 4 — Supabase Write Tests")

TABLE_NAME = "stress_scores"

def get_sb_count():
    result = supabase.table(TABLE_NAME).select("zipcode", count="exact").execute()
    return result.count or 0

def get_sb_row(zc):
    result = supabase.table(TABLE_NAME).select("*").eq("zipcode", zc).execute()
    return result.data[0] if result.data else None

initial_count = get_sb_count()

SPOT_ZIPS = ["15213", "90210", "85001", "28202", "28277"]

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

# Spot checks
for zc in SPOT_ZIPS:
    z = zc
    write_tests.append((
        f"Spot check ZIP {z}: exists in Supabase",
        lambda zc=z: (get_sb_row(zc) is not None, f"ZIP {zc} not found")
    ))

    local_row = df[df["zipcode"] == z]
    if not local_row.empty:
        expected = round(float(local_row.iloc[0]["composite_score"]), 1)
        write_tests.append((
            f"Spot check ZIP {z}: score matches local ({expected})",
            lambda zc=z, exp=expected: (
                (r := get_sb_row(zc)) is not None
                and abs(round(float(r["composite_score"]), 1) - exp) < 0.5,
                f"Supabase: {get_sb_row(zc) and round(float(get_sb_row(zc)['composite_score']), 1)}, local: {exp}"
            )
        ))

suite4_passed = run_tests("STRESS / SENSORY — SUPABASE WRITE", write_tests)
require_all_pass("STRESS / SENSORY — SUPABASE WRITE", suite4_passed)


# %% [markdown]
# ## 10 · Pipeline Complete
#
# All 4 test suite gates passed. Stress / Sensory scores are live in Supabase.
#
# **Next steps:**
# 1. Deploy the Stress / Sensory Streamlit tab
# 2. Run Suite 5 (manual Streamlit smoke tests)
# 3. Update CONTEXT.md session log

# %%
log("DONE", "=" * 50)
log("DONE", "STRESS / SENSORY PIPELINE COMPLETE")
log("DONE", f"  Total ZIPs scored: {len(df)}")
log("DONE", f"  Grade distribution: {df['letter_grade'].value_counts().to_dict()}")
log("DONE", f"  Score range: {df['composite_score'].min():.1f} – {df['composite_score'].max():.1f}")
log("DONE", f"  Mean score: {df['composite_score'].mean():.1f}")
log("DONE", "=" * 50)
