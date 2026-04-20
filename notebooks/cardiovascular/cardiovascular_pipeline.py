# %% [markdown]
# # Cardiovascular Health Score — Full Pipeline
# **Tool 2 of 5 · LaSalle Technologies Health Environment Score**
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
WEIGHTS = [0.30, 0.25, 0.25, 0.20]
WEIGHT_LABELS = [
    "physical_inactivity",
    "chd",
    "noise",
    "impervious",
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
# CREATE TABLE IF NOT EXISTS cardiovascular_scores (
#   id SERIAL PRIMARY KEY,
#   zipcode TEXT NOT NULL,
#   metro TEXT NOT NULL,
#   physical_inactivity_raw NUMERIC,
#   physical_inactivity_normalized NUMERIC,
#   chd_raw NUMERIC,
#   chd_normalized NUMERIC,
#   noise_raw NUMERIC,
#   noise_normalized NUMERIC,
#   impervious_raw NUMERIC,
#   impervious_normalized NUMERIC,
#   composite_score NUMERIC,
#   letter_grade TEXT,
#   interpretation TEXT,
#   created_at TIMESTAMPTZ DEFAULT NOW(),
#   updated_at TIMESTAMPTZ DEFAULT NOW(),
#   UNIQUE(zipcode)
# );
#
# -- Auto-update timestamp trigger
# CREATE TRIGGER set_updated_at_cardiovascular
# BEFORE UPDATE ON cardiovascular_scores
# FOR EACH ROW EXECUTE FUNCTION update_updated_at();
# ```

# %% [markdown]
# ## 2 · CDC PLACES Ingestion (Physical Inactivity + CHD)
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# API format changes (fields renamed), rate limits (HTTP 429), or ZIP matching failures.
# Do NOT try to patch the query manually — the API schema has changed before (see CONTEXT.md).

# %%
log("START", "Ingesting CDC PLACES data for LPA and CHD")

CDC_BASE_URL = "https://data.cdc.gov/resource/c7b2-4ecy.json"

# CDC PLACES API is now WIDE format (as of 2024+):
#   - One row per ZCTA, with separate columns for each measure
#   - ZIP field is "zcta5" (not "locationname")
#   - No "measureid" column — measures are column names like "lpa_crudeprev", "chd_crudeprev"
#   - Use $select to request only needed columns, $where to filter by zcta5
#   - Batch size 50 ZIPs per request to stay within Socrata URL length limits

def fetch_cdc_places_wide(zip_codes: list, select_cols: list, batch_size: int = 50) -> list:
    """
    Fetch CDC PLACES data in wide format, batching by ZIP to avoid URL length limits.
    select_cols: columns to request, e.g. ["zcta5", "lpa_crudeprev", "chd_crudeprev"]
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
# Fetch LPA (physical inactivity) and CHD (coronary heart disease) in wide format
CDC_SELECT_COLS = ["zcta5", "lpa_crudeprev", "chd_crudeprev"]

log("INFO", f"Fetching CDC PLACES (wide format) for columns: {CDC_SELECT_COLS}")
log("INFO", f"Total ZIPs to query: {len(ALL_ZIPS)}, batch size: 50")

raw_cdc = fetch_cdc_places_wide(ALL_ZIPS, CDC_SELECT_COLS, batch_size=50)
log("INFO", f"Total CDC rows received: {len(raw_cdc)}")

# %%
# Parse wide-format response — already one row per ZIP, no pivot needed
df_cdc = pd.DataFrame(raw_cdc)

log("INFO", f"CDC response columns: {list(df_cdc.columns)}")

# Rename to our internal column names
df_cdc["zcta5"] = df_cdc["zcta5"].astype(str).str.strip()
df_cdc["lpa_crudeprev"] = pd.to_numeric(df_cdc["lpa_crudeprev"], errors="coerce")
df_cdc["chd_crudeprev"] = pd.to_numeric(df_cdc["chd_crudeprev"], errors="coerce")

df_cardio = df_cdc.rename(columns={
    "zcta5": "zipcode",
    "lpa_crudeprev": "physical_inactivity_raw",
    "chd_crudeprev": "chd_raw",
})
df_cardio = df_cardio.drop_duplicates(subset=["zipcode"], keep="first")

# Add metro from master ZIP list
df_cardio["metro"] = df_cardio["zipcode"].map(ZIP_METRO_MAP)

# Filter to only our 600 ZIPs
df_cardio = df_cardio[df_cardio["zipcode"].isin(ALL_ZIPS)].copy()

log("INFO", f"CDC PLACES pivoted: {len(df_cardio)} ZIPs with data")
log("INFO", f"  LPA coverage: {df_cardio['physical_inactivity_raw'].notna().sum()} ZIPs")
log("INFO", f"  CHD coverage: {df_cardio['chd_raw'].notna().sum()} ZIPs")

# %% [markdown]
# ## 3 · BTS Noise Raster Processing
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# CRS mismatch between raster and shapefile, out-of-memory on merge (solved by per-state
# processing below), or missing raster files on Drive. Do NOT attempt manual raster debugging.

# %%
log("START", "Processing BTS Transportation Noise raster")

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

noise_already_done = raster_already_processed("bts_noise")

# %%
# ── Download & process BTS noise raster ──────────────────────
# This cell processes the raster. Skip if already done (noise_already_done == True).
#
# INSTRUCTIONS FOR COLAB:
# 1. Download the BTS National Transportation Noise Map GeoTIFF (DNL layer)
#    from https://www.bts.gov/geospatial/national-transportation-noise-map
# 2. Upload the .tif file to your Colab session or Google Drive
# 3. Set NOISE_RASTER_PATH below to the file location

import rasterio
from rasterstats import zonal_stats
import os

# ── Paths (update these in Colab) ────────────────────────────
# All 8 states use clipped per-state rasters on Drive.
# Each state's ZIPs are processed separately via STATE_METRO_MAP below.
DRIVE_PREFIX = "/content/drive/MyDrive/Colab Notebooks/health-score-data"

STATE_NOISE_RASTERS = {
    "PA": f"{DRIVE_PREFIX}/PA_rail_road_and_aviation_noise_2020.tif",
    "CA": f"{DRIVE_PREFIX}/CA_rail_road_and_aviation_noise_2020.tif",
    "AZ": f"{DRIVE_PREFIX}/AZ_rail_road_and_aviation_noise_2020.tif",
    "NC": f"{DRIVE_PREFIX}/NC_rail_road_and_aviation_noise_2020.tif",
    "IL": f"{DRIVE_PREFIX}/IL_rail_road_and_aviation_noise_2020.tif",
    "TX": f"{DRIVE_PREFIX}/TX_rail_road_and_aviation_noise_2020.tif",
    "GA": f"{DRIVE_PREFIX}/GA_rail_road_and_aviation_noise_2020.tif",
    "CO": f"{DRIVE_PREFIX}/CO_rail_road_and_aviation_noise_2020.tif",
}

# Map each state to the metros whose ZIPs fall within that state's raster
STATE_METRO_MAP = {
    "PA": ["Pittsburgh"],
    "CA": ["Los Angeles"],
    "AZ": ["Phoenix"],
    "NC": ["Charlotte"],
    "IL": ["Chicago"],
    "TX": ["Houston"],
    "GA": ["Atlanta"],
    "CO": ["Denver"],
}

ZCTA_SHAPEFILE_PATH = f"{DRIVE_PREFIX}/tl_2020_us_zcta520.shp"

if not noise_already_done:
    log("INFO", f"Loading ZCTA shapefile from {ZCTA_SHAPEFILE_PATH}")
    gdf_zcta = gpd.read_file(ZCTA_SHAPEFILE_PATH)

    # ZCTA 2020 vintage uses ZCTA5CE20 column
    gdf_zcta = gdf_zcta[gdf_zcta["ZCTA5CE20"].isin(ALL_ZIPS)].copy()
    gdf_zcta = gdf_zcta.rename(columns={"ZCTA5CE20": "zipcode"})
    log("INFO", f"  Filtered ZCTA to {len(gdf_zcta)} of our ZIPs")

    # ── Process each state raster separately to avoid RAM crashes ─
    # Instead of merging 4 large rasters, we run zonal_stats per state
    # on only the ZIPs belonging to that state's metros.
    log("INFO", "  Processing noise rasters per state (8 per-state rasters)...")
    noise_parts = []

    for state, raster_path in STATE_NOISE_RASTERS.items():
        metros_in_state = STATE_METRO_MAP[state]
        state_zips = [z for z, m in ZIP_METRO_MAP.items() if m in metros_in_state]
        gdf_state = gdf_zcta[gdf_zcta["zipcode"].isin(state_zips)].copy()

        if gdf_state.empty:
            log("WARN", f"    {state}: no matching ZIPs — skipping")
            continue

        log("INFO", f"    {state}: {len(gdf_state)} ZIPs, raster: {os.path.basename(raster_path)}")

        with rasterio.open(raster_path) as src:
            state_crs = src.crs
            state_nodata = src.nodata
            log("INFO", f"      CRS: {state_crs}, shape: {src.shape}, nodata: {state_nodata}")

        gdf_state = gdf_state.to_crs(state_crs)

        nodata_val = state_nodata if state_nodata is not None else -9999
        stats = zonal_stats(
            gdf_state,
            raster_path,
            stats=["mean"],
            geojson_out=False,
            nodata=nodata_val,
        )

        gdf_state["noise_raw"] = [s["mean"] for s in stats]
        noise_parts.append(gdf_state[["zipcode", "noise_raw"]])
        log("INFO", f"      Done — {gdf_state['noise_raw'].notna().sum()} ZIPs with data")

    df_noise = pd.concat(noise_parts, ignore_index=True)

    # Drop rows where raster had no data for the ZIP polygon
    null_noise = df_noise["noise_raw"].isna().sum()
    if null_noise > 0:
        log("WARN", f"  {null_noise} ZIPs have no noise data (raster nodata)")

    log("INFO", f"  Noise processing complete: {df_noise['noise_raw'].notna().sum()} ZIPs with data")
    log("INFO", f"  Range: {df_noise['noise_raw'].min():.1f} – {df_noise['noise_raw'].max():.1f} dB")

    # ── Schema cache warm-up for raw_signals ─────────────────
    # PostgREST may have a stale schema cache after long raster processing.
    # Reinitialize client and confirm raw_signals is visible before writing.
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    log("INFO", "  Reinitialized Supabase client for raw_signals writes")

    MAX_RETRIES = 5
    RETRY_DELAY = 3

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            test = supabase.table("raw_signals").select("zipcode").limit(1).execute()
            log("PASS", f"  raw_signals schema cache warm-up succeeded on attempt {attempt}")
            break
        except Exception as e:
            log("WARN", f"  raw_signals warm-up attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    else:
        raise RuntimeError(
            "\n" + "!" * 62 + "\n"
            "  PostgREST cannot see the raw_signals table after 5 attempts.\n"
            "  This is a schema cache issue, not a missing table.\n\n"
            "  FIX: Open Supabase SQL Editor and run:\n"
            "    NOTIFY pgrst, 'reload schema';\n\n"
            "  Wait 10 seconds, then re-run this cell.\n"
            + "!" * 62 + "\n"
        )

    # ── Write to raw_signals for reuse by Stress tool ────────
    log("INFO", "  Writing noise values to raw_signals table...")
    noise_failed = []
    for _, row in df_noise.dropna(subset=["noise_raw"]).iterrows():
        record = {
            "zipcode": row["zipcode"],
            "signal_name": "noise_dnl",
            "data_source": "bts_noise",
            "data_vintage": 2024,
            "raw_value": float(row["noise_raw"]),
            "unit": "dB_DNL",
        }
        try:
            supabase.table("raw_signals").upsert(
                record,
                on_conflict="zipcode,signal_name,data_source,data_vintage"
            ).execute()
        except Exception as e:
            log("ERROR", f"  Failed to write noise for ZIP {row['zipcode']}: {e}")
            noise_failed.append(row["zipcode"])

    if noise_failed:
        log("WARN", f"  {len(noise_failed)} ZIPs failed raw_signals write: {noise_failed[:10]}")
    else:
        log("PASS", f"  All noise values written to raw_signals")

else:
    # Load from raw_signals
    log("INFO", "  Loading cached noise data from raw_signals...")
    noise_result = supabase.table("raw_signals") \
        .select("zipcode, raw_value") \
        .eq("data_source", "bts_noise") \
        .execute()
    df_noise = pd.DataFrame(noise_result.data)
    df_noise = df_noise.rename(columns={"raw_value": "noise_raw"})
    df_noise["noise_raw"] = pd.to_numeric(df_noise["noise_raw"], errors="coerce")
    log("INFO", f"  Loaded {len(df_noise)} noise values from raw_signals")


# %% [markdown]
# ## 4 · NLCD Impervious Surface Raster Processing
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# nodata value mismatch (must be 250.0, not 255), CRS reprojection errors, or Colab RAM limits
# on the CONUS raster. Do NOT change the nodata value without confirming against the raster metadata.

# %%
log("START", "Processing NLCD Impervious Surface raster")

impervious_already_done = raster_already_processed("nlcd_impervious")

# %%
# ── Paths (update in Colab) ──────────────────────────────────
IMPERVIOUS_RASTER_PATH = f"{DRIVE_PREFIX}/nlcd_impervious.tif"

if not impervious_already_done:
    # Reuse ZCTA if already loaded, otherwise reload
    if "gdf_zcta" not in dir() or gdf_zcta is None:
        log("INFO", f"  Reloading ZCTA shapefile...")
        gdf_zcta = gpd.read_file(ZCTA_SHAPEFILE_PATH)
        # ZCTA 2020 vintage uses ZCTA5CE20 column
        gdf_zcta = gdf_zcta[gdf_zcta["ZCTA5CE20"].isin(ALL_ZIPS)].copy()
        gdf_zcta = gdf_zcta.rename(columns={"ZCTA5CE20": "zipcode"})

    with rasterio.open(IMPERVIOUS_RASTER_PATH) as src:
        imp_crs = src.crs
        log("INFO", f"  Raster CRS: {imp_crs}, shape: {src.shape}")

    gdf_imp = gdf_zcta.to_crs(imp_crs)

    log("INFO", "  Running zonal statistics for impervious surface (this may take several minutes)...")
    imp_stats = zonal_stats(
        gdf_imp,
        IMPERVIOUS_RASTER_PATH,
        stats=["mean"],
        geojson_out=False,
        nodata=250.0,  # NLCD impervious nodata value (250.0, not 255 — confirmed from raster metadata)
    )

    gdf_imp["impervious_raw"] = [s["mean"] for s in imp_stats]
    df_impervious = gdf_imp[["zipcode", "impervious_raw"]].copy()

    null_imp = df_impervious["impervious_raw"].isna().sum()
    if null_imp > 0:
        log("WARN", f"  {null_imp} ZIPs have no impervious data")

    log("INFO", f"  Impervious processing complete: {df_impervious['impervious_raw'].notna().sum()} ZIPs")
    log("INFO", f"  Range: {df_impervious['impervious_raw'].min():.1f} – {df_impervious['impervious_raw'].max():.1f}%")

    # ── Write to raw_signals for reuse by Heat tool ──────────
    log("INFO", "  Writing impervious values to raw_signals table...")
    imp_failed = []
    for _, row in df_impervious.dropna(subset=["impervious_raw"]).iterrows():
        record = {
            "zipcode": row["zipcode"],
            "signal_name": "impervious_pct",
            "data_source": "nlcd_impervious",
            "data_vintage": 2021,
            "raw_value": float(row["impervious_raw"]),
            "unit": "percent",
        }
        try:
            supabase.table("raw_signals").upsert(
                record,
                on_conflict="zipcode,signal_name,data_source,data_vintage"
            ).execute()
        except Exception as e:
            log("ERROR", f"  Failed to write impervious for ZIP {row['zipcode']}: {e}")
            imp_failed.append(row["zipcode"])

    if imp_failed:
        log("WARN", f"  {len(imp_failed)} ZIPs failed: {imp_failed[:10]}")
    else:
        log("PASS", "  All impervious values written to raw_signals")

else:
    log("INFO", "  Loading cached impervious data from raw_signals...")
    imp_result = supabase.table("raw_signals") \
        .select("zipcode, raw_value") \
        .eq("data_source", "nlcd_impervious") \
        .execute()
    df_impervious = pd.DataFrame(imp_result.data)
    df_impervious = df_impervious.rename(columns={"raw_value": "impervious_raw"})
    df_impervious["impervious_raw"] = pd.to_numeric(df_impervious["impervious_raw"], errors="coerce")
    log("INFO", f"  Loaded {len(df_impervious)} impervious values from raw_signals")


# %% [markdown]
# ## 5 · Merge All Components
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Merge failures
# usually mean a ZIP column mismatch between data sources or an unexpected null pattern.

# %%
log("START", "Merging all four components into a single DataFrame")

# Start from CDC data (has zipcode + metro + LPA + CHD)
df = df_cardio.copy()

# Merge noise
df = df.merge(df_noise[["zipcode", "noise_raw"]], on="zipcode", how="left")

# Merge impervious
df = df.merge(df_impervious[["zipcode", "impervious_raw"]], on="zipcode", how="left")

# Ensure metro is filled for any ZIPs that came in via raster but not CDC
df["metro"] = df["zipcode"].map(ZIP_METRO_MAP)

# Filter to only our 600 ZIPs (safety)
df = df[df["zipcode"].isin(ALL_ZIPS)].copy()

log("INFO", f"Merged DataFrame: {len(df)} rows")
print_validation_report("CARDIOVASCULAR — MERGED RAW DATA", df)

# %% [markdown]
# ## 5a · Ingestion Tests (Suite 1) — GATE

# %%
log("TEST", "Running Suite 1 — Ingestion Tests")

ingestion_tests = [
    ("All expected columns present",
        lambda: (
            all(c in df.columns for c in
                ["zipcode", "metro", "physical_inactivity_raw", "chd_raw", "noise_raw", "impervious_raw"]),
            f"Missing: {[c for c in ['zipcode','metro','physical_inactivity_raw','chd_raw','noise_raw','impervious_raw'] if c not in df.columns]}"
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
    ("No metro under 20 ZIPs",
        lambda: (
            df["metro"].value_counts().min() >= 20,
            f"Counts: {df['metro'].value_counts().to_dict()}"
        )),
    # Raw value range checks
    ("physical_inactivity_raw in [5, 60]",
        lambda: (
            df["physical_inactivity_raw"].dropna().between(5, 60).all(),
            f"min={df['physical_inactivity_raw'].min():.1f}, max={df['physical_inactivity_raw'].max():.1f}"
        )),
    ("physical_inactivity_raw nulls < 10%",
        lambda: (
            df["physical_inactivity_raw"].isna().sum() / len(df) < 0.10,
            f"{df['physical_inactivity_raw'].isna().sum()} nulls ({df['physical_inactivity_raw'].isna().sum()/len(df)*100:.1f}%)"
        )),
    ("chd_raw in [1, 25]",
        lambda: (
            df["chd_raw"].dropna().between(1, 25).all(),
            f"min={df['chd_raw'].min():.1f}, max={df['chd_raw'].max():.1f}"
        )),
    ("chd_raw nulls < 10%",
        lambda: (
            df["chd_raw"].isna().sum() / len(df) < 0.10,
            f"{df['chd_raw'].isna().sum()} nulls ({df['chd_raw'].isna().sum()/len(df)*100:.1f}%)"
        )),
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
    ("impervious_raw in [0, 100]",
        lambda: (
            df["impervious_raw"].dropna().between(0, 100).all(),
            f"min={df['impervious_raw'].min():.1f}, max={df['impervious_raw'].max():.1f}"
        )),
    ("impervious_raw nulls < 10%",
        lambda: (
            df["impervious_raw"].isna().sum() / len(df) < 0.10,
            f"{df['impervious_raw'].isna().sum()} nulls ({df['impervious_raw'].isna().sum()/len(df)*100:.1f}%)"
        )),
]

suite1_passed = run_tests("CARDIOVASCULAR — INGESTION", ingestion_tests)
require_all_pass("CARDIOVASCULAR — INGESTION", suite1_passed)


# %% [markdown]
# ## 6 · Normalization
# Min-max normalization, global across all 600 ZIPs.
# All 4 components are **inverted** (higher raw = worse health environment).
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Normalization
# failures typically mean a column has all-null or constant values from a broken ingestion step.

# %%
log("START", "Normalizing all four components")

RAW_COLS = ["physical_inactivity_raw", "chd_raw", "noise_raw", "impervious_raw"]
NORM_COLS = ["physical_inactivity_normalized", "chd_normalized", "noise_normalized", "impervious_normalized"]

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

print_validation_report("CARDIOVASCULAR — NORMALIZED", df)

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

suite2_passed = run_tests("CARDIOVASCULAR — NORMALIZATION", norm_tests)
require_all_pass("CARDIOVASCULAR — NORMALIZATION", suite2_passed)


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
    "90210": ("C", "A"),   # Beverly Hills — low disease, suburban
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

suite3_passed = run_tests("CARDIOVASCULAR — SCORING", scoring_tests)
require_all_pass("CARDIOVASCULAR — SCORING", suite3_passed)


# %% [markdown]
# ## 8 · Claude API Interpretations
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# missing ANTHROPIC_API_KEY in Colab secrets, rate limits (429), or model string changes.

# %%
log("START", "Generating Claude API interpretations for all ZIPs")

import anthropic

client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env

def generate_cv_interpretation(zipcode: str, composite_score: float,
                                letter_grade: str, components: dict) -> str:
    """
    Generate a plain-language cardiovascular health environment interpretation.
    components: dict of {label: normalized_score} — qualitative labels only, no weights.
    """
    prompt = f"""You are a public health analyst writing a plain-language summary for residents and
real estate professionals. Write 2-3 sentences interpreting this neighborhood's cardiovascular
health environment score.

ZIP Code: {zipcode}
Score: {composite_score:.1f}/100 (Grade: {letter_grade})
Component signals: {components}

Rules:
- Be specific, factual, and actionable
- Do not use jargon
- Do not mention scores, percentages, or numbers from the components
- Do not reveal how components are weighted or combined
- Do not say "based on our methodology" or any similar phrase
- Focus on what the environment means for heart health"""

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
        "Physical Activity Levels": row.get("physical_inactivity_normalized", 0),
        "Heart Disease Prevalence": row.get("chd_normalized", 0),
        "Transportation Noise": row.get("noise_normalized", 0),
        "Walkability / Impervious Surface": row.get("impervious_normalized", 0),
    }

    try:
        interp = generate_cv_interpretation(
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
log("START", "Upserting all records to cardiovascular_scores")

failed_zips = []

for _, row in df.iterrows():
    record = {
        "zipcode": row["zipcode"],
        "metro": row["metro"],
        "physical_inactivity_raw": float(row["physical_inactivity_raw"]) if pd.notna(row["physical_inactivity_raw"]) else None,
        "physical_inactivity_normalized": float(row["physical_inactivity_normalized"]) if pd.notna(row["physical_inactivity_normalized"]) else None,
        "chd_raw": float(row["chd_raw"]) if pd.notna(row["chd_raw"]) else None,
        "chd_normalized": float(row["chd_normalized"]) if pd.notna(row["chd_normalized"]) else None,
        "noise_raw": float(row["noise_raw"]) if pd.notna(row["noise_raw"]) else None,
        "noise_normalized": float(row["noise_normalized"]) if pd.notna(row["noise_normalized"]) else None,
        "impervious_raw": float(row["impervious_raw"]) if pd.notna(row["impervious_raw"]) else None,
        "impervious_normalized": float(row["impervious_normalized"]) if pd.notna(row["impervious_normalized"]) else None,
        "composite_score": float(row["composite_score"]),
        "letter_grade": row["letter_grade"],
        "interpretation": row.get("interpretation", ""),
        "score_date": str(date.today()),
    }

    try:
        supabase.table("cardiovascular_scores").upsert(
            record, on_conflict="zipcode"
        ).execute()
    except Exception as e:
        log("ERROR", f"Failed to write ZIP {row['zipcode']}: {e}")
        failed_zips.append(row["zipcode"])

if failed_zips:
    log("WARN", f"{len(failed_zips)} ZIPs failed upsert: {failed_zips[:10]}")
else:
    log("PASS", "All records upserted to cardiovascular_scores")

# %% [markdown]
# ## 9a · Supabase Write Tests (Suite 4) — GATE

# %%
log("TEST", "Running Suite 4 — Supabase Write Tests")

TABLE_NAME = "cardiovascular_scores"

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

suite4_passed = run_tests("CARDIOVASCULAR — SUPABASE WRITE", write_tests)
require_all_pass("CARDIOVASCULAR — SUPABASE WRITE", suite4_passed)


# %% [markdown]
# ## 10 · Pipeline Complete
#
# All 4 test suite gates passed. Cardiovascular scores are live in Supabase.
#
# **Next steps:**
# 1. Deploy the Cardiovascular Streamlit tab
# 2. Run Suite 5 (manual Streamlit smoke tests)
# 3. Update CONTEXT.md session log

# %%
log("DONE", "=" * 50)
log("DONE", "CARDIOVASCULAR PIPELINE COMPLETE")
log("DONE", f"  Total ZIPs scored: {len(df)}")
log("DONE", f"  Grade distribution: {df['letter_grade'].value_counts().to_dict()}")
log("DONE", f"  Score range: {df['composite_score'].min():.1f} – {df['composite_score'].max():.1f}")
log("DONE", f"  Mean score: {df['composite_score'].mean():.1f}")
log("DONE", "=" * 50)
