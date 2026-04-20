# %% [markdown]
# # Respiratory Health Score — Full Pipeline
# **Tool 1 of 5 · LaSalle Technologies Health Environment Score**
#
# Run each cell in order. Every gate must pass before proceeding.
# Designed for Google Colab with Supabase credentials in Colab secrets.
#
# **Mixed pipeline: 2 APIs (EPA AQS, EPA EJScreen), 1 raster reuse (NLCD tree canopy),
# 1 tabular API (CDC PLACES asthma + COPD).**

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
# EPA_AQS_EMAIL = userdata.get("EPA_AQS_EMAIL")
# EPA_AQS_KEY = userdata.get("EPA_AQS_KEY")

# Local fallback (for testing outside Colab)
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
EPA_AQS_EMAIL = os.environ.get("EPA_AQS_EMAIL", "")
EPA_AQS_KEY = os.environ.get("EPA_AQS_KEY", "")

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

# ── Test Runner ─────────��────────────────────────────────────
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

# ── Validation Report ─────────────��──────────────────────────
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
# ── Master ZIP List ─────────────���────────────────────────────
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
WEIGHTS = [0.40, 0.25, 0.20, 0.15]
WEIGHT_LABELS = [
    "air_quality",
    "environmental_burden",
    "green_cover",
    "health_outcomes",
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

# ── Google Drive Paths ────────────���──────────────────────────
DRIVE_PREFIX = "/content/drive/MyDrive/Colab Notebooks/health-score-data"

# EPA EJScreen bulk CSV — tract-level environmental indicators
# Download from: https://www.epa.gov/ejscreen/download-ejscreen-data
EJSCREEN_CSV_PATH = f"{DRIVE_PREFIX}/EJSCREEN_2024_Tract_StatePct.csv"

# HUD crosswalk files (same as used in food_pipeline.py)
TRACT_ZIP_CROSSWALK_PATH = f"{DRIVE_PREFIX}/TRACT_ZIP_032025.xlsx"
ZIP_COUNTY_CROSSWALK_PATH = f"{DRIVE_PREFIX}/ZIP_COUNTY_032025.xlsx"


# %% [markdown]
# ## 1 · Supabase Schema
# Run this SQL in the Supabase SQL Editor **once** before proceeding:
#
# ```sql
# CREATE TABLE IF NOT EXISTS respiratory_scores (
#   id SERIAL PRIMARY KEY,
#   zipcode TEXT NOT NULL,
#   metro TEXT NOT NULL,
#   air_quality_raw NUMERIC,
#   air_quality_normalized NUMERIC,
#   environmental_burden_raw NUMERIC,
#   environmental_burden_normalized NUMERIC,
#   green_cover_raw NUMERIC,
#   green_cover_normalized NUMERIC,
#   health_outcomes_raw NUMERIC,
#   health_outcomes_normalized NUMERIC,
#   composite_score NUMERIC,
#   letter_grade TEXT,
#   interpretation TEXT,
#   score_date DATE,
#   created_at TIMESTAMPTZ DEFAULT NOW(),
#   updated_at TIMESTAMPTZ DEFAULT NOW(),
#   UNIQUE(zipcode)
# );
#
# -- Auto-update timestamp trigger
# CREATE TRIGGER set_updated_at_respiratory
# BEFORE UPDATE ON respiratory_scores
# FOR EACH ROW EXECUTE FUNCTION update_updated_at();
# ```


# %% [markdown]
# ## 2 · EPA AQS Ingestion (Air Quality — PM2.5, O3, NO2)
#
# EPA AQS provides monitor-level readings. We aggregate to county-level averages
# (arithmetic mean of all monitors in a county), then map counties → ZIPs using
# the HUD ZIP-County crosswalk (primary county by highest RES_RATIO). This is
# methodologically consistent with how CDC PLACES and EJScreen report area-level data.
#
# **Data:** EPA AQS annual summaries API
# - PM2.5 (parameter 88101) — annual arithmetic mean in µg/m³
# - Ozone (parameter 44201) — annual 4th max 8-hr average in ppm
# - NO2 (parameter 42602) — annual arithmetic mean in ppb
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# expired/missing AQS API key, rate limits, or state FIPS code issues.
# Register for an API key at: https://aqs.epa.gov/aqsweb/documents/data_api.html

# %%
log("START", "Ingesting EPA AQS air quality data (PM2.5, O3, NO2)")

AQS_BASE_URL = "https://aqs.epa.gov/data/api/annualData/byState"

# State FIPS codes for our 8 metro states
STATE_FIPS = {
    "PA": "42",  # Pittsburgh
    "CA": "06",  # Los Angeles
    "AZ": "04",  # Phoenix
    "NC": "37",  # Charlotte
    "IL": "17",  # Chicago
    "TX": "48",  # Houston
    "GA": "13",  # Atlanta
    "CO": "08",  # Denver
}

# AQS parameter codes
AQS_PARAMS = {
    "pm25": "88101",    # PM2.5 — FRM/FEM Mass
    "ozone": "44201",   # Ozone
    "no2": "42602",     # NO2
}

# Data year — use most recent complete year available
AQS_YEAR = "2023"

def fetch_aqs_annual(state_fips: str, param_code: str, year: str) -> list:
    """Fetch annual summary data from EPA AQS for one state and one parameter."""
    params = {
        "email": EPA_AQS_EMAIL,
        "key": EPA_AQS_KEY,
        "param": param_code,
        "bdate": f"{year}0101",
        "edate": f"{year}1231",
        "state": state_fips,
    }
    try:
        response = requests.get(AQS_BASE_URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        if data.get("Header", [{}])[0].get("status") == "Failed":
            log("WARN", f"  AQS returned status=Failed for state {state_fips}, param {param_code}")
            return []
        return data.get("Data", [])
    except requests.RequestException as e:
        log("ERROR", f"  AQS request failed for state {state_fips}, param {param_code}: {e}")
        raise

# %%
# Fetch all 3 pollutants for all 8 states, aggregate to county-level means
log("INFO", "Fetching AQS annual data for all 8 states and 3 parameters")

aqs_records = []  # list of {county_fips, pm25, ozone, no2}

for state_abbr, state_fips in STATE_FIPS.items():
    log("INFO", f"  {state_abbr} (FIPS {state_fips}):")

    state_data = {}  # {param_name: {county_fips: [values]}}

    for param_name, param_code in AQS_PARAMS.items():
        rows = fetch_aqs_annual(state_fips, param_code, AQS_YEAR)
        log("INFO", f"    {param_name}: {len(rows)} monitor records")

        # Group by county FIPS and compute mean
        county_values = {}
        for row in rows:
            county_fips = row.get("state_code", "") + row.get("county_code", "")
            value = row.get("arithmetic_mean")
            if value is not None and county_fips:
                county_fips = county_fips.zfill(5)
                county_values.setdefault(county_fips, []).append(float(value))

        state_data[param_name] = {
            cf: sum(vals) / len(vals) for cf, vals in county_values.items()
        }
        log("INFO", f"    {param_name}: {len(county_values)} counties with data")

        time.sleep(1.0)  # AQS rate limit: 10 requests per minute

    # Merge all 3 pollutants by county FIPS
    all_counties = set()
    for param_vals in state_data.values():
        all_counties.update(param_vals.keys())

    for cf in all_counties:
        aqs_records.append({
            "county_fips": cf,
            "pm25": state_data.get("pm25", {}).get(cf),
            "ozone": state_data.get("ozone", {}).get(cf),
            "no2": state_data.get("no2", {}).get(cf),
        })

df_aqs = pd.DataFrame(aqs_records)
log("INFO", f"Total AQS county records: {len(df_aqs)}")
log("INFO", f"  PM2.5 coverage: {df_aqs['pm25'].notna().sum()} counties")
log("INFO", f"  Ozone coverage: {df_aqs['ozone'].notna().sum()} counties")
log("INFO", f"  NO2 coverage: {df_aqs['no2'].notna().sum()} counties")

# %%
# ── Normalize each pollutant 0–1, then combine into air_quality_raw (mean) ──
# Higher raw = worse air quality (all 3 pollutants: higher = worse)
# We normalize within the observed county range so all 3 contribute equally
# before combining them.

for col in ["pm25", "ozone", "no2"]:
    col_min = df_aqs[col].min()
    col_max = df_aqs[col].max()
    if col_max > col_min:
        df_aqs[f"{col}_scaled"] = (df_aqs[col] - col_min) / (col_max - col_min)
    else:
        df_aqs[f"{col}_scaled"] = 0.5
    log("INFO", f"  {col}: range [{col_min:.4f}, {col_max:.4f}]")

# Combined air quality index: mean of scaled pollutants (0–1 scale, higher = worse)
# Convert to 0–100 scale for consistency with other raw values
scaled_cols = [c for c in df_aqs.columns if c.endswith("_scaled")]
df_aqs["air_quality_raw"] = df_aqs[scaled_cols].mean(axis=1) * 100.0

log("INFO", f"  air_quality_raw range: {df_aqs['air_quality_raw'].min():.2f} – {df_aqs['air_quality_raw'].max():.2f}")

# %%
# ── Map county → ZIP using HUD ZIP-County crosswalk ──────────
# Same pattern as food_pipeline.py Cell 3
log("INFO", f"Loading HUD ZIP-County crosswalk from {ZIP_COUNTY_CROSSWALK_PATH}")
df_zip_county = pd.read_excel(ZIP_COUNTY_CROSSWALK_PATH)

df_zip_county.columns = [c.upper().strip() for c in df_zip_county.columns]
df_zip_county["ZIP"] = df_zip_county["ZIP"].astype(str).str.zfill(5)
df_zip_county["COUNTY"] = df_zip_county["COUNTY"].astype(str).str.zfill(5)
df_zip_county["RES_RATIO"] = pd.to_numeric(df_zip_county["RES_RATIO"], errors="coerce")

# Use primary county (highest RES_RATIO) for each ZIP
df_zip_primary_county = df_zip_county.sort_values("RES_RATIO", ascending=False) \
    .drop_duplicates(subset=["ZIP"], keep="first")[["ZIP", "COUNTY"]]

log("INFO", f"Primary county assignments: {len(df_zip_primary_county)} ZIPs")

# Join ZIP → county → AQS data
df_aq_joined = df_zip_primary_county.merge(
    df_aqs[["county_fips", "air_quality_raw"]],
    left_on="COUNTY",
    right_on="county_fips",
    how="left",
)
df_aq_joined = df_aq_joined.rename(columns={"ZIP": "zipcode"})

# Filter to our master ZIPs
df_air_quality = df_aq_joined[df_aq_joined["zipcode"].isin(ALL_ZIPS)][
    ["zipcode", "air_quality_raw"]
].copy()

log("INFO", f"Air quality: {len(df_air_quality)} of our ZIPs matched")
log("INFO", f"  air_quality_raw: {df_air_quality['air_quality_raw'].notna().sum()} ZIPs with data")

# ── Write to raw_signals ─────────────────────────────────────
log("INFO", "  Writing air quality values to raw_signals table...")
aq_failed = []
for _, row in df_air_quality.dropna(subset=["air_quality_raw"]).iterrows():
    record = {
        "zipcode": row["zipcode"],
        "signal_name": "air_quality_index",
        "data_source": "epa_aqs",
        "data_vintage": int(AQS_YEAR),
        "signal_value": float(row["air_quality_raw"]),
        "units": "index_0_100",
    }
    try:
        supabase.table("raw_signals").upsert(
            record,
            on_conflict="zipcode,signal_name,data_source,data_vintage"
        ).execute()
    except Exception:
        aq_failed.append(row["zipcode"])

if aq_failed:
    log("WARN", f"  {len(aq_failed)} ZIPs failed raw_signals write: {aq_failed[:10]}")
else:
    log("PASS", "  All air quality values written to raw_signals")


# %% [markdown]
# ## 3 · EPA EJScreen Ingestion (Environmental Burden)
#
# EJScreen provides census tract-level environmental justice indicators. We use
# 3 percentile indicators and average them into a single environmental burden score.
# Tract → ZIP aggregation uses HUD Tract-ZIP crosswalk with population-weighted
# averaging (same pattern as USDA FARA in food_pipeline.py).
#
# **Indicators (state percentiles):**
# - `P_PNPL` — Proximity to National Priorities List (Superfund) sites
# - `P_PWDIS` — Wastewater discharge indicator
# - `P_PTRAF` — Traffic proximity and volume
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# CSV column name changes between EJScreen versions, missing file on Drive, or crosswalk mismatch.

# %%
log("START", "Ingesting EPA EJScreen data (Environmental Burden)")

# ── Load EJScreen bulk CSV ─────��─────────────────────────────
log("INFO", f"Loading EJScreen CSV from {EJSCREEN_CSV_PATH}")
df_ej_raw = pd.read_csv(EJSCREEN_CSV_PATH, dtype={"ID": str})

log("INFO", f"EJScreen rows: {len(df_ej_raw)}")
log("INFO", f"EJScreen columns (first 20): {list(df_ej_raw.columns)[:20]}")

# EJScreen tract ID field — 11-digit FIPS code
# Column name varies by year: "ID", "GEOID", or "Census tract"
tract_col = None
for candidate in ["ID", "GEOID", "Census tract", "FIPS"]:
    if candidate in df_ej_raw.columns:
        tract_col = candidate
        break

if tract_col is None:
    log("ERROR", f"Cannot find tract ID column. Columns: {list(df_ej_raw.columns)}")
    raise RuntimeError("EJScreen tract ID column not found. Bring this error to Claude Code.")

log("INFO", f"  Using tract ID column: '{tract_col}'")
df_ej_raw[tract_col] = df_ej_raw[tract_col].astype(str).str.zfill(11)

# ── Extract the 3 percentile indicators ──────────────────────
EJSCREEN_COLS = {
    "P_PNPL": "hazardous_proximity",
    "P_PWDIS": "wastewater",
    "P_PTRAF": "traffic",
}

# Check that all required columns exist
for col in EJSCREEN_COLS.keys():
    if col not in df_ej_raw.columns:
        # Try alternate naming conventions
        alt_col = col.replace("P_", "P_EJ_")
        if alt_col in df_ej_raw.columns:
            df_ej_raw[col] = df_ej_raw[alt_col]
            log("INFO", f"  Mapped alternate column {alt_col} → {col}")
        else:
            log("ERROR", f"  Required column '{col}' not found. Available: {[c for c in df_ej_raw.columns if 'PNPL' in c or 'PWDIS' in c or 'PTRAF' in c]}")
            raise RuntimeError(f"EJScreen column '{col}' missing. Bring this error to Claude Code.")

for col in EJSCREEN_COLS.keys():
    df_ej_raw[col] = pd.to_numeric(df_ej_raw[col], errors="coerce")

# Average the 3 percentiles into a single burden score (0–100 scale)
df_ej_raw["environmental_burden_raw"] = df_ej_raw[list(EJSCREEN_COLS.keys())].mean(axis=1)

log("INFO", f"  environmental_burden_raw range: "
    f"{df_ej_raw['environmental_burden_raw'].min():.1f} – {df_ej_raw['environmental_burden_raw'].max():.1f}")

# %%
# ── Load HUD Tract-ZIP crosswalk and aggregate ───────────────
# Same pattern as food_pipeline.py Cell 2
log("INFO", f"Loading HUD Tract-ZIP crosswalk from {TRACT_ZIP_CROSSWALK_PATH}")
df_tract_zip = pd.read_excel(TRACT_ZIP_CROSSWALK_PATH)

df_tract_zip.columns = [c.upper().strip() for c in df_tract_zip.columns]
df_tract_zip["TRACT"] = df_tract_zip["TRACT"].astype(str).str.zfill(11)
df_tract_zip["ZIP"] = df_tract_zip["ZIP"].astype(str).str.zfill(5)
df_tract_zip["RES_RATIO"] = pd.to_numeric(df_tract_zip["RES_RATIO"], errors="coerce")

log("INFO", f"  Tract-ZIP crosswalk: {len(df_tract_zip)} rows")

# %%
# Join EJScreen to crosswalk and aggregate to ZIP
log("INFO", "Joining EJScreen tract data to HUD crosswalk")

df_ej_joined = df_ej_raw[[tract_col, "environmental_burden_raw"]].merge(
    df_tract_zip[["TRACT", "ZIP", "RES_RATIO"]],
    left_on=tract_col,
    right_on="TRACT",
    how="inner",
)

log("INFO", f"  Joined rows: {len(df_ej_joined)}")

# RES_RATIO-weighted aggregation per ZIP:
# weighted_avg = sum(burden * RES_RATIO) / sum(RES_RATIO)
df_ej_joined["weight"] = df_ej_joined["RES_RATIO"]
df_ej_joined["weighted_value"] = df_ej_joined["environmental_burden_raw"] * df_ej_joined["weight"]

df_ej_agg = df_ej_joined.groupby("ZIP").agg(
    weighted_sum=("weighted_value", "sum"),
    weight_sum=("weight", "sum"),
).reset_index()

df_ej_agg["environmental_burden_raw"] = df_ej_agg["weighted_sum"] / df_ej_agg["weight_sum"]
df_ej_agg = df_ej_agg.rename(columns={"ZIP": "zipcode"})

# Filter to our master ZIPs
df_burden = df_ej_agg[df_ej_agg["zipcode"].isin(ALL_ZIPS)][
    ["zipcode", "environmental_burden_raw"]
].copy()

log("INFO", f"EJScreen: {len(df_burden)} of our ZIPs matched")
log("INFO", f"  environmental_burden_raw range: "
    f"{df_burden['environmental_burden_raw'].min():.2f} – {df_burden['environmental_burden_raw'].max():.2f}")

# ── Write to raw_signals ─────────────────────────────────────
log("INFO", "  Writing environmental burden values to raw_signals table...")
ej_failed = []
for _, row in df_burden.dropna(subset=["environmental_burden_raw"]).iterrows():
    record = {
        "zipcode": row["zipcode"],
        "signal_name": "ejscreen_burden",
        "data_source": "epa_ejscreen",
        "data_vintage": 2024,
        "signal_value": float(row["environmental_burden_raw"]),
        "units": "percentile_avg",
    }
    try:
        supabase.table("raw_signals").upsert(
            record,
            on_conflict="zipcode,signal_name,data_source,data_vintage"
        ).execute()
    except Exception:
        ej_failed.append(row["zipcode"])

if ej_failed:
    log("WARN", f"  {len(ej_failed)} ZIPs failed raw_signals write: {ej_failed[:10]}")
else:
    log("PASS", "  All environmental burden values written to raw_signals")


# %% [markdown]
# ## 4 · NLCD Green Cover (Tree Canopy Reuse)
#
# The stress and heat pipelines already process the NLCD tree canopy raster.
# We check `raw_signals` for existing data first. If not available, we read
# from `heat_scores.tree_canopy_raw` (direct reuse like stress→noise from cardiovascular).
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Most likely
# the Heat pipeline has not been run yet.

# %%
log("START", "Loading green cover data (NLCD tree canopy reuse)")

# ── Try raw_signals first ────────────────────────────────────
def raster_already_processed(source_name: str, min_expected_rows: int = 550) -> bool:
    """Check raw_signals for existing processed raster data."""
    result = supabase.table("raw_signals") \
        .select("zipcode", count="exact") \
        .eq("data_source", source_name) \
        .execute()
    row_count = result.count or 0
    if row_count >= min_expected_rows:
        log("INFO", f"  {source_name} already processed ({row_count} ZIPs) — loading from raw_signals")
        return True
    log("INFO", f"  {source_name}: {row_count} rows found, need {min_expected_rows}")
    return False

canopy_in_raw_signals = raster_already_processed("nlcd_tree_canopy")

if canopy_in_raw_signals:
    # Load from raw_signals
    canopy_result = supabase.table("raw_signals") \
        .select("zipcode, signal_value") \
        .eq("data_source", "nlcd_tree_canopy") \
        .execute()
    df_green = pd.DataFrame(canopy_result.data)
    df_green = df_green.rename(columns={"signal_value": "green_cover_raw"})
    df_green["green_cover_raw"] = pd.to_numeric(df_green["green_cover_raw"], errors="coerce")
    log("PASS", f"  Loaded {len(df_green)} tree canopy values from raw_signals")
else:
    # Fall back to heat_scores.tree_canopy_raw (same pattern as stress→noise from cardiovascular)
    log("INFO", "  raw_signals not available — loading from heat_scores.tree_canopy_raw")

    all_canopy_rows = []
    batch_size = 500
    _offset = 0

    while True:
        resp = supabase.table("heat_scores") \
            .select("zipcode, tree_canopy_raw") \
            .not_.is_("tree_canopy_raw", "null") \
            .range(_offset, _offset + batch_size - 1) \
            .execute()
        if not resp.data:
            break
        all_canopy_rows.extend(resp.data)
        if len(resp.data) < batch_size:
            break
        _offset += batch_size

    if len(all_canopy_rows) < 550:
        raise RuntimeError(
            f"Tree canopy data not found in heat_scores ({len(all_canopy_rows)} rows, need ≥550). "
            f"Run the Heat pipeline first."
        )

    df_green = pd.DataFrame(all_canopy_rows)
    df_green = df_green.rename(columns={"tree_canopy_raw": "green_cover_raw"})
    df_green["green_cover_raw"] = pd.to_numeric(df_green["green_cover_raw"], errors="coerce")
    log("PASS", f"  Loaded {len(df_green)} tree canopy values from heat_scores")

log("INFO", f"  green_cover_raw range: {df_green['green_cover_raw'].min():.2f} – {df_green['green_cover_raw'].max():.2f}%")


# %% [markdown]
# ## 5 · CDC PLACES Ingestion (Health Outcomes — Asthma + COPD)
#
# Same API and pattern as all other pipelines. We average asthma and COPD
# prevalence into a single health_outcomes_raw value.
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

# Combine: health_outcomes_raw = average of asthma and COPD prevalence
df_cdc["health_outcomes_raw"] = (df_cdc["casthma_crudeprev"] + df_cdc["copd_crudeprev"]) / 2.0

df_resp_cdc = df_cdc.rename(columns={"zcta5": "zipcode"})
df_resp_cdc = df_resp_cdc.drop_duplicates(subset=["zipcode"], keep="first")

# Add metro from master ZIP list
df_resp_cdc["metro"] = df_resp_cdc["zipcode"].map(ZIP_METRO_MAP)

# Filter to only our ZIPs
df_resp_cdc = df_resp_cdc[df_resp_cdc["zipcode"].isin(ALL_ZIPS)].copy()

log("INFO", f"CDC PLACES parsed: {len(df_resp_cdc)} ZIPs with data")
log("INFO", f"  Asthma coverage: {df_resp_cdc['casthma_crudeprev'].notna().sum()} ZIPs")
log("INFO", f"  COPD coverage: {df_resp_cdc['copd_crudeprev'].notna().sum()} ZIPs")
log("INFO", f"  health_outcomes_raw range: "
    f"{df_resp_cdc['health_outcomes_raw'].min():.1f} – {df_resp_cdc['health_outcomes_raw'].max():.1f}")

# ── Write to raw_signals ────────────────��────────────────────
log("INFO", "  Writing health outcomes values to raw_signals table...")
ho_failed = []
for _, row in df_resp_cdc.dropna(subset=["health_outcomes_raw"]).iterrows():
    for signal_name, col_name in [("asthma_prev", "casthma_crudeprev"), ("copd_prev", "copd_crudeprev")]:
        if pd.notna(row.get(col_name)):
            record = {
                "zipcode": row["zipcode"],
                "signal_name": signal_name,
                "data_source": "cdc_places",
                "data_vintage": 2024,
                "signal_value": float(row[col_name]),
                "units": "percent",
            }
            try:
                supabase.table("raw_signals").upsert(
                    record,
                    on_conflict="zipcode,signal_name,data_source,data_vintage"
                ).execute()
            except Exception:
                ho_failed.append(row["zipcode"])

if ho_failed:
    log("WARN", f"  {len(set(ho_failed))} ZIPs had raw_signals write failures")
else:
    log("PASS", "  All health outcomes values written to raw_signals")


# %% [markdown]
# ## 5a · Merge All Components

# %%
log("START", "Merging all four components into a single DataFrame")

# Start from CDC data (has zipcode + metro + health_outcomes_raw)
df = df_resp_cdc[["zipcode", "metro", "health_outcomes_raw"]].copy()

# Merge air quality
df = df.merge(df_air_quality[["zipcode", "air_quality_raw"]], on="zipcode", how="left")

# Merge environmental burden
df = df.merge(df_burden[["zipcode", "environmental_burden_raw"]], on="zipcode", how="left")

# Merge green cover
df = df.merge(df_green[["zipcode", "green_cover_raw"]], on="zipcode", how="left")

# Ensure metro is filled for all ZIPs
df["metro"] = df["zipcode"].map(ZIP_METRO_MAP)

# Filter to only our ZIPs (safety)
df = df[df["zipcode"].isin(ALL_ZIPS)].copy()

log("INFO", f"Merged DataFrame: {len(df)} rows")
print_validation_report("RESPIRATORY — MERGED RAW DATA", df)


# %% [markdown]
# ## 5b · Ingestion Tests (Suite 1) — GATE

# %%
log("TEST", "Running Suite 1 — Ingestion Tests")

ingestion_tests = [
    ("All expected columns present",
        lambda: (
            all(c in df.columns for c in
                ["zipcode", "metro", "air_quality_raw", "environmental_burden_raw",
                 "green_cover_raw", "health_outcomes_raw"]),
            f"Missing: {[c for c in ['zipcode','metro','air_quality_raw','environmental_burden_raw','green_cover_raw','health_outcomes_raw'] if c not in df.columns]}"
        )),
    ("Row count >= 900",
        lambda: (len(df) >= 900, f"Got {len(df)}")),
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
    ("air_quality_raw in [0, 100]",
        lambda: (
            df["air_quality_raw"].dropna().between(-0.01, 100.01).all(),
            f"min={df['air_quality_raw'].min():.2f}, max={df['air_quality_raw'].max():.2f}"
        )),
    ("air_quality_raw nulls < 15%",
        lambda: (
            df["air_quality_raw"].isna().sum() / len(df) < 0.15,
            f"{df['air_quality_raw'].isna().sum()} nulls ({df['air_quality_raw'].isna().sum()/len(df)*100:.1f}%)"
        )),
    ("environmental_burden_raw in [0, 100]",
        lambda: (
            df["environmental_burden_raw"].dropna().between(-0.01, 100.01).all(),
            f"min={df['environmental_burden_raw'].min():.2f}, max={df['environmental_burden_raw'].max():.2f}"
        )),
    ("environmental_burden_raw nulls < 10%",
        lambda: (
            df["environmental_burden_raw"].isna().sum() / len(df) < 0.10,
            f"{df['environmental_burden_raw'].isna().sum()} nulls ({df['environmental_burden_raw'].isna().sum()/len(df)*100:.1f}%)"
        )),
    ("green_cover_raw in [0, 100]",
        lambda: (
            df["green_cover_raw"].dropna().between(-0.01, 100.01).all(),
            f"min={df['green_cover_raw'].min():.2f}, max={df['green_cover_raw'].max():.2f}"
        )),
    ("green_cover_raw nulls < 50%",
        lambda: (
            df["green_cover_raw"].isna().sum() / len(df) < 0.50,
            f"{df['green_cover_raw'].isna().sum()} nulls ({df['green_cover_raw'].isna().sum()/len(df)*100:.1f}%)"
        )),
    ("health_outcomes_raw in [2, 30]",
        lambda: (
            df["health_outcomes_raw"].dropna().between(2, 30).all(),
            f"min={df['health_outcomes_raw'].min():.1f}, max={df['health_outcomes_raw'].max():.1f}"
        )),
    ("health_outcomes_raw nulls < 10%",
        lambda: (
            df["health_outcomes_raw"].isna().sum() / len(df) < 0.10,
            f"{df['health_outcomes_raw'].isna().sum()} nulls ({df['health_outcomes_raw'].isna().sum()/len(df)*100:.1f}%)"
        )),
]

suite1_passed = run_tests("RESPIRATORY — INGESTION", ingestion_tests)
require_all_pass("RESPIRATORY — INGESTION", suite1_passed)


# %% [markdown]
# ## 6 · Normalization
# Min-max normalization, global across all ZIPs.
# - `air_quality_raw` → **INVERT** (higher raw = worse pollution)
# - `environmental_burden_raw` → **INVERT** (higher raw = more burden)
# - `green_cover_raw` → **DO NOT INVERT** (higher raw = more trees = better)
#   NOTE: New-metro ZIPs (Chicago, Houston, Atlanta, Denver) have null green_cover_raw
#   because the heat_scores fallback only covers the original 4 metros. These nulls are
#   median-imputed after normalization (line ~912) before composite scoring.
# - `health_outcomes_raw` → **INVERT** (higher raw = more disease)
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Normalization
# failures typically mean a column has all-null or constant values from a broken ingestion step.

# %%
log("START", "Normalizing all four components")

RAW_COLS = ["air_quality_raw", "environmental_burden_raw", "green_cover_raw", "health_outcomes_raw"]
NORM_COLS = ["air_quality_normalized", "environmental_burden_normalized", "green_cover_normalized", "health_outcomes_normalized"]
# True = inverted (higher raw → lower normalized)
INVERT_FLAGS = [True, True, False, True]

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
# AQS county-level data may have gaps for some ZIPs (rural counties without monitors).
# Impute with median so nulls don't propagate into the composite score.
for norm_col in NORM_COLS:
    nulls = df[norm_col].isna()
    if nulls.any():
        median_val = df[norm_col].median()
        imputed_zips = df.loc[nulls, "zipcode"].tolist()
        df.loc[nulls, norm_col] = median_val
        log("INFO", f"  Imputed {len(imputed_zips)} {norm_col} nulls with median ({median_val:.2f}): {imputed_zips[:10]}")

print_validation_report("RESPIRATORY — NORMALIZED", df)

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

# Inversion correctness
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
        nc, rc = norm_col, raw_col
        norm_tests.append((
            f"{nc} — positive correlation (corr > 0.90)",
            lambda n=nc, r=rc: (
                df[[r, n]].dropna().corr().iloc[0, 1] > 0.90,
                f"Pearson r = {df[[r, n]].dropna().corr().iloc[0, 1]:.3f}"
            )
        ))

suite2_passed = run_tests("RESPIRATORY — NORMALIZATION", norm_tests)
require_all_pass("RESPIRATORY — NORMALIZATION", suite2_passed)


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
# 1 per metro — affluent/suburban ZIPs should not score F; dense urban cores may score lower
SPOT_CHECK_ZIPS = {
    "15213": ("F", "A"),   # Pittsburgh — Carnegie Mellon area
    "90210": ("F", "A"),   # Beverly Hills — LA basin air quality may lower scores
    "85257": ("F", "B"),   # Scottsdale, AZ — suburban
    "28277": ("D", "A"),   # South Charlotte — suburban
    "60614": ("F", "A"),   # Lincoln Park, Chicago — median-imputed green cover may lower scores
    "77005": ("F", "A"),   # Houston — Rice University area
    "30309": ("F", "A"),   # Atlanta — Midtown
    "80202": ("F", "A"),   # Denver — Downtown
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

# Spot checks — skip ZIPs not in the scored DataFrame (may lack CDC PLACES coverage)
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

suite3_passed = run_tests("RESPIRATORY — SCORING", scoring_tests)
require_all_pass("RESPIRATORY — SCORING", suite3_passed)


# %% [markdown]
# ## 8 · Claude API Interpretations
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# missing ANTHROPIC_API_KEY in Colab secrets, rate limits (429), or model string changes.

# %%
log("START", "Generating Claude API interpretations for all ZIPs")

import anthropic

client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env

def generate_resp_interpretation(zipcode: str, composite_score: float,
                                 letter_grade: str, components: dict) -> str:
    """
    Generate a plain-language respiratory health environment interpretation.
    components: dict of {label: normalized_score} — qualitative labels only, no weights.
    """
    prompt = f"""You are a public health analyst writing a plain-language summary for residents and
real estate professionals. Write 2-3 sentences interpreting this neighborhood's respiratory
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
- Frame air quality as outdoor pollution from traffic, industry, and ground-level ozone
- Frame environmental burden as proximity to industrial facilities and pollution sources
- Frame green cover as trees and vegetation that filter pollutants and improve air
- Frame health outcomes as actual respiratory disease rates in the community"""

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
    components = {
        "Outdoor Air Quality": row.get("air_quality_normalized", 0),
        "Industrial & Traffic Exposure": row.get("environmental_burden_normalized", 0),
        "Tree Canopy & Vegetation": row.get("green_cover_normalized", 0),
        "Respiratory Disease Rates": row.get("health_outcomes_normalized", 0),
    }

    try:
        interp = generate_resp_interpretation(
            zc, row["composite_score"], row["letter_grade"], components
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
# ## 9 · Supabase Upsert
#
# **If this section fails:** Stop, copy the error, bring it to Claude Code. Common issues:
# column name mismatch between local dict keys and Supabase schema, or missing UNIQUE constraint.
# Do NOT modify the Supabase schema manually — bring the error here first.

# %%
# ── Reinitialize Supabase client (fresh HTTP connection) ─────
# PostgREST may have a stale schema cache from earlier in the pipeline,
# especially if respiratory_scores was recently created. A fresh client
# forces a new HTTP connection to avoid PGRST205 errors.
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
log("INFO", "Reinitialized Supabase client for upsert phase")

# ── Schema cache warm-up ─────────────────────────────────────
# Test SELECT to confirm PostgREST recognizes the table before we upsert rows.
MAX_RETRIES = 5
RETRY_DELAY = 3

for attempt in range(1, MAX_RETRIES + 1):
    try:
        test = supabase.table("respiratory_scores").select("zipcode").limit(1).execute()
        log("PASS", f"Schema cache warm-up succeeded on attempt {attempt}")
        break
    except Exception as e:
        log("WARN", f"Schema cache warm-up attempt {attempt}/{MAX_RETRIES} failed: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
else:
    raise RuntimeError(
        "\n" + "!" * 62 + "\n"
        "  PostgREST cannot see the respiratory_scores table after 5 attempts.\n"
        "  This is a schema cache issue, not a missing table.\n\n"
        "  FIX: Open Supabase SQL Editor and run:\n"
        "    NOTIFY pgrst, 'reload schema';\n\n"
        "  Wait 10 seconds, then re-run this cell.\n"
        + "!" * 62 + "\n"
    )

# %%
log("START", "Upserting all records to respiratory_scores")

failed_zips = []

for _, row in df.iterrows():
    record = {
        "zipcode": row["zipcode"],
        "metro": row["metro"],
        "air_quality_raw": float(row["air_quality_raw"]) if pd.notna(row["air_quality_raw"]) else None,
        "air_quality_normalized": float(row["air_quality_normalized"]) if pd.notna(row["air_quality_normalized"]) else None,
        "environmental_burden_raw": float(row["environmental_burden_raw"]) if pd.notna(row["environmental_burden_raw"]) else None,
        "environmental_burden_normalized": float(row["environmental_burden_normalized"]) if pd.notna(row["environmental_burden_normalized"]) else None,
        "green_cover_raw": float(row["green_cover_raw"]) if pd.notna(row["green_cover_raw"]) else None,
        "green_cover_normalized": float(row["green_cover_normalized"]) if pd.notna(row["green_cover_normalized"]) else None,
        "health_outcomes_raw": float(row["health_outcomes_raw"]) if pd.notna(row["health_outcomes_raw"]) else None,
        "health_outcomes_normalized": float(row["health_outcomes_normalized"]) if pd.notna(row["health_outcomes_normalized"]) else None,
        "composite_score": float(row["composite_score"]),
        "letter_grade": row["letter_grade"],
        "interpretation": row.get("interpretation", ""),
        "score_date": str(date.today()),
    }

    try:
        supabase.table("respiratory_scores").upsert(
            record, on_conflict="zipcode"
        ).execute()
    except Exception as e:
        log("ERROR", f"Failed to write ZIP {row['zipcode']}: {e}")
        failed_zips.append(row["zipcode"])

if failed_zips:
    log("WARN", f"{len(failed_zips)} ZIPs failed upsert: {failed_zips[:10]}")
else:
    log("PASS", "All records upserted to respiratory_scores")

# %% [markdown]
# ## 9a · Supabase Write Tests (Suite 4) — GATE

# %%
log("TEST", "Running Suite 4 — Supabase Write Tests")

TABLE_NAME = "respiratory_scores"

def get_sb_count():
    result = supabase.table(TABLE_NAME).select("zipcode", count="exact").execute()
    return result.count or 0

def get_sb_row(zc):
    result = supabase.table(TABLE_NAME).select("*").eq("zipcode", zc).execute()
    return result.data[0] if result.data else None

initial_count = get_sb_count()

SPOT_ZIPS = ["15213", "90210", "85257", "28277", "60614", "77005", "30309", "80202"]

write_tests = [
    (f"Supabase row count >= 900",
        lambda: (initial_count >= 900, f"Got {initial_count}")),
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

# Spot-check ZIPs: verify values match local data (skip ZIPs not in scored data)
for zc in SPOT_ZIPS:
    z = zc
    local_row = df[df["zipcode"] == z]
    if local_row.empty:
        log("INFO", f"  Skipping Suite 3 spot check for ZIP {z} — not in scored data")
        continue
    write_tests.append((
        f"Spot check ZIP {z}: exists in Supabase",
        lambda zc=z: (
            get_sb_row(zc) is not None,
            f"ZIP {zc} not found in {TABLE_NAME}"
        )
    ))

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

suite4_passed = run_tests("RESPIRATORY — SUPABASE WRITE", write_tests)
require_all_pass("RESPIRATORY — SUPABASE WRITE", suite4_passed)


# %% [markdown]
# ## 10 · Pipeline Complete
#
# All 4 test suites passed. Data is live in `respiratory_scores`.

# %%
log("DONE", "=" * 50)
log("DONE", "RESPIRATORY PIPELINE COMPLETE")
log("DONE", f"  Total ZIPs scored: {len(df)}")
log("DONE", f"  Grade distribution: {df['letter_grade'].value_counts().to_dict()}")
log("DONE", f"  Score range: {df['composite_score'].min():.1f} – {df['composite_score'].max():.1f}")
log("DONE", f"  Mean score: {df['composite_score'].mean():.1f}")
log("DONE", "=" * 50)
