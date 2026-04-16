# %% [markdown]
# # Metro Expansion — Preparation Script
# **LaSalle Technologies Health Environment Score**
#
# This notebook prepares the 4-metro → 8-metro expansion:
# 1. Backs up all current scores (574 ZIPs) before normalization shifts
# 2. Assembles ZIP lists for Chicago, Houston, Atlanta, Denver
# 3. Inserts new ZIPs into the `zip_codes` Supabase table
# 4. Instructions for downloading BTS state noise rasters
#
# Run each cell in order. Do not run any tool pipelines until this
# notebook completes successfully and all 4 BTS rasters are uploaded.
#
# **IF ANY CELL FAILS:** Stop immediately. Copy the full error traceback
# and bring it to Claude Code for diagnosis.

# %% [markdown]
# ## 0 · Setup & Configuration

# %%
# ── Installs (run once per Colab session) ────────────────────
# !pip install -q supabase pandas requests

# %%
import os
import time
import traceback
import requests
import pandas as pd
from datetime import datetime, date

# ── Mount Google Drive ───────────────────────────────────────
from google.colab import drive
drive.mount("/content/drive")

# ── Colab secrets ────────────────────────────────────────────
from google.colab import userdata
SUPABASE_URL = userdata.get("SUPABASE_URL")
SUPABASE_KEY = userdata.get("SUPABASE_KEY")

from supabase import create_client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Paths ────────────────────────────────────────────────────
DRIVE_PREFIX = "/content/drive/MyDrive/Colab Notebooks/health-score-data"
BACKUP_DIR = f"{DRIVE_PREFIX}/backups"
os.makedirs(BACKUP_DIR, exist_ok=True)

TODAY = date.today().isoformat()  # e.g. "2026-04-16"

# ── Logging ──────────────────────────────────────────────────
def log(level: str, message: str):
    """Structured logging: INFO | WARN | ERROR | PASS | FAIL | START | DONE"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    icons = {
        "INFO": "ℹ️ ", "WARN": "⚠️ ", "ERROR": "❌ ", "PASS": "✅ ",
        "FAIL": "❌ ", "TEST": "🧪 ", "START": "🚀 ", "DONE": "🏁 ",
    }
    icon = icons.get(level, "   ")
    print(f"[{timestamp}] {icon} [{level}] {message}")

log("START", "Metro expansion prep — setup complete")
log("INFO", f"Backup directory: {BACKUP_DIR}")
log("INFO", f"Date stamp: {TODAY}")

# %% [markdown]
# ## 1 · Backup Current Scores (CRITICAL — run before any pipeline)
#
# Exports all rows from all 6 score tables to CSV on Google Drive.
# Global min/max normalization WILL shift when new metros are added,
# changing existing scores. This backup is your safety net.

# %%
log("START", "Backing up current scores from all 6 tables")

SCORE_TABLES = [
    "composite_scores",
    "cardiovascular_scores",
    "stress_scores",
    "food_access_scores",
    "heat_scores",
    "overall_scores",
]

BATCH_SIZE = 200

for table in SCORE_TABLES:
    log("INFO", f"  Exporting {table}...")

    all_rows = []
    offset = 0

    while True:
        try:
            resp = supabase.table(table)\
                .select("*")\
                .range(offset, offset + BATCH_SIZE - 1)\
                .execute()
        except Exception as e:
            log("ERROR", f"  Failed to fetch {table} at offset {offset}: {e}")
            break

        if not resp.data:
            break
        all_rows.extend(resp.data)
        if len(resp.data) < BATCH_SIZE:
            break
        offset += BATCH_SIZE

    if all_rows:
        df = pd.DataFrame(all_rows)
        csv_path = f"{BACKUP_DIR}/pre_expansion_{table}_{TODAY}.csv"
        df.to_csv(csv_path, index=False)
        log("PASS", f"  {table}: {len(df)} rows → {csv_path}")
    else:
        log("WARN", f"  {table}: 0 rows fetched — table may be empty")

log("DONE", "Score backup complete")

# %% [markdown]
# ### Backup Verification
# Run this cell to confirm all 6 CSVs were saved with expected row counts.

# %%
log("TEST", "Verifying backup files")

expected_rows = 574  # current ZIP count before expansion
all_ok = True

for table in SCORE_TABLES:
    csv_path = f"{BACKUP_DIR}/pre_expansion_{table}_{TODAY}.csv"
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        status = "PASS" if len(df) >= expected_rows - 5 else "WARN"
        log(status, f"  {table}: {len(df)} rows in backup")
        if status == "WARN":
            all_ok = False
    else:
        log("FAIL", f"  {table}: backup file not found at {csv_path}")
        all_ok = False

if all_ok:
    log("DONE", f"All 6 backups verified — safe to proceed with expansion")
else:
    log("WARN", "Some backups are missing or have unexpected row counts — review before continuing")

# %% [markdown]
# ## 2 · Assemble New Metro ZIP Lists
#
# Queries the CDC PLACES Socrata API to find ZCTAs in each new metro's ZIP prefix
# ranges. This confirms every ZIP we insert actually has CDC PLACES data coverage,
# guaranteeing it can be scored by all 5 pipelines.
#
# **Data source:** CDC PLACES (same API used by all tool pipelines)
# `https://data.cdc.gov/resource/c7b2-4ecy.json`
#
# **ZIP prefix ranges (from USPS 3-digit ZIP code areas):**
# - Chicago: 606, 607, 608
# - Houston: 770, 771, 772, 773, 774
# - Atlanta: 300, 301, 302, 303
# - Denver: 800, 801, 802, 803, 804

# %%
log("START", "Assembling ZIP lists for 4 new metros via CDC PLACES API")

# ── Metro definitions: ZIP prefix ranges ─────────────────────
METRO_ZIP_PREFIXES = {
    "Chicago": ["606", "607", "608"],
    "Houston": ["770", "771", "772", "773", "774"],
    "Atlanta": ["300", "301", "302", "303"],
    "Denver":  ["800", "801", "802", "803", "804"],
}

# ── Query CDC PLACES for ZCTAs matching each prefix ──────────
# Same API endpoint used by all tool pipelines (cardiovascular, stress, food, heat)
CDC_BASE_URL = "https://data.cdc.gov/resource/c7b2-4ecy.json"

new_zip_records = []

for metro_label, prefixes in METRO_ZIP_PREFIXES.items():
    metro_zips = set()

    for prefix in prefixes:
        params = {
            "$select": "zcta5",
            "$where": f"zcta5 LIKE '{prefix}%'",
            "$limit": 50000,
        }
        try:
            api_resp = requests.get(CDC_BASE_URL, params=params, timeout=30)
            api_resp.raise_for_status()
            rows = api_resp.json()
            found_zips = {r["zcta5"].zfill(5) for r in rows if r.get("zcta5")}
            metro_zips.update(found_zips)
            log("INFO", f"  {metro_label} prefix {prefix}: {len(found_zips)} ZCTAs from CDC PLACES")
        except requests.RequestException as e:
            log("ERROR", f"  {metro_label} prefix {prefix}: API request failed — {e}")
            raise

        # Brief pause between requests to respect Socrata rate limits
        time.sleep(0.5)

    log("INFO", f"  {metro_label} total: {len(metro_zips)} unique ZCTAs with CDC PLACES coverage")

    for z in sorted(metro_zips):
        new_zip_records.append({"zipcode": z, "metro": metro_label})

df_new = pd.DataFrame(new_zip_records)
log("INFO", f"Total new ZIP records assembled: {len(df_new)}")

# ── Fetch existing ZIPs from Supabase to check for overlaps ──
log("INFO", "Fetching existing zip_codes from Supabase...")

existing_zips = []
offset = 0
while True:
    resp = supabase.table("zip_codes")\
        .select("zipcode,metro")\
        .range(offset, offset + BATCH_SIZE - 1)\
        .execute()
    if not resp.data:
        break
    existing_zips.extend(resp.data)
    if len(resp.data) < BATCH_SIZE:
        break
    offset += BATCH_SIZE

df_existing = pd.DataFrame(existing_zips)
existing_set = set(df_existing["zipcode"].tolist())
log("INFO", f"Existing zip_codes table: {len(df_existing)} rows across {df_existing['metro'].nunique()} metros")

# ── Remove any ZIPs that already exist ───────────────────────
overlap = df_new[df_new["zipcode"].isin(existing_set)]
if len(overlap) > 0:
    log("WARN", f"  {len(overlap)} ZIPs overlap with existing metros — removing them from insert list")
    log("WARN", f"  Overlap ZIPs (first 10): {overlap['zipcode'].tolist()[:10]}")
    df_new = df_new[~df_new["zipcode"].isin(existing_set)].copy()

# ── Summary ──────────────────────────────────────────────────
log("INFO", "New metro ZIP counts (after dedup):")
for metro_label in METRO_ZIP_PREFIXES.keys():
    count = len(df_new[df_new["metro"] == metro_label])
    log("INFO", f"  {metro_label}: {count} ZIPs")

log("INFO", f"Total new ZIPs to insert: {len(df_new)}")
log("INFO", f"Expected total after insert: {len(df_existing) + len(df_new)}")

# %% [markdown]
# ### Review Before Inserting
#
# Check the counts above. Expected approximate counts:
# - Chicago: ~250 ZIPs
# - Houston: ~200 ZIPs
# - Atlanta: ~150 ZIPs
# - Denver: ~120 ZIPs
# - Total new: ~720 ZIPs
# - Total after insert: ~1,290 ZIPs
#
# If counts are wildly different, STOP and investigate before running the next cell.
# Every ZIP in this list has confirmed CDC PLACES data, so all are scoreable.

# %% [markdown]
# ## 3 · Insert New ZIPs into Supabase `zip_codes`

# %%
log("START", f"Inserting {len(df_new)} new ZIPs into zip_codes table")

failed_zips = []
inserted = 0

STATE_MAP = {"Chicago": "IL", "Houston": "TX", "Atlanta": "GA", "Denver": "CO"}

for i, row in df_new.iterrows():
    record = {
        "zipcode": row["zipcode"],
        "metro": row["metro"],
        "state": STATE_MAP[row["metro"]],
    }
    try:
        supabase.table("zip_codes").upsert(
            record, on_conflict="zipcode"
        ).execute()
        inserted += 1
    except Exception as e:
        log("ERROR", f"  Failed to insert ZIP {record['zipcode']}: {e}")
        failed_zips.append(record["zipcode"])

    # Progress indicator every 100 ZIPs
    if (inserted + len(failed_zips)) % 100 == 0:
        log("INFO", f"  Progress: {inserted + len(failed_zips)}/{len(df_new)} processed")

if failed_zips:
    log("WARN", f"{len(failed_zips)} ZIPs failed to insert: {failed_zips[:20]}")
else:
    log("PASS", f"All {inserted} new ZIPs inserted successfully")

# ── Verify total count ───────────────────────────────────────
log("INFO", "Verifying total zip_codes count...")

verify_zips = []
offset = 0
while True:
    resp = supabase.table("zip_codes")\
        .select("zipcode,metro")\
        .range(offset, offset + BATCH_SIZE - 1)\
        .execute()
    if not resp.data:
        break
    verify_zips.extend(resp.data)
    if len(resp.data) < BATCH_SIZE:
        break
    offset += BATCH_SIZE

df_verify = pd.DataFrame(verify_zips)
log("INFO", f"Total rows in zip_codes: {len(df_verify)}")
log("INFO", "Metro breakdown:")
for metro, count in df_verify["metro"].value_counts().sort_index().items():
    log("INFO", f"  {metro}: {count} ZIPs")

# ── Confirm test ZIPs exist ──────────────────────────────────
TEST_ZIPS = {
    "15213": "Pittsburgh",
    "90210": "Los Angeles",
    "28277": "Charlotte",
    "85257": "Phoenix",
    "60614": "Chicago",
    "77002": "Houston",
    "30309": "Atlanta",
    "80202": "Denver",
}

log("TEST", "Confirming all 8 test ZIPs are present...")
all_test_ok = True
verify_set = set(df_verify["zipcode"].tolist())

for zc, metro in TEST_ZIPS.items():
    if zc in verify_set:
        log("PASS", f"  {zc} ({metro}) — present")
    else:
        log("FAIL", f"  {zc} ({metro}) — NOT FOUND")
        all_test_ok = False

if all_test_ok:
    log("DONE", "All 8 test ZIPs confirmed in zip_codes table")
else:
    log("WARN", "Some test ZIPs are missing — check the crosswalk data or insert manually")

# %% [markdown]
# ## 4 · BTS State Noise Raster Downloads
#
# The cardiovascular pipeline's `STATE_NOISE_RASTERS` now expects 8 state rasters.
# The original 4 (PA, CA, AZ, NC) are already on Google Drive. You need to download
# 4 new state rasters for **IL, TX, GA, CO**.
#
# ### Download Instructions
#
# 1. Go to **BTS National Transportation Noise Map**:
#    https://www.bts.gov/geospatial/national-transportation-noise-map
#
# 2. Navigate to the **Data Download** section. Select **"Aviation and Road Noise"**
#    or **"Rail, Road, and Aviation Noise"** (whichever matches your existing rasters —
#    check the filenames of your PA/CA/AZ/NC files).
#
# 3. Download the following 4 state rasters:
#    - **Illinois (IL)**: `IL_rail_road_and_aviation_noise_2020.tif`
#    - **Texas (TX)**: `TX_rail_road_and_aviation_noise_2020.tif`
#    - **Georgia (GA)**: `GA_rail_road_and_aviation_noise_2020.tif`
#    - **Colorado (CO)**: `CO_rail_road_and_aviation_noise_2020.tif`
#
# 4. Upload all 4 files to Google Drive at:
#    `/content/drive/MyDrive/Colab Notebooks/health-score-data/`
#
#    They must be in the same directory as the existing state rasters.
#
# ### Verification
#
# Run the cell below after uploading to confirm all 8 rasters are present.
#
# ### CONUS Rasters (No Action Needed)
#
# The following CONUS-wide rasters already cover all 8 metros — no new downloads:
# - **NLCD Impervious Surface** — used by Cardiovascular + Heat pipelines
# - **NLCD Tree Canopy** — used by Stress + Heat pipelines
# - **NASA VIIRS** — used by Stress pipeline
#
# These are continental-US datasets that include IL, TX, GA, and CO by default.

# %%
log("TEST", "Checking for all 8 BTS state noise rasters on Google Drive")

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

missing = []
for state, path in STATE_NOISE_RASTERS.items():
    if os.path.exists(path):
        size_mb = os.path.getsize(path) / (1024 * 1024)
        log("PASS", f"  {state}: found ({size_mb:.1f} MB)")
    else:
        log("FAIL", f"  {state}: NOT FOUND at {path}")
        missing.append(state)

if missing:
    log("WARN", f"Missing rasters for: {', '.join(missing)}")
    log("INFO", "Download from: https://www.bts.gov/geospatial/national-transportation-noise-map")
    log("INFO", f"Upload to: {DRIVE_PREFIX}/")
else:
    log("DONE", "All 8 BTS state noise rasters present — ready for pipeline execution")

# %% [markdown]
# ## 5 · Final Verification Summary

# %%
log("START", "Running final expansion prep verification")

checks_passed = 0
checks_total = 0

# Check 1: Backup files exist
for table in SCORE_TABLES:
    checks_total += 1
    csv_path = f"{BACKUP_DIR}/pre_expansion_{table}_{TODAY}.csv"
    if os.path.exists(csv_path):
        checks_passed += 1
        log("PASS", f"  Backup: {table}")
    else:
        log("FAIL", f"  Backup: {table} — file not found")

# Check 2: zip_codes has 8 metros
checks_total += 1
verify_zips2 = []
offset = 0
while True:
    resp = supabase.table("zip_codes")\
        .select("zipcode,metro")\
        .range(offset, offset + BATCH_SIZE - 1)\
        .execute()
    if not resp.data:
        break
    verify_zips2.extend(resp.data)
    if len(resp.data) < BATCH_SIZE:
        break
    offset += BATCH_SIZE

df_final = pd.DataFrame(verify_zips2)
metro_count = df_final["metro"].nunique()
if metro_count == 8:
    checks_passed += 1
    log("PASS", f"  zip_codes: {metro_count} metros, {len(df_final)} total ZIPs")
else:
    log("FAIL", f"  zip_codes: expected 8 metros, got {metro_count}")

# Check 3: All 8 test ZIPs present
final_set = set(df_final["zipcode"].tolist())
for zc, metro in TEST_ZIPS.items():
    checks_total += 1
    if zc in final_set:
        checks_passed += 1
    else:
        log("FAIL", f"  Test ZIP {zc} ({metro}) missing")

# Check 4: BTS rasters
for state, path in STATE_NOISE_RASTERS.items():
    checks_total += 1
    if os.path.exists(path):
        checks_passed += 1
    else:
        log("FAIL", f"  BTS raster {state} missing")

# ── Summary ──────────────────────────────────────────────────
print(f"\n{'='*62}")
print(f"  EXPANSION PREP — FINAL SUMMARY")
print(f"{'='*62}")
print(f"  Checks passed: {checks_passed}/{checks_total}")
print(f"  Backup CSVs:   {len(SCORE_TABLES)} tables backed up")
print(f"  zip_codes:     {len(df_final)} ZIPs across {metro_count} metros")
print(f"  BTS rasters:   {8 - len(missing) if 'missing' in dir() else '?'}/8 present")
print(f"{'='*62}")

if checks_passed == checks_total:
    log("DONE", "ALL CHECKS PASSED — ready to run tool pipelines for full 8-metro dataset")
    log("INFO", "Next: run each tool pipeline (Respiratory → Cardiovascular → Stress → Food → Heat → Overall)")
    log("INFO", "After all pipelines: run recalibration audit comparing new scores to backup CSVs")
else:
    log("WARN", f"{checks_total - checks_passed} checks failed — resolve before running pipelines")
