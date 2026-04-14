# %% [markdown]
# # Backfill BTS Noise → raw_signals
# Reads `noise_raw` from `cardiovascular_scores` and writes to `raw_signals`
# so the Stress pipeline can reuse it.
#
# Run this ONCE before executing `stress_pipeline.py`.

# %%
# !pip install -q supabase

# %%
import os
import time
import traceback
import pandas as pd
from datetime import datetime

# Colab secrets — uncomment in Colab
# from google.colab import userdata
# SUPABASE_URL = userdata.get("SUPABASE_URL")
# SUPABASE_KEY = userdata.get("SUPABASE_KEY")

# Local fallback
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

from supabase import create_client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Logging ──────────────────────────────────────────────────
def log(level: str, message: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    icons = {
        "INFO": "ℹ️ ", "WARN": "⚠️ ", "ERROR": "❌ ",
        "PASS": "✅ ", "START": "🚀 ", "DONE": "🏁 ",
    }
    icon = icons.get(level, "   ")
    print(f"[{timestamp}] {icon} [{level}] {message}")

# %%
log("START", "Backfilling BTS noise from cardiovascular_scores → raw_signals")

# ── Read noise_raw from cardiovascular_scores ────────────────
# Supabase paginates at 1000 rows; fetch all with pagination
all_rows = []
batch_size = 500
offset = 0

while True:
    resp = supabase.table("cardiovascular_scores") \
        .select("zipcode, noise_raw") \
        .not_.is_("noise_raw", "null") \
        .range(offset, offset + batch_size - 1) \
        .execute()
    if not resp.data:
        break
    all_rows.extend(resp.data)
    if len(resp.data) < batch_size:
        break
    offset += batch_size

log("INFO", f"Read {len(all_rows)} rows with noise_raw from cardiovascular_scores")

if len(all_rows) == 0:
    raise RuntimeError("No noise_raw data found in cardiovascular_scores — nothing to backfill")

# ── Refresh PostgREST schema cache ────────────────────────────
# Required after creating/altering tables so PostgREST sees new columns.
# Prerequisite: run this DDL once in the Supabase SQL Editor:
#
#   CREATE OR REPLACE FUNCTION notify_pgrst() RETURNS void AS $$
#   BEGIN
#     NOTIFY pgrst, 'reload schema';
#   END;
#   $$ LANGUAGE plpgsql;
#
log("INFO", "Refreshing PostgREST schema cache")
supabase.rpc("notify_pgrst", {}).execute()
log("PASS", "Schema cache refreshed")

# ── Write to raw_signals with compound upsert ────────────────
failed_zips = []
written = 0

for row in all_rows:
    record = {
        "zipcode": row["zipcode"],
        "signal_name": "noise_dnl",
        "data_source": "bts_noise",
        "data_vintage": 2020,
        "raw_value": float(row["noise_raw"]),
        "unit": "dB_DNL",
    }
    try:
        supabase.table("raw_signals").upsert(
            record,
            on_conflict="zipcode,signal_name,data_source,data_vintage"
        ).execute()
        written += 1
    except Exception as e:
        log("ERROR", f"Failed ZIP {row['zipcode']}: {e}")
        failed_zips.append(row["zipcode"])

# ── Report ───────────────────────────────────────────────────
if failed_zips:
    log("WARN", f"{len(failed_zips)} ZIPs failed: {failed_zips[:10]}")
else:
    log("PASS", f"All {written} noise values written to raw_signals")

# ── Verify ───────────────────────────────────────────────────
verify = supabase.table("raw_signals") \
    .select("zipcode", count="exact") \
    .eq("data_source", "bts_noise") \
    .execute()
count = verify.count or 0
log("INFO", f"Verification: raw_signals now has {count} rows with data_source='bts_noise'")

if count >= 550:
    log("DONE", "Backfill complete — stress_pipeline.py noise check will now pass")
else:
    log("WARN", f"Only {count} rows — stress pipeline expects ≥550")
