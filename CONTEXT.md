# CONTEXT.md — Current Project State & Session Notes
> Update this file at the end of every Claude Code session. It is the handoff document between sessions.
> Keep it current — stale context is worse than no context.

---

## Current Status (as of April 2026)

### Active Phase
**Tool 3 — 🧠 Stress / Sensory Environment Score**

### What's Complete
- Tool 1 (Respiratory) is fully live on Streamlit Community Cloud
- Supabase schema established: `zip_codes`, `raw_signals`, `score_components`, `composite_scores`, `interpretations`, `score_config`
- Respiratory reference implementation is the pattern for all subsequent tools
- 600 ZIP codes confirmed across Pittsburgh, LA, Phoenix, Charlotte

### What's In Progress
- Cardiovascular pipeline script corrected with Colab-tested fixes — ready for re-execution
- Cardiovascular Streamlit tab built — needs data in Supabase before it will display

### What's Blocked / At Risk
- BTS noise raster processing is the highest-risk step — rasterio setup in Colab must be validated before proceeding
- Stress/Sensory 4th component not yet confirmed (crowding metric vs. social isolation)
- Heat tool 4th component not yet confirmed

---

## Key Decisions Made

| Decision | Rationale |
|---|---|
| Public scores, proprietary methodology | "Public score, private engine" model — maximizes adoption while protecting moat |
| Federal data sources only | No paid data licenses (e.g., Prophetic at $9K/county is non-starter at scale) |
| Global min-max normalization (not per-metro) | Prevents score inflation in lower-performing metros |
| Upsert on UNIQUE(zip_code) | Ensures safe re-runnability without duplicate rows |
| Claude API for interpretations | Differentiates from raw score tools; plain language is a product feature |
| No Walk Score dependency | Walk Score measures built environment opportunity; our scores measure outcomes — divergence is analytically valuable |

---

## Strategic Context (for generating any public-facing content)

**What this platform is:**
A neighborhood-level health environment scoring system measuring the environmental conditions that shape long-term health outcomes — across five dimensions: respiratory, cardiovascular, stress/sensory, food access, and heat/climate resilience.

**What makes it different:**
- Measures health *outcomes* (not just built environment opportunity like Walk Score)
- Multi-dimensional (5 interlocking scores, not a single metric)
- ZIP-level specificity across four major metros
- All inputs from authoritative federal sources (CDC, USDA, NASA, USGS, BTS, EPA)
- Plain-language interpretations via Claude API

**Target audiences:**
- Researchers / academics
- Real estate developers and relocation professionals
- Healthcare systems and insurers
- City planners and policy organizations

**Partnership targets:**
- Atrium Health, Novant (Charlotte)
- Kaiser Permanente (LA)
- Heinz Endowments (Pittsburgh)
- Bloomberg Philanthropies

**Go-to-market timing:**
- Dedicated LaSalle Technologies web page launches after full suite is complete (May 2026)
- Streamlit tool is the public-facing product
- Website page is the front door with segmented CTAs per audience

---

## Session Log
> Append a new entry at the end of each Claude Code session.

### Session Template
```
### [Date] — [Tool/Phase]
**Completed:**
- 

**Left off at:**
- 

**Next session should start with:**
- 

**Any issues or surprises:**
- 
```

### 2026-04-13 — Tool 2 Cardiovascular (Pipeline & UI Build)
**Completed:**
- Full cardiovascular pipeline script: `notebooks/cardiovascular/cardiovascular_pipeline.py` (980 lines)
  - CDC PLACES ingestion (LPA + CHD) with Socrata batch fetching
  - BTS noise raster processing with zonal stats → writes to `raw_signals` for Stress tool reuse
  - NLCD impervious surface raster processing → writes to `raw_signals` for Heat tool reuse
  - Min-max normalization (all 4 components inverted)
  - Composite scoring + letter grade assignment
  - Claude API interpretation generation (600 ZIPs, rate-limited)
  - Supabase upsert to `cardiovascular_scores`
  - All 4 test suite gates embedded (Ingestion, Normalization, Scoring, Supabase Write)
- Supabase CREATE TABLE SQL: `notebooks/cardiovascular/create_table.sql`
- Streamlit UI: modified `app.py` to add `st.tabs()` with Respiratory + Cardiovascular tabs
  - Cardiovascular tab matches Respiratory pattern exactly (disc viz, component breakdown, interpretation, metro comparison)
  - Red/Pink color palette applied
  - Queries `cardiovascular_scores` table directly (all data in one table, unlike Respiratory's split across `composite_scores` + `interpretations`)

**Left off at:**
- Pipeline script is written but not yet executed — needs Colab with raster files and Supabase credentials

**Next session should start with:**
1. Run `create_table.sql` in Supabase SQL Editor
2. Upload to Google Drive: BTS noise GeoTIFF, NLCD impervious GeoTIFF, ZCTA shapefile
3. Update 3 file paths in pipeline script (NOISE_RASTER_PATH, IMPERVIOUS_RASTER_PATH, ZCTA_SHAPEFILE_PATH)
4. Execute pipeline cells sequentially in Colab — each gate must pass before proceeding
5. After pipeline completes, verify Streamlit cardiovascular tab renders correctly
6. Run Suite 5 manual Streamlit smoke tests

**Any issues or surprises:**
- `notebooks/respiratory/` directory does not exist in repo — Respiratory reference is only `app.py`
- Respiratory tab queries `composite_scores` + `interpretations` tables (separate); Cardiovascular stores everything in `cardiovascular_scores` (single table) — minor schema divergence, works fine but worth noting
- Respiratory grade scale in the UI showed non-standard thresholds (70–100, 55–69, etc.); updated Cardiovascular to use the correct scale from AGENTS.md (≥80, 65–79, 50–64, 35–49, <35)

### 2026-04-13 — Tool 2 Cardiovascular (Noise Raster Fix)
**Completed:**
- Updated `notebooks/cardiovascular/cardiovascular_pipeline.py` to load 4 individual state BTS noise rasters (PA, CA, AZ, NC) instead of a single CONUS file
  - BTS data downloaded as per-state files, not one CONUS GeoTIFF
  - Added `rasterio.merge` to combine the 4 state rasters into a single temp file before running zonal stats
  - Each file's CRS, shape, and nodata value are logged on load; CRS mismatches produce a warning
  - Nodata value is read from raster metadata instead of hardcoded `-9999`
  - Temp merged file is cleaned up after zonal stats complete
  - All downstream logic (df_noise, raw_signals write, test suites) unchanged

**Left off at:**
- Pipeline script is written but not yet executed — needs Colab with raster files and Supabase credentials

**Next session should start with:**
1. Run `create_table.sql` in Supabase SQL Editor
2. Upload to Google Drive: 4 BTS state noise GeoTIFFs, NLCD impervious GeoTIFF, ZCTA shapefile
3. Update file paths in pipeline script (`NOISE_RASTER_PATHS` list, `IMPERVIOUS_RASTER_PATH`, `ZCTA_SHAPEFILE_PATH`)
4. Execute pipeline cells sequentially in Colab — each gate must pass before proceeding
5. After pipeline completes, verify Streamlit cardiovascular tab renders correctly
6. Run Suite 5 manual Streamlit smoke tests

**Any issues or surprises:**
- BTS noise data comes as individual state rasters, not a single CONUS file — required merge step added to pipeline

### 2026-04-13 — Tool 2 Cardiovascular (Pipeline Corrections from Colab Run)
**Completed:**
- Applied 5 corrections to `notebooks/cardiovascular/cardiovascular_pipeline.py` based on actual Colab execution findings:
  1. **CDC PLACES API → wide format:** API no longer returns `locationname`/`measureid` columns. ZIP field is `zcta5`. Measures are column names (`lpa_crudeprev`, `chd_crudeprev`). Rewrote fetch function to use `$select` + `$where=zcta5 IN (...)` with batch size 50.
  2. **BTS noise → per-state processing:** Merging 4 state rasters crashed Colab RAM. Replaced merge approach with `STATE_METRO_MAP` that filters ZIPs per state and runs `zonal_stats` on each state raster independently.
  3. **NLCD impervious nodata → 250.0:** The correct nodata value is 250.0, not 255.
  4. **ZCTA shapefile column → ZCTA5CE20:** Removed fallback to `ZCTA5CE10` — we use the 2020 vintage shapefile.
  5. **Google Drive paths → correct prefix:** Updated to `/content/drive/MyDrive/Colab Notebooks/health-score-data/` (note space in "Colab Notebooks").
- Added failure-handling comments to every major pipeline section directing users to bring errors to Claude Code rather than debugging manually in Colab.
- Updated TOOL_SPECS.md CDC PLACES reference section.

**Left off at:**
- Pipeline script corrected, ready for Colab re-execution.

**Next session should start with:**
1. Execute corrected pipeline in Colab — each gate must pass before proceeding
2. After pipeline completes, verify Streamlit cardiovascular tab renders correctly
3. Run Suite 5 manual Streamlit smoke tests

**Any issues or surprises:**
- CDC PLACES API changed from long format (one row per measure per ZIP) to wide format (one row per ZIP with measure columns). This is a breaking change that will affect Tools 3–5. See Lessons Learned below.

### 2026-04-13 — Tool 3 Stress / Sensory (Pipeline & UI Build)
**Completed:**
- Full stress/sensory pipeline script: `notebooks/stress/stress_pipeline.py` (~850 lines)
  - BTS noise reuse from `raw_signals` table (no raster reprocessing)
  - NASA VIIRS light pollution raster processing (per-state, writes to `raw_signals`)
  - CDC PLACES ingestion (depression + poor mental health days) with wide-format batch fetch
  - Min-max normalization (all 4 components inverted)
  - Composite scoring + letter grade assignment
  - Claude API interpretation generation (stress/sensory framing)
  - Supabase upsert to `stress_scores`
  - All 4 test suite gates embedded (Ingestion, Normalization, Scoring, Supabase Write)
- Supabase CREATE TABLE SQL: `notebooks/stress/create_table.sql`
- Streamlit UI: modified `app.py` to add 3rd tab (Stress / Sensory)
  - Blue/Purple color palette (#3A0CA3, #4361EE, #4CC9F0, #7B2FBE)
  - Matches cardiovascular tab pattern exactly (disc viz, component breakdown, interpretation, metro comparison)
  - Queries `stress_scores` table
  - Footer updated to include NASA VIIRS in data source list
- Applied all Lessons Learned from Tool 2 (CDC wide format, per-state raster, ZCTA5CE20, DRIVE_PREFIX)

**Left off at:**
- Pipeline script is written but not yet executed — needs Colab with VIIRS raster and Supabase credentials

**Next session should start with:**
1. Run `create_table.sql` in Supabase SQL Editor
2. Download NASA VIIRS annual VNL v2 composite GeoTIFF and upload to Google Drive as `viirs_vnl_v2_annual.tif`
3. Execute pipeline cells sequentially in Colab — each gate must pass before proceeding
4. After pipeline completes, verify Streamlit stress/sensory tab renders correctly
5. Run Suite 5 manual Streamlit smoke tests
6. Begin Tool 4 (Food Access) planning

**Any issues or surprises:**
- Noise data reuse originally went through raw_signals; simplified in a later session to read directly from cardiovascular_scores
- VIIRS global composite is large (~1.5GB) — per-state processing implemented to avoid RAM crashes
- Stress tool 4th component confirmed as Poor Mental Health Days (MHLTH_CrudePrev), not crowding or social isolation

### 2026-04-13 — Tool 3 Stress (Schema Cache Fix + Noise Source Simplification)
**Completed:**
- Added `NOTIFY pgrst, 'reload schema'` call to `notebooks/stress/backfill_noise_to_raw_signals.py` via `supabase.rpc("notify_pgrst")` — fixes PostgREST schema cache error when writing to newly created `raw_signals` table
- Rewrote BTS noise ingestion in `notebooks/stress/stress_pipeline.py` to read directly from `cardiovascular_scores.noise_raw` instead of `raw_signals` — eliminates the backfill script dependency for Tool 3
- Updated markdown documentation within both files to reflect new data flow
- Updated Lessons Learned section (noise reuse pattern)

**Left off at:**
- Both scripts updated and pushed to GitHub
- `notify_pgrst()` Postgres function must be created in Supabase SQL Editor before running backfill script
- Stress pipeline can now run without the backfill script (for noise — VIIRS still writes to `raw_signals`)

**Next session should start with:**
1. Run `notify_pgrst()` DDL + `create_table.sql` in Supabase SQL Editor
2. Execute stress pipeline in Colab — each gate must pass before proceeding
3. After pipeline completes, verify Streamlit stress/sensory tab renders correctly
4. Begin Tool 4 (Food Access) planning

**Any issues or surprises:**
- PostgREST schema cache must be refreshed after creating new tables — `NOTIFY pgrst, 'reload schema'` via an RPC function is the standard Supabase pattern
- The `raw_signals` table is still used by VIIRS light pollution (write) and will be used by Heat tool (read impervious surface) — only the noise read was removed from the stress pipeline

---

## Lessons Learned — Inherited by Tools 3–5

> These corrections were discovered during the Cardiovascular (Tool 2) Colab run.
> All subsequent tools MUST follow these patterns. Do not repeat the original assumptions.

### 1. CDC PLACES API is Wide Format
- **Wrong:** `$where=locationname IN (...) AND measureid IN (...)`
- **Right:** `$select=zcta5,measure1_crudeprev,measure2_crudeprev` + `$where=zcta5 IN (...)`
- The ZIP field is `zcta5`, not `locationname`. There is no `measureid` column.
- Each measure is a separate column (e.g., `lpa_crudeprev`, `chd_crudeprev`, `depression_crudeprev`).
- Use batch size 50 to stay within Socrata URL length limits.
- Response is already one row per ZIP — no pivot needed.

### 2. BTS Noise Raster: Per-State, Not Merged
- BTS data comes as individual state rasters, not a single CONUS GeoTIFF.
- **Do NOT merge state rasters** — this crashes Colab free-tier RAM.
- Use `STATE_METRO_MAP` to filter ZIPs per state, then run `zonal_stats` per state independently.
- Tool 3 (Stress) reuses noise values directly from `cardiovascular_scores.noise_raw` — no raster processing or `raw_signals` dependency needed.

### 3. NLCD Impervious Surface Nodata = 250.0
- The correct nodata value for NLCD impervious surface rasters is `250.0`, not `255`.
- Tool 5 (Heat) reuses impervious values from `raw_signals` — no raster processing needed.
- If Tool 3 or 5 processes a new NLCD raster (e.g., tree canopy), check its nodata value from raster metadata before hardcoding.

### 4. ZCTA Shapefile Column = ZCTA5CE20
- The 2020 ZCTA shapefile uses column `ZCTA5CE20` for the ZIP/ZCTA code.
- Do not use `ZCTA5CE10` (2010 vintage) — our shapefile is 2020.

### 5. Google Drive Path Prefix
- All raster and shapefile paths in Colab use:
  `/content/drive/MyDrive/Colab Notebooks/health-score-data/`
- Note the space in "Colab Notebooks" — this is the actual folder name on Drive.

---

## Environment & Credentials

- **Supabase URL:** stored as `SUPABASE_URL` in Colab secrets
- **Supabase Key:** stored as `SUPABASE_KEY` in Colab secrets  
- **Claude API Key:** stored as `ANTHROPIC_API_KEY` in Colab secrets
- **GitHub repo:** `health-score-tool`
- **Streamlit app:** deployed on Streamlit Community Cloud (linked to GitHub repo)

---

## File Locations (update as repo evolves)

```
health-score-tool/
├── CLAUDE.md                    ← auto-read by Claude Code at session start (entry point)
├── AGENTS.md                    ← full methodology, rules, schema, standards
├── TOOL_SPECS.md                ← component weights and source specs (PROPRIETARY — do not expose)
├── CONTEXT.md                   ← this file — session state and handoff log
├── ARCHITECTURE.md              ← stable system design reference
├── SESSION_KICKOFF.md           ← prompt template for starting each Claude Code session
├── data/
│   ├── zips/                    ← ZCTA shapefiles for raster aggregation
│   └── rasters/                 ← cached processed raster outputs (gitignored — too large for repo)
├── notebooks/
│   ├── respiratory/             ← REFERENCE IMPLEMENTATION — read before building any new tool
│   ├── cardiovascular/
│   ├── stress/
│   ├── food_access/
│   └── heat/
├── streamlit/
│   ├── app.py                   ← main app entry point, tab-based navigation
│   ├── tabs/
│   │   ├── respiratory.py       ← REFERENCE IMPLEMENTATION for UI pattern
│   │   ├── cardiovascular.py
│   │   └── ...
│   └── components/
│       ├── disc_viz.py          ← shared Apple Health-style disc visualization
│       └── interpretation.py    ← Claude API interpretation wrapper with caching
└── utils/
    ├── supabase_client.py       ← all Supabase interactions go through here
    ├── normalization.py         ← PROPRIETARY — gitignored on public branch
    └── scoring.py               ← PROPRIETARY — gitignored on public branch
```
