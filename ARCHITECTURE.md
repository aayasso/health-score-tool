# ARCHITECTURE.md — System Design Reference
> Stable reference for infrastructure decisions. Update only when a structural change is made.

---

## System Overview

```
[Federal Data Sources]
        ↓
[Google Colab Notebooks]  ← rasterio, geopandas, requests, pandas
        ↓
[Supabase PostgreSQL]     ← persistent storage, upsert pattern, REST API auto-exposed
        ↓
[Streamlit Community Cloud] ← current public-facing tool (being replaced)
[Lovable / React]           ← next frontend — consumes Supabase REST API directly
        ↓
[Claude API]              ← plain-language interpretation layer (stored, not live-called)
```

**Supabase project ref:** `hakiksjnpipgstomzzjy`
**Streamlit app:** `health-score-tool-gnoxoobgjrakzvwnj4ktec.streamlit.app`
**LaSalle site:** `lasalletech.ai` (built in Lovable)

---

## Data Flow Per Tool

```
1. INGEST      Pull raw data from federal source (API or raster download)
2. VALIDATE    Coverage check, null audit, outlier flags → print report
3. NORMALIZE   Min-max per component, global across all 600 ZIPs, 0–100
4. SCORE       Weighted composite → letter grade
5. INTERPRET   Claude API prompt → plain-language summary
6. WRITE       Upsert to Supabase [tool]_scores table
7. VERIFY      Query Supabase, spot-check 10 ZIPs across all 8 metros
```

---

## Supabase Design Principles

- One table per tool for scores (clean separation, easy querying per dimension)
- 6 score tables: `composite_scores`, `cardiovascular_scores`, `stress_scores`, `food_access_scores`, `heat_scores`, `overall_scores`
- All score tables include `score_date` column for historical tracking / versioning
- Shared `zip_codes` master table — never duplicate ZIP metadata
- `score_config` table holds methodology config — never queried by frontend (internal only)
- All tables use `UNIQUE(zipcode)` constraint to enable safe upserts
- Supabase auto-exposes REST endpoints per table — Lovable can fetch directly using anon key
- `updated_at` timestamp on every row for auditability — requires a Postgres trigger to auto-update on row change:

```sql
-- Run once in Supabase SQL editor for each table
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_updated_at
BEFORE UPDATE ON [tool]_scores
FOR EACH ROW EXECUTE FUNCTION update_updated_at();
-- Replace [tool]_scores with the actual table name for each tool
```

---

## Frontend Architecture

### Streamlit (Current)
- Single `app.py` entry point with tab-based navigation
- Each tool is a self-contained module in `streamlit/tabs/`
- Shared components in `streamlit/components/`:
  - `disc_viz.py` — the Apple Health-style disc visualization (parameterized by color palette and components)
  - `interpretation.py` — Claude API call wrapper with caching
- All Supabase queries go through `utils/supabase_client.py`
- No raw SQL in tab files — use the client utility

### Lovable / React (Next)
- Target: replace Streamlit with React components at `lasalletech.ai`
- **Architecture:** Lovable → Supabase REST API (anon key) → score tables
- No middleware or backend proxy needed — Supabase auto-exposes REST endpoints per table
- All data is pre-computed and stored; frontend is purely a read layer
- Disc SVG visualization must be rebuilt in React/JavaScript
- Must maintain exact same component configs, colors, and grade scales as current `app.py`
- Must maintain public/proprietary boundary — no weights or methodology in frontend code

---

## Raster Processing Architecture

Rasters are large files processed once and cached. The pattern:

```
Download GeoTIFF → Process in Colab (rasterio + geopandas) → 
Extract ZIP-level values → Save as CSV → Upload to Supabase raw_signals
```

**Never re-download a raster that's already been processed.** Before any raster processing step, check whether values already exist in `raw_signals` using this pattern:

```python
def raster_already_processed(supabase, source_name: str, min_expected_rows: int = 550) -> bool:
    """
    Check if a raster has already been processed and written to raw_signals.
    source_name: e.g. "bts_noise", "nlcd_impervious", "nlcd_tree_canopy", "nasa_viirs", "usgs_heat"
    Returns True if enough rows exist to skip reprocessing.
    """
    result = supabase.table("raw_signals") \
        .select("zip_code", count="exact") \
        .eq("source", source_name) \
        .execute()
    row_count = result.count or 0
    if row_count >= min_expected_rows:
        print(f"✓ {source_name} already processed ({row_count} ZIPs found) — skipping download")
        return True
    print(f"✗ {source_name} not yet processed ({row_count} rows found, need {min_expected_rows}) — proceeding")
    return False
```

Reuse map:
- BTS noise → Cardiovascular + Stress (process once in Tool 2)
- NLCD impervious → Cardiovascular + Heat (process once in Tool 2)
- NLCD tree canopy → Stress + Heat (process once in Tool 3)

---

## Privacy & IP Architecture

Two-layer protection for proprietary methodology:

**Layer 1 — Code separation**
- `utils/normalization.py` and `utils/scoring.py` contain the proprietary logic
- These files are excluded from any public documentation or README examples
- If the repo is ever made partially public, these files stay private

**Layer 2 — UI separation**
- Streamlit UI displays scores and grades only
- Component descriptions are qualitative ("Transportation noise levels in your area")
- No weights, formulas, or normalization details appear anywhere in the UI

---

## Deployment

- **Streamlit (current):** Deployed via Streamlit Community Cloud, linked to `main` branch of GitHub repo
  - App URL: `health-score-tool-gnoxoobgjrakzvwnj4ktec.streamlit.app`
  - Deploy trigger: Push to `main` → automatic redeploy
  - Secrets: `SUPABASE_URL`, `SUPABASE_KEY`, `ANTHROPIC_API_KEY` in Streamlit secrets manager
- **Lovable (next):** React frontend at `lasalletech.ai`, consuming Supabase REST API directly
  - Supabase project ref: `hakiksjnpipgstomzzjy`
  - No backend proxy needed — Lovable fetches via Supabase anon key
- **Colab notebooks:** Not deployed — run manually for data processing phases

---

## Scalability Notes

Current scope: ~1,290 ZIPs scored, 8 metros, all 5 tools complete.

Design decisions made with future scale in mind:
- Upsert pattern supports expanding to new ZIPs without schema changes
- Per-tool tables support adding new tools without restructuring
- Global normalization will need recalibration when new metros are added (document any recalibration)
- Claude API interpretations are generated once and stored — not called on every user request
- `score_date` column on all tables enables longitudinal tracking — annual refreshes planned
- Validation against actual health outcomes (hospital admissions, chronic disease prevalence) is a planned future study

---

## Known Constraints

- Google Colab free tier has RAM limits — large rasters (NLCD CONUS) may require Colab Pro or chunked processing
- Streamlit Community Cloud has resource limits — keep Supabase queries efficient
- CDC PLACES Socrata API rate limits — batch ZIP queries, don't query one at a time
