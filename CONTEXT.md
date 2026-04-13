# CONTEXT.md — Current Project State & Session Notes
> Update this file at the end of every Claude Code session. It is the handoff document between sessions.
> Keep it current — stale context is worse than no context.

---

## Current Status (as of April 2026)

### Active Phase
**Tool 2 — ❤️ Cardiovascular Health Score**

### What's Complete
- Tool 1 (Respiratory) is fully live on Streamlit Community Cloud
- Supabase schema established: `zip_codes`, `raw_signals`, `score_components`, `composite_scores`, `interpretations`, `score_config`
- Respiratory reference implementation is the pattern for all subsequent tools
- 600 ZIP codes confirmed across Pittsburgh, LA, Phoenix, Charlotte

### What's In Progress
- Cardiovascular data ingestion (CDC PLACES components confirmed, raster processing TBD)

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
