# 🌿 Respiratory Health Score
**LaSalle Technologies** · Neighborhood air quality & environmental health intelligence

---

## Overview

A ZIP code-level respiratory health scoring tool covering four major U.S. metros: **Pittsburgh, Los Angeles, Phoenix, and Charlotte**. Enter any ZIP code to see a composite Respiratory Health Score (0–100, graded A–F), a breakdown of the four contributing factors, an AI-generated plain-language interpretation, and a comparison to peer ZIP codes in the same metro.

Built as an MVP data product demonstrating the market opportunity in location intelligence for real estate developers, homebuilders, and platforms like Zillow and Redfin.

---

## Score Methodology

The composite score is a weighted average of four components:

| Component | Weight | Data Source |
|---|---|---|
| Air Quality (PM2.5, ozone, NO2) | 40% | EPA AQS |
| Environmental Burden (diesel, traffic, industrial) | 25% | EJScreen |
| Green Cover (tree canopy) | 20% | NLCD 2021 |
| Health Outcomes (asthma, COPD prevalence) | 15% | CDC PLACES |

All components are min-max normalized to 0–100 before weighting. Higher scores = healthier respiratory environment.

**Grade thresholds:**
- A: 70–100
- B: 55–69
- C: 40–54
- D: 25–39
- F: 0–24

---

## Tech Stack

| Layer | Tool |
|---|---|
| Frontend | Streamlit |
| Database | Supabase (PostgreSQL) |
| Interpretations | Anthropic Claude Haiku |
| Data processing | Google Colab + Python |

---

## Local Development

**1. Clone the repo**
```bash
git clone https://github.com/your-org/respiratory-health-score.git
cd respiratory-health-score
```

**2. Install dependencies**
```bash
pip install -r requirements.txt
```

**3. Add secrets**

Create `.streamlit/secrets.toml`:
```toml
SUPABASE_URL = "https://your-project.supabase.co"
SUPABASE_KEY = "your-anon-key"
```

**4. Run**
```bash
streamlit run app.py
```

---

## Deployment (Streamlit Community Cloud)

1. Push repo to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io) → **New app**
3. Connect your GitHub repo, set main file to `app.py`
4. Under **Settings → Secrets**, paste:
```toml
SUPABASE_URL = "https://your-project.supabase.co"
SUPABASE_KEY = "your-anon-key"
```
5. Click **Deploy** — live in ~2 minutes

---

## Coverage

**600 ZIP codes across 4 metros:**
- Pittsburgh, PA
- Los Angeles, CA
- Phoenix, AZ
- Charlotte, NC

---

## Repo Structure

```
├── app.py               # Streamlit application
├── requirements.txt     # Python dependencies
└── README.md            # This file
```

---

## Data Notes

- Air quality values are county-level interpolated where monitor coverage is sparse
- Tree canopy uses NLCD 2021 county-level averages as MVP fallback; ZCTA-level values via Google Earth Engine are the planned upgrade
- CDC PLACES health outcomes suppressed for ~4% of ZIP codes with small populations; these ZIPs receive partial weight scores
- All data vintaged to 2023 except tree canopy (2021)

---

*LaSalle Technologies · Built to last.*
