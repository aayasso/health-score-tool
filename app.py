"""
LaSalle Technologies — Respiratory Health Score
Streamlit app with Apple Health-inspired score card UI
"""

import streamlit as st
import math
import re
from supabase import create_client, Client

# ── PAGE CONFIG ──────────────────────────────────────────────
st.set_page_config(
    page_title="Respiratory Health Score",
    page_icon="🌿",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# ── STYLES ───────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=DM+Serif+Display&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
    background-color: #F7F7F5;
    color: #1A1A1A;
}

/* Hide Streamlit chrome */
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding-top: 2rem; padding-bottom: 3rem; max-width: 680px; }

/* Search box */
.stTextInput > div > div > input {
    border: 2px solid #E0E0DB;
    border-radius: 14px;
    padding: 14px 18px;
    font-size: 1.1rem;
    font-family: 'DM Sans', sans-serif;
    background: #FFFFFF;
    color: #1A1A1A;
    box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    transition: border-color 0.2s;
}
.stTextInput > div > div > input:focus {
    border-color: #4A7C59;
    box-shadow: 0 0 0 3px rgba(74,124,89,0.12);
}

/* Cards */
.card {
    background: #FFFFFF;
    border-radius: 20px;
    padding: 28px;
    margin-bottom: 16px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.06);
    border: 1px solid #EBEBEB;
}
.card-tight {
    background: #FFFFFF;
    border-radius: 20px;
    padding: 22px 28px;
    margin-bottom: 16px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.06);
    border: 1px solid #EBEBEB;
}

/* Grade badge */
.grade-badge {
    display: inline-block;
    font-family: 'DM Serif Display', serif;
    font-size: 3rem;
    line-height: 1;
    color: #1A1A1A;
}

/* Component row */
.component-row {
    display: flex;
    align-items: flex-start;
    padding: 14px 0;
    border-bottom: 1px solid #F0F0EC;
    gap: 14px;
}
.component-row:last-child { border-bottom: none; }
.comp-dot {
    width: 12px;
    height: 12px;
    border-radius: 50%;
    margin-top: 5px;
    flex-shrink: 0;
}
.comp-label {
    font-weight: 600;
    font-size: 0.95rem;
    color: #1A1A1A;
}
.comp-sublabel {
    font-size: 0.82rem;
    color: #888;
    margin-top: 2px;
}
.comp-score {
    margin-left: auto;
    font-weight: 600;
    font-size: 0.95rem;
    color: #1A1A1A;
    white-space: nowrap;
}

/* Interpretation text */
.interp-text {
    font-size: 0.97rem;
    line-height: 1.7;
    color: #3A3A3A;
}

/* Percentile pills */
.pill-row { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 4px; }
.pill {
    background: #F0F5F2;
    color: #3A6647;
    border-radius: 20px;
    padding: 5px 14px;
    font-size: 0.82rem;
    font-weight: 500;
}

/* Metro comparison table */
.metro-row {
    display: flex;
    align-items: center;
    padding: 10px 0;
    border-bottom: 1px solid #F0F0EC;
    gap: 12px;
    font-size: 0.9rem;
}
.metro-row:last-child { border-bottom: none; }
.metro-zip { font-weight: 600; color: #1A1A1A; min-width: 60px; }
.metro-bar-wrap { flex: 1; background: #F0F0EC; border-radius: 4px; height: 8px; overflow: hidden; }
.metro-bar { height: 8px; border-radius: 4px; }
.metro-score { min-width: 40px; text-align: right; color: #666; font-size: 0.85rem; }
.metro-grade { min-width: 28px; text-align: right; font-weight: 600; }

/* Expander */
details > summary {
    font-size: 0.9rem;
    color: #4A7C59;
    font-weight: 500;
    cursor: pointer;
    padding: 8px 0;
    list-style: none;
}
details > summary::-webkit-details-marker { display: none; }
details > summary::before { content: "▸  "; }
details[open] > summary::before { content: "▾  "; }

/* Section header */
.section-header {
    font-family: 'DM Serif Display', serif;
    font-size: 1.25rem;
    color: #1A1A1A;
    margin-bottom: 4px;
}
.section-sub {
    font-size: 0.85rem;
    color: #999;
    margin-bottom: 16px;
}

/* App header */
.app-header {
    text-align: center;
    margin-bottom: 2rem;
}
.app-title {
    font-family: 'DM Serif Display', serif;
    font-size: 1.9rem;
    color: #1A1A1A;
    letter-spacing: -0.5px;
}
.app-subtitle {
    font-size: 0.9rem;
    color: #999;
    margin-top: 4px;
}

/* Error / info */
.info-box {
    background: #F0F5F2;
    border-radius: 14px;
    padding: 18px 22px;
    color: #3A6647;
    font-size: 0.92rem;
}
</style>
""", unsafe_allow_html=True)


# ── SUPABASE ──────────────────────────────────────────────────
@st.cache_resource
def get_supabase() -> Client:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = get_supabase()


# ── DATA HELPERS ──────────────────────────────────────────────
@st.cache_data(ttl=3600)
def fetch_score(zipcode: str):
    r = supabase.table("composite_scores")\
        .select("*")\
        .eq("zipcode", zipcode)\
        .eq("score_dimension", "respiratory")\
        .limit(1).execute()
    return r.data[0] if r.data else None

@st.cache_data(ttl=3600)
def fetch_interpretation(zipcode: str):
    r = supabase.table("interpretations")\
        .select("interpretation_text,key_strengths,key_concerns")\
        .eq("zipcode", zipcode)\
        .eq("score_dimension", "respiratory")\
        .limit(1).execute()
    return r.data[0] if r.data else None

@st.cache_data(ttl=3600)
def fetch_zip_meta(zipcode: str):
    r = supabase.table("zip_codes")\
        .select("metro,state")\
        .eq("zipcode", zipcode)\
        .limit(1).execute()
    return r.data[0] if r.data else None

@st.cache_data(ttl=3600)
def fetch_metro_peers(metro: str, limit: int = 12):
    r = supabase.table("composite_scores")\
        .select("zipcode,composite_score,score_grade,metro_percentile")\
        .eq("score_dimension", "respiratory")\
        .eq("metro", metro)\
        .order("composite_score", desc=True)\
        .limit(limit).execute()
    return r.data


# ── CONSTANTS ─────────────────────────────────────────────────
COMPONENT_CONFIG = {
    "air_quality": {
        "label":    "Air Quality",
        "color":    "#4A7C59",   # deep green
        "weight":   40,
        "sublabel": "Outdoor air pollution levels",
        "explain":  "Measures three key air pollutants in your neighborhood: fine particles (PM2.5) from traffic and industry that penetrate deep into lungs; ozone, a ground-level gas that irritates airways; and nitrogen dioxide (NO2) from vehicles and power plants. High scores mean cleaner air.",
    },
    "green_cover": {
        "label":    "Green Cover",
        "color":    "#82B89A",   # medium green
        "weight":   20,
        "sublabel": "Tree canopy & vegetation",
        "explain":  "Reflects how much of the neighborhood is covered by trees and vegetation. Green spaces filter air pollutants, reduce heat, and are linked to better respiratory and mental health. Higher scores mean more tree canopy.",
    },
    "environmental_burden": {
        "label":    "Environmental Burden",
        "color":    "#2D5C3F",   # darkest green
        "weight":   25,
        "sublabel": "Industrial & traffic exposure",
        "explain":  "Captures cumulative environmental stressors beyond air quality alone — proximity to diesel traffic, industrial facilities, and other pollution sources that disproportionately affect some neighborhoods. Lower burden = higher score.",
    },
    "health_outcomes": {
        "label":    "Health Outcomes",
        "color":    "#A8D5B5",   # lightest green
        "weight":   15,
        "sublabel": "Local respiratory disease rates",
        "explain":  "Reflects actual asthma and COPD rates among residents in this ZIP code, based on CDC health surveys. This captures the cumulative effect of environmental conditions on real people's health. Higher scores mean lower disease rates.",
    },
}

GRADE_DESCRIPTIONS = {
    "A": ("Excellent", "#2D7D46", "This neighborhood has among the cleanest air and lowest respiratory health burdens in the country. A strong choice for families, seniors, or anyone with respiratory sensitivities."),
    "B": ("Good",      "#5A9E6F", "Air quality and environmental conditions here are above average. Most residents experience a healthy respiratory environment with only minor concerns."),
    "C": ("Fair",      "#C8882A", "This neighborhood has moderate respiratory health conditions — better than many urban areas, but some factors warrant attention, particularly for sensitive groups."),
    "D": ("Poor",      "#C85A2A", "Elevated air pollution or environmental burdens create meaningful respiratory health risks here. Those with asthma or lung conditions should factor this in carefully."),
    "F": ("Very Poor", "#B02020", "This neighborhood faces significant air quality and environmental health challenges. Respiratory health risks are substantially higher than the national average."),
}


# ── SVG DISC ──────────────────────────────────────────────────
def make_disc_svg(component_scores: dict, composite: float, grade: str) -> str:
    """
    Renders an Apple Health-style multi-arc disc.
    Each arc corresponds to one component, sized by weight,
    filled proportionally to that component's normalized score.
    """
    size       = 220
    cx, cy     = size / 2, size / 2
    stroke_w   = 16
    gap_deg    = 3          # gap between arcs in degrees
    order      = ["air_quality", "environmental_burden", "green_cover", "health_outcomes"]
    radii      = [82, 64, 46, 28]   # outer → inner rings

    def arc_path(cx, cy, r, start_deg, end_deg):
        start_rad = math.radians(start_deg - 90)
        end_rad   = math.radians(end_deg - 90)
        x1 = cx + r * math.cos(start_rad)
        y1 = cy + r * math.sin(start_rad)
        x2 = cx + r * math.cos(end_rad)
        y2 = cy + r * math.sin(end_rad)
        large = 1 if (end_deg - start_deg) > 180 else 0
        return f"M {x1:.2f} {y1:.2f} A {r} {r} 0 {large} 1 {x2:.2f} {y2:.2f}"

    paths = []
    for i, key in enumerate(order):
        cfg   = COMPONENT_CONFIG[key]
        r     = radii[i]
        color = cfg["color"]
        score = component_scores.get(key, 0) or 0   # 0-100

        # Arc spans full 360 minus small gap
        arc_span = 360 - gap_deg
        fill_deg = arc_span * (score / 100)

        # Background track
        paths.append(
            f'<path d="{arc_path(cx, cy, r, 0, arc_span)}" '
            f'fill="none" stroke="{color}" stroke-width="{stroke_w}" '
            f'stroke-opacity="0.15" stroke-linecap="round"/>'
        )
        # Filled arc
        if fill_deg > 1:
            paths.append(
                f'<path d="{arc_path(cx, cy, r, 0, fill_deg)}" '
                f'fill="none" stroke="{color}" stroke-width="{stroke_w}" '
                f'stroke-linecap="round"/>'
            )

    grade_color = GRADE_DESCRIPTIONS.get(grade, ("", "#1A1A1A", ""))[1]

    svg = f"""
    <svg width="{size}" height="{size}" viewBox="0 0 {size} {size}"
         xmlns="http://www.w3.org/2000/svg">
      {''.join(paths)}
      <text x="{cx}" y="{cy - 10}" text-anchor="middle"
            font-family="'DM Sans', sans-serif"
            font-size="32" font-weight="600" fill="{grade_color}">{composite:.0f}</text>
      <text x="{cx}" y="{cy + 16}" text-anchor="middle"
            font-family="'DM Serif Display', serif"
            font-size="22" fill="{grade_color}">{grade}</text>
    </svg>
    """
    return svg


# ── CLEAN INTERPRETATION TEXT ─────────────────────────────────
def clean_interp(text: str) -> str:
    if not text:
        return ""
    # Strip markdown headers
    text = re.sub(r"^#+\s.*\n?", "", text, flags=re.MULTILINE)
    text = re.sub(r"\*\*.*?\*\*\n?", "", text)
    return text.strip()


# ── MAIN APP ──────────────────────────────────────────────────
st.markdown("""
<div class="app-header">
  <div class="app-title">🌿 Respiratory Health Score</div>
  <div class="app-subtitle">Neighborhood air quality &amp; environmental health intelligence</div>
</div>
""", unsafe_allow_html=True)

# Search
zip_input = st.text_input(
    "", placeholder="Enter a ZIP code  (e.g. 15213, 90210, 28277)",
    label_visibility="collapsed"
)

if not zip_input:
    st.markdown("""
    <div class="info-box">
    Enter any ZIP code in Pittsburgh, Los Angeles, Phoenix, or Charlotte
    to see its Respiratory Health Score — powered by EPA, CDC, and environmental data.
    </div>
    """, unsafe_allow_html=True)
    st.stop()

zipcode = zip_input.strip().zfill(5)

# Fetch data
with st.spinner("Loading score…"):
    score_data  = fetch_score(zipcode)
    interp_data = fetch_interpretation(zipcode)
    zip_meta    = fetch_zip_meta(zipcode)

if not score_data:
    st.markdown(f"""
    <div class="info-box" style="background:#FFF5F0;color:#8B3A2A;">
    No data found for ZIP code <strong>{zipcode}</strong>.
    This MVP covers Pittsburgh, Los Angeles, Phoenix, and Charlotte.
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# ── PARSE DATA ────────────────────────────────────────────────
composite   = score_data["composite_score"]
grade       = score_data["score_grade"]
nat_pct     = score_data.get("national_percentile", 0) or 0
metro_pct   = score_data.get("metro_percentile", 0) or 0
comp_scores = score_data.get("component_scores") or {}
metro       = score_data.get("metro") or (zip_meta.get("metro") if zip_meta else "")

# ── SCORE CARD ────────────────────────────────────────────────
grade_label, grade_color, grade_desc = GRADE_DESCRIPTIONS.get(
    grade, ("Unknown", "#666", "")
)

disc_svg = make_disc_svg(comp_scores, composite, grade)

col1, col2 = st.columns([1, 1.3], gap="medium")

with col1:
    st.markdown(disc_svg, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div style="padding-top:16px;">
      <div style="font-size:0.82rem;color:#999;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:4px;">
        ZIP {zipcode} · {metro}
      </div>
      <div style="font-family:'DM Serif Display',serif;font-size:2rem;color:{grade_color};line-height:1.1;">
        {grade_label}
      </div>
      <div style="font-size:0.9rem;color:#555;margin-top:8px;line-height:1.6;">
        {grade_desc}
      </div>
      <div class="pill-row" style="margin-top:14px;">
        <span class="pill">Top {100 - int(nat_pct)}% nationally</span>
        <span class="pill">Top {100 - int(metro_pct)}% in {metro}</span>
      </div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

# ── COMPONENT BREAKDOWN ───────────────────────────────────────
st.markdown('<div class="card">', unsafe_allow_html=True)
st.markdown('<div class="section-header">Score Breakdown</div>', unsafe_allow_html=True)
st.markdown('<div class="section-sub">Four factors combine to form the overall score</div>', unsafe_allow_html=True)

for key, cfg in COMPONENT_CONFIG.items():
    raw   = comp_scores.get(key)
    label = cfg["label"]
    color = cfg["color"]
    wt    = cfg["weight"]
    sub   = cfg["sublabel"]

    if raw is not None:
        component_pts = round(raw * wt / 100, 1)
        score_str     = f"{component_pts:.0f}/{wt}"
    else:
        score_str = "—"

    st.markdown(f"""
    <div class="component-row">
      <div class="comp-dot" style="background:{color}"></div>
      <div style="flex:1;">
        <div class="comp-label">{label}</div>
        <div class="comp-sublabel">{sub}</div>
      </div>
      <div class="comp-score">{score_str}</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("</div>", unsafe_allow_html=True)

# ── INTERPRETATION ────────────────────────────────────────────
if interp_data:
    clean = clean_interp(interp_data.get("interpretation_text", ""))
    if clean:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="section-header">About This ZIP Code</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="interp-text">{clean}</div>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

# ── COMPONENT EXPLANATIONS ────────────────────────────────────
st.markdown('<div class="card">', unsafe_allow_html=True)
st.markdown('<div class="section-header">How the Score Is Calculated</div>', unsafe_allow_html=True)
st.markdown('<div class="section-sub">Tap any factor to learn more</div>', unsafe_allow_html=True)

for key, cfg in COMPONENT_CONFIG.items():
    st.markdown(f"""
    <details>
      <summary>
        <span style="display:inline-block;width:10px;height:10px;border-radius:50%;
          background:{cfg['color']};margin-right:8px;vertical-align:middle;"></span>
        <strong>{cfg['label']}</strong>
        <span style="color:#999;font-weight:400;"> · {cfg['weight']}% of total</span>
      </summary>
      <div style="padding:10px 0 6px 22px;font-size:0.88rem;color:#555;line-height:1.65;">
        {cfg['explain']}
      </div>
    </details>
    """, unsafe_allow_html=True)

st.markdown("""
<details>
  <summary><strong>Grade Scale</strong></summary>
  <div style="padding:10px 0 6px 22px;font-size:0.88rem;color:#555;line-height:1.9;">
    <strong style="color:#2D7D46">A (70–100)</strong> — Excellent respiratory health environment<br>
    <strong style="color:#5A9E6F">B (55–69)</strong> — Above average air quality conditions<br>
    <strong style="color:#C8882A">C (40–54)</strong> — Moderate conditions; some concerns<br>
    <strong style="color:#C85A2A">D (25–39)</strong> — Elevated pollution and health burden<br>
    <strong style="color:#B02020">F (0–24)</strong> — Significant respiratory health risks
  </div>
</details>
""", unsafe_allow_html=True)

st.markdown("</div>", unsafe_allow_html=True)

# ── METRO COMPARISON ──────────────────────────────────────────
if metro:
    with st.expander(f"📊  Compare to other ZIP codes in {metro}"):
        peers = fetch_metro_peers(metro, limit=15)
        if peers:
            max_score = max(p["composite_score"] for p in peers)
            st.markdown('<div style="margin-top:8px">', unsafe_allow_html=True)
            for p in peers:
                z        = p["zipcode"]
                s        = p["composite_score"]
                g        = p["score_grade"]
                bar_pct  = int((s / max_score) * 100) if max_score else 0
                is_curr  = "font-weight:700;" if z == zipcode else ""
                bg_color = GRADE_DESCRIPTIONS.get(g, ("", "#888", ""))[1]
                highlight = "background:#F0F5F2;border-radius:8px;padding:0 6px;" if z == zipcode else ""

                st.markdown(f"""
                <div class="metro-row" style="{highlight}">
                  <div class="metro-zip" style="{is_curr}">{z}</div>
                  <div class="metro-bar-wrap">
                    <div class="metro-bar" style="width:{bar_pct}%;background:{bg_color}"></div>
                  </div>
                  <div class="metro-score">{s:.0f}</div>
                  <div class="metro-grade" style="color:{bg_color}">{g}</div>
                </div>
                """, unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

# ── FOOTER ────────────────────────────────────────────────────
st.markdown("""
<div style="text-align:center;margin-top:2rem;font-size:0.78rem;color:#BBB;">
  LaSalle Technologies · Data: EPA AQS, CDC PLACES, EJScreen, NLCD · 2023
</div>
""", unsafe_allow_html=True)
