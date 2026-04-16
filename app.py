"""
LaSalle Technologies — Health Environment Score
Streamlit app with Apple Health-inspired score card UI
Multi-tool tabbed interface: Respiratory · Cardiovascular
"""

import streamlit as st
import math
import re
from supabase import create_client, Client

# ── PAGE CONFIG ──────────────────────────────────────────────
st.set_page_config(
    page_title="Health Environment Score",
    page_icon="🏥",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# ── STYLES ───────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,wght@0,300;0,400;0,500;0,600;1,400&family=DM+Serif+Display&display=swap');

html, body, [class*="css"], .stApp {
    font-family: 'DM Sans', sans-serif !important;
    background-color: #F5F5F2 !important;
    color: #1A1A1A !important;
}

.stApp { background-color: #F5F5F2 !important; }

#MainMenu, footer, header { visibility: hidden; }
.block-container {
    padding-top: 2.5rem;
    padding-bottom: 3rem;
    max-width: 660px;
}

.stTextInput > div > div > input {
    border: 2px solid #DDDDD8 !important;
    border-radius: 14px !important;
    padding: 14px 18px !important;
    font-size: 1.05rem !important;
    font-family: 'DM Sans', sans-serif !important;
    background: #FFFFFF !important;
    color: #1A1A1A !important;
    box-shadow: 0 1px 6px rgba(0,0,0,0.05) !important;
}
.stTextInput > div > div > input:focus {
    border-color: #3A7D54 !important;
    box-shadow: 0 0 0 3px rgba(58,125,84,0.1) !important;
}

.streamlit-expanderHeader {
    background: #FFFFFF !important;
    border-radius: 14px !important;
    border: 1px solid #E8E8E3 !important;
    color: #3A7D54 !important;
    font-family: 'DM Sans', sans-serif !important;
    font-weight: 500 !important;
}
.streamlit-expanderContent {
    background: #FFFFFF !important;
    border: 1px solid #E8E8E3 !important;
    border-top: none !important;
    border-radius: 0 0 14px 14px !important;
}

.card {
    background: #FFFFFF;
    border-radius: 20px;
    padding: 26px 28px;
    margin-bottom: 14px;
    box-shadow: 0 1px 8px rgba(0,0,0,0.06);
    border: 1px solid #E8E8E3;
}

.section-header {
    font-family: 'DM Serif Display', serif;
    font-size: 1.15rem;
    color: #1A1A1A;
    margin-bottom: 2px;
}
.section-sub {
    font-size: 0.82rem;
    color: #AAAAAA;
    margin-bottom: 14px;
}

.comp-row {
    display: flex;
    align-items: center;
    padding: 12px 0;
    border-bottom: 1px solid #F2F2EE;
    gap: 12px;
}
.comp-row:last-child { border-bottom: none; }
.comp-dot {
    width: 11px;
    height: 11px;
    border-radius: 50%;
    flex-shrink: 0;
}
.comp-info { flex: 1; min-width: 0; }
.comp-label {
    font-size: 0.93rem;
    font-weight: 600;
    color: #1A1A1A;
    line-height: 1.3;
}
.comp-sub {
    font-size: 0.79rem;
    color: #AAAAAA;
    margin-top: 1px;
}
.comp-score {
    font-size: 0.93rem;
    font-weight: 600;
    color: #444;
    white-space: nowrap;
    flex-shrink: 0;
}

.interp-text {
    font-size: 0.95rem;
    line-height: 1.75;
    color: #3A3A3A;
}

.pill {
    display: inline-block;
    background: #EEF5F1;
    color: #2D6644;
    border-radius: 20px;
    padding: 5px 13px;
    font-size: 0.80rem;
    font-weight: 500;
    margin-right: 6px;
    margin-top: 6px;
}

.metro-row {
    display: flex;
    align-items: center;
    padding: 9px 0;
    border-bottom: 1px solid #F2F2EE;
    gap: 10px;
    font-size: 0.88rem;
}
.metro-row:last-child { border-bottom: none; }
.metro-zip { font-weight: 600; color: #1A1A1A; min-width: 58px; }
.metro-bar-wrap {
    flex: 1;
    background: #F0F0EC;
    border-radius: 4px;
    height: 7px;
    overflow: hidden;
}
.metro-bar { height: 7px; border-radius: 4px; }
.metro-score { min-width: 36px; text-align: right; color: #888; font-size: 0.82rem; }
.metro-grade { min-width: 24px; text-align: right; font-weight: 700; font-size: 0.88rem; }

details {
    border-bottom: 1px solid #F2F2EE;
    padding: 4px 0;
}
details:last-of-type { border-bottom: none; }
details > summary {
    font-size: 0.88rem;
    color: #2D6644;
    font-weight: 500;
    cursor: pointer;
    padding: 8px 0;
    list-style: none;
    display: flex;
    align-items: center;
    gap: 8px;
}
details > summary::-webkit-details-marker { display: none; }
details > div {
    padding: 4px 0 10px 22px;
    font-size: 0.86rem;
    color: #555;
    line-height: 1.65;
}

.app-title {
    font-family: 'DM Serif Display', serif;
    font-size: 1.75rem;
    color: #1A1A1A;
    letter-spacing: -0.3px;
    text-align: center;
}
.app-sub {
    font-size: 0.85rem;
    color: #AAAAAA;
    text-align: center;
    margin-top: 4px;
    margin-bottom: 1.8rem;
}

.info-box {
    background: #EEF5F1;
    border-radius: 14px;
    padding: 16px 20px;
    color: #2D6644;
    font-size: 0.9rem;
    line-height: 1.6;
}
.info-box-cv {
    background: #FFF2EE;
    border-radius: 14px;
    padding: 16px 20px;
    color: #8B3A2A;
    font-size: 0.9rem;
    line-height: 1.6;
}
.error-box {
    background: #FFF2EE;
    border-radius: 14px;
    padding: 16px 20px;
    color: #8B3A2A;
    font-size: 0.9rem;
}

/* Tab labels */
.stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.95rem !important;
    font-weight: 500 !important;
    color: #1A1A1A !important;
}
.stTabs [data-baseweb="tab-list"] button[aria-selected="true"] [data-testid="stMarkdownContainer"] p {
    font-weight: 600 !important;
    color: #1A1A1A !important;
}
.stTabs [data-baseweb="tab-highlight"] {
    background-color: #3A7D54 !important;
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
    r = supabase.table("respiratory_scores")\
        .select("*")\
        .eq("zipcode", zipcode)\
        .limit(1).execute()
    if not r.data:
        return None
    row = r.data[0]
    # Build component_scores dict and map column names for UI compatibility
    row["score_grade"] = row.get("letter_grade")
    row["component_scores"] = {
        "air_quality": row.get("air_quality_normalized"),
        "environmental_burden": row.get("environmental_burden_normalized"),
        "green_cover": row.get("green_cover_normalized"),
        "health_outcomes": row.get("health_outcomes_normalized"),
    }
    return row

@st.cache_data(ttl=3600)
def fetch_interpretation(zipcode: str):
    r = supabase.table("respiratory_scores")\
        .select("interpretation")\
        .eq("zipcode", zipcode)\
        .limit(1).execute()
    if not r.data:
        return None
    # Map to legacy format expected by UI
    return {"interpretation_text": r.data[0].get("interpretation", "")}

@st.cache_data(ttl=3600)
def fetch_zip_meta(zipcode: str):
    r = supabase.table("zip_codes")\
        .select("metro,state")\
        .eq("zipcode", zipcode)\
        .limit(1).execute()
    return r.data[0] if r.data else None

@st.cache_data(ttl=3600)
def fetch_metro_peers(metro: str, limit: int = 15):
    # Step 1: get all zipcodes in this metro
    zips_resp = supabase.table("zip_codes")\
        .select("zipcode")\
        .eq("metro", metro)\
        .execute()
    if not zips_resp.data:
        return []

    metro_zips = [r["zipcode"] for r in zips_resp.data]

    # Step 2: fetch scores in batches (Supabase IN limit ~100)
    all_scores = []
    batch_size = 50
    for i in range(0, len(metro_zips), batch_size):
        batch = metro_zips[i:i + batch_size]
        resp = supabase.table("respiratory_scores")\
            .select("zipcode,composite_score,letter_grade")\
            .in_("zipcode", batch)\
            .execute()
        # Map letter_grade → score_grade for UI compatibility
        for row in resp.data:
            row["score_grade"] = row.get("letter_grade")
            row["metro_percentile"] = None
        all_scores.extend(resp.data)

    all_scores.sort(key=lambda x: x["composite_score"], reverse=True)
    return all_scores[:limit]


# ── CARDIOVASCULAR DATA HELPERS ───────────────────────────────
@st.cache_data(ttl=3600)
def fetch_cv_score(zipcode: str):
    r = supabase.table("cardiovascular_scores")\
        .select("*")\
        .eq("zipcode", zipcode)\
        .limit(1).execute()
    return r.data[0] if r.data else None

@st.cache_data(ttl=3600)
def fetch_cv_metro_peers(metro: str, limit: int = 15):
    zips_resp = supabase.table("zip_codes")\
        .select("zipcode")\
        .eq("metro", metro)\
        .execute()
    if not zips_resp.data:
        return []
    metro_zips = [r["zipcode"] for r in zips_resp.data]
    all_scores = []
    batch_size = 50
    for i in range(0, len(metro_zips), batch_size):
        batch = metro_zips[i:i + batch_size]
        resp = supabase.table("cardiovascular_scores")\
            .select("zipcode,composite_score,letter_grade")\
            .in_("zipcode", batch)\
            .execute()
        all_scores.extend(resp.data)
    all_scores.sort(key=lambda x: x["composite_score"], reverse=True)
    return all_scores[:limit]


# ── RESPIRATORY CONSTANTS ─────────────────────────────────────
COMPONENT_CONFIG = {
    "air_quality": {
        "label":   "Air Quality",
        "color":   "#1A6B3C",
        "weight":  40,
        "sublabel":"Outdoor air pollution levels",
        "explain": "Measures three key air pollutants: fine particles (PM2.5) from traffic and industry that penetrate deep into the lungs; ground-level ozone, a gas that irritates airways on hot days; and nitrogen dioxide (NO2) from vehicles and power plants. Higher scores mean cleaner air.",
    },
    "environmental_burden": {
        "label":   "Environmental Burden",
        "color":   "#5BA882",
        "weight":  25,
        "sublabel":"Industrial & traffic exposure",
        "explain": "Captures cumulative environmental stressors beyond air quality alone — proximity to diesel traffic corridors, industrial facilities, and other pollution sources that affect some neighborhoods more than others. Lower burden equals a higher score here.",
    },
    "green_cover": {
        "label":   "Green Cover",
        "color":   "#A8D5B5",
        "weight":  20,
        "sublabel":"Tree canopy & vegetation",
        "explain": "Reflects how much of the neighborhood is covered by trees and vegetation. Green spaces filter airborne pollutants, reduce urban heat, and are consistently linked to better respiratory and mental health outcomes. Higher scores mean more tree canopy.",
    },
    "health_outcomes": {
        "label":   "Health Outcomes",
        "color":   "#2E9E5B",
        "weight":  15,
        "sublabel":"Local respiratory disease rates",
        "explain": "Reflects actual asthma and COPD rates among residents in this ZIP code, based on CDC health surveys. This captures the real-world cumulative effect of environmental conditions on people's health over time. Higher scores mean lower disease prevalence.",
    },
}

GRADE_INFO = {
    "A": ("#1A6B3C", "Excellent",  "This neighborhood has among the cleanest air and lowest respiratory health burdens in the country. A strong choice for families, seniors, or anyone with respiratory sensitivities."),
    "B": ("#3A8C5C", "Good",       "Air quality and environmental conditions here are above average. Most residents experience a healthy respiratory environment with only minor concerns."),
    "C": ("#B87A1A", "Fair",       "This neighborhood has moderate respiratory health conditions — better than many urban areas, but some factors warrant attention, particularly for sensitive groups."),
    "D": ("#C05020", "Poor",       "Elevated air pollution or environmental burdens create meaningful respiratory health risks. Those with asthma or lung conditions should factor this in carefully."),
    "F": ("#A01818", "Very Poor",  "This neighborhood faces significant air quality and environmental health challenges. Respiratory health risks are substantially higher than the national average."),
}

# ── CARDIOVASCULAR CONSTANTS ──────────────────────────────────
CV_COMPONENT_CONFIG = {
    "physical_inactivity": {
        "label":   "Physical Inactivity",
        "color":   "#C1121F",
        "weight":  30,
        "sublabel": "Local physical inactivity rates",
        "explain": "Reflects the share of adults in this ZIP code who report no leisure-time physical activity. Sedentary lifestyles are one of the strongest modifiable risk factors for heart disease. Higher scores mean more active communities.",
    },
    "chd": {
        "label":   "Heart Disease Prevalence",
        "color":   "#E63946",
        "weight":  25,
        "sublabel": "Coronary heart disease rates",
        "explain": "Measures the prevalence of coronary heart disease among residents, drawn from CDC health surveys. This captures the cumulative cardiovascular burden in the neighborhood. Higher scores mean lower disease rates.",
    },
    "noise": {
        "label":   "Transportation Noise",
        "color":   "#FF6B6B",
        "weight":  25,
        "sublabel": "Highway & aviation noise exposure",
        "explain": "Chronic noise from highways, railways, and flight paths is linked to elevated blood pressure, disrupted sleep, and increased cardiovascular events. This measures the average day-night noise level in the area. Higher scores mean quieter neighborhoods.",
    },
    "impervious": {
        "label":   "Walkability / Surface Cover",
        "color":   "#FF9999",
        "weight":  20,
        "sublabel": "Impervious surface coverage",
        "explain": "Measures how much of the neighborhood is paved or built over — roads, parking lots, and rooftops. High impervious surface signals car-dependent design with fewer green spaces for walking and exercise. Higher scores mean more natural ground cover.",
    },
}

CV_GRADE_INFO = {
    "A": ("#C1121F", "Excellent",  "This neighborhood supports strong cardiovascular health — low disease rates, active residents, quiet surroundings, and walkable green spaces."),
    "B": ("#E63946", "Good",       "Cardiovascular health conditions here are above average. Most environmental and lifestyle factors favor heart health."),
    "C": ("#B87A1A", "Fair",       "Mixed cardiovascular health signals — some protective factors are present, but noise, inactivity, or built environment warrant attention."),
    "D": ("#C05020", "Poor",       "Several environmental factors here elevate cardiovascular risk. Noise exposure, sedentary patterns, or high impervious surface coverage are concerns."),
    "F": ("#A01818", "Very Poor",  "This neighborhood presents significant cardiovascular health challenges across multiple dimensions. Heart disease risk factors are well above average."),
}

# ── STRESS / SENSORY CONSTANTS ──────────────────────────────
STRESS_COMPONENT_CONFIG = {
    "noise": {
        "label":   "Transportation Noise",
        "color":   "#3A0CA3",
        "weight":  35,
        "sublabel": "Highway & aviation noise exposure",
        "explain": "Chronic noise from highways, railways, and flight paths disrupts sleep, increases stress hormones, and is linked to anxiety and cognitive difficulties. This measures the average day-night noise level. Higher scores mean quieter, more restful neighborhoods.",
    },
    "light_pollution": {
        "label":   "Light Pollution",
        "color":   "#4361EE",
        "weight":  25,
        "sublabel": "Nighttime artificial light intensity",
        "explain": "Excessive artificial light at night disrupts circadian rhythms, impairs sleep quality, and is associated with increased stress and mood disorders. This measures satellite-observed nighttime radiance. Higher scores mean darker, more sleep-friendly skies.",
    },
    "depression": {
        "label":   "Depression Prevalence",
        "color":   "#4CC9F0",
        "weight":  25,
        "sublabel": "Local depression rates",
        "explain": "Reflects the share of adults reporting depression in this ZIP code, based on CDC health surveys. This captures the cumulative mental health burden shaped by neighborhood-level stressors. Higher scores indicate lower depression prevalence.",
    },
    "mental_health": {
        "label":   "Poor Mental Health Days",
        "color":   "#7B2FBE",
        "weight":  15,
        "sublabel": "Self-reported poor mental health",
        "explain": "Measures how frequently residents report poor mental health days, reflecting the lived experience of stress, anxiety, and emotional difficulty in the neighborhood. Higher scores indicate fewer poor mental health days.",
    },
}

STRESS_GRADE_INFO = {
    "A": ("#3A0CA3", "Excellent",  "This neighborhood offers a calm, low-stress sensory environment — quiet surroundings, dark skies, and strong community mental health indicators."),
    "B": ("#4361EE", "Good",       "Stress and sensory conditions here are above average. Most environmental and mental health factors support rest and well-being."),
    "C": ("#B87A1A", "Fair",       "Moderate sensory stressors are present — noise, light, or mental health indicators suggest some areas for attention, particularly for sensitive individuals."),
    "D": ("#C05020", "Poor",       "Elevated stress factors in this neighborhood — noise exposure, light pollution, or mental health burden may affect sleep quality and daily well-being."),
    "F": ("#A01818", "Very Poor",  "This neighborhood faces significant sensory and mental health challenges. Multiple stress factors are well above average."),
}

# ── FOOD ACCESS CONSTANTS ────────────────────────────────────
FA_COMPONENT_CONFIG = {
    "low_access": {
        "label":   "Supermarket Access",
        "color":   "#386641",
        "weight":  35,
        "sublabel": "Proximity to fresh food sources",
        "explain": "Measures the share of the population living more than half a mile from the nearest supermarket. Neighborhoods where most residents can easily walk to a full-service grocery store score higher. Higher scores mean better supermarket proximity.",
    },
    "grocery_density": {
        "label":   "Grocery Store Availability",
        "color":   "#6A994E",
        "weight":  35,
        "sublabel": "Grocery stores per capita",
        "explain": "Reflects the number of grocery stores available relative to the local population. More stores per capita means more options, shorter travel times, and greater competition — all of which support access to affordable, fresh food. Higher scores mean more grocery availability.",
    },
    "health_outcome": {
        "label":   "Diet-Related Health",
        "color":   "#A7C957",
        "weight":  30,
        "sublabel": "Diabetes & obesity prevalence",
        "explain": "Captures the combined prevalence of diabetes and obesity among residents, drawn from CDC health surveys. These conditions are strongly linked to food environment quality over time. Higher scores indicate lower rates of diet-related disease.",
    },
}

FA_GRADE_INFO = {
    "A": ("#386641", "Excellent",  "This neighborhood has strong food access — close proximity to supermarkets, good grocery availability, and low rates of diet-related health conditions."),
    "B": ("#6A994E", "Good",       "Food access here is above average. Most residents have reasonable access to fresh food and the community shows favorable diet-related health indicators."),
    "C": ("#B87A1A", "Fair",       "Moderate food access conditions — some gaps in supermarket proximity or grocery availability may affect certain residents, particularly those without vehicles."),
    "D": ("#C05020", "Poor",       "Limited food access in this neighborhood. Fewer grocery options, greater distance to supermarkets, or elevated diet-related health concerns are present."),
    "F": ("#A01818", "Very Poor",  "This neighborhood faces significant food access challenges. Fresh food options are limited and diet-related health conditions are well above average."),
}

# ── HEAT & CLIMATE RESILIENCE CONSTANTS ──────────────────────
HEAT_COMPONENT_CONFIG = {
    "tree_canopy": {
        "label":   "Tree Canopy & Shade",
        "color":   "#E85D04",
        "weight":  35,
        "sublabel": "Overhead tree coverage",
        "explain": "Captures the percentage of the neighborhood shaded by tree canopy. Trees provide direct shade, cool the air through evapotranspiration, and are the single most effective natural defense against urban heat. Higher scores mean more tree cover and natural cooling.",
    },
    "impervious": {
        "label":   "Pavement & Built Surface",
        "color":   "#F48C06",
        "weight":  30,
        "sublabel": "Impervious surface coverage",
        "explain": "Reflects the share of the neighborhood covered by roads, parking lots, rooftops, and other surfaces that absorb heat during the day and radiate it at night. More pavement means hotter conditions and less natural cooling. Higher scores mean more natural ground cover.",
    },
    "health_outcome": {
        "label":   "Heat-Sensitive Health",
        "color":   "#FFBA08",
        "weight":  35,
        "sublabel": "Asthma & COPD prevalence",
        "explain": "Captures the combined prevalence of asthma and COPD among residents — respiratory conditions that are directly worsened by heat exposure and poor air quality common in neighborhoods with extensive pavement and sparse tree cover. Higher scores indicate lower rates of heat-sensitive respiratory disease.",
    },
}

HEAT_GRADE_INFO = {
    "A": ("#E85D04", "Excellent",  "This neighborhood has strong heat resilience — ample tree canopy, limited pavement, and low rates of heat-sensitive respiratory conditions make it well-protected against extreme heat."),
    "B": ("#F48C06", "Good",       "Heat resilience here is above average. Good tree coverage, moderate built surface, and favorable respiratory health indicators support well-being during heat events."),
    "C": ("#B87A1A", "Fair",       "Moderate heat resilience — some protective factors like tree canopy are present, but pavement coverage or respiratory health concerns may increase vulnerability during summer peaks."),
    "D": ("#C05020", "Poor",       "Limited heat resilience in this neighborhood. High pavement coverage, sparse tree canopy, or elevated respiratory health concerns increase vulnerability to extreme heat."),
    "F": ("#A01818", "Very Poor",  "This neighborhood faces significant heat vulnerability. Extensive pavement, minimal tree cover, and high rates of heat-sensitive respiratory conditions create compounding risks during heat waves."),
}


# ── STRESS DATA HELPERS ──────────────────────────────────────
@st.cache_data(ttl=3600)
def fetch_stress_score(zipcode: str):
    r = supabase.table("stress_scores")\
        .select("*")\
        .eq("zipcode", zipcode)\
        .limit(1).execute()
    return r.data[0] if r.data else None

@st.cache_data(ttl=3600)
def fetch_stress_metro_peers(metro: str, limit: int = 15):
    zips_resp = supabase.table("zip_codes")\
        .select("zipcode")\
        .eq("metro", metro)\
        .execute()
    if not zips_resp.data:
        return []
    metro_zips = [r["zipcode"] for r in zips_resp.data]
    all_scores = []
    batch_size = 50
    for i in range(0, len(metro_zips), batch_size):
        batch = metro_zips[i:i + batch_size]
        resp = supabase.table("stress_scores")\
            .select("zipcode,composite_score,letter_grade")\
            .in_("zipcode", batch)\
            .execute()
        all_scores.extend(resp.data)
    all_scores.sort(key=lambda x: x["composite_score"], reverse=True)
    return all_scores[:limit]


# ── FOOD ACCESS DATA HELPERS ─────────────────────────────────
@st.cache_data(ttl=3600)
def fetch_fa_score(zipcode: str):
    r = supabase.table("food_access_scores")\
        .select("*")\
        .eq("zipcode", zipcode)\
        .limit(1).execute()
    return r.data[0] if r.data else None

@st.cache_data(ttl=3600)
def fetch_fa_metro_peers(metro: str, limit: int = 15):
    zips_resp = supabase.table("zip_codes")\
        .select("zipcode")\
        .eq("metro", metro)\
        .execute()
    if not zips_resp.data:
        return []
    metro_zips = [r["zipcode"] for r in zips_resp.data]
    all_scores = []
    batch_size = 50
    for i in range(0, len(metro_zips), batch_size):
        batch = metro_zips[i:i + batch_size]
        resp = supabase.table("food_access_scores")\
            .select("zipcode,composite_score,letter_grade")\
            .in_("zipcode", batch)\
            .execute()
        all_scores.extend(resp.data)
    all_scores.sort(key=lambda x: x["composite_score"], reverse=True)
    return all_scores[:limit]


# ── HEAT DATA HELPERS ─────────────────────────────────────────
@st.cache_data(ttl=3600)
def fetch_heat_score(zipcode: str):
    r = supabase.table("heat_scores")\
        .select("*")\
        .eq("zipcode", zipcode)\
        .limit(1).execute()
    return r.data[0] if r.data else None

@st.cache_data(ttl=3600)
def fetch_heat_metro_peers(metro: str, limit: int = 15):
    zips_resp = supabase.table("zip_codes")\
        .select("zipcode")\
        .eq("metro", metro)\
        .execute()
    if not zips_resp.data:
        return []
    metro_zips = [r["zipcode"] for r in zips_resp.data]
    all_scores = []
    batch_size = 50
    for i in range(0, len(metro_zips), batch_size):
        batch = metro_zips[i:i + batch_size]
        resp = supabase.table("heat_scores")\
            .select("zipcode,composite_score,letter_grade")\
            .in_("zipcode", batch)\
            .execute()
        all_scores.extend(resp.data)
    all_scores.sort(key=lambda x: x["composite_score"], reverse=True)
    return all_scores[:limit]


# ── OVERALL SCORE HELPER ─────────────────────────────────────
@st.cache_data(ttl=3600)
def fetch_overall_score(zipcode: str):
    r = supabase.table("overall_scores")\
        .select("composite_score,letter_grade")\
        .eq("zipcode", zipcode)\
        .limit(1).execute()
    return r.data[0] if r.data else None

OVERALL_GRADE_COLORS = {
    "A": "#1A6B3C", "B": "#3A8C5C", "C": "#B87A1A", "D": "#C05020", "F": "#A01818"
}

def render_overall_card(zipcode: str):
    overall = fetch_overall_score(zipcode)
    if overall:
        ov_score = overall["composite_score"]
        ov_grade = overall["letter_grade"]
        ov_color = OVERALL_GRADE_COLORS.get(ov_grade, "#444")
        st.markdown(f'''
        <div class="card" style="padding:16px 22px;display:flex;align-items:center;gap:16px;">
          <div style="font-size:1.6rem;font-weight:700;color:{ov_color};font-family:'DM Serif Display',serif;">
            {ov_score:.0f}
          </div>
          <div style="flex:1;">
            <div style="font-size:0.82rem;color:#AAAAAA;text-transform:uppercase;letter-spacing:0.05em;">
              Overall Health Score
            </div>
            <div style="font-size:0.95rem;font-weight:600;color:{ov_color};">
              Grade {ov_grade}
            </div>
          </div>
        </div>
        ''', unsafe_allow_html=True)


# ── SINGLE DISC SVG ───────────────────────────────────────────
def make_disc_svg(component_scores: dict, composite: float, grade: str,
                  comp_config: dict = None, grade_info: dict = None) -> str:
    """
    Single ring divided into arc segments sized by component weight.
    Each segment fills proportionally to its 0-100 score.
    comp_config: component config dict (defaults to COMPONENT_CONFIG for respiratory)
    grade_info: grade info dict (defaults to GRADE_INFO for respiratory)
    """
    comp_config = comp_config or COMPONENT_CONFIG
    grade_info = grade_info or GRADE_INFO

    size    = 210
    cx, cy  = size / 2, size / 2
    r       = 80
    sw      = 22
    gap_deg = 3.0
    order   = list(comp_config.keys())
    total_w = sum(comp_config[k]["weight"] for k in order)
    usable  = 360 - gap_deg * len(order)

    segments = []
    cursor   = -90.0
    for key in order:
        span = usable * (comp_config[key]["weight"] / total_w)
        segments.append((key, cursor, cursor + span))
        cursor += span + gap_deg

    def arc_path(a1, a2):
        a1r = math.radians(a1)
        a2r = math.radians(a2)
        x1  = cx + r * math.cos(a1r)
        y1  = cy + r * math.sin(a1r)
        x2  = cx + r * math.cos(a2r)
        y2  = cy + r * math.sin(a2r)
        lg  = 1 if (a2 - a1) > 180 else 0
        return f"M {x1:.2f},{y1:.2f} A {r},{r} 0 {lg},1 {x2:.2f},{y2:.2f}"

    paths = []
    for key, a_start, a_end in segments:
        cfg   = comp_config[key]
        color = cfg["color"]
        score = component_scores.get(key) or 0
        span  = a_end - a_start
        fill  = span * (score / 100)

        paths.append(
            f'<path d="{arc_path(a_start, a_end)}" fill="none" '
            f'stroke="{color}" stroke-width="{sw}" stroke-opacity="0.18" stroke-linecap="butt"/>'
        )
        if fill > 0.5:
            paths.append(
                f'<path d="{arc_path(a_start, a_start + fill)}" fill="none" '
                f'stroke="{color}" stroke-width="{sw}" stroke-linecap="butt"/>'
            )

    grade_color = grade_info.get(grade, ("#1A1A1A",))[0]
    inner_r     = r - sw / 2 - 4

    return f"""<svg width="{size}" height="{size}" viewBox="0 0 {size} {size}" xmlns="http://www.w3.org/2000/svg">
  {''.join(paths)}
  <circle cx="{cx}" cy="{cy}" r="{inner_r:.1f}" fill="#FFFFFF"/>
  <text x="{cx}" y="{cy - 6}" text-anchor="middle"
        font-family="'DM Sans',sans-serif" font-size="30" font-weight="600"
        fill="{grade_color}">{composite:.0f}</text>
  <text x="{cx}" y="{cy + 20}" text-anchor="middle"
        font-family="'DM Serif Display',serif" font-size="20"
        fill="{grade_color}">{grade}</text>
</svg>"""


# ── CLEAN INTERPRETATION ──────────────────────────────────────
def clean_interp(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"^#+\s.*\n?", "", text, flags=re.MULTILINE)
    text = re.sub(r"\*\*.*?\*\*\n?", "", text)
    return text.strip()


# ═══════════════════════════════════════════════════════════════
# MAIN APP
# ═══════════════════════════════════════════════════════════════
st.markdown('<div class="app-title">🏥 Health Environment Score</div>', unsafe_allow_html=True)
st.markdown('<div class="app-sub">Neighborhood-level health environment intelligence</div>', unsafe_allow_html=True)

tab_resp, tab_cv, tab_stress, tab_fa, tab_heat = st.tabs(["🌿 Respiratory", "❤️ Cardiovascular", "🧠 Stress / Sensory", "🥦 Food Access", "🌡️ Heat & Climate"])

# ═══════════════════════════════════════════════════════════════
# TAB 1 — RESPIRATORY
# ═══════════════════════════════════════════════════════════════
with tab_resp:
    zip_input_r = st.text_input(
        "", placeholder="Enter a ZIP code  (e.g. 15213, 90210, 28277, 85001)",
        label_visibility="collapsed",
        key="zip_resp",
    )

    if not zip_input_r:
        st.markdown("""
        <div class="info-box">
        Enter any ZIP code in <strong>Pittsburgh, Los Angeles, Phoenix, or Charlotte</strong>
        to see its Respiratory Health Score — powered by EPA, CDC, and environmental data.
        </div>
        """, unsafe_allow_html=True)
    else:
        zipcode_r = zip_input_r.strip().zfill(5)

        with st.spinner("Loading…"):
            score_data  = fetch_score(zipcode_r)
            interp_data = fetch_interpretation(zipcode_r)
            zip_meta    = fetch_zip_meta(zipcode_r)

        if not score_data:
            st.markdown(f"""
            <div class="error-box">
            No data found for ZIP code <strong>{zipcode_r}</strong>.
            This MVP covers Pittsburgh, Los Angeles, Phoenix, and Charlotte.
            </div>
            """, unsafe_allow_html=True)
        else:
            composite   = score_data["composite_score"]
            grade       = score_data["score_grade"]
            nat_pct     = score_data.get("national_percentile") or 0
            metro_pct   = score_data.get("metro_percentile") or 0
            comp_scores = score_data.get("component_scores") or {}
            metro       = (zip_meta.get("metro") if zip_meta else "") or ""
            metro_title = metro.title() if metro else ""

            grade_color, grade_label, grade_desc = GRADE_INFO.get(grade, ("#444", "Unknown", ""))

            render_overall_card(zipcode_r)

            # Score card
            st.markdown('<div class="card">', unsafe_allow_html=True)
            col_disc, col_info = st.columns([1, 1.25], gap="medium")

            with col_disc:
                st.markdown(make_disc_svg(comp_scores, composite, grade), unsafe_allow_html=True)

            with col_info:
                metro_tag = f" &middot; {metro_title}" if metro_title else ""
                metro_pill = f'<span class="pill">Top {100 - int(metro_pct)}% in {metro_title}</span>' if metro_title else ""
                st.markdown(f"""
                <div style="padding-top:12px;">
                  <div style="font-size:0.78rem;color:#AAAAAA;text-transform:uppercase;
                              letter-spacing:0.07em;margin-bottom:6px;">
                    ZIP {zipcode_r}{metro_tag}
                  </div>
                  <div style="font-family:'DM Serif Display',serif;font-size:1.9rem;
                              color:{grade_color};line-height:1.1;margin-bottom:8px;">
                    {grade_label}
                  </div>
                  <div style="font-size:0.88rem;color:#555;line-height:1.65;margin-bottom:12px;">
                    {grade_desc}
                  </div>
                  <span class="pill">Top {100 - int(nat_pct)}% nationally</span>
                  {metro_pill}
                </div>
                """, unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)

            # Component breakdown
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.markdown('<div class="section-header">Score Breakdown</div>', unsafe_allow_html=True)
            st.markdown('<div class="section-sub">Four weighted factors combine to form the overall score</div>', unsafe_allow_html=True)

            for key, cfg in COMPONENT_CONFIG.items():
                raw = comp_scores.get(key)
                wt  = cfg["weight"]
                if raw is not None:
                    score_str = f"{raw * wt / 100:.0f}/{wt}"
                else:
                    score_str = "—"

                st.markdown(f"""
                <div class="comp-row">
                  <div class="comp-dot" style="background:{cfg['color']};"></div>
                  <div class="comp-info">
                    <div class="comp-label">{cfg['label']}</div>
                    <div class="comp-sub">{cfg['sublabel']}</div>
                  </div>
                  <div class="comp-score">{score_str}</div>
                </div>
                """, unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)

            # Interpretation
            if interp_data:
                clean = clean_interp(interp_data.get("interpretation_text", ""))
                if clean:
                    st.markdown('<div class="card">', unsafe_allow_html=True)
                    st.markdown('<div class="section-header">About This ZIP Code</div>', unsafe_allow_html=True)
                    st.markdown(f'<div class="interp-text">{clean}</div>', unsafe_allow_html=True)
                    st.markdown("</div>", unsafe_allow_html=True)

            # How it's calculated
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.markdown('<div class="section-header">How the Score Is Calculated</div>', unsafe_allow_html=True)
            st.markdown('<div class="section-sub">Tap any factor to learn more</div>', unsafe_allow_html=True)

            for key, cfg in COMPONENT_CONFIG.items():
                st.markdown(f"""
                <details>
                  <summary>
                    <span style="display:inline-block;width:9px;height:9px;border-radius:50%;
                          background:{cfg['color']};flex-shrink:0;"></span>
                    <strong>{cfg['label']}</strong>
                    <span style="color:#AAAAAA;font-weight:400;font-size:0.82rem;margin-left:4px;">
                      · {cfg['weight']}% of total
                    </span>
                  </summary>
                  <div>{cfg['explain']}</div>
                </details>
                """, unsafe_allow_html=True)

            st.markdown("""
            <details>
              <summary>
                <strong>Grade Scale</strong>
              </summary>
              <div style="line-height:2.1;">
                <span style="color:#1A6B3C;font-weight:700;">A (≥ 80)</span> — Excellent respiratory health environment<br>
                <span style="color:#3A8C5C;font-weight:700;">B (65–79)</span>  — Above average air quality conditions<br>
                <span style="color:#B87A1A;font-weight:700;">C (50–64)</span>  — Moderate conditions; some concerns<br>
                <span style="color:#C05020;font-weight:700;">D (35–49)</span>  — Elevated pollution and health burden<br>
                <span style="color:#A01818;font-weight:700;">F (< 35)</span>   — Significant respiratory health risks
              </div>
            </details>
            """, unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)

            # Metro comparison
            if metro:
                with st.expander(f"📊  Compare to other ZIP codes in {metro_title}"):
                    with st.spinner("Loading metro data…"):
                        peers = fetch_metro_peers(metro, limit=15)

                    if not peers:
                        st.markdown(
                            '<div style="color:#AAA;font-size:0.88rem;padding:8px 0;">No peer data available.</div>',
                            unsafe_allow_html=True
                        )
                    else:
                        max_score = max(p["composite_score"] for p in peers)
                        for p in peers:
                            z       = p["zipcode"]
                            s       = p["composite_score"]
                            g       = p["score_grade"]
                            bar_pct = int((s / max_score) * 100) if max_score else 0
                            g_color = GRADE_INFO.get(g, ("#888",))[0]
                            bold    = "font-weight:700;" if z == zipcode_r else ""
                            hi      = "background:#F0F8F3;border-radius:8px;padding:2px 6px;" if z == zipcode_r else ""

                            st.markdown(f"""
                            <div class="metro-row" style="{hi}">
                              <div class="metro-zip" style="{bold}">{z}</div>
                              <div class="metro-bar-wrap">
                                <div class="metro-bar" style="width:{bar_pct}%;background:{g_color};"></div>
                              </div>
                              <div class="metro-score">{s:.0f}</div>
                              <div class="metro-grade" style="color:{g_color};">{g}</div>
                            </div>
                            """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════
# TAB 2 — CARDIOVASCULAR
# ═══════════════════════════════════════════════════════════════
with tab_cv:
    zip_input_cv = st.text_input(
        "", placeholder="Enter a ZIP code  (e.g. 15213, 90210, 28277, 85001)",
        label_visibility="collapsed",
        key="zip_cv",
    )

    if not zip_input_cv:
        st.markdown("""
        <div class="info-box" style="background:#FFF2EE;color:#8B3A2A;">
        Enter any ZIP code in <strong>Pittsburgh, Los Angeles, Phoenix, or Charlotte</strong>
        to see its Cardiovascular Health Score — powered by CDC, BTS, and NLCD data.
        </div>
        """, unsafe_allow_html=True)
    else:
        zipcode_cv = zip_input_cv.strip().zfill(5)

        with st.spinner("Loading…"):
            cv_data   = fetch_cv_score(zipcode_cv)
            zip_meta  = fetch_zip_meta(zipcode_cv)

        if not cv_data:
            st.markdown(f"""
            <div class="error-box">
            No cardiovascular data found for ZIP code <strong>{zipcode_cv}</strong>.
            This MVP covers Pittsburgh, Los Angeles, Phoenix, and Charlotte.
            </div>
            """, unsafe_allow_html=True)
        else:
            cv_composite = cv_data["composite_score"]
            cv_grade     = cv_data["letter_grade"]
            cv_metro     = (zip_meta.get("metro") if zip_meta else "") or ""
            cv_metro_t   = cv_metro.title() if cv_metro else ""

            # Build component scores dict for the disc visualization
            cv_comp_scores = {
                "physical_inactivity": cv_data.get("physical_inactivity_normalized") or 0,
                "chd":                 cv_data.get("chd_normalized") or 0,
                "noise":               cv_data.get("noise_normalized") or 0,
                "impervious":          cv_data.get("impervious_normalized") or 0,
            }

            cv_grade_color, cv_grade_label, cv_grade_desc = CV_GRADE_INFO.get(
                cv_grade, ("#444", "Unknown", "")
            )

            render_overall_card(zipcode_cv)

            # Score card
            st.markdown('<div class="card">', unsafe_allow_html=True)
            col_disc_cv, col_info_cv = st.columns([1, 1.25], gap="medium")

            with col_disc_cv:
                st.markdown(
                    make_disc_svg(cv_comp_scores, cv_composite, cv_grade,
                                  comp_config=CV_COMPONENT_CONFIG, grade_info=CV_GRADE_INFO),
                    unsafe_allow_html=True
                )

            with col_info_cv:
                cv_metro_tag = f" &middot; {cv_metro_t}" if cv_metro_t else ""
                st.markdown(f"""
                <div style="padding-top:12px;">
                  <div style="font-size:0.78rem;color:#AAAAAA;text-transform:uppercase;
                              letter-spacing:0.07em;margin-bottom:6px;">
                    ZIP {zipcode_cv}{cv_metro_tag}
                  </div>
                  <div style="font-family:'DM Serif Display',serif;font-size:1.9rem;
                              color:{cv_grade_color};line-height:1.1;margin-bottom:8px;">
                    {cv_grade_label}
                  </div>
                  <div style="font-size:0.88rem;color:#555;line-height:1.65;margin-bottom:12px;">
                    {cv_grade_desc}
                  </div>
                </div>
                """, unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)

            # Component breakdown
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.markdown('<div class="section-header">Score Breakdown</div>', unsafe_allow_html=True)
            st.markdown('<div class="section-sub">Four weighted factors combine to form the overall score</div>', unsafe_allow_html=True)

            for key, cfg in CV_COMPONENT_CONFIG.items():
                raw = cv_comp_scores.get(key)
                wt  = cfg["weight"]
                if raw is not None and raw > 0:
                    score_str = f"{raw * wt / 100:.0f}/{wt}"
                else:
                    score_str = "—"

                st.markdown(f"""
                <div class="comp-row">
                  <div class="comp-dot" style="background:{cfg['color']};"></div>
                  <div class="comp-info">
                    <div class="comp-label">{cfg['label']}</div>
                    <div class="comp-sub">{cfg['sublabel']}</div>
                  </div>
                  <div class="comp-score">{score_str}</div>
                </div>
                """, unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)

            # Interpretation
            cv_interp = cv_data.get("interpretation", "")
            if cv_interp:
                clean_cv = clean_interp(cv_interp)
                if clean_cv:
                    st.markdown('<div class="card">', unsafe_allow_html=True)
                    st.markdown('<div class="section-header">About This ZIP Code</div>', unsafe_allow_html=True)
                    st.markdown(f'<div class="interp-text">{clean_cv}</div>', unsafe_allow_html=True)
                    st.markdown("</div>", unsafe_allow_html=True)

            # How it's calculated
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.markdown('<div class="section-header">How the Score Is Calculated</div>', unsafe_allow_html=True)
            st.markdown('<div class="section-sub">Tap any factor to learn more</div>', unsafe_allow_html=True)

            for key, cfg in CV_COMPONENT_CONFIG.items():
                st.markdown(f"""
                <details>
                  <summary>
                    <span style="display:inline-block;width:9px;height:9px;border-radius:50%;
                          background:{cfg['color']};flex-shrink:0;"></span>
                    <strong>{cfg['label']}</strong>
                    <span style="color:#AAAAAA;font-weight:400;font-size:0.82rem;margin-left:4px;">
                      · {cfg['weight']}% of total
                    </span>
                  </summary>
                  <div>{cfg['explain']}</div>
                </details>
                """, unsafe_allow_html=True)

            st.markdown("""
            <details>
              <summary>
                <strong>Grade Scale</strong>
              </summary>
              <div style="line-height:2.1;">
                <span style="color:#C1121F;font-weight:700;">A (≥ 80)</span> — Excellent cardiovascular health environment<br>
                <span style="color:#E63946;font-weight:700;">B (65–79)</span>  — Above average heart health conditions<br>
                <span style="color:#B87A1A;font-weight:700;">C (50–64)</span>  — Moderate conditions; some concerns<br>
                <span style="color:#C05020;font-weight:700;">D (35–49)</span>  — Elevated cardiovascular health risks<br>
                <span style="color:#A01818;font-weight:700;">F (< 35)</span>   — Significant cardiovascular health challenges
              </div>
            </details>
            """, unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)

            # Metro comparison
            if cv_metro:
                with st.expander(f"📊  Compare to other ZIP codes in {cv_metro_t}"):
                    with st.spinner("Loading metro data…"):
                        cv_peers = fetch_cv_metro_peers(cv_metro, limit=15)

                    if not cv_peers:
                        st.markdown(
                            '<div style="color:#AAA;font-size:0.88rem;padding:8px 0;">No peer data available.</div>',
                            unsafe_allow_html=True
                        )
                    else:
                        cv_max_score = max(p["composite_score"] for p in cv_peers)
                        for p in cv_peers:
                            z       = p["zipcode"]
                            s       = p["composite_score"]
                            g       = p["letter_grade"]
                            bar_pct = int((s / cv_max_score) * 100) if cv_max_score else 0
                            g_color = CV_GRADE_INFO.get(g, ("#888",))[0]
                            bold    = "font-weight:700;" if z == zipcode_cv else ""
                            hi      = "background:#FFF2EE;border-radius:8px;padding:2px 6px;" if z == zipcode_cv else ""

                            st.markdown(f"""
                            <div class="metro-row" style="{hi}">
                              <div class="metro-zip" style="{bold}">{z}</div>
                              <div class="metro-bar-wrap">
                                <div class="metro-bar" style="width:{bar_pct}%;background:{g_color};"></div>
                              </div>
                              <div class="metro-score">{s:.0f}</div>
                              <div class="metro-grade" style="color:{g_color};">{g}</div>
                            </div>
                            """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════
# TAB 3 — STRESS / SENSORY
# ═══════════════════════════════════════════════════════════════
with tab_stress:
    zip_input_st = st.text_input(
        "", placeholder="Enter a ZIP code  (e.g. 15213, 90210, 28277, 85001)",
        label_visibility="collapsed",
        key="zip_stress",
    )

    if not zip_input_st:
        st.markdown("""
        <div class="info-box" style="background:#EDE8F5;color:#3A0CA3;">
        Enter any ZIP code in <strong>Pittsburgh, Los Angeles, Phoenix, or Charlotte</strong>
        to see its Stress &amp; Sensory Environment Score — powered by CDC, BTS, and NASA data.
        </div>
        """, unsafe_allow_html=True)
    else:
        zipcode_st = zip_input_st.strip().zfill(5)

        with st.spinner("Loading…"):
            st_data   = fetch_stress_score(zipcode_st)
            zip_meta  = fetch_zip_meta(zipcode_st)

        if not st_data:
            st.markdown(f"""
            <div class="error-box">
            No stress/sensory data found for ZIP code <strong>{zipcode_st}</strong>.
            This MVP covers Pittsburgh, Los Angeles, Phoenix, and Charlotte.
            </div>
            """, unsafe_allow_html=True)
        else:
            st_composite = st_data["composite_score"]
            st_grade     = st_data["letter_grade"]
            st_metro     = (zip_meta.get("metro") if zip_meta else "") or ""
            st_metro_t   = st_metro.title() if st_metro else ""

            # Build component scores dict for the disc visualization
            st_comp_scores = {
                "noise":            st_data.get("noise_normalized") or 0,
                "light_pollution":  st_data.get("light_pollution_normalized") or 0,
                "depression":       st_data.get("depression_normalized") or 0,
                "mental_health":    st_data.get("mental_health_normalized") or 0,
            }

            st_grade_color, st_grade_label, st_grade_desc = STRESS_GRADE_INFO.get(
                st_grade, ("#444", "Unknown", "")
            )

            render_overall_card(zipcode_st)

            # Score card
            st.markdown('<div class="card">', unsafe_allow_html=True)
            col_disc_st, col_info_st = st.columns([1, 1.25], gap="medium")

            with col_disc_st:
                st.markdown(
                    make_disc_svg(st_comp_scores, st_composite, st_grade,
                                  comp_config=STRESS_COMPONENT_CONFIG, grade_info=STRESS_GRADE_INFO),
                    unsafe_allow_html=True
                )

            with col_info_st:
                st_metro_tag = f" &middot; {st_metro_t}" if st_metro_t else ""
                st.markdown(f"""
                <div style="padding-top:12px;">
                  <div style="font-size:0.78rem;color:#AAAAAA;text-transform:uppercase;
                              letter-spacing:0.07em;margin-bottom:6px;">
                    ZIP {zipcode_st}{st_metro_tag}
                  </div>
                  <div style="font-family:'DM Serif Display',serif;font-size:1.9rem;
                              color:{st_grade_color};line-height:1.1;margin-bottom:8px;">
                    {st_grade_label}
                  </div>
                  <div style="font-size:0.88rem;color:#555;line-height:1.65;margin-bottom:12px;">
                    {st_grade_desc}
                  </div>
                </div>
                """, unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)

            # Component breakdown
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.markdown('<div class="section-header">Score Breakdown</div>', unsafe_allow_html=True)
            st.markdown('<div class="section-sub">Four weighted factors combine to form the overall score</div>', unsafe_allow_html=True)

            for key, cfg in STRESS_COMPONENT_CONFIG.items():
                raw = st_comp_scores.get(key)
                wt  = cfg["weight"]
                if raw is not None and raw > 0:
                    score_str = f"{raw * wt / 100:.0f}/{wt}"
                else:
                    score_str = "—"

                st.markdown(f"""
                <div class="comp-row">
                  <div class="comp-dot" style="background:{cfg['color']};"></div>
                  <div class="comp-info">
                    <div class="comp-label">{cfg['label']}</div>
                    <div class="comp-sub">{cfg['sublabel']}</div>
                  </div>
                  <div class="comp-score">{score_str}</div>
                </div>
                """, unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)

            # Interpretation
            st_interp = st_data.get("interpretation", "")
            if st_interp:
                clean_st = clean_interp(st_interp)
                if clean_st:
                    st.markdown('<div class="card">', unsafe_allow_html=True)
                    st.markdown('<div class="section-header">About This ZIP Code</div>', unsafe_allow_html=True)
                    st.markdown(f'<div class="interp-text">{clean_st}</div>', unsafe_allow_html=True)
                    st.markdown("</div>", unsafe_allow_html=True)

            # How it's calculated
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.markdown('<div class="section-header">How the Score Is Calculated</div>', unsafe_allow_html=True)
            st.markdown('<div class="section-sub">Tap any factor to learn more</div>', unsafe_allow_html=True)

            for key, cfg in STRESS_COMPONENT_CONFIG.items():
                st.markdown(f"""
                <details>
                  <summary>
                    <span style="display:inline-block;width:9px;height:9px;border-radius:50%;
                          background:{cfg['color']};flex-shrink:0;"></span>
                    <strong>{cfg['label']}</strong>
                    <span style="color:#AAAAAA;font-weight:400;font-size:0.82rem;margin-left:4px;">
                      · {cfg['weight']}% of total
                    </span>
                  </summary>
                  <div>{cfg['explain']}</div>
                </details>
                """, unsafe_allow_html=True)

            st.markdown("""
            <details>
              <summary>
                <strong>Grade Scale</strong>
              </summary>
              <div style="line-height:2.1;">
                <span style="color:#3A0CA3;font-weight:700;">A (≥ 80)</span> — Excellent stress &amp; sensory environment<br>
                <span style="color:#4361EE;font-weight:700;">B (65–79)</span>  — Above average sensory conditions<br>
                <span style="color:#B87A1A;font-weight:700;">C (50–64)</span>  — Moderate conditions; some stressors present<br>
                <span style="color:#C05020;font-weight:700;">D (35–49)</span>  — Elevated stress and sensory burden<br>
                <span style="color:#A01818;font-weight:700;">F (< 35)</span>   — Significant stress and sensory challenges
              </div>
            </details>
            """, unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)

            # Metro comparison
            if st_metro:
                with st.expander(f"📊  Compare to other ZIP codes in {st_metro_t}"):
                    with st.spinner("Loading metro data…"):
                        st_peers = fetch_stress_metro_peers(st_metro, limit=15)

                    if not st_peers:
                        st.markdown(
                            '<div style="color:#AAA;font-size:0.88rem;padding:8px 0;">No peer data available.</div>',
                            unsafe_allow_html=True
                        )
                    else:
                        st_max_score = max(p["composite_score"] for p in st_peers)
                        for p in st_peers:
                            z       = p["zipcode"]
                            s       = p["composite_score"]
                            g       = p["letter_grade"]
                            bar_pct = int((s / st_max_score) * 100) if st_max_score else 0
                            g_color = STRESS_GRADE_INFO.get(g, ("#888",))[0]
                            bold    = "font-weight:700;" if z == zipcode_st else ""
                            hi      = "background:#EDE8F5;border-radius:8px;padding:2px 6px;" if z == zipcode_st else ""

                            st.markdown(f"""
                            <div class="metro-row" style="{hi}">
                              <div class="metro-zip" style="{bold}">{z}</div>
                              <div class="metro-bar-wrap">
                                <div class="metro-bar" style="width:{bar_pct}%;background:{g_color};"></div>
                              </div>
                              <div class="metro-score">{s:.0f}</div>
                              <div class="metro-grade" style="color:{g_color};">{g}</div>
                            </div>
                            """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════
# TAB 4 — FOOD ACCESS
# ═══════════════════════════════════════════════════════════════
with tab_fa:
    zip_input_fa = st.text_input(
        "", placeholder="Enter a ZIP code  (e.g. 15213, 90210, 28277, 85001)",
        label_visibility="collapsed",
        key="zip_fa",
    )

    if not zip_input_fa:
        st.markdown("""
        <div class="info-box" style="background:#EEF5E6;color:#386641;">
        Enter any ZIP code in <strong>Pittsburgh, Los Angeles, Phoenix, or Charlotte</strong>
        to see its Food Access Score — powered by USDA and CDC data.
        </div>
        """, unsafe_allow_html=True)
    else:
        zipcode_fa = zip_input_fa.strip().zfill(5)

        with st.spinner("Loading…"):
            fa_data   = fetch_fa_score(zipcode_fa)
            zip_meta  = fetch_zip_meta(zipcode_fa)

        if not fa_data:
            st.markdown(f"""
            <div class="error-box">
            No food access data found for ZIP code <strong>{zipcode_fa}</strong>.
            This MVP covers Pittsburgh, Los Angeles, Phoenix, and Charlotte.
            </div>
            """, unsafe_allow_html=True)
        else:
            fa_composite = fa_data["composite_score"]
            fa_grade     = fa_data["letter_grade"]
            fa_metro     = (zip_meta.get("metro") if zip_meta else "") or ""
            fa_metro_t   = fa_metro.title() if fa_metro else ""

            # Build component scores dict for the disc visualization
            fa_comp_scores = {
                "low_access":       fa_data.get("low_access_normalized") or 0,
                "grocery_density":  fa_data.get("grocery_density_normalized") or 0,
                "health_outcome":   fa_data.get("health_outcome_normalized") or 0,
            }

            fa_grade_color, fa_grade_label, fa_grade_desc = FA_GRADE_INFO.get(
                fa_grade, ("#444", "Unknown", "")
            )

            render_overall_card(zipcode_fa)

            # Score card
            st.markdown('<div class="card">', unsafe_allow_html=True)
            col_disc_fa, col_info_fa = st.columns([1, 1.25], gap="medium")

            with col_disc_fa:
                st.markdown(
                    make_disc_svg(fa_comp_scores, fa_composite, fa_grade,
                                  comp_config=FA_COMPONENT_CONFIG, grade_info=FA_GRADE_INFO),
                    unsafe_allow_html=True
                )

            with col_info_fa:
                fa_metro_tag = f" &middot; {fa_metro_t}" if fa_metro_t else ""
                st.markdown(f"""
                <div style="padding-top:12px;">
                  <div style="font-size:0.78rem;color:#AAAAAA;text-transform:uppercase;
                              letter-spacing:0.07em;margin-bottom:6px;">
                    ZIP {zipcode_fa}{fa_metro_tag}
                  </div>
                  <div style="font-family:'DM Serif Display',serif;font-size:1.9rem;
                              color:{fa_grade_color};line-height:1.1;margin-bottom:8px;">
                    {fa_grade_label}
                  </div>
                  <div style="font-size:0.88rem;color:#555;line-height:1.65;margin-bottom:12px;">
                    {fa_grade_desc}
                  </div>
                </div>
                """, unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)

            # Component breakdown
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.markdown('<div class="section-header">Score Breakdown</div>', unsafe_allow_html=True)
            st.markdown('<div class="section-sub">Three weighted factors combine to form the overall score</div>', unsafe_allow_html=True)

            for key, cfg in FA_COMPONENT_CONFIG.items():
                raw = fa_comp_scores.get(key)
                wt  = cfg["weight"]
                if raw is not None and raw > 0:
                    score_str = f"{raw * wt / 100:.0f}/{wt}"
                else:
                    score_str = "—"

                st.markdown(f"""
                <div class="comp-row">
                  <div class="comp-dot" style="background:{cfg['color']};"></div>
                  <div class="comp-info">
                    <div class="comp-label">{cfg['label']}</div>
                    <div class="comp-sub">{cfg['sublabel']}</div>
                  </div>
                  <div class="comp-score">{score_str}</div>
                </div>
                """, unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)

            # Interpretation
            fa_interp = fa_data.get("interpretation", "")
            if fa_interp:
                clean_fa = clean_interp(fa_interp)
                if clean_fa:
                    st.markdown('<div class="card">', unsafe_allow_html=True)
                    st.markdown('<div class="section-header">About This ZIP Code</div>', unsafe_allow_html=True)
                    st.markdown(f'<div class="interp-text">{clean_fa}</div>', unsafe_allow_html=True)
                    st.markdown("</div>", unsafe_allow_html=True)

            # How it's calculated
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.markdown('<div class="section-header">How the Score Is Calculated</div>', unsafe_allow_html=True)
            st.markdown('<div class="section-sub">Tap any factor to learn more</div>', unsafe_allow_html=True)

            for key, cfg in FA_COMPONENT_CONFIG.items():
                st.markdown(f"""
                <details>
                  <summary>
                    <span style="display:inline-block;width:9px;height:9px;border-radius:50%;
                          background:{cfg['color']};flex-shrink:0;"></span>
                    <strong>{cfg['label']}</strong>
                    <span style="color:#AAAAAA;font-weight:400;font-size:0.82rem;margin-left:4px;">
                      · {cfg['weight']}% of total
                    </span>
                  </summary>
                  <div>{cfg['explain']}</div>
                </details>
                """, unsafe_allow_html=True)

            st.markdown("""
            <details>
              <summary>
                <strong>Grade Scale</strong>
              </summary>
              <div style="line-height:2.1;">
                <span style="color:#386641;font-weight:700;">A (≥ 80)</span> — Excellent food access environment<br>
                <span style="color:#6A994E;font-weight:700;">B (65–79)</span>  — Above average food access conditions<br>
                <span style="color:#B87A1A;font-weight:700;">C (50–64)</span>  — Moderate conditions; some food access gaps<br>
                <span style="color:#C05020;font-weight:700;">D (35–49)</span>  — Limited food access and elevated health concerns<br>
                <span style="color:#A01818;font-weight:700;">F (< 35)</span>   — Significant food access challenges
              </div>
            </details>
            """, unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)

            # Metro comparison
            if fa_metro:
                with st.expander(f"📊  Compare to other ZIP codes in {fa_metro_t}"):
                    with st.spinner("Loading metro data…"):
                        fa_peers = fetch_fa_metro_peers(fa_metro, limit=15)

                    if not fa_peers:
                        st.markdown(
                            '<div style="color:#AAA;font-size:0.88rem;padding:8px 0;">No peer data available.</div>',
                            unsafe_allow_html=True
                        )
                    else:
                        fa_max_score = max(p["composite_score"] for p in fa_peers)
                        for p in fa_peers:
                            z       = p["zipcode"]
                            s       = p["composite_score"]
                            g       = p["letter_grade"]
                            bar_pct = int((s / fa_max_score) * 100) if fa_max_score else 0
                            g_color = FA_GRADE_INFO.get(g, ("#888",))[0]
                            bold    = "font-weight:700;" if z == zipcode_fa else ""
                            hi      = "background:#EEF5E6;border-radius:8px;padding:2px 6px;" if z == zipcode_fa else ""

                            st.markdown(f"""
                            <div class="metro-row" style="{hi}">
                              <div class="metro-zip" style="{bold}">{z}</div>
                              <div class="metro-bar-wrap">
                                <div class="metro-bar" style="width:{bar_pct}%;background:{g_color};"></div>
                              </div>
                              <div class="metro-score">{s:.0f}</div>
                              <div class="metro-grade" style="color:{g_color};">{g}</div>
                            </div>
                            """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════
# TAB 5 — HEAT & CLIMATE RESILIENCE
# ═══════════════════════════════════════════════════════════════
with tab_heat:
    zip_input_ht = st.text_input(
        "", placeholder="Enter a ZIP code  (e.g. 15213, 90210, 28277, 85281)",
        label_visibility="collapsed",
        key="zip_heat",
    )

    if not zip_input_ht:
        st.markdown("""
        <div class="info-box" style="background:#FEF3E2;color:#7C3A00;">
        Enter any ZIP code in <strong>Pittsburgh, Los Angeles, Phoenix, or Charlotte</strong>
        to see its Heat &amp; Climate Resilience Score — powered by USGS, NLCD, and environmental data.
        </div>
        """, unsafe_allow_html=True)
    else:
        zipcode_ht = zip_input_ht.strip().zfill(5)

        with st.spinner("Loading…"):
            ht_data   = fetch_heat_score(zipcode_ht)
            zip_meta  = fetch_zip_meta(zipcode_ht)

        if not ht_data:
            st.markdown(f"""
            <div class="error-box">
            No heat resilience data found for ZIP code <strong>{zipcode_ht}</strong>.
            This MVP covers Pittsburgh, Los Angeles, Phoenix, and Charlotte.
            </div>
            """, unsafe_allow_html=True)
        else:
            ht_composite = ht_data["composite_score"]
            ht_grade     = ht_data["letter_grade"]
            ht_metro     = (zip_meta.get("metro") if zip_meta else "") or ""
            ht_metro_t   = ht_metro.title() if ht_metro else ""

            # Build component scores dict for the disc visualization
            ht_comp_scores = {
                "impervious":      ht_data.get("impervious_normalized") or 0,
                "tree_canopy":     ht_data.get("tree_canopy_normalized") or 0,
                "health_outcome":  ht_data.get("health_outcome_normalized") or 0,
            }

            ht_grade_color, ht_grade_label, ht_grade_desc = HEAT_GRADE_INFO.get(
                ht_grade, ("#444", "Unknown", "")
            )

            render_overall_card(zipcode_ht)

            # Score card
            st.markdown('<div class="card">', unsafe_allow_html=True)
            col_disc_ht, col_info_ht = st.columns([1, 1.25], gap="medium")

            with col_disc_ht:
                st.markdown(
                    make_disc_svg(ht_comp_scores, ht_composite, ht_grade,
                                  comp_config=HEAT_COMPONENT_CONFIG, grade_info=HEAT_GRADE_INFO),
                    unsafe_allow_html=True
                )

            with col_info_ht:
                ht_metro_tag = f" &middot; {ht_metro_t}" if ht_metro_t else ""
                st.markdown(f"""
                <div style="padding-top:12px;">
                  <div style="font-size:0.78rem;color:#AAAAAA;text-transform:uppercase;
                              letter-spacing:0.07em;margin-bottom:6px;">
                    ZIP {zipcode_ht}{ht_metro_tag}
                  </div>
                  <div style="font-family:'DM Serif Display',serif;font-size:1.9rem;
                              color:{ht_grade_color};line-height:1.1;margin-bottom:8px;">
                    {ht_grade_label}
                  </div>
                  <div style="font-size:0.88rem;color:#555;line-height:1.65;margin-bottom:12px;">
                    {ht_grade_desc}
                  </div>
                </div>
                """, unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)

            # Component breakdown
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.markdown('<div class="section-header">Score Breakdown</div>', unsafe_allow_html=True)
            st.markdown('<div class="section-sub">Three weighted factors combine to form the overall score</div>', unsafe_allow_html=True)

            for key, cfg in HEAT_COMPONENT_CONFIG.items():
                raw = ht_comp_scores.get(key)
                wt  = cfg["weight"]
                if raw is not None and raw > 0:
                    score_str = f"{raw * wt / 100:.0f}/{wt}"
                else:
                    score_str = "—"

                st.markdown(f"""
                <div class="comp-row">
                  <div class="comp-dot" style="background:{cfg['color']};"></div>
                  <div class="comp-info">
                    <div class="comp-label">{cfg['label']}</div>
                    <div class="comp-sub">{cfg['sublabel']}</div>
                  </div>
                  <div class="comp-score">{score_str}</div>
                </div>
                """, unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)

            # Interpretation
            ht_interp = ht_data.get("interpretation", "")
            if ht_interp:
                clean_ht = clean_interp(ht_interp)
                if clean_ht:
                    st.markdown('<div class="card">', unsafe_allow_html=True)
                    st.markdown('<div class="section-header">About This ZIP Code</div>', unsafe_allow_html=True)
                    st.markdown(f'<div class="interp-text">{clean_ht}</div>', unsafe_allow_html=True)
                    st.markdown("</div>", unsafe_allow_html=True)

            # How it's calculated
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.markdown('<div class="section-header">How the Score Is Calculated</div>', unsafe_allow_html=True)
            st.markdown('<div class="section-sub">Tap any factor to learn more</div>', unsafe_allow_html=True)

            for key, cfg in HEAT_COMPONENT_CONFIG.items():
                st.markdown(f"""
                <details>
                  <summary>
                    <span style="display:inline-block;width:9px;height:9px;border-radius:50%;
                          background:{cfg['color']};flex-shrink:0;"></span>
                    <strong>{cfg['label']}</strong>
                    <span style="color:#AAAAAA;font-weight:400;font-size:0.82rem;margin-left:4px;">
                      · {cfg['weight']}% of total
                    </span>
                  </summary>
                  <div>{cfg['explain']}</div>
                </details>
                """, unsafe_allow_html=True)

            st.markdown("""
            <details>
              <summary>
                <strong>Grade Scale</strong>
              </summary>
              <div style="line-height:2.1;">
                <span style="color:#E85D04;font-weight:700;">A (≥ 80)</span> — Excellent heat resilience<br>
                <span style="color:#F48C06;font-weight:700;">B (65–79)</span>  — Above average climate resilience<br>
                <span style="color:#B87A1A;font-weight:700;">C (50–64)</span>  — Moderate conditions; some heat vulnerability<br>
                <span style="color:#C05020;font-weight:700;">D (35–49)</span>  — Limited heat resilience; elevated risk<br>
                <span style="color:#A01818;font-weight:700;">F (< 35)</span>   — Significant heat vulnerability
              </div>
            </details>
            """, unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)

            # Metro comparison
            if ht_metro:
                with st.expander(f"📊  Compare to other ZIP codes in {ht_metro_t}"):
                    with st.spinner("Loading metro data…"):
                        ht_peers = fetch_heat_metro_peers(ht_metro, limit=15)

                    if not ht_peers:
                        st.markdown(
                            '<div style="color:#AAA;font-size:0.88rem;padding:8px 0;">No peer data available.</div>',
                            unsafe_allow_html=True
                        )
                    else:
                        ht_max_score = max(p["composite_score"] for p in ht_peers)
                        for p in ht_peers:
                            z       = p["zipcode"]
                            s       = p["composite_score"]
                            g       = p["letter_grade"]
                            bar_pct = int((s / ht_max_score) * 100) if ht_max_score else 0
                            g_color = HEAT_GRADE_INFO.get(g, ("#888",))[0]
                            bold    = "font-weight:700;" if z == zipcode_ht else ""
                            hi      = "background:#FEF3E2;border-radius:8px;padding:2px 6px;" if z == zipcode_ht else ""

                            st.markdown(f"""
                            <div class="metro-row" style="{hi}">
                              <div class="metro-zip" style="{bold}">{z}</div>
                              <div class="metro-bar-wrap">
                                <div class="metro-bar" style="width:{bar_pct}%;background:{g_color};"></div>
                              </div>
                              <div class="metro-score">{s:.0f}</div>
                              <div class="metro-grade" style="color:{g_color};">{g}</div>
                            </div>
                            """, unsafe_allow_html=True)


# ── FOOTER ────────────────────────────────────────────────────
st.markdown("""
<div style="text-align:center;margin-top:2.5rem;font-size:0.75rem;color:#CCCCCC;">
  LaSalle Technologies &nbsp;·&nbsp; Data: EPA, CDC PLACES, BTS, NLCD, NASA VIIRS, USDA &nbsp;·&nbsp; 2023–2024
</div>
""", unsafe_allow_html=True)
