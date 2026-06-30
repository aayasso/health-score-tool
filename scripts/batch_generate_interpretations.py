"""
D3 — Batch interpretation generator.
Replicates D1 edge function prompt logic with qualitative tercile conversion.
Run in Colab with SUPABASE_URL, SUPABASE_KEY (service role), ANTHROPIC_API_KEY in secrets.

Usage:
  python batch_generate_interpretations.py --mode test         # Print test ZIPs, no DB write
  python batch_generate_interpretations.py --mode test-write   # Write test ZIPs to DB for verification
  python batch_generate_interpretations.py --mode full         # Write all ZIPs (skip existing)
  python batch_generate_interpretations.py --mode full --force # Regenerate ALL, even existing
  python batch_generate_interpretations.py --mode audit        # Scan existing for tone contradictions (read-only)
"""

import os
import sys
import time
import argparse
import re
import anthropic
from supabase import create_client

# Add project root to path for shared config
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from scripts.lib.metros import all_metros, pilot_metros
from scripts.lib.pipeline import (
    claude_model, write_and_verify as _shared_write_and_verify,
    WriteVerificationError, preflight,
)

# ── Dimension config (identical to D1 index.ts) ─────────────────────
# Normalization already inverts bad-is-high components,
# so higher normalized = better for ALL components.
# Tercile boundaries: low = 0–33.33, moderate = 33.34–66.66, high = 67–100.

DIMENSIONS = {
    "respiratory": {
        "table": "respiratory_scores",
        "label": "Respiratory Health",
        "components": [
            {"column": "air_quality_normalized", "label": "Air quality",
             "low": "poor air quality conditions",
             "moderate": "moderate air quality",
             "high": "good air quality conditions"},
            {"column": "environmental_burden_normalized", "label": "Environmental burden",
             "low": "high environmental burden from nearby pollution sources",
             "moderate": "moderate environmental burden",
             "high": "low environmental burden"},
            {"column": "green_cover_normalized", "label": "Green space",
             "low": "limited green space",
             "moderate": "moderate green coverage",
             "high": "extensive green coverage"},
            {"column": "health_outcomes_normalized", "label": "Respiratory illness",
             "low": "elevated respiratory illness rates",
             "moderate": "moderate respiratory illness rates",
             "high": "low respiratory illness rates"},
        ],
    },
    "cardiovascular": {
        "table": "cardiovascular_scores",
        "label": "Cardiovascular Health",
        "components": [
            {"column": "physical_inactivity_normalized", "label": "Physical activity",
             "low": "high physical inactivity",
             "moderate": "moderate physical activity levels",
             "high": "strong physical activity levels"},
            {"column": "chd_normalized", "label": "Heart disease",
             "low": "elevated heart disease rates",
             "moderate": "moderate heart disease rates",
             "high": "low heart disease rates"},
            {"column": "noise_normalized", "label": "Noise exposure",
             "low": "high noise exposure",
             "moderate": "moderate noise levels",
             "high": "low noise exposure"},
            {"column": "impervious_normalized", "label": "Surface permeability",
             "low": "high impervious surface coverage",
             "moderate": "moderate surface permeability",
             "high": "good surface permeability"},
        ],
    },
    "stress": {
        "table": "stress_scores",
        "label": "Stress & Sensory Environment",
        "components": [
            {"column": "noise_normalized", "label": "Noise exposure",
             "low": "high noise exposure",
             "moderate": "moderate noise levels",
             "high": "low noise exposure"},
            {"column": "light_pollution_normalized", "label": "Light pollution",
             "low": "high light pollution",
             "moderate": "moderate light pollution",
             "high": "low light pollution"},
            {"column": "depression_normalized", "label": "Depression prevalence",
             "low": "elevated depression rates",
             "moderate": "moderate depression rates",
             "high": "low depression rates"},
            {"column": "mental_health_normalized", "label": "Mental health",
             "low": "elevated poor mental health rates",
             "moderate": "moderate mental health outcomes",
             "high": "strong mental health outcomes"},
        ],
    },
    "food_access": {
        "table": "food_access_scores",
        "label": "Food Access",
        "components": [
            {"column": "low_access_normalized", "label": "Supermarket access",
             "low": "limited supermarket access",
             "moderate": "moderate supermarket access",
             "high": "strong supermarket access"},
            {"column": "grocery_density_normalized", "label": "Grocery store density",
             "low": "low grocery store density",
             "moderate": "moderate grocery store density",
             "high": "high grocery store density"},
            {"column": "health_outcome_normalized", "label": "Diet-related illness",
             "low": "elevated diet-related illness rates",
             "moderate": "moderate diet-related illness rates",
             "high": "low diet-related illness rates"},
        ],
    },
    "heat": {
        "table": "heat_scores",
        "label": "Heat & Climate Resilience",
        "components": [
            {"column": "impervious_normalized", "label": "Surface permeability",
             "low": "high impervious surface coverage limiting natural cooling",
             "moderate": "moderate impervious surface coverage",
             "high": "good surface permeability with limited heat-trapping pavement"},
            {"column": "tree_canopy_normalized", "label": "Tree canopy",
             "low": "very limited tree canopy with minimal natural shade",
             "moderate": "moderate tree coverage providing some natural cooling",
             "high": "extensive tree canopy"},
            {"column": "health_outcome_normalized", "label": "Climate-health sensitivity",
             "low": "elevated rates of heat-sensitive respiratory conditions",
             "moderate": "moderate rates of heat-sensitive respiratory conditions",
             "high": "low rates of heat-sensitive respiratory conditions"},
        ],
    },
}

IN_SCOPE_METROS = all_metros()

# 10 test ZIPs — 2 per metro + 2 extra, spanning A/C/F grades.
# UPDATE THESE after running the test-ZIP query.
TEST_ZIPS = [
    "28078",  # Charlotte (pilot) — A-range
    "90006",  # Los Angeles (pilot) — F-range
    "85382",  # Phoenix (pilot) — C-range
    "15213",  # Pittsburgh (pilot) — mid-range
    "60614",  # Chicago (expansion)
    "77005",  # Houston (expansion)
    "30309",  # Atlanta (expansion)
    "80202",  # Denver (expansion)
]


def to_qualitative(value, component):
    """Map normalized value (0-100) to qualitative tercile description."""
    if value is None:
        return "data not available"
    if value >= 66.67:
        return component["high"]
    if value >= 33.34:
        return component["moderate"]
    return component["low"]


TONE_MAP = {
    "A": "among the strongest-performing neighborhoods in this dimension",
    "B": "above average among the covered neighborhoods in this dimension",
    "C": "near the middle of the pack among covered neighborhoods",
    "D": "below average among the covered neighborhoods in this dimension",
    "F": "among the most vulnerable neighborhoods in this dimension",
}


def build_prompt(zipcode, dim_key, letter_grade, row):
    """Build the per-dimension prompt with tone-anchored relative standing."""
    dim = DIMENSIONS[dim_key]
    lines = []
    for comp in dim["components"]:
        val = row.get(comp["column"])
        desc = to_qualitative(val, comp)
        lines.append(f"- {comp['label']}: This area has {desc}.")
    component_text = "\n".join(lines)

    tone_anchor = TONE_MAP.get(letter_grade, "in the middle range")

    return (
        "You are a public health analyst writing a plain-language summary "
        "for residents and real estate professionals.\n"
        "\n"
        f"Write 2-3 sentences interpreting this neighborhood's {dim['label']} "
        "environment.\n"
        "\n"
        "IMPORTANT — how to use the two inputs below:\n"
        f"1. RELATIVE STANDING (primary signal): This neighborhood ranks "
        f"{tone_anchor}. The overall tone and framing of your response MUST "
        "match this standing. Convey this standing in your own varied wording — "
        "do not copy the phrasing above verbatim. This is a relative ranking "
        "against all other neighborhoods in the covered metro areas — it is not "
        "an absolute or national health judgment.\n"
        "2. COMPONENT CONDITIONS (supporting detail): The conditions below "
        "describe what residents experience on the ground. Use them to add "
        "specificity, but frame them in a way that is consistent with the "
        "relative standing above. If a condition sounds negative but the "
        "standing is strong, frame it as a remaining consideration within an "
        "otherwise favorable environment. If a condition sounds positive but "
        "the standing is weak, frame it as a relative bright spot in an "
        "otherwise challenging environment.\n"
        "\n"
        f"Component conditions:\n"
        f"{component_text}\n"
        "\n"
        "Rules:\n"
        "- Write plain prose only. No markdown headers, bullet points, or formatting.\n"
        "- Do not mention any numbers, scores, percentages, or percentiles.\n"
        "- Do not mention or echo the ZIP code number.\n"
        "- Do not state or name the letter grade itself (for example A, B, C, D, "
        "or F) and do not refer to it as a grade. Describe the neighborhood's "
        "relative standing qualitatively in words instead.\n"
        "- Do not compare to other dimensions (e.g., \"better than its food score\").\n"
        "- Do not reference methodology, weighting, or how grades are computed.\n"
        "- Do not imply the grade is an absolute or national health judgment — "
        "it is a relative standing within this dataset for this one dimension.\n"
        "- Describe what residents experience in this neighborhood for this dimension.\n"
        "- Frame in terms of livability and long-term health outcomes.\n"
        "- Be specific and actionable — name the conditions, not abstract categories."
    )


# ── Tone contradiction detection ─────────────────────────────────
# Keyword heuristic: flag when interpretation tone contradicts grade.
# This is a WARNING — it does not block writes (false positives expected).

NEGATIVE_MARKERS = [
    "significant challenges", "considerable challenges", "notable challenges",
    "significant health challenges", "significant respiratory health challenges",
    "vulnerable", "at higher risk", "at elevated risk",
    "concerning", "problematic",
    "faces considerable", "faces significant", "struggles with",
    "elevated rates", "elevated levels", "high levels of",
    "limited access", "lack of", "lacking",
    "poor air quality", "poor conditions",
    "puts residents at risk", "put residents at risk",
]

POSITIVE_MARKERS = [
    "strongest", "strongest-performing", "exceptionally well",
    "favorable", "thriving", "excellent", "outstanding",
    "well-positioned", "enviable",
]


def detect_contradiction(letter_grade, interpretation):
    """Return the triggering marker if tone contradicts grade, else None."""
    if not interpretation:
        return None
    text_lower = interpretation.lower()

    if letter_grade in ("A", "B"):
        for marker in NEGATIVE_MARKERS:
            if marker in text_lower:
                return marker

    if letter_grade in ("D", "F"):
        for marker in POSITIVE_MARKERS:
            if marker in text_lower:
                return marker

    return None


def check_output_quality(text, letter_grade=None):
    """Check interpretation for score leaks, markdown, digits, and tone.

    Returns (hard_issues, tone_warning) where hard_issues is a list of
    blocking quality problems and tone_warning is a non-blocking string
    or None.
    """
    issues = []
    if re.search(r"[0-9]", text):
        issues.append("DIGITS found")
    if re.search(r"(^|\n)#", text):
        issues.append("MARKDOWN HEADER found")
    if re.search(r"\*\*|__", text):
        issues.append("MARKDOWN BOLD found")
    if re.search(r"^[\s]*[-•]", text, re.MULTILINE):
        issues.append("BULLET POINT found")
    # Grade-naming: the word "grade", or "earns a B" / "an A" style letter calls.
    # Negative lookbehind for a hyphen avoids false positives on "low-grade" /
    # "high-grade" stress, which is normal language, not a grade reference.
    if re.search(r"(?<!-)\bgrade[ds]?\b", text, re.IGNORECASE):
        issues.append("GRADE WORD found")
    if re.search(r"\b(?:earns?|earned|rated|scores?|received?)\s+an?\s+[A-F]\b", text):
        issues.append("GRADE LETTER found")

    # Tone contradiction — warning only, does not block writes
    tone_warning = None
    if letter_grade:
        marker = detect_contradiction(letter_grade, text)
        if marker:
            tone_warning = f"TONE CONTRADICTION ('{marker}')"

    return issues, tone_warning


def write_and_verify(supabase, table, zipcode, interpretation):
    """Delegate to shared write_and_verify for the interpretation column."""
    _shared_write_and_verify(supabase, table, zipcode, "interpretation", interpretation)


def _fetch_dimension_rows(supabase, dim, select_cols, mode):
    """Paginated fetch of in-scope rows for a dimension."""
    rows = []
    batch_size = 1000
    offset = 0
    while True:
        query = supabase.table(dim["table"]) \
            .select(select_cols) \
            .in_("metro", IN_SCOPE_METROS) \
            .not_.is_("letter_grade", "null")

        if mode in ("test", "test-write"):
            query = query.in_("zipcode", TEST_ZIPS)

        resp = query.range(offset, offset + batch_size - 1).execute()
        if not resp.data:
            break
        rows.extend(resp.data)
        if len(resp.data) < batch_size:
            break
        offset += batch_size
    return rows


def run_audit(supabase):
    """Read-only scan: detect tone contradictions in existing interpretations."""
    grand_total = 0
    grand_contradictions = 0

    for dim_key, dim in DIMENSIONS.items():
        comp_cols = [c["column"] for c in dim["components"]]
        select_cols = ",".join(["zipcode", "metro", "letter_grade", "interpretation"] + comp_cols)
        rows = _fetch_dimension_rows(supabase, dim, select_cols, "full")

        # Only check rows that have both a grade and an interpretation
        has_interp = [r for r in rows if r.get("interpretation") and r.get("letter_grade")]
        contradictions = []
        for row in has_interp:
            marker = detect_contradiction(row["letter_grade"], row["interpretation"])
            if marker:
                contradictions.append((row["zipcode"], row["letter_grade"], row["metro"], marker, row["interpretation"]))

        print(f"\n{'='*70}")
        print(f"  {dim['label']}  |  {len(has_interp)} with interpretations  |  {len(contradictions)} contradictions")
        print(f"{'='*70}")

        for zipcode, grade, metro, marker, interp in contradictions:
            print(f"  {zipcode} ({metro}) grade={grade}  marker='{marker}'")
            print(f"    {interp[:100]}...")

        grand_total += len(has_interp)
        grand_contradictions += len(contradictions)

    print(f"\n{'='*70}")
    print(f"  AUDIT SUMMARY")
    print(f"{'='*70}")
    print(f"  Total interpretations scanned: {grand_total}")
    print(f"  Tone contradictions found:     {grand_contradictions}")
    if grand_contradictions > 0:
        print(f"  Rate: {grand_contradictions / grand_total * 100:.1f}%")
    print()


def run(mode, force=False):
    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = os.environ.get("SUPABASE_KEY", "")
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")

    if mode == "audit":
        # Audit mode: read-only scan, no Claude API needed
        if not all([supabase_url, supabase_key]):
            print("ERROR: Set SUPABASE_URL and SUPABASE_KEY in environment.")
            sys.exit(1)
        supabase = create_client(supabase_url, supabase_key)
        run_audit(supabase)
        return

    if not all([supabase_url, supabase_key, anthropic_key]):
        print("ERROR: Set SUPABASE_URL, SUPABASE_KEY, ANTHROPIC_API_KEY in environment.")
        sys.exit(1)

    preflight(supabase_key, anthropic_key=anthropic_key, check_git=(mode == "full"))

    supabase = create_client(supabase_url, supabase_key)
    client = anthropic.Anthropic(api_key=anthropic_key)

    total_generated = 0
    total_errors = 0
    total_quality_issues = 0
    total_tone_warnings = 0

    for dim_key, dim in DIMENSIONS.items():
        comp_cols = [c["column"] for c in dim["components"]]
        select_cols = ",".join(["zipcode", "metro", "letter_grade", "interpretation"] + comp_cols)

        rows = _fetch_dimension_rows(supabase, dim, select_cols, mode)

        # In full mode, skip rows that already have an interpretation (unless --force)
        skipped = 0
        if mode == "full" and not force:
            pending = []
            for row in rows:
                if row.get("interpretation"):
                    skipped += 1
                else:
                    pending.append(row)
            rows = pending

        print(f"\n{'='*70}")
        skip_msg = f"  |  {skipped} skipped (existing)" if skipped else ""
        print(f"  {dim['label']}  |  {len(rows)} to generate{skip_msg}  |  mode={mode}")
        print(f"{'='*70}")

        for i, row in enumerate(rows):
            zipcode = row["zipcode"]
            letter_grade = row["letter_grade"]
            prompt = build_prompt(zipcode, dim_key, letter_grade, row)

            try:
                resp = client.messages.create(
                    model=claude_model(),
                    max_tokens=300,
                    messages=[{"role": "user", "content": prompt}],
                )
                interpretation = resp.content[0].text.strip()
                total_generated += 1

                # Quality check — hard issues block, tone warnings are advisory
                hard_issues, tone_warning = check_output_quality(interpretation, letter_grade=letter_grade)
                flag_parts = []
                if hard_issues:
                    total_quality_issues += 1
                    flag_parts.extend(hard_issues)
                if tone_warning:
                    total_tone_warnings += 1
                    flag_parts.append(tone_warning)
                flag = (" *** " + ", ".join(flag_parts) + " ***") if flag_parts else ""

                if mode == "test":
                    # Print only, no DB write
                    print(f"\n  [{zipcode}] {row['metro']} | Grade {letter_grade}{flag}")
                    print(f"  {interpretation}")
                elif mode in ("test-write", "full"):
                    # Write to DB, then verify it actually landed.
                    write_and_verify(supabase, dim["table"], zipcode, interpretation)

                    if mode == "test-write" or (i + 1) % 50 == 0 or (i + 1) == len(rows):
                        print(f"  [{i+1}/{len(rows)}] {zipcode} ({letter_grade}): written + verified{flag}")

                    if hard_issues:
                        print(f"    QUALITY: {interpretation[:100]}...")
                    if tone_warning:
                        print(f"    TONE: {interpretation[:100]}...")

            except WriteVerificationError as e:
                # A write that did not land must stop the batch immediately —
                # do not keep generating (and paying) into a DB that isn't saving.
                print(f"\n  *** WRITE VERIFICATION FAILED ***")
                print(f"  {e}")
                print(f"  Halting batch. Generated so far: {total_generated}. "
                      f"Nothing further will be written.")
                raise
            except Exception as e:
                total_errors += 1
                print(f"  ERROR [{zipcode}]: {e}")

            # Rate limit: 1 request per second
            time.sleep(1.0)

    # Final summary
    print(f"\n{'='*70}")
    print(f"  SUMMARY")
    print(f"{'='*70}")
    print(f"  Generated:        {total_generated}")
    print(f"  Errors:           {total_errors}")
    print(f"  Quality issues:   {total_quality_issues}")
    print(f"  Tone warnings:    {total_tone_warnings}")
    if total_quality_issues > 0:
        print(f"  *** Review flagged interpretations before approving full batch ***")
    if total_errors > 0:
        print(f"  *** {total_errors} errors — check logs above ***")
    if total_tone_warnings > 0:
        print(f"  (Tone warnings are advisory — they do not block writes)")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="D3 batch interpretation generator")
    parser.add_argument(
        "--mode",
        choices=["test", "test-write", "full", "audit"],
        required=True,
        help="test=print only, test-write=write test ZIPs, full=write all, audit=scan for contradictions",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate all interpretations, even those already present (default: skip existing)",
    )
    args = parser.parse_args()
    run(args.mode, force=args.force)
