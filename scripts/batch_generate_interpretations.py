"""
D3 — Batch interpretation generator.
Replicates D1 edge function prompt logic with qualitative tercile conversion.
Run in Colab with SUPABASE_URL, SUPABASE_KEY (service role), ANTHROPIC_API_KEY in secrets.

Usage:
  python batch_generate_interpretations.py --mode test         # Print 50 interpretations, no DB write
  python batch_generate_interpretations.py --mode test-write   # Write 50 to DB for SQL verification
  python batch_generate_interpretations.py --mode full         # Write all 2,870 (574 ZIPs x 5 dims)
"""

import os
import sys
import time
import argparse
import re
import anthropic
from supabase import create_client

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

IN_SCOPE_METROS = ["Pittsburgh", "Los Angeles", "Phoenix", "Charlotte"]

# 10 test ZIPs — 2 per metro + 2 extra, spanning A/C/F grades.
# UPDATE THESE after running the test-ZIP query.
TEST_ZIPS = [
    "28078",  # Charlotte — heat A
    "90290",  # Los Angeles — heat A
    "90006",  # Los Angeles — heat F
    "85382",  # Phoenix — heat C
    "85301",  # Phoenix — heat F
    "15046",  # Pittsburgh — heat A
    "15213",  # Pittsburgh — heat C
    "15110",  # Pittsburgh — heat F
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


def build_prompt(zipcode, dim_key, letter_grade, row):
    """Build the D1 per-dimension prompt with qualitative tercile inputs."""
    dim = DIMENSIONS[dim_key]
    lines = []
    for comp in dim["components"]:
        val = row.get(comp["column"])
        desc = to_qualitative(val, comp)
        lines.append(f"- {comp['label']}: This area has {desc}.")
    component_text = "\n".join(lines)

    return (
        "You are a public health analyst writing a plain-language summary "
        "for residents and real estate professionals.\n"
        "\n"
        f"Write 2-3 sentences interpreting this neighborhood's {dim['label']} "
        "environment. The letter grade reflects how this ZIP code ranks "
        "relative to other neighborhoods in the covered metro areas for this "
        "specific dimension — it is a relative standing, not an absolute or "
        "national health judgment.\n"
        "\n"
        f"{dim['label']} Grade: {letter_grade}\n"
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
        "relative standing qualitatively in words instead (for example "
        "\"a relatively strong standing\" or \"ranks among the more vulnerable "
        "areas\").\n"
        "- Do not compare to other dimensions (e.g., \"better than its food score\").\n"
        "- Do not reference methodology, weighting, or how grades are computed.\n"
        "- Do not imply the grade is an absolute or national health judgment — "
        "it is a relative standing within this dataset for this one dimension.\n"
        "- Describe what residents experience in this neighborhood for this dimension.\n"
        "- Frame in terms of livability and long-term health outcomes.\n"
        "- Be specific and actionable — name the conditions, not abstract categories."
    )


def check_output_quality(text):
    """Check interpretation for score leaks, markdown, and digits."""
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
    return issues


class WriteVerificationError(Exception):
    """Raised when a DB write did not land as expected. Halts the batch."""
    pass


def write_and_verify(supabase, table, zipcode, interpretation):
    """Update one row's interpretation, then re-read it and confirm it landed.

    Guards against silent RLS/permission failures: supabase-py does NOT raise
    when an UPDATE affects zero rows (e.g. wrong key, no UPDATE policy), it just
    returns an empty result. This re-reads the row and raises if the stored text
    does not match what we just wrote, so a silent no-op stops the batch loudly
    instead of being reported as success.
    """
    update_resp = supabase.table(table) \
        .update({"interpretation": interpretation}) \
        .eq("zipcode", zipcode) \
        .execute()

    # PostgREST returns the updated rows by default. Empty == nothing written.
    if not getattr(update_resp, "data", None):
        raise WriteVerificationError(
            f"{table} / {zipcode}: UPDATE returned no rows — write was blocked "
            f"(likely RLS/permission: confirm SUPABASE_KEY is the service role key)."
        )

    # Re-read the row and confirm the persisted text matches.
    check = supabase.table(table) \
        .select("interpretation") \
        .eq("zipcode", zipcode) \
        .limit(1) \
        .execute()

    if not check.data:
        raise WriteVerificationError(
            f"{table} / {zipcode}: row not found on re-read after UPDATE."
        )

    stored = check.data[0].get("interpretation")
    if stored != interpretation:
        raise WriteVerificationError(
            f"{table} / {zipcode}: stored text does not match generated text "
            f"after UPDATE — write did not persist correctly."
        )


def run(mode):
    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = os.environ.get("SUPABASE_KEY", "")
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")

    if not all([supabase_url, supabase_key, anthropic_key]):
        print("ERROR: Set SUPABASE_URL, SUPABASE_KEY, ANTHROPIC_API_KEY in environment.")
        sys.exit(1)

    supabase = create_client(supabase_url, supabase_key)
    client = anthropic.Anthropic(api_key=anthropic_key)

    total_generated = 0
    total_errors = 0
    total_quality_issues = 0

    for dim_key, dim in DIMENSIONS.items():
        comp_cols = [c["column"] for c in dim["components"]]
        select_cols = ",".join(["zipcode", "metro", "letter_grade"] + comp_cols)

        # Fetch in-scope rows
        query = supabase.table(dim["table"]) \
            .select(select_cols) \
            .in_("metro", IN_SCOPE_METROS) \
            .not_.is_("letter_grade", "null")

        if mode in ("test", "test-write"):
            query = query.in_("zipcode", TEST_ZIPS)

        # Supabase client paginates at 1000 by default; 574 fits in one page
        response = query.execute()
        rows = response.data

        print(f"\n{'='*70}")
        print(f"  {dim['label']}  |  {len(rows)} ZIPs  |  mode={mode}")
        print(f"{'='*70}")

        for i, row in enumerate(rows):
            zipcode = row["zipcode"]
            letter_grade = row["letter_grade"]
            prompt = build_prompt(zipcode, dim_key, letter_grade, row)

            try:
                resp = client.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=300,
                    messages=[{"role": "user", "content": prompt}],
                )
                interpretation = resp.content[0].text.strip()
                total_generated += 1

                # Quality check
                issues = check_output_quality(interpretation)
                if issues:
                    total_quality_issues += 1
                    flag = " *** " + ", ".join(issues) + " ***"
                else:
                    flag = ""

                if mode == "test":
                    # Print only, no DB write
                    print(f"\n  [{zipcode}] {row['metro']} | Grade {letter_grade}{flag}")
                    print(f"  {interpretation}")
                elif mode in ("test-write", "full"):
                    # Write to DB, then verify it actually landed.
                    write_and_verify(supabase, dim["table"], zipcode, interpretation)

                    if mode == "test-write" or (i + 1) % 50 == 0 or (i + 1) == len(rows):
                        print(f"  [{i+1}/{len(rows)}] {zipcode} ({letter_grade}): written + verified{flag}")

                    if issues:
                        print(f"    QUALITY: {interpretation[:100]}...")

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
    print(f"  Generated:      {total_generated}")
    print(f"  Errors:         {total_errors}")
    print(f"  Quality issues: {total_quality_issues}")
    if total_quality_issues > 0:
        print(f"  *** Review flagged interpretations before approving full batch ***")
    if total_errors > 0:
        print(f"  *** {total_errors} errors — check logs above ***")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="D3 batch interpretation generator")
    parser.add_argument(
        "--mode",
        choices=["test", "test-write", "full"],
        required=True,
        help="test=print only, test-write=write 10 ZIPs, full=write all 574 ZIPs",
    )
    args = parser.parse_args()
    run(args.mode)
