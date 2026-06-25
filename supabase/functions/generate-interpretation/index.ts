import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "jsr:@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

// ── Dimension definitions ───────────────────────────────────────────
// Each dimension: table name, display label, and component descriptors.
// Components list their normalized column, a human label, and qualitative
// descriptions for each tercile. Normalization already flips inverted
// components, so higher normalized = better for ALL components.
//   Tercile boundaries: low = 0–33.33, moderate = 33.34–66.66, high = 67–100.

interface ComponentDef {
  column: string;
  label: string;
  low: string;
  moderate: string;
  high: string;
}

interface DimensionDef {
  table: string;
  label: string;
  components: ComponentDef[];
}

const DIMENSIONS: Record<string, DimensionDef> = {
  respiratory: {
    table: "respiratory_scores",
    label: "Respiratory Health",
    components: [
      { column: "air_quality_normalized", label: "Air quality",
        low: "poor air quality conditions",
        moderate: "moderate air quality",
        high: "good air quality conditions" },
      { column: "environmental_burden_normalized", label: "Environmental burden",
        low: "high environmental burden from nearby pollution sources",
        moderate: "moderate environmental burden",
        high: "low environmental burden" },
      { column: "green_cover_normalized", label: "Green space",
        low: "limited green space",
        moderate: "moderate green coverage",
        high: "extensive green coverage" },
      { column: "health_outcomes_normalized", label: "Respiratory illness",
        low: "elevated respiratory illness rates",
        moderate: "moderate respiratory illness rates",
        high: "low respiratory illness rates" },
    ],
  },
  cardiovascular: {
    table: "cardiovascular_scores",
    label: "Cardiovascular Health",
    components: [
      { column: "physical_inactivity_normalized", label: "Physical activity",
        low: "high physical inactivity",
        moderate: "moderate physical activity levels",
        high: "strong physical activity levels" },
      { column: "chd_normalized", label: "Heart disease",
        low: "elevated heart disease rates",
        moderate: "moderate heart disease rates",
        high: "low heart disease rates" },
      { column: "noise_normalized", label: "Noise exposure",
        low: "high noise exposure",
        moderate: "moderate noise levels",
        high: "low noise exposure" },
      { column: "impervious_normalized", label: "Surface permeability",
        low: "high impervious surface coverage",
        moderate: "moderate surface permeability",
        high: "good surface permeability" },
    ],
  },
  stress: {
    table: "stress_scores",
    label: "Stress & Sensory Environment",
    components: [
      { column: "noise_normalized", label: "Noise exposure",
        low: "high noise exposure",
        moderate: "moderate noise levels",
        high: "low noise exposure" },
      { column: "light_pollution_normalized", label: "Light pollution",
        low: "high light pollution",
        moderate: "moderate light pollution",
        high: "low light pollution" },
      { column: "depression_normalized", label: "Depression prevalence",
        low: "elevated depression rates",
        moderate: "moderate depression rates",
        high: "low depression rates" },
      { column: "mental_health_normalized", label: "Mental health",
        low: "elevated poor mental health rates",
        moderate: "moderate mental health outcomes",
        high: "strong mental health outcomes" },
    ],
  },
  food_access: {
    table: "food_access_scores",
    label: "Food Access",
    components: [
      { column: "low_access_normalized", label: "Supermarket access",
        low: "limited supermarket access",
        moderate: "moderate supermarket access",
        high: "strong supermarket access" },
      { column: "grocery_density_normalized", label: "Grocery store density",
        low: "low grocery store density",
        moderate: "moderate grocery store density",
        high: "high grocery store density" },
      { column: "health_outcome_normalized", label: "Diet-related illness",
        low: "elevated diet-related illness rates",
        moderate: "moderate diet-related illness rates",
        high: "low diet-related illness rates" },
    ],
  },
  heat: {
    table: "heat_scores",
    label: "Heat & Climate Resilience",
    components: [
      { column: "impervious_normalized", label: "Surface permeability",
        low: "high impervious surface coverage limiting natural cooling",
        moderate: "moderate impervious surface coverage",
        high: "good surface permeability with limited heat-trapping pavement" },
      { column: "tree_canopy_normalized", label: "Tree canopy",
        low: "very limited tree canopy with minimal natural shade",
        moderate: "moderate tree coverage providing some natural cooling",
        high: "extensive tree canopy" },
      { column: "health_outcome_normalized", label: "Climate-health sensitivity",
        low: "elevated rates of heat-sensitive respiratory conditions",
        moderate: "moderate rates of heat-sensitive respiratory conditions",
        high: "low rates of heat-sensitive respiratory conditions" },
    ],
  },
};

const VALID_DIMENSIONS = Object.keys(DIMENSIONS);

// Map a normalized value (0-100) to its qualitative tercile description.
function toQualitative(value: number, comp: ComponentDef): string {
  if (value >= 66.67) return comp.high;
  if (value >= 33.34) return comp.moderate;
  return comp.low;
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  try {
    const { zipcode, dimension } = await req.json();

    if (!zipcode || typeof zipcode !== "string" || zipcode.length !== 5) {
      return new Response(
        JSON.stringify({ error: "Invalid zipcode" }),
        { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } },
      );
    }

    if (!dimension || !VALID_DIMENSIONS.includes(dimension)) {
      return new Response(
        JSON.stringify({ error: `Invalid dimension. Must be one of: ${VALID_DIMENSIONS.join(", ")}` }),
        { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } },
      );
    }

    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const serviceRoleKey = Deno.env.get("SERVICE_ROLE_KEY")!;
    const anthropicKey = Deno.env.get("ANTHROPIC_API_KEY")!;

    const supabase = createClient(supabaseUrl, serviceRoleKey);
    const dim = DIMENSIONS[dimension];

    // Build the SELECT list: letter_grade + all normalized columns for this dimension.
    // Deliberately excludes composite_score — no numbers reach the prompt.
    const selectCols = ["letter_grade", "interpretation", ...dim.components.map(c => c.column)].join(", ");

    const { data: row, error: readErr } = await supabase
      .from(dim.table)
      .select(selectCols)
      .eq("zipcode", zipcode)
      .maybeSingle();

    if (readErr) throw readErr;
    if (!row) {
      return new Response(
        JSON.stringify({ error: "ZIP not found in this dimension" }),
        { status: 404, headers: { ...corsHeaders, "Content-Type": "application/json" } },
      );
    }

    if (row.interpretation) {
      return new Response(
        JSON.stringify({ interpretation: row.interpretation, cached: true }),
        { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } },
      );
    }

    // Build qualitative component descriptions from normalized values.
    const componentLines = dim.components
      .map(comp => {
        const val = row[comp.column] as number | null;
        const desc = val != null ? toQualitative(val, comp) : "data not available";
        return `- ${comp.label}: This area has ${desc}.`;
      })
      .join("\n");

    const prompt = `You are a public health analyst writing a plain-language summary for residents and real estate professionals.

Write 2-3 sentences interpreting this neighborhood's ${dim.label} environment. The letter grade reflects how this ZIP code ranks relative to other neighborhoods in the covered metro areas for this specific dimension — it is a relative standing, not an absolute or national health judgment.

${dim.label} Grade: ${row.letter_grade}

Component conditions:
${componentLines}

Rules:
- Write plain prose only. No markdown headers, bullet points, or formatting.
- Do not mention any numbers, scores, percentages, or percentiles.
- Do not mention or echo the ZIP code number.
- Do not name the letter grade directly — describe the standing qualitatively instead.
- Do not compare to other dimensions (e.g., "better than its food score").
- Do not reference methodology, weighting, or how grades are computed.
- Do not imply the grade is an absolute or national health judgment — it is a relative standing within this dataset for this one dimension.
- Describe what residents experience in this neighborhood for this dimension.
- Frame in terms of livability and long-term health outcomes.
- Be specific and actionable — name the conditions, not abstract categories.`;

    const anthropicRes = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "x-api-key": anthropicKey,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-6",
        max_tokens: 300,
        messages: [{ role: "user", content: prompt }],
      }),
    });

    if (!anthropicRes.ok) {
      const errText = await anthropicRes.text();
      throw new Error(`Anthropic API error ${anthropicRes.status}: ${errText}`);
    }

    const anthropicData = await anthropicRes.json();
    const interpretation = anthropicData.content[0].text.trim();

    const { error: writeErr } = await supabase
      .from(dim.table)
      .update({ interpretation })
      .eq("zipcode", zipcode);

    if (writeErr) throw writeErr;

    return new Response(
      JSON.stringify({ interpretation, cached: false }),
      { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } },
    );
  } catch (err) {
    console.error("generate-interpretation error:", err);
    return new Response(
      JSON.stringify({ error: err instanceof Error ? err.message : "Unknown error" }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } },
    );
  }
});
