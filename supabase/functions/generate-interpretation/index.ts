import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "jsr:@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  try {
    const { zipcode } = await req.json();

    if (!zipcode || typeof zipcode !== "string" || zipcode.length !== 5) {
      return new Response(
        JSON.stringify({ error: "Invalid zipcode" }),
        { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const serviceRoleKey = Deno.env.get("SERVICE_ROLE_KEY")!;
    const anthropicKey = Deno.env.get("ANTHROPIC_API_KEY")!;

    const supabase = createClient(supabaseUrl, serviceRoleKey);

    const { data: existing, error: readErr } = await supabase
      .from("overall_scores")
      .select("composite_score, letter_grade, interpretation")
      .eq("zipcode", zipcode)
      .maybeSingle();

    if (readErr) throw readErr;
    if (!existing) {
      return new Response(
        JSON.stringify({ error: "ZIP not found" }),
        { status: 404, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    if (existing.interpretation) {
      return new Response(
        JSON.stringify({ interpretation: existing.interpretation, cached: true }),
        { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    const domainTables = [
      { table: "respiratory_scores", label: "Respiratory" },
      { table: "cardiovascular_scores", label: "Cardiovascular" },
      { table: "stress_scores", label: "Stress & Sensory" },
      { table: "food_access_scores", label: "Food Access" },
      { table: "heat_scores", label: "Heat Resilience" },
    ];

    const domainScores: Record<string, number> = {};
    for (const { table, label } of domainTables) {
      const { data, error } = await supabase
        .from(table)
        .select("composite_score")
        .eq("zipcode", zipcode)
        .maybeSingle();
      if (error) throw error;
      if (data) domainScores[label] = data.composite_score;
    }

    const prompt = `You are a public health analyst writing a plain-language summary for residents and real estate professionals. Write 2-3 sentences interpreting this neighborhood's overall health environment score, which combines five dimensions: respiratory health, cardiovascular health, stress and sensory environment, food access, and heat resilience.

ZIP Code: ${zipcode}
Overall Score: ${existing.composite_score}/100 (Grade: ${existing.letter_grade})
Dimension scores:
  Respiratory: ${domainScores["Respiratory"] ?? "n/a"}
  Cardiovascular: ${domainScores["Cardiovascular"] ?? "n/a"}
  Stress & Sensory: ${domainScores["Stress & Sensory"] ?? "n/a"}
  Food Access: ${domainScores["Food Access"] ?? "n/a"}
  Heat Resilience: ${domainScores["Heat Resilience"] ?? "n/a"}

Rules:
- Be specific, factual, and actionable
- Do not use jargon
- Do not mention exact scores or numbers from the dimensions
- Do not reveal how dimensions are weighted or combined
- Do not say "based on our methodology" or any similar phrase
- Highlight the strongest and weakest dimensions
- Frame in terms of livability and long-term health outcomes`;

    const anthropicRes = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "x-api-key": anthropicKey,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
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
      .from("overall_scores")
      .update({ interpretation })
      .eq("zipcode", zipcode);

    if (writeErr) throw writeErr;

    return new Response(
      JSON.stringify({ interpretation, cached: false }),
      { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  } catch (err) {
    console.error("generate-interpretation error:", err);
    return new Response(
      JSON.stringify({ error: err instanceof Error ? err.message : "Unknown error" }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  }
});