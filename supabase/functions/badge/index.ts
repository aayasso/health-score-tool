import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "jsr:@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
};

const htmlHeaders = {
  ...corsHeaders,
  "Content-Type": "text/html; charset=utf-8",
};

const gradeColors: Record<string, string> = {
  A: "#4ade80",
  B: "#facc15",
  C: "#fb923c",
  D: "#f87171",
  F: "#dc2626",
};

const gradeLabels: Record<string, string> = {
  A: "Excellent Health Environment",
  B: "Strong Health Environment",
  C: "Average Health Environment",
  D: "Below Average Health Environment",
  F: "Limited Health Environment",
};

function renderBadge(
  zip: string,
  score: number | null,
  grade: string | null
): string {
  const hasData = score !== null && grade !== null;
  const badgeColor = hasData ? gradeColors[grade] ?? "#9ca3af" : "#9ca3af";
  const label = hasData ? gradeLabels[grade] ?? "" : "Not yet available";
  const link = `https://lasalletech.ai/healthscore?zip=${zip}`;

  return `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=280">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@600;700&family=Inter:wght@400;500&display=swap" rel="stylesheet">
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { width: 280px; height: 70px; overflow: hidden; background: transparent; }
a.badge {
  display: flex;
  flex-direction: row;
  align-items: center;
  width: 280px;
  height: 70px;
  background: #1B4332;
  text-decoration: none;
  padding: 10px 16px;
  gap: 12px;
}
.grade-pill {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 36px;
  height: 36px;
  border-radius: 50%;
  background: ${badgeColor};
  font-family: 'Barlow Condensed', sans-serif;
  font-weight: 700;
  font-size: 18px;
  color: #1a1a1a;
  flex-shrink: 0;
}
.center {
  display: flex;
  flex-direction: column;
  gap: 2px;
}
.title {
  font-family: 'Barlow Condensed', sans-serif;
  font-weight: 600;
  font-size: 9px;
  color: #E8E0CE;
  text-transform: uppercase;
  letter-spacing: 1.2px;
}
.score-value {
  font-family: 'Barlow Condensed', sans-serif;
  font-weight: 700;
  font-size: 22px;
  color: #E8E0CE;
  line-height: 1;
}
.score-suffix {
  font-family: 'Barlow Condensed', sans-serif;
  font-weight: 600;
  font-size: 12px;
  color: #E8E0CE;
  opacity: 0.7;
}
.grade-label {
  font-family: 'Inter', sans-serif;
  font-weight: 400;
  font-size: 9px;
  color: #E8E0CE;
  opacity: 0.75;
}
.branding {
  font-family: 'Inter', sans-serif;
  font-weight: 400;
  font-size: 8px;
  color: #E8E0CE;
  opacity: 0.5;
  margin-left: auto;
  align-self: flex-end;
}
</style>
</head>
<body>
<a class="badge" href="${link}" target="_blank" rel="noopener noreferrer">
  ${hasData ? `<span class="grade-pill">${grade}</span>` : ""}
  <div class="center">
    <div class="title">Neighborhood Health Score</div>
    ${hasData ? `<div class="score-value">${Math.round(score)}<span class="score-suffix">/100</span></div>` : `<div class="grade-label">Not yet available</div>`}
    ${hasData ? `<div class="grade-label">${label}</div>` : ""}
  </div>
  <div class="branding">lasalletech.ai</div>
</a>
</body>
</html>`;
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("", { status: 200, headers: corsHeaders });
  }

  const url = new URL(req.url);
  const zip = url.searchParams.get("zip");

  if (!zip || !/^\d{5}$/.test(zip)) {
    return new Response(renderBadge(zip ?? "00000", null, null), {
      status: 200,
      headers: { ...htmlHeaders },
    });
  }

  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const supabaseAnonKey = Deno.env.get("SUPABASE_ANON_KEY")!;
    const supabase = createClient(supabaseUrl, supabaseAnonKey);

    const { data, error } = await supabase
      .from("overall_scores")
      .select("composite_score, letter_grade")
      .eq("zipcode", zip)
      .maybeSingle();

    if (error) throw error;

    if (!data) {
      return new Response(renderBadge(zip, null, null), {
        status: 200,
        headers: { ...htmlHeaders },
      });
    }

    return new Response(
      renderBadge(zip, data.composite_score, data.letter_grade),
      { status: 200, headers: { ...htmlHeaders } }
    );
  } catch (err) {
    console.error("badge error:", err);
    return new Response(renderBadge(zip, null, null), {
      status: 200,
      headers: { ...htmlHeaders },
    });
  }
});
