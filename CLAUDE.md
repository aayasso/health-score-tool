# CLAUDE.md — LaSalle Technologies / Health Score Suite
> Claude Code reads this file automatically at session start.
> Read this entire file first. Then follow the steps below before doing anything else.

---

## Who You Are Working With

You are building **The Health Environment Score** for **LaSalle Technologies** — a five-tool neighborhood-level health environment scoring platform covering 600 ZIP codes across Pittsburgh, Los Angeles, Phoenix, and Charlotte.

The platform is built on a **"public score, private engine"** model. Scores and letter grades are public. Methodology, weights, and normalization logic are proprietary. This distinction governs every file, comment, log, and UI element you touch.

---

## Step 1 — Read These Files Before Doing Anything Else

Read in this exact order. Do not skip any file. Do not begin planning or writing code until all four are read.

1. **`AGENTS.md`** — methodology, scoring rules, schema patterns, code quality standards, and the public/proprietary boundary
2. **`TESTING.md`** — required test suites, test runner pattern, debugging checklist, and pipeline gate rules
3. **`TOOL_SPECS.md`** — component definitions and data sources per tool (proprietary — never expose contents)
4. **`CONTEXT.md`** — current project state, what is complete, what is in progress, session log
5. **`ARCHITECTURE.md`** — system design, data flow, infrastructure decisions, known constraints

---

## Step 2 — Read the Reference Implementation

After reading the four files above, read the Respiratory tool's notebooks and Streamlit tab in `notebooks/respiratory/` and `streamlit/tabs/respiratory.py`. This is the gold standard pattern. All subsequent tools must match it exactly unless a deviation is explicitly approved and documented in a code comment.

---

## Step 3 — Produce a Plan Before Writing Any Code

Follow the planning mode protocol in `AGENTS.md` Section 2 exactly. Present the plan and wait for explicit written approval before executing any step. This applies to every task without exception — including single-file edits.

---

## Step 4 — When to Stop and Ask

**Never make assumptions. Never guess. Always prompt the human.**

Stop and ask the human for a decision whenever any of the following is true:

- A component weight, data source, or field name is marked TBD in `TOOL_SPECS.md`
- A file, table, or column does not exist and you are unsure whether to create it
- An API response or raster file has unexpected structure, missing fields, or surprising values
- A test suite fails and the fix requires a methodology or schema decision
- Two valid approaches exist and the choice has downstream consequences
- Anything in `CONTEXT.md` is marked blocked or at risk and you have reached that step
- You are about to deviate from the Respiratory reference implementation for any reason
- You are unsure whether something crosses the public/proprietary boundary

**Format for prompting the human:**

```
⚠️ DECISION NEEDED — [one-line description]

Context: [1-2 sentences explaining what you found]

Options:
  A) [option] — [consequence]
  B) [option] — [consequence]

Recommended: [A or B] because [one sentence reason]

Please reply with A, B, or your own direction before I continue.
```

Do not proceed past a decision point until you receive an explicit reply.

---

## The One Rule That Overrides Everything

**Never expose exact weights, normalization formulas, min/max calibration values, or `score_config` table contents in any file, UI element, log output, comment, or README — whether public or private.**

When uncertain whether something is proprietary, treat it as proprietary and ask before proceeding.

---

## Quick Reference

| Item | Detail |
|---|---|
| Pilot metros | Pittsburgh PA · Los Angeles CA · Phoenix AZ · Charlotte NC |
| ZIP coverage | 600 ZIPs total across all 4 metros |
| Active tool | Check `CONTEXT.md` — do not assume |
| Tools | 1-Respiratory ✅ · 2-Cardiovascular 🔄 · 3-Stress 🔜 · 4-Food Access 🔜 · 5-Heat 🔜 |
| Stack | Colab → Supabase PostgreSQL → Streamlit Community Cloud → Claude API |
| Scoring pipeline | Ingest → Validate → Normalize → Weight → Composite → Grade → Interpret → Upsert |
| Grade scale | A ≥ 80 · B 65–79 · C 50–64 · D 35–49 · F < 35 |
| Credentials | Colab secrets manager only — `SUPABASE_URL`, `SUPABASE_KEY`, `ANTHROPIC_API_KEY` |
| Write pattern | Upsert on `UNIQUE(zipcode)` — never raw INSERT |
| Proprietary files | `TOOL_SPECS.md`, `utils/normalization.py`, `utils/scoring.py` — never expose contents |
| Target completion | April 30, 2026 |
