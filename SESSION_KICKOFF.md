# SESSION_KICKOFF.md — Claude Code Session Start Template
> Copy and paste this prompt at the start of every Claude Code session.
> Fill in the bracketed fields before sending.

---

## Kickoff Prompt (copy this verbatim, fill in brackets)

```
CLAUDE.md has been auto-read. Now please read the following files in order before doing anything else:
1. AGENTS.md (full methodology, rules, schema, quality standards)
2. TESTING.md (test suites, test runner pattern, pipeline gate rules, debugging checklist)
3. TOOL_SPECS.md (component specs for the active tool)
4. CONTEXT.md (current project state and session log)
5. ARCHITECTURE.md (system design and infrastructure constraints)

Then read the Respiratory reference implementation:
- notebooks/respiratory/ (data processing pattern)
- streamlit/tabs/respiratory.py (UI pattern)

Today's session goal: [DESCRIBE THE SPECIFIC TASK — be precise, e.g., "Ingest CDC PLACES Physical Inactivity and CHD data for all 600 ZIPs and write raw values to Supabase raw_signals table for the Cardiovascular tool"]

Current phase: [Tool 2 Cardiovascular / Tool 3 Stress / Tool 4 Food Access / Tool 5 Heat]

Before writing any code, produce a full plan following the planning mode protocol in AGENTS.md Section 2. Wait for my explicit written approval before executing any step.
```

---

## Phase-Specific Kickoff Notes

### Starting Tool 2 (Cardiovascular)
Add to kickoff: *"The highest-risk step is the BTS noise raster processing. Plan for this first and flag any Colab environment setup needed for rasterio."*

### Starting Tool 3 (Stress / Sensory)
Add to kickoff: *"BTS noise is already processed — read the cached values from Supabase raw_signals before touching any raster files. Confirm the 4th component with me before starting."*

### Starting Tool 4 (Food Access)
Add to kickoff: *"No raster processing in this phase — purely tabular. The USDA FARA data is at census tract level and requires a ZCTA crosswalk. Plan for that mapping step explicitly."*

### Starting Tool 5 (Heat & Climate)
Add to kickoff: *"NLCD impervious and tree canopy are already processed from Tools 2 and 3. Read cached values from Supabase. Only USGS heat island data requires fresh processing."*

### Starting Integration & QA
Add to kickoff: *"Read all five [tool]_scores tables from Supabase. The goal is cross-tool consistency — do scores tell a coherent story for known neighborhoods? Flag any ZIP codes that look anomalous across multiple dimensions."*

---

## Mid-Session Reorientation Prompt

If Claude Code seems to drift or lose context mid-session:

```
Stop. Re-read AGENTS.md Section [X] and CONTEXT.md before continuing.
Summarize what you understand the current task to be and what you've done so far.
```

---

## End-of-Session Wrap-Up Prompt

```
Before we close this session:
1. Summarize what was completed
2. Note where we left off
3. Identify what the next session should start with
4. Flag any unresolved issues or surprises
5. Update CONTEXT.md with a new session log entry
```
