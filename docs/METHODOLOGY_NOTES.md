# Methodology Notes — Health Environment Score

Running log of methodology decisions. These feed into the F2 methodology brief but are captured here as they're made so nothing is lost.

---

### 2026-05-24 — Percentile-based grades are per-dimension and not cross-comparable

Grades are percentile-based and computed PER DIMENSION over the in-scope population (Pittsburgh, LA, Phoenix, Charlotte). A letter grade means rank-within-that-dimension, NOT an absolute score, and NOT comparable across dimensions. Example: a respiratory composite of ~59 earns an A (top 10% for respiratory), while a stress composite of ~59 earns an F (bottom decile for stress), because each dimension is graded against its own distribution.

Distribution is bell-ish: A = top 10% (≥90th pct), B = 70th–90th, C = 30th–70th, D = 10th–30th, F = bottom 10%.

Precedent: CDC/ATSDR SVI and CalEnviroScreen use percentile/rank-based classification.

Implication for the product: grades must be presented as within-dimension standings; the F2 methodology brief and any frontend copy must not imply cross-dimension grade comparability.
