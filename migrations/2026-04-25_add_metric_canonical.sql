-- Migration: add metric_canonical to lab_results
-- Adds a canonical metric column for normalized lab metric names.
-- metric_code remains the raw extracted value (unchanged).

ALTER TABLE public.lab_results
    ADD COLUMN IF NOT EXISTS metric_canonical text;

-- Backfill: strip trailing parenthetical expressions and uppercase.
-- e.g. "ALT (SGPT)" -> "ALT", "  alt  " -> "ALT"
UPDATE public.lab_results
SET metric_canonical = UPPER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(metric_code, '\s*\(.*\)\s*$', ''), '\s+', ' ')))
WHERE metric_canonical IS NULL;

-- Apply well-known synonym aliases so that e.g. bare "SGPT" rows resolve to "ALT".
UPDATE public.lab_results
SET metric_canonical = 'ALT'
WHERE metric_canonical IN ('SGPT', 'ALT/SGPT');

-- Index to keep trend queries fast.
CREATE INDEX IF NOT EXISTS lab_results_metric_canonical_idx
    ON public.lab_results (metric_canonical);
