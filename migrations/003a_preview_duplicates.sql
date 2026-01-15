-- PREVIEW ONLY: See which companies would be merged
-- Run this first to review before running 003_deduplicate_companies.sql

-- First, ensure clean_name is populated
-- (Run 002_add_clean_name.sql first if not done)

-- Find duplicates by clean_domain
SELECT
  'clean_domain duplicate' AS match_type,
  c1.id AS keep_id,
  c1.company_key AS keep_key,
  c1.name AS keep_name,
  c1.orgnr AS keep_orgnr,
  c2.id AS dup_id,
  c2.company_key AS dup_key,
  c2.name AS dup_name,
  c2.orgnr AS dup_orgnr,
  c1.clean_domain AS matched_on
FROM leadgen.companies c1
JOIN leadgen.companies c2
  ON c1.clean_domain = c2.clean_domain
  AND c1.id < c2.id
WHERE c1.clean_domain IS NOT NULL

UNION ALL

-- Find duplicates by clean_name (that don't already match on domain)
SELECT
  'clean_name duplicate' AS match_type,
  c1.id AS keep_id,
  c1.company_key AS keep_key,
  c1.name AS keep_name,
  c1.orgnr AS keep_orgnr,
  c2.id AS dup_id,
  c2.company_key AS dup_key,
  c2.name AS dup_name,
  c2.orgnr AS dup_orgnr,
  c1.clean_name AS matched_on
FROM leadgen.companies c1
JOIN leadgen.companies c2
  ON c1.clean_name = c2.clean_name
  AND c1.id < c2.id
WHERE c1.clean_name IS NOT NULL
  AND (c1.clean_domain IS NULL OR c2.clean_domain IS NULL OR c1.clean_domain != c2.clean_domain)

ORDER BY matched_on, keep_name;

-- Summary counts
SELECT
  'Total companies' AS metric,
  COUNT(*)::text AS value
FROM leadgen.companies

UNION ALL

SELECT
  'Companies with clean_domain' AS metric,
  COUNT(*)::text
FROM leadgen.companies
WHERE clean_domain IS NOT NULL

UNION ALL

SELECT
  'Companies with clean_name' AS metric,
  COUNT(*)::text
FROM leadgen.companies
WHERE clean_name IS NOT NULL

UNION ALL

SELECT
  'Duplicate groups by domain' AS metric,
  COUNT(*)::text
FROM (
  SELECT clean_domain
  FROM leadgen.companies
  WHERE clean_domain IS NOT NULL
  GROUP BY clean_domain
  HAVING COUNT(*) > 1
) d

UNION ALL

SELECT
  'Duplicate groups by name' AS metric,
  COUNT(*)::text
FROM (
  SELECT clean_name
  FROM leadgen.companies
  WHERE clean_name IS NOT NULL
  GROUP BY clean_name
  HAVING COUNT(*) > 1
) d;
