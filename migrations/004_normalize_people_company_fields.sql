-- Normalize people.normalized_company_name to match companies.clean_name format
-- This ensures both use the same normalization (strip legal suffixes, lowercase, alphanumeric only)

-- The normalization logic matches normalizeCompanyNameForMatching() in TypeScript:
-- 1. Trim and lowercase
-- 2. Strip legal suffixes (AS, ASA, Ltd, Inc, etc.)
-- 3. Remove all non-alphanumeric characters

BEGIN;

-- Update normalized_company_name on people to match companies.clean_name format
UPDATE leadgen.people
SET normalized_company_name = LOWER(
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            TRIM(normalized_company_name),
            '\s+(ASA|AS|A/S|ANS|DA|BA|SA|NUF|KS)$', '', 'i'
          ),
          '\s+(Corporation|Incorporated|Limited|Company|Corp\.?|Inc\.?|Ltd\.?|LLC|LLP|GmbH|AG|BV|NV|PLC|Co\.?)$', '', 'i'
        ),
        '\s+(Group|Holding|Holdings)$', '', 'i'
      ),
      '[^a-zA-Z0-9æøåäöüÆØÅÄÖÜ]+', '', 'g'
    ),
    '(.+)', '\1'
  )
)
WHERE normalized_company_name IS NOT NULL;

-- Verify: Check if people.normalized_company_name now matches companies.clean_name
SELECT
  'People with matching company by name' AS metric,
  COUNT(DISTINCT p.id)::text AS value
FROM leadgen.people p
JOIN leadgen.companies c ON p.normalized_company_name = c.clean_name
WHERE p.normalized_company_name IS NOT NULL

UNION ALL

SELECT
  'People with matching company by domain' AS metric,
  COUNT(DISTINCT p.id)::text
FROM leadgen.people p
JOIN leadgen.companies c ON p.normalized_company_domain = c.clean_domain
WHERE p.normalized_company_domain IS NOT NULL

UNION ALL

SELECT
  'Total people with normalized_company_name' AS metric,
  COUNT(*)::text
FROM leadgen.people
WHERE normalized_company_name IS NOT NULL;

COMMIT;
