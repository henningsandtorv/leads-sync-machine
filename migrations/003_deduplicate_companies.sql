-- Deduplicate companies by merging duplicates based on clean_domain and clean_name
-- This script:
-- 1. Identifies duplicate groups
-- 2. Picks the "best" record in each group (prefers orgnr, then most data)
-- 3. Updates all foreign keys to point to the kept record
-- 4. Deletes the duplicate records

-- Run this AFTER 002_add_clean_name.sql

BEGIN;

-- Step 1: Create a temp table to identify duplicates and pick winners
-- Priority: records with orgnr > records with more non-null fields > older records (lower id)
CREATE TEMP TABLE company_duplicates AS
WITH ranked_companies AS (
  SELECT
    id,
    company_key,
    name,
    clean_domain,
    clean_name,
    orgnr,
    -- Score based on data completeness (higher = better)
    (CASE WHEN orgnr IS NOT NULL THEN 100 ELSE 0 END) +
    (CASE WHEN clean_domain IS NOT NULL THEN 10 ELSE 0 END) +
    (CASE WHEN proff_url IS NOT NULL THEN 5 ELSE 0 END) +
    (CASE WHEN industry IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN company_size IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN location IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN profit_before_tax IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN turnover IS NOT NULL THEN 1 ELSE 0 END) AS data_score,
    created_at
  FROM leadgen.companies
),
-- Find duplicates by clean_domain (excluding nulls)
domain_groups AS (
  SELECT
    clean_domain,
    array_agg(id ORDER BY data_score DESC, created_at ASC) AS ids
  FROM ranked_companies
  WHERE clean_domain IS NOT NULL
  GROUP BY clean_domain
  HAVING COUNT(*) > 1
),
-- Find duplicates by clean_name (excluding nulls and those already grouped by domain)
name_groups AS (
  SELECT
    rc.clean_name,
    array_agg(rc.id ORDER BY rc.data_score DESC, rc.created_at ASC) AS ids
  FROM ranked_companies rc
  WHERE rc.clean_name IS NOT NULL
    -- Exclude companies that are already in a domain group
    AND NOT EXISTS (
      SELECT 1 FROM domain_groups dg
      WHERE rc.id = ANY(dg.ids)
    )
  GROUP BY rc.clean_name
  HAVING COUNT(*) > 1
),
-- Combine all duplicate groups
all_groups AS (
  SELECT ids FROM domain_groups
  UNION ALL
  SELECT ids FROM name_groups
)
-- For each group, first element is the keeper, rest are duplicates
SELECT
  ids[1] AS keep_id,
  unnest(ids[2:]) AS duplicate_id
FROM all_groups;

-- Show what we're about to merge (for review)
SELECT
  d.keep_id,
  d.duplicate_id,
  k.company_key AS keep_key,
  k.name AS keep_name,
  k.clean_domain AS keep_domain,
  k.orgnr AS keep_orgnr,
  dup.company_key AS dup_key,
  dup.name AS dup_name,
  dup.clean_domain AS dup_domain,
  dup.orgnr AS dup_orgnr
FROM company_duplicates d
JOIN leadgen.companies k ON k.id = d.keep_id
JOIN leadgen.companies dup ON dup.id = d.duplicate_id
ORDER BY k.name, dup.name;

-- Step 2: Update job_posts to point to the kept company
UPDATE leadgen.job_posts jp
SET company_id = d.keep_id
FROM company_duplicates d
WHERE jp.company_id = d.duplicate_id;

-- Step 3: Update company_people links
-- First, handle potential conflicts (same person linked to both keeper and duplicate)
-- Delete the duplicate links that would conflict
DELETE FROM leadgen.company_people cp
USING company_duplicates d
WHERE cp.company_id = d.duplicate_id
  AND EXISTS (
    SELECT 1 FROM leadgen.company_people existing
    WHERE existing.company_id = d.keep_id
      AND existing.person_id = cp.person_id
      AND existing.role = cp.role
  );

-- Now update remaining company_people links
UPDATE leadgen.company_people cp
SET company_id = d.keep_id
FROM company_duplicates d
WHERE cp.company_id = d.duplicate_id;

-- Step 4: Merge data from duplicates into keeper (only fill in nulls)
UPDATE leadgen.companies k
SET
  domain = COALESCE(k.domain, dup.domain),
  clean_domain = COALESCE(k.clean_domain, dup.clean_domain),
  clean_name = COALESCE(k.clean_name, dup.clean_name),
  orgnr = COALESCE(k.orgnr, dup.orgnr),
  proff_url = COALESCE(k.proff_url, dup.proff_url),
  industry = COALESCE(k.industry, dup.industry),
  company_size = COALESCE(k.company_size, dup.company_size),
  location = COALESCE(k.location, dup.location),
  sector = COALESCE(k.sector, dup.sector),
  profit_before_tax = COALESCE(k.profit_before_tax, dup.profit_before_tax),
  turnover = COALESCE(k.turnover, dup.turnover)
FROM company_duplicates d
JOIN leadgen.companies dup ON dup.id = d.duplicate_id
WHERE k.id = d.keep_id;

-- Step 5: Delete duplicate companies
DELETE FROM leadgen.companies c
USING company_duplicates d
WHERE c.id = d.duplicate_id;

-- Step 6: Report results
SELECT
  'Duplicates merged' AS action,
  COUNT(*) AS count
FROM company_duplicates;

-- Clean up
DROP TABLE company_duplicates;

COMMIT;
