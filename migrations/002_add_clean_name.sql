-- Add clean_name column for fuzzy company matching
-- Stores normalized company name (lowercase, no legal suffixes like AS/ASA/Ltd)

ALTER TABLE leadgen.companies ADD COLUMN IF NOT EXISTS clean_name text NULL;

-- Create index for efficient lookups
CREATE INDEX IF NOT EXISTS companies_clean_name_idx ON leadgen.companies USING btree (clean_name);

-- Backfill existing companies with clean_name
-- This uses a simple approach; the full normalization is done in application code
UPDATE leadgen.companies
SET clean_name = LOWER(
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            TRIM(name),
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
WHERE clean_name IS NULL AND name IS NOT NULL;
