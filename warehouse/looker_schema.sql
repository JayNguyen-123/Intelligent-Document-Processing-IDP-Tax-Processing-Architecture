-- warehouse/looker_schema.sql
-- Replace YOUR_PROJECT_ID before running manually (load_to_bigquery.py does this for you).
-- Column list must match shared/tax_record.py::BQ_COLUMNS (enforced by unit test).

CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.tax_processing_ds.verified_w2_records`
(
  document_id             STRING        OPTIONS(description="Deterministic id: sha256(bucket/object#generation)"),
  employer_name           STRING        OPTIONS(description="Box c - employer legal name"),
  employer_ein            STRING        OPTIONS(description="Box b - employer EIN, NN-NNNNNNN"),
  wages                   NUMERIC       OPTIONS(description="Box 1 - wages, tips, other compensation"),
  fed_income_tax_withheld NUMERIC       OPTIONS(description="Box 2 - federal income tax withheld"),
  tax_year                INT64         OPTIONS(description="Tax year of the form"),
  review_source           STRING        OPTIONS(description="AUTOMATED or HUMAN"),
  reviewed_by             STRING        OPTIONS(description="Reviewer email for HUMAN rows"),
  low_confidence_fields   ARRAY<STRING> OPTIONS(description="Fields below threshold at ingestion"),
  source_bucket           STRING        OPTIONS(description="Raw upload bucket"),
  source_file             STRING        OPTIONS(description="Raw upload object name"),
  schema_version          STRING        OPTIONS(description="Payload contract version"),
  processed_at            TIMESTAMP     OPTIONS(description="UTC time the row was written")
)
PARTITION BY DATE(processed_at)
CLUSTER BY tax_year, employer_ein;


-- Semantic view for Looker Studio.
--  * De-duplicates at-least-once deliveries / re-approvals: latest row per document_id wins.
--  * NULL-safe review flags (the previous version labelled rows with NULL wages as "Compliant").
--  * Amounts stay NUMERIC (exact); only the ratio is FLOAT64.
--  * Flags are heuristics for data-quality review, NOT an IRS audit-risk score.

CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.tax_processing_ds.vw_looker_tax_analytics` AS
WITH latest AS (
  SELECT *
  FROM `YOUR_PROJECT_ID.tax_processing_ds.verified_w2_records`
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY COALESCE(document_id, TO_HEX(MD5(TO_JSON_STRING(STRUCT(employer_ein, tax_year, wages, source_file)))))
    ORDER BY processed_at DESC
  ) = 1
),
rated AS (
  SELECT
    *,
    SAFE_DIVIDE(CAST(fed_income_tax_withheld AS FLOAT64), CAST(wages AS FLOAT64)) AS withholding_rate
  FROM latest
)
SELECT
  document_id                    AS Document_ID,
  employer_name                  AS Employer_Name,
  employer_ein                   AS Employer_EIN,
  tax_year                       AS Tax_Year,
  processed_at                   AS Processing_Timestamp,
  DATE(processed_at)             AS Ingestion_Date,
  review_source                  AS Review_Source,
  reviewed_by                    AS Reviewed_By,
  ARRAY_LENGTH(low_confidence_fields) AS Low_Confidence_Field_Count,
  wages                          AS Taxable_Wages,
  fed_income_tax_withheld        AS Federal_Withholding,
  withholding_rate               AS Effective_Withholding_Rate,
  CASE
    WHEN wages IS NULL OR fed_income_tax_withheld IS NULL THEN 'Incomplete record'
    WHEN wages <= 0                                       THEN 'Invalid: non-positive wages'
    WHEN fed_income_tax_withheld > wages                  THEN 'Invalid: withholding exceeds wages'
    WHEN withholding_rate < 0.05                          THEN 'Review: low withholding rate (<5%)'
    WHEN withholding_rate > 0.40                          THEN 'Review: high withholding rate (>40%)'
    ELSE 'Within expected range'
  END AS Withholding_Review_Flag
FROM rated;
