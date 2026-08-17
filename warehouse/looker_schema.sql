-- DDL Script: Manually Create the Core Table Structure inside BigQuery
-- Path: warehouse/looker_schema.sql

CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.tax_processing_ds.verified_w2_records`
(
  employer_name STRING OPTIONS(description="Legal company name parsed from the tax document"),
  employer_ein STRING OPTIONS(description="Federal Employer Identification Number mapping"),
  wages NUMERIC OPTIONS(description="Gross taxable wages, tips, and other compensation"),
  fed_income_tax_withheld NUMERIC OPTIONS(description="Total federal income tax withheld from the employee"),
  tax_year INT64 OPTIONS(description="The calendar tax year reporting code"),
  processed_at TIMESTAMP OPTIONS(description="System timestamp tracking pipeline ingestion execution time")
)
PARTITION BY DATE(processed_at)
CLUSTER BY employer_name, tax_year;


-- Semantic View Layer: Optimized Data Source Model for Google Looker Studio
-- This view abstracts raw types, casts numeric values, and adds calculated business metrics
-- dynamically to optimize performance and prevent analytical calculation drift.

CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.tax_processing_ds.vw_looker_tax_analytics` AS
SELECT
  -- Core Field Dimensions
  employer_name AS Employer_Name,
  employer_ein AS Employer_EIN,
  tax_year AS Tax_Year,
  processed_at AS Processing_Timestamp,
  EXTRACT(DATE FROM processed_at) AS Ingestion_Date,

  -- Financial Fact Metrics (Cast to safe descriptive floating targets)
  CAST(wages AS FLOAT64) AS Taxable_Wages,
  CAST(fed_income_tax_withheld AS FLOAT64) AS Federal_Withholding,

  -- Pre-computed Business Logic Insights
  SAFE_DIVIDE(CAST(fed_income_tax_withheld AS FLOAT64), CAST(wages AS FLOAT64)) AS Effective_Withholding_Rate,

  -- Algorithmic System Risk Tagging
  CASE 
    WHEN wages <= 0 THEN '❌ Invalid Wages Data'
    WHEN SAFE_DIVIDE(CAST(fed_income_tax_withheld AS FLOAT64), CAST(wages AS FLOAT64)) < 0.05 THEN '⚠️ High Audit Risk (Under-withholding)'
    WHEN SAFE_DIVIDE(CAST(fed_income_tax_withheld AS FLOAT64), CAST(wages AS FLOAT64)) > 0.40 THEN '⚠️ High Audit Risk (Over-withholding)'
    ELSE '✅ Compliant Standard Range'
  END AS Compliance_Audit_Status

FROM
  `YOUR_PROJECT_ID.tax_processing_ds.verified_w2_records`;
