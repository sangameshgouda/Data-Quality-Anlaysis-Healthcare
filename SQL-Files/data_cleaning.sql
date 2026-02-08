USE dq_healthcare;

--  dq_dashboard_summary (KPIs + trends)



CREATE VIEW dq_dashboard_summary AS
WITH totals AS (
  SELECT
    batch_id,
    DATE(loaded_at) AS load_dt,
    COUNT(*) AS total_rows
  FROM claims_raw
  GROUP BY batch_id, DATE(loaded_at)
),
fails AS (
  SELECT
    batch_id,
    DATE(loaded_at) AS load_dt,
    COUNT(*) AS failed_rows
  FROM claims_error
  GROUP BY batch_id, DATE(loaded_at)
),
checks AS (
  SELECT
    batch_id,
    DATE(run_at) AS run_dt,
    dataset_name,
    check_name,
    total_rows,
    failed_rows,
    fail_pct,
    severity
  FROM dq_results
)
SELECT
  t.batch_id,
  t.load_dt AS dt,

  t.total_rows,
  COALESCE(f.failed_rows, 0) AS failed_rows,
  (t.total_rows - COALESCE(f.failed_rows, 0)) AS passed_rows,

  ROUND(100 * (t.total_rows - COALESCE(f.failed_rows, 0)) / NULLIF(t.total_rows,0), 2) AS pass_rate_pct,
  ROUND(100 * COALESCE(f.failed_rows, 0) / NULLIF(t.total_rows,0), 2) AS fail_rate_pct,

  -- simple DQ score (pass rate)
  ROUND(100 * (t.total_rows - COALESCE(f.failed_rows, 0)) / NULLIF(t.total_rows,0), 2) AS dq_score,

  c.dataset_name,
  c.check_name,
  c.failed_rows AS check_failed_rows,
  c.fail_pct AS check_fail_pct,
  c.severity
FROM totals t
LEFT JOIN fails f
  ON t.batch_id = f.batch_id AND t.load_dt = f.load_dt
LEFT JOIN checks c
  ON t.batch_id = c.batch_id AND t.load_dt = c.run_dt;
  
  SELECT *
  FROM dq_dashboard_summary;
  
  
  --  q_error_detail (drill-down table with provider/facility slicing)
  
  DROP VIEW IF EXISTS dq_error_detail;

CREATE VIEW dq_error_detail AS
SELECT
  e.batch_id,
  DATE(e.loaded_at) AS dt,
  e.source_file,
  e.loaded_at,

  e.claim_id,
  e.patient_id,
  e.provider_id,
  e.member_id,
  e.service_date,
  e.icd10_code,
  e.cpt_code,
  e.paid_amount,
  e.claim_status,

  e.error_reason,

  pr.provider_name,
  pr.npi,
  pr.specialty,
  pr.facility_id,

  f.facility_name,
  f.state_code AS facility_state
FROM claims_error e
LEFT JOIN providers pr ON e.provider_id = pr.provider_id
LEFT JOIN facilities f ON pr.facility_id = f.facility_id;

SELECT *
FROM dq_error_detail;

--  dq_error_bucket_daily (best for bar charts + heatmaps)

CREATE VIEW dq_error_bucket_daily AS
SELECT
  batch_id,
  DATE(loaded_at) AS dt,
  CASE
    WHEN error_reason LIKE '%Missing claim_id%' THEN 'Missing claim_id'
    WHEN error_reason LIKE '%Missing patient_id%' THEN 'Missing patient_id'
    WHEN error_reason LIKE '%Missing provider_id%' THEN 'Missing provider_id'
    WHEN error_reason LIKE '%Missing member_id%' THEN 'Missing member_id'
    WHEN error_reason LIKE '%Missing ICD10%' THEN 'Missing ICD10'
    WHEN error_reason LIKE '%Invalid service_date%' THEN 'Invalid service_date'
    WHEN error_reason LIKE '%Future service_date%' THEN 'Future service_date'
    WHEN error_reason LIKE '%Invalid paid_amount%' THEN 'Invalid paid_amount'
    WHEN error_reason LIKE '%Negative paid_amount%' THEN 'Negative paid_amount'
    WHEN error_reason LIKE '%Orphan patient_id%' THEN 'Orphan patient_id'
    WHEN error_reason LIKE '%Orphan provider_id%' THEN 'Orphan provider_id'
    WHEN error_reason LIKE '%Duplicate claim%' THEN 'Duplicate claim'
    ELSE 'Other'
  END AS error_bucket,
  COUNT(*) AS error_rows
FROM claims_error
GROUP BY batch_id, DATE(loaded_at), error_bucket;


SELECT *
FROM dq_error_bucket_daily;