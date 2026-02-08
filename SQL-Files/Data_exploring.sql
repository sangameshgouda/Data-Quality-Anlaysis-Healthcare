USE dq_healthcare;

--  claims raw
SELECT *
FROM claims_raw
LIMIT 100;

-- facilities
SELECT *
FROM dq_healthcare.facilities;

-- PATIENTS

SELECT *
FROM patients
LIMIT 100;

--  PROVIDERS

SELECT *
FROM providers
LIMIT 100;

-- Step 1 — Confirm row counts + batch
SELECT batch_id,
       count(*) as count_records
FROM claims_raw
GROUP BY 1
ORDER BY 1;

SELECT
  (SELECT COUNT(*) FROM facilities) AS facilities_cnt,
  (SELECT COUNT(*) FROM providers)  AS providers_cnt,
  (SELECT COUNT(*) FROM patients)   AS patients_cnt,
  (SELECT COUNT(*) FROM claims_raw WHERE batch_id = 101) AS claims_raw_cnt;


-- Step 2 — Data profiling (distributions + date range)
  
 -- CLAIM STATUS distribution 
 
 SELECT claim_status,
        count(*) as cnt,
        SUM(COUNT(*)) OVER(order by claim_status) AS rolling_number
 FROM claims_raw
 GROUP BY 1;

-- 2.2 Service date profile (detect invalid dates)

-- 2025-13-01 invalid month → converts to NULL → IS NULL = 1
SELECT COUNT(*) AS row_cnt,
       SUM(STR_TO_DATE(service_date, '%Y-%m-%d') IS NULL) AS invalid_service_date_rows,
       MIN(STR_TO_DATE(service_date, '%Y-%m-%d')) AS min_service_date,
	   MAX(STR_TO_DATE(service_date, '%Y-%m-%d')) AS max_service_date
FROM claims_raw
WHERE batch_id = 101;

-- 2.3 Paid amount profile (detect invalid numeric)

SELECT count(*) as cnt,
       SUM((paid_amount REGEXP '^-?[0-9]+(\\.[0-9]+)?$') = 0) AS invalid_paid_amount_rows,
       SUM((paid_amount REGEXP '^-?[0-9]+(\\.[0-9]+)?$') = 1 AND CAST(paid_amount AS DECIMAL(18,2)) < 0) AS negative_paid_amount_row,
       sum(case when paid_amount<0 then 1 else 0 end) as neagtive_paid_amount,
       MIN(paid_amount) AS min_paid_amount,
	   MAX(paid_amount) AS max_paid_amount
FROM claims_raw
LIMIT 100;


 -- Step 3 — Basic DQ tests (Completeness + Validity)
 -- completeness checks 
 
 SELECT
  COUNT(*) AS total_rows,
  SUM(patient_id IS NULL OR TRIM(patient_id)='') AS missing_patient_id,
  SUM(provider_id IS NULL OR TRIM(provider_id)='') AS missing_provider_id,
  SUM(member_id  IS NULL OR TRIM(member_id)='') AS missing_member_id,
  SUM(icd10_code IS NULL OR TRIM(icd10_code)='') AS missing_icd10
FROM claims_raw
WHERE batch_id = 101;

-- or 

 SELECT
  COUNT(*) AS total_rows,
  sum(case when patient_id is NULL  OR TRIM(patient_id)='' then 1 else 0 end ) as missing_patient_id,
  sum(case when provider_id is NULL OR TRIM(provider_id)='' then 1 else 0 end ) as missing_patient_id
FROM claims_raw
WHERE batch_id = 101;

-- validitychecks

SELECT
  COUNT(*) AS total_rows,

  SUM(STR_TO_DATE(service_date, '%Y-%m-%d') IS NULL) AS invalid_service_date,
  SUM(STR_TO_DATE(service_date, '%Y-%m-%d') > CURDATE()) AS future_service_date,
  SUM((paid_amount REGEXP '^-?[0-9]+(\\.[0-9]+)?$') = 0) AS invalid_paid_amount,
  SUM((paid_amount REGEXP '^-?[0-9]+(\\.[0-9]+)?$') = 1 AND CAST(paid_amount AS DECIMAL(18,2)) < 0) AS negative_paid_amount
FROM claims_raw
WHERE batch_id = 101;


-- Pull samples (always do this in interviews)

SELECT *
FROM claims_raw
WHERE batch_id =101 and STR_TO_DATE(service_date, '%Y-%m-%d') IS NULL 
LIMIT 25;

-- Invalid paid_amount samples

 SELECT claim_id, patient_id, provider_id, paid_amount
FROM claims_raw
WHERE batch_id = 101
  AND (paid_amount REGEXP '^-?[0-9]+(\\.[0-9]+)?$') = 0
LIMIT 25;

--  Negative paid amount

SELECT claim_id, patient_id, provider_id, paid_amount
FROM claims_raw
WHERE batch_id = 101
  AND (paid_amount REGEXP '^-?[0-9]+(\\.[0-9]+)?$') = 1
  AND CAST(paid_amount AS DECIMAL(18,2)) < 0
LIMIT 25;

-- Advanced DQ Checks (Orphans + Duplicates) 
-- 1A) Orphan patient_id (claims pointing to a patient that doesn’t exist)

SELECT *
FROM   patients
limit 10;      

SELECT *
FROM claims_raw as c
LEFT JOIN patients as p
on c.patient_id = p.patient_id 
WHERE c.batch_id = 101
  AND c.patient_id IS NOT NULL AND TRIM(c.patient_id) <> '' AND p.patient_id IS NULL
LIMIT 10;

SELECT COUNT(*) AS count_of_patient_id
FROM claims_raw as c
LEFT JOIN patients as p
on c.patient_id = p.patient_id 
WHERE c.batch_id = 101
  AND c.patient_id IS NOT NULL AND TRIM(c.patient_id) <> '' AND p.patient_id IS NULL
LIMIT 10;

-- 1A)Orphan provider_id

SELECT *
FROM providers
LIMIT 10;

SELECT *
FROM claims_raw
LIMIT 10;

SELECT COUNT(*) AS count_of_providers
FROM claims_raw as c
LEFT JOIN providers as p
on c.provider_id = p.provider_id 
WHERE c.batch_id = 101
  AND c.provider_id IS NOT NULL AND TRIM(c.provider_id) <> '' AND p.provider_id IS NULL
LIMIT 10;


-- Duplicate detection (business key)

-- Duplicate key = patient_id + service_date + icd10_code + paid_amount

SELECT *
FROM claims_raw;

SELECT count(*) as duplicates
FROM(
SELECT *,
       row_number() over(PARTITION BY patient_id, service_date, icd10_code,paid_amount) as rn
FROM claims_raw) as t
WHERE rn>=2;

-- method
SELECT COUNT(*) AS CNT
FROM (
SELECT
  patient_id, service_date, icd10_code, paid_amount,
  COUNT(*) AS dup_cnt
FROM claims_raw
WHERE batch_id = 101
GROUP BY patient_id, service_date, icd10_code, paid_amount
HAVING COUNT(*) > 1
ORDER BY dup_cnt DESC) AS T;

 
-- Part 2 — Build DQ Evaluation Layer (creates reason text per row)

 -- We’ll create a table dq_eval so it’s reusable for routing + dashboard drilldowns. 

DROP TABLE IF EXISTS dq_eval;

DROP TABLE IF EXISTS dq_eval;

CREATE TABLE dq_eval AS
WITH base AS (
  SELECT
    c.*,

    /* SAFE DATE PARSE: only parse if it matches YYYY-MM-DD */
    CASE
      WHEN c.service_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
      THEN STR_TO_DATE(c.service_date, '%Y-%m-%d')
      ELSE NULL
    END AS service_date_dt,

    /* SAFE NUMERIC PARSE */
    CASE
      WHEN c.paid_amount REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
      THEN CAST(c.paid_amount AS DECIMAL(18,2))
      ELSE NULL
    END AS paid_amount_amt

  FROM claims_raw c
  WHERE c.batch_id = 101
),
joined AS (
  SELECT
    b.*,
    p.patient_id   AS patient_match,
    pr.provider_id AS provider_match
  FROM base b
  LEFT JOIN patients p  ON b.patient_id  = p.patient_id
  LEFT JOIN providers pr ON b.provider_id = pr.provider_id
),
dup_ranked AS (
  SELECT
    j.*,

    /* Duplicate grouping uses the SAFE typed columns */
    ROW_NUMBER() OVER (
      PARTITION BY j.patient_id, j.service_date_dt, j.icd10_code, j.paid_amount_amt
      ORDER BY j.loaded_at ASC, j.claim_id ASC
    ) AS dup_rn,

    COUNT(*) OVER (
      PARTITION BY j.patient_id, j.service_date_dt, j.icd10_code, j.paid_amount_amt
    ) AS dup_cnt

  FROM joined j
)
SELECT
  batch_id,
  source_file,
  loaded_at,

  claim_id,
  patient_id,
  provider_id,
  member_id,
  service_date,
  icd10_code,
  cpt_code,
  paid_amount,
  claim_status,

  service_date_dt,
  paid_amount_amt,

  patient_match,
  provider_match,

  dup_rn,
  dup_cnt,

  /* Error reason */
  CONCAT(
    /* Completeness */
    CASE WHEN claim_id   IS NULL OR TRIM(claim_id)='' THEN 'Missing claim_id; ' ELSE '' END,
    CASE WHEN patient_id IS NULL OR TRIM(patient_id)='' THEN 'Missing patient_id; ' ELSE '' END,
    CASE WHEN provider_id IS NULL OR TRIM(provider_id)='' THEN 'Missing provider_id; ' ELSE '' END,
    CASE WHEN member_id  IS NULL OR TRIM(member_id)='' THEN 'Missing member_id; ' ELSE '' END,
    CASE WHEN icd10_code IS NULL OR TRIM(icd10_code)='' THEN 'Missing ICD10; ' ELSE '' END,

    /* Validity (safe) */
    CASE WHEN service_date_dt IS NULL THEN 'Invalid service_date; ' ELSE '' END,
    CASE WHEN service_date_dt IS NOT NULL AND service_date_dt > CURDATE() THEN 'Future service_date; ' ELSE '' END,

    CASE WHEN paid_amount_amt IS NULL THEN 'Invalid paid_amount; ' ELSE '' END,
    CASE WHEN paid_amount_amt IS NOT NULL AND paid_amount_amt < 0 THEN 'Negative paid_amount; ' ELSE '' END,

    /* Referential integrity (only if id present) */
    CASE WHEN patient_id IS NOT NULL AND TRIM(patient_id)<>'' AND patient_match IS NULL THEN 'Orphan patient_id; ' ELSE '' END,
    CASE WHEN provider_id IS NOT NULL AND TRIM(provider_id)<>'' AND provider_match IS NULL THEN 'Orphan provider_id; ' ELSE '' END,

    /* Duplicates: reject only extra rows */
    CASE WHEN dup_cnt > 1 AND dup_rn > 1 THEN 'Duplicate claim (kept first record); ' ELSE '' END
  ) AS error_reason

FROM dup_ranked;


SELECT *
FROM dq_eval;

 
SELECT *
FROM claims_raw;

-- 2B) Quick summary: how many pass vs fail

SELECT
  COUNT(*) AS total_rows,
  SUM(error_reason IS NULL OR error_reason='') AS passed_rows,
  SUM(error_reason IS NOT NULL AND error_reason<>'') AS failed_rows
FROM dq_eval;

-- Top error reasons (for dashboard)
SELECT
  CASE
    WHEN error_reason LIKE '%Missing member_id%' THEN 'Missing member_id'
    WHEN error_reason LIKE '%Missing ICD10%' THEN 'Missing ICD10'
    WHEN error_reason LIKE '%Invalid service_date%' THEN 'Invalid service_date'
    WHEN error_reason LIKE '%Invalid paid_amount%' THEN 'Invalid paid_amount'
    WHEN error_reason LIKE '%Negative paid_amount%' THEN 'Negative paid_amount'
    WHEN error_reason LIKE '%Orphan patient_id%' THEN 'Orphan patient_id'
    WHEN error_reason LIKE '%Orphan provider_id%' THEN 'Orphan provider_id'
    WHEN error_reason LIKE '%Duplicate claim%' THEN 'Duplicate claim'
    ELSE 'Other'
  END AS error_bucket,
  COUNT(*) AS cnt
FROM dq_eval
WHERE error_reason IS NOT NULL AND error_reason <> ''
GROUP BY 1
ORDER BY cnt DESC;


-- Part 3 — Route data + log metrics (framework)
-- 3A) Clear previous run (if you’re re-running)

DELETE FROM claims_clean WHERE batch_id = 101;
DELETE FROM claims_error WHERE batch_id = 101;
DELETE FROM dq_results WHERE batch_id = 101;


INSERT INTO claims_error(
  batch_id, source_file, loaded_at,
  claim_id, patient_id, provider_id, member_id,
  service_date, icd10_code, cpt_code, paid_amount, claim_status,
  error_reason
)
SELECT
  batch_id,
  source_file,
  loaded_at,
  claim_id, 
  patient_id,
  provider_id, 
  member_id,
  service_date,
  icd10_code,
  cpt_code,
  paid_amount, 
  claim_status,
  error_reason
FROM dq_eval
WHERE error_reason IS NOT NULL AND error_reason <> '';

-- Check for error table

SELECT *
FROM claims_error; 


--  Instering correct values into claim_clean 


INSERT INTO claims_clean(
  claim_id, patient_id, provider_id, member_id,
  service_date, icd10_code, cpt_code, paid_amount,
  claim_status, batch_id, loaded_at
)
SELECT
  claim_id,
  patient_id,
  provider_id,
  member_id,
  service_date_dt,
  icd10_code,
  cpt_code,
  paid_amount_amt,
  claim_status,
  batch_id,
  loaded_at
FROM dq_eval
WHERE (error_reason IS NULL OR error_reason = '');

SELECT *
FROM claims_clean
LIMIT 100;


--  Insert check-level metrics into dq_results (dashboard table)
SELECT *
FROM dq_results;

INSERT INTO dq_results(run_at, batch_id, dataset_name, check_name, total_rows, failed_rows, fail_pct, severity, sample_query)
SELECT
  NOW(),
  101,
  'claims_raw',
  'Missing ICD10',
  COUNT(*) AS total_rows,
  SUM(icd10_code IS NULL OR TRIM(icd10_code)='') AS failed_rows,
  ROUND(100 * SUM(icd10_code IS NULL OR TRIM(icd10_code)='') / NULLIF(COUNT(*),0), 2) AS fail_pct,
  CASE
    WHEN ROUND(100 * SUM(icd10_code IS NULL OR TRIM(icd10_code)='') / NULLIF(COUNT(*),0), 2) >= 1.00 THEN 'High'
    WHEN ROUND(100 * SUM(icd10_code IS NULL OR TRIM(icd10_code)='') / NULLIF(COUNT(*),0), 2) >= 0.10 THEN 'Medium'
    ELSE 'Low'
  END AS severity,
  'SELECT claim_id, service_date FROM claims_raw WHERE batch_id=101 AND (icd10_code IS NULL OR TRIM(icd10_code)='''') LIMIT 50;' AS sample_query
FROM dq_eval;

SELECT *
FROM dq_results;

INSERT INTO dq_results(run_at, batch_id, dataset_name, check_name, total_rows, failed_rows, fail_pct, severity, sample_query)
SELECT
  NOW(), 101, 'claims_raw', 'Orphan patient_id',
  COUNT(*),
  SUM(patient_id IS NOT NULL AND TRIM(patient_id)<>'' AND patient_match IS NULL),
  ROUND(100 * SUM(patient_id IS NOT NULL AND TRIM(patient_id)<>'' AND patient_match IS NULL) / NULLIF(COUNT(*),0), 2),
  CASE
    WHEN ROUND(100 * SUM(patient_id IS NOT NULL AND TRIM(patient_id)<>'' AND patient_match IS NULL) / NULLIF(COUNT(*),0), 2) >= 1.00 THEN 'High'
    WHEN ROUND(100 * SUM(patient_id IS NOT NULL AND TRIM(patient_id)<>'' AND patient_match IS NULL) / NULLIF(COUNT(*),0), 2) >= 0.10 THEN 'Medium'
    ELSE 'Low'
  END,
  'SELECT claim_id, patient_id FROM claims_raw WHERE batch_id=101 LIMIT 50;'
FROM dq_eval;

SELECT *
FROM dq_results;

-- Duplicate rows

INSERT INTO dq_results(run_at, batch_id, dataset_name, check_name, total_rows, failed_rows, fail_pct, severity, sample_query)
SELECT
  NOW(), 101, 'claims_raw', 'Duplicate claims (extra rows)',
  COUNT(*),
  SUM(dup_cnt > 1 AND dup_rn > 1),
  ROUND(100 * SUM(dup_cnt > 1 AND dup_rn > 1) / NULLIF(COUNT(*),0), 2),
  CASE
    WHEN ROUND(100 * SUM(dup_cnt > 1 AND dup_rn > 1) / NULLIF(COUNT(*),0), 2) >= 1.00 THEN 'High'
    WHEN ROUND(100 * SUM(dup_cnt > 1 AND dup_rn > 1) / NULLIF(COUNT(*),0), 2) >= 0.10 THEN 'Medium'
    ELSE 'Low'
  END,
  'SELECT claim_id, patient_id, service_date, icd10_code, paid_amount FROM claims_raw WHERE batch_id=101 LIMIT 50;'
FROM dq_eval;

SELECT *
FROM dq_results;

-- Final Verification

SELECT COUNT(*) AS clean_rows FROM claims_clean WHERE batch_id=101;

SELECT COUNT(*) AS error_rows FROM claims_error WHERE batch_id=101;

SELECT * FROM dq_results WHERE batch_id=101 ORDER BY run_at DESC, check_name;
 
