USE dq_healthcare;

DELIMITER $$

DROP PROCEDURE IF EXISTS sp_run_claims_dq $$
CREATE PROCEDURE sp_run_claims_dq(IN p_batch_id INT)
BEGIN
  DECLARE v_total_rows INT DEFAULT 0;

  SELECT COUNT(*) INTO v_total_rows
  FROM claims_raw
  WHERE batch_id = p_batch_id;

  /* Clean old outputs for this batch (optional but recommended for re-runs) */
  DELETE FROM claims_clean WHERE batch_id = p_batch_id;
  DELETE FROM claims_error WHERE batch_id = p_batch_id;
  DELETE FROM dq_results  WHERE batch_id = p_batch_id;

  /* 2) Build ONE evaluation table for the batch */
  DROP TEMPORARY TABLE IF EXISTS tmp_dq_eval;

  CREATE TEMPORARY TABLE tmp_dq_eval AS
  WITH typed AS (
    SELECT
      c.batch_id,
      c.source_file,
      c.loaded_at,

      NULLIF(TRIM(c.claim_id), '')     AS claim_id,
      NULLIF(TRIM(c.patient_id), '')   AS patient_id,
      NULLIF(TRIM(c.provider_id), '')  AS provider_id,
      NULLIF(TRIM(c.member_id), '')    AS member_id,

      /* SAFE date parsing: only parse if YYYY-MM-DD */
      CASE
        WHEN c.service_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        THEN STR_TO_DATE(c.service_date, '%Y-%m-%d')
        ELSE NULL
      END AS service_date_dt,

      NULLIF(UPPER(TRIM(c.icd10_code)), '') AS icd10_code,
      NULLIF(UPPER(TRIM(c.cpt_code)),  '') AS cpt_code,

      /* SAFE numeric parsing */
      CASE
        WHEN c.paid_amount REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
        THEN CAST(c.paid_amount AS DECIMAL(18,2))
        ELSE NULL
      END AS paid_amount_amt,

      NULLIF(TRIM(c.claim_status), '') AS claim_status,

      /* preserve raw */
      c.service_date AS service_date_raw,
      c.paid_amount  AS paid_amount_raw

    FROM claims_raw c
    WHERE c.batch_id = p_batch_id
  ),
  ref_checked AS (
    SELECT
      t.*,
      p.patient_id   AS patient_match,
      pr.provider_id AS provider_match
    FROM typed t
    LEFT JOIN patients  p  ON t.patient_id  = p.patient_id
    LEFT JOIN providers pr ON t.provider_id = pr.provider_id
  ),
  dup_ranked AS (
    SELECT
      r.*,

      ROW_NUMBER() OVER (
        PARTITION BY r.patient_id, r.service_date_dt, r.icd10_code, r.paid_amount_amt
        ORDER BY r.loaded_at ASC, r.claim_id ASC
      ) AS dup_rn,

      COUNT(*) OVER (
        PARTITION BY r.patient_id, r.service_date_dt, r.icd10_code, r.paid_amount_amt
      ) AS dup_cnt

    FROM ref_checked r
  )
  SELECT
    batch_id,
    source_file,
    loaded_at,

    claim_id,
    patient_id,
    provider_id,
    member_id,
    service_date_dt,
    icd10_code,
    cpt_code,
    paid_amount_amt,
    claim_status,

    service_date_raw,
    paid_amount_raw,

    patient_match,
    provider_match,

    dup_rn,
    dup_cnt,

    /* final_reason: pass/fail switch */
    CONCAT(
      /* Completeness */
      CASE WHEN claim_id   IS NULL THEN 'Missing claim_id; '   ELSE '' END,
      CASE WHEN patient_id IS NULL THEN 'Missing patient_id; ' ELSE '' END,
      CASE WHEN provider_id IS NULL THEN 'Missing provider_id; ' ELSE '' END,
      CASE WHEN member_id  IS NULL THEN 'Missing member_id; '  ELSE '' END,
      CASE WHEN icd10_code IS NULL THEN 'Missing ICD10; '      ELSE '' END,

      /* Validity */
      CASE WHEN service_date_dt IS NULL THEN 'Invalid/Missing service_date; ' ELSE '' END,
      CASE WHEN service_date_dt IS NOT NULL AND service_date_dt > CURDATE() THEN 'Future service_date; ' ELSE '' END,
      CASE WHEN paid_amount_amt IS NULL THEN 'Invalid paid_amount; ' ELSE '' END,
      CASE WHEN paid_amount_amt IS NOT NULL AND paid_amount_amt < 0 THEN 'Negative paid_amount; ' ELSE '' END,

      /* Referential integrity */
      CASE WHEN patient_id IS NOT NULL AND patient_match IS NULL THEN 'Orphan patient_id; ' ELSE '' END,
      CASE WHEN provider_id IS NOT NULL AND provider_match IS NULL THEN 'Orphan provider_id; ' ELSE '' END,

      /* Duplicates: reject extra rows only */
      CASE WHEN dup_cnt > 1 AND dup_rn > 1 THEN 'Duplicate claim (kept first record); ' ELSE '' END
    ) AS final_reason

  FROM dup_ranked;

  /* 3) Log metrics into dq_results (severity based on fail_pct thresholds) */
  INSERT INTO dq_results(run_at, batch_id, dataset_name, check_name, total_rows, failed_rows, fail_pct, severity, sample_query)
  SELECT
    NOW(),
    p_batch_id,
    'claims_raw',
    'Missing ICD10',
    v_total_rows,
    SUM(CASE WHEN dup_cnt > 1 AND dup_rn > 1 THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN dup_cnt > 1 AND dup_rn > 1 THEN 1 ELSE 0 END) / NULLIF(v_total_rows,0), 2),
    CASE
      WHEN ROUND(100.0 * SUM(CASE WHEN dup_cnt > 1 AND dup_rn > 1 THEN 1 ELSE 0 END) / NULLIF(v_total_rows,0), 2) >= 1.00 THEN 'High'
      WHEN ROUND(100.0 * SUM(CASE WHEN dup_cnt > 1 AND dup_rn > 1 THEN 1 ELSE 0 END) / NULLIF(v_total_rows,0), 2) >= 0.10 THEN 'Medium'
      ELSE 'Low'
    END,
    CONCAT('SELECT * FROM claims_raw WHERE batch_id=', p_batch_id, ' ORDER BY patient_id, service_date, icd10_code;')
  FROM tmp_dq_eval;

  /* 4) Route rows */
  INSERT INTO claims_error(
    batch_id, source_file, loaded_at,
    claim_id, patient_id, provider_id, member_id, service_date,
    icd10_code, cpt_code, paid_amount, claim_status,
    error_reason
  )
  SELECT
    batch_id, source_file, loaded_at,
    claim_id, patient_id, provider_id, member_id, service_date_raw,
    icd10_code, cpt_code, paid_amount_raw, claim_status,
    final_reason
  FROM tmp_dq_eval
  WHERE final_reason IS NOT NULL AND final_reason <> '';

  INSERT INTO claims_clean(
    claim_id, patient_id, provider_id, member_id,
    service_date, icd10_code, cpt_code, paid_amount,
    claim_status, batch_id, loaded_at
  )
  SELECT
    claim_id, patient_id, provider_id, member_id,
    service_date_dt, icd10_code, cpt_code, paid_amount_amt,
    claim_status, batch_id, loaded_at
  FROM tmp_dq_eval
  WHERE final_reason IS NULL OR final_reason = '';

  /* 5) Return summary */
  SELECT
    p_batch_id AS batch_id,
    v_total_rows AS total_rows,
    SUM(CASE WHEN final_reason IS NULL OR final_reason = '' THEN 1 ELSE 0 END) AS passed_rows,
    SUM(CASE WHEN final_reason IS NOT NULL AND final_reason <> '' THEN 1 ELSE 0 END) AS failed_rows,
    SUM(CASE WHEN dup_cnt > 1 AND dup_rn > 1 THEN 1 ELSE 0 END) AS duplicate_extra_rows
  FROM tmp_dq_eval;

END $$

DELIMITER ;







