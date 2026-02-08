-- Step 0.1: Create database 

CREATE DATABASE IF NOT EXISTS dq_healthcare;
USE dq_healthcare;

--  Step 0.2: Create master (dimension) tables

DROP TABLE IF EXISTS facilities;
CREATE TABLE facilities (
  facility_id VARCHAR(20) PRIMARY KEY,
  facility_name VARCHAR(100) NOT NULL,
  state_code CHAR(2) NOT NULL
);

DROP TABLE IF EXISTS providers;
CREATE TABLE providers (
  provider_id VARCHAR(20) PRIMARY KEY,
  npi VARCHAR(20) NOT NULL,
  provider_name VARCHAR(100) NOT NULL,
  specialty VARCHAR(50) NOT NULL,
  facility_id VARCHAR(20) NOT NULL,
  INDEX (facility_id)
);

DROP TABLE IF EXISTS patients;
CREATE TABLE patients (
  patient_id VARCHAR(20) PRIMARY KEY,
  mrn VARCHAR(20) NOT NULL,
  first_name VARCHAR(50) NOT NULL,
  last_name VARCHAR(50) NOT NULL,
  dob DATE NOT NULL,
  gender ENUM('M','F','U') NOT NULL,
  city VARCHAR(60) NOT NULL,
  state_code CHAR(2) NOT NULL
);

-- Step 0.3: Create staging + clean + error + DQ metrics tables

DROP TABLE IF EXISTS claims_raw;
CREATE TABLE claims_raw (
  batch_id INT NOT NULL,
  source_file VARCHAR(255) NOT NULL,
  loaded_at DATETIME NOT NULL,

  claim_id VARCHAR(50),
  patient_id VARCHAR(20),
  provider_id VARCHAR(20),
  member_id VARCHAR(30),
  service_date VARCHAR(30),   -- raw text on purpose
  icd10_code VARCHAR(20),
  cpt_code VARCHAR(20),
  paid_amount VARCHAR(50),    -- raw text on purpose
  claim_status VARCHAR(30),

  INDEX (batch_id),
  INDEX (patient_id),
  INDEX (provider_id),
  INDEX (loaded_at)
);

DROP TABLE IF EXISTS claims_clean;
CREATE TABLE claims_clean (
  claim_id VARCHAR(50) NOT NULL,
  patient_id VARCHAR(20) NOT NULL,
  provider_id VARCHAR(20) NOT NULL,
  member_id VARCHAR(30) NOT NULL,
  service_date DATE NOT NULL,
  icd10_code VARCHAR(20) NOT NULL,
  cpt_code VARCHAR(20),
  paid_amount DECIMAL(18,2) NOT NULL,
  claim_status VARCHAR(30),
  batch_id INT NOT NULL,
  loaded_at DATETIME NOT NULL,
  PRIMARY KEY (claim_id, batch_id),
  INDEX (service_date),
  INDEX (icd10_code)
);

DROP TABLE IF EXISTS claims_error;
CREATE TABLE claims_error (
  batch_id INT NOT NULL,
  source_file VARCHAR(255) NOT NULL,
  loaded_at DATETIME NOT NULL,

  claim_id VARCHAR(50),
  patient_id VARCHAR(20),
  provider_id VARCHAR(20),
  member_id VARCHAR(30),
  service_date VARCHAR(30),
  icd10_code VARCHAR(20),
  cpt_code VARCHAR(20),
  paid_amount VARCHAR(50),
  claim_status VARCHAR(30),

  error_reason TEXT NOT NULL,
  INDEX (batch_id),
  INDEX (loaded_at)
);

DROP TABLE IF EXISTS dq_results;
CREATE TABLE dq_results (
  run_at DATETIME NOT NULL,
  batch_id INT NOT NULL,
  dataset_name VARCHAR(100) NOT NULL,
  check_name VARCHAR(200) NOT NULL,
  total_rows INT NOT NULL,
  failed_rows INT NOT NULL,
  fail_pct DECIMAL(6,2) NOT NULL,
  severity ENUM('High','Medium','Low') NOT NULL,
  sample_query TEXT
);



