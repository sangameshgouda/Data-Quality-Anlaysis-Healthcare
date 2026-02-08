# Healthcare Claims Data Quality Framework (MySQL + BI Dashboard)

## Objective
Build an end-to-end **Data Quality (DQ) framework** for healthcare claims data that:
- Validates raw claims data for **completeness, validity, referential integrity, and duplicates**
- Routes bad records to a quarantine table with clear reasons
- Loads clean, typed data into a trusted table for reporting
- Logs DQ metrics (fail counts, fail %, severity) for **dashboarding in Tableau/Looker**

This project demonstrates a real-world Data Quality Analyst workflow using SQL and BI-ready outputs.

---

## Project Highlights
- Batch-based processing using `batch_id`
- Safe parsing of dirty fields like `service_date = 'abc'` and `paid_amount = 'xyz'`
- Duplicate detection using a business key (patient + service_date + ICD10 + paid_amount)
- Quarantine pattern: **clean vs error tables**
- Metrics table for dashboard KPIs and trend analysis

---

## Tech Stack
- **Database:** MySQL 8.x (concepts align with SQL Server)
- **Development:** MySQL Workbench
- **BI:** Tableau 
- **Data Quality:** Rule-based checks implemented in SQL / Stored Procedure

---

## Dataset Description (Tables)

### Dimension / Master Tables
These tables act as reference sources for referential integrity checks and dashboard slicing.

#### `facilities`
| Column | Description |
|---|---|
| facility_id (PK) | Unique facility identifier |
| facility_name | Hospital/Clinic name |
| state_code | Facility state |

#### `providers`
| Column | Description |
|---|---|
| provider_id (PK) | Unique provider identifier |
| npi | National Provider Identifier |
| provider_name | Provider name |
| specialty | Provider specialty |
| facility_id (FK) | Links provider to a facility |

#### `patients`
| Column | Description |
|---|---|
| patient_id (PK) | Unique patient identifier |
| mrn | Medical record number |
| first_name, last_name | Patient name |
| dob | Date of birth |
| gender | M/F/U |
| city, state_code | Patient location |

---

### Fact / Processing Tables

#### `claims_raw` (Staging / Raw)
Raw claims data loaded from source files (dirty values allowed).
| Column | Description |
|---|---|
| batch_id | Load batch id |
| source_file | Input filename |
| loaded_at | Load timestamp |
| claim_id | Claim identifier |
| patient_id | Links to patients |
| provider_id | Links to providers |
| member_id | Insurance member identifier |
| service_date | Date of service (raw string) |
| icd10_code | Diagnosis code |
| cpt_code | Procedure code |
| paid_amount | Paid amount (raw string) |
| claim_status | Claim status |

#### `dq_eval` / `tmp_dq_eval` (Evaluation Layer)
Row-level evaluation output: typed fields, duplicate ranking, and `final_reason`.
Key fields:
- `service_date_dt` (DATE) → parsed safely
- `paid_amount_amt` (DECIMAL) → parsed safely
- `dup_rn`, `dup_cnt` → duplicate detection
- `final_reason` → pass/fail decision

#### `claims_clean` (Trusted / Curated)
Only valid records with correct data types (DATE/DECIMAL). Used for reporting and analytics.

#### `claims_error` (Quarantine)
Invalid records with `error_reason` explaining failures. Used for triage and drill-down.

#### `dq_results` (DQ Metrics Log)
Check-level metrics (failed rows, fail %, severity, sample query) for dashboards.

---

## Data Quality Checks Implemented

### 1) Completeness
- Missing `claim_id`
- Missing `patient_id`
- Missing `provider_id`
- Missing `member_id`
- Missing `icd10_code`

### 2) Validity
- Invalid or missing `service_date` (must be `YYYY-MM-DD`)
- Future `service_date`
- Invalid `paid_amount` (must be numeric)
- Negative `paid_amount`

### 3) Referential Integrity (Orphans)
- `patient_id` exists in `patients`
- `provider_id` exists in `providers`

### 4) Duplicates
- Duplicate key:
  - `patient_id + service_date_dt + icd10_code + paid_amount_amt`
- Keep first record (`dup_rn = 1`), route extra (`dup_rn > 1`) to errors

---

## End-to-End Workflow

### Step 1 — Create Tables
Create schema and tables:
- Master: `patients`, `providers`, `facilities`
- Staging: `claims_raw`
- DQ outputs: `claims_clean`, `claims_error`, `dq_results`
- Optional: `dq_eval` (persistent evaluation table)

### Step 2 — Load Data
Load CSV data into MySQL tables (Workbench Import Wizard recommended).

### Step 3 — Run Data Profiling
- Row counts, null checks, invalid pattern checks
- Distribution by status, date ranges, numeric ranges

### Step 4 — Run Advanced DQ Evaluation
- Build evaluation table with typed fields + joins + duplicate ranking
- Create `final_reason` string to explain failures

### Step 5 — Route Data
- `final_reason` empty → insert into `claims_clean`
- `final_reason` not empty → insert into `claims_error`

### Step 6 — Log Metrics
Insert check-level results into `dq_results`:
- `failed_rows`, `fail_pct`, severity (High/Medium/Low)
- Sample queries for investigation

### Step 7 — BI Dashboard
Use `dq_results`, `claims_error`, and dashboard-ready views to build:
- DQ Score & Pass Rate
- Error trends by date/batch
- Top failing checks

---

## Pictorial Flow (Mermaid)

```mermaid
flowchart LR
  A[Create Schema & Tables] --> B[Load CSV Data]
  B --> C[claims_raw (Staging)]
  B --> D[patients/providers/facilities (Master Data)]

  C --> E[Basic Profiling<br/>Counts, Nulls, Distributions]
  C --> F[Advanced DQ Checks<br/>Validity, Orphans, Duplicates]

  C --> G[dq_eval / tmp_dq_eval<br/>Typed Fields + dup_rn/dup_cnt + final_reason]
  D --> G

  G -->|final_reason empty| H[claims_clean<br/>Trusted Data]
  G -->|final_reason not empty| I[claims_error<br/>Quarantine + Reasons]

  G --> J[dq_results<br/>Check Metrics + Severity]
  J --> K[Dashboard Views / Extracts]
  I --> K
  H --> K

  K --> L[BI Dashboard (Tableau/Looker)<br/>KPI + Trends + Drilldowns]
