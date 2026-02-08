use dq_healthcare;

LOAD DATA LOCAL INFILE '/path/to/facilities.csv'
INTO TABLE facilities
FIELDS TERMINATED BY ',' ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '/path/to/providers.csv'
INTO TABLE providers
FIELDS TERMINATED BY ',' ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '/path/to/patients.csv'
INTO TABLE patients
FIELDS TERMINATED BY ',' ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '/Users/sangameshgoudahorapeti/Documents/Health_care_demo/dq_healthcare_dataset/claims_raw.csv'
INTO TABLE claims_raw
FIELDS TERMINATED BY ',' ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

SHOW VARIABLES LIKE 'local_infile';
SHOW VARIABLES LIKE 'secure_file_priv';


SET GLOBAL local_infile = 1;

SHOW VARIABLES LIKE 'local_infile';


