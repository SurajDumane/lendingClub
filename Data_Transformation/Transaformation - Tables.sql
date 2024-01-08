
-- Creation of hive tables 

-- CREATE DATABASE IF NOT EXISTS lending;
-- USE lending;
SHOW TABLES;
-- DROP TABLE lending.customer_external;
 
 
-- customer_key|         ingest_date|customer_id|member_id|first_name|last_name|premium_status|age|         state|country|

CREATE EXTERNAL TABLE IF NOT EXISTS lending.customer_external(
customer_key STRING,
ingest_date TIMESTAMP,
customer_id STRING,
member_id STRING,
first_name STRING,
last_name STRING,
premium_status STRING,
age INT,
state STRING,
country STRING
)
USING PARQUET
LOCATION "/mnt/financeaccountdatlake/cleaned-data/customer_details/"


SELECT count(*) as count FROM lending.customer_external;



CREATE EXTERNAL TABLE IF NOT EXISTS lending.account_details_external(
account_key STRING,
ingest_date TIMESTAMP,
account_id STRING,
member_id STRING,
loan_id STRING,
grade STRING,
sub_grade STRING,
employee_designation STRING,
employee_expreince INT,
home_ownership STRING,
annual_income DOUBLE,
verification_status STRING,
total_high_credit_limit DOUBLE,
application_type STRING,
annual_income_joint DOUBLE,
verification_status_joint STRING
)
USING PARQUET
LOCATION "/mnt/financeaccountdatlake/cleaned-data/account_details/"




OK
SELECT count(*) as count FROM lending.account_details_external;


CREATE EXTERNAL TABLE IF NOT EXISTS lending.defaulters_external(
loan_default_key STRING,
ingest_date TIMESTAMP,
loan_id STRING,
member_id STRING,
loan_default_id STRING,
defaulters_2yrs INT,
defaulters_amount INT,
public_records INT,
public_records_bankruptcies INT,
enquiries_6mnths INT,
late_fee INT,
hardship_flag STRING,
hardship_type STRING,
hardship_length STRING,
hardship_amount STRING
)
USING PARQUET
LOCATION "/mnt/financeaccountdatlake/cleaned-data/defaulters_details/"



OK
SELECT count(*) as count FROM lending.defaulters_external;



 
CREATE EXTERNAL TABLE IF NOT EXISTS lending.payment_external(
payment_key STRING,
ingest_date TIMESTAMP,
loan_id STRING,
member_id STRING,
latest_transaction_id STRING,
funded_amount_investor DOUBLE,
total_payment_recorded FLOAT,
installment FLOAT,
last_payment_amount FLOAT,
last_payment_date DATE,
next_payment_date DATE,
payment_method STRING
)
USING PARQUET
LOCATION "/mnt/financeaccountdatlake/cleaned-data/payment_details/"

SELECT count(*) as count FROM lending.payment_external;
Table
 

SHOW TABLES;