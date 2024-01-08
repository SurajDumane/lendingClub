#  We want to calculate loan-score based on cleaned data and dump the processed data to process layer
#  We will create spark variables to store loan-scores
 
spark.conf.set("spark.sql.unacceptable_rated_pts", 0)
spark.conf.set("spark.sql.very_bad_rated_pts", 100)
spark.conf.set("spark.sql.bad_rated_pts",250)
spark.conf.set("spark.sql.good_rated_pts",500)
spark.conf.set("spark.sql.very_good_rated_pts",650)
spark.conf.set("spark.sql.excellent_rated_pts",800)
 
spark.conf.set("spark.sql.unacceptable_grade_pts",750)
spark.conf.set("spark.sql.very_bad_grade_pts", 1000)
spark.conf.set("spark.sql.bad_grade_pts", 1500)
spark.conf.set("spark.sql.good_grade_pts", 2000)
spark.conf.set("spark.sql.very_good_grade_pts", 2500)
spark.conf.set("spark.sql.excellent_grade_pts", 3000)
 
 
 
spark.conf.set("spark.sql.unacceptable_grade", "F")
spark.conf.set("spark.sql.very_bad_grade", "E")
spark.conf.set("spark.sql.bad_grade", "D")
spark.conf.set("spark.sql.good_grade", "C")
spark.conf.set("spark.sql.very_good_grade", "B")
spark.conf.set("spark.sql.excellent_grade", "A")



%run "/Users/kirandumane880@gmail.com/Call_variables"
# 
account_df = spark.read.format("parquet").option("header",True).option("path", f"{cleanedFiles_file_path}account_details/").load()


# 
account_df = spark.read.parquet(f"{cleanedFiles_file_path}/account_details")

payment_df = spark.read.format("parquet").option("header",True).option("path",f"{cleanedFiles_file_path}payment_details/").load()
defaulter_df = spark.read.format("parquet").option("header",True).option("path",f"{cleanedFiles_file_path}defaulters_details/").load()
loan_df = spark.read.format("parquet").option("header",True).option("path",f"{cleanedFiles_file_path}loan_details/").load()
customer_df = spark.read.format("parquet").option("header",True).option("path",f"{cleanedFiles_file_path}customer_details/").load()
# Now we will create temp views on each dataframe to do our transformations:
 
account_df.createOrReplaceTempView("account_details")
payment_df.createOrReplaceTempView("payment_details")
defaulter_df.createOrReplaceTempView("defaulter_details")
loan_df.createOrReplaceTempView("loan_details")
customer_df.createOrReplaceTempView("customer_details")
spark.sql("SELECT * FROM loan_details LIMIT 10").show()



payment_pts_df = spark.sql("SELECT c.member_id, c.state,c.country,c.first_name,c.last_name, \
CASE \
    WHEN p.last_payment_amount < (p.installment * 0.5) THEN ${spark.sql.very_bad_rated_pts} \
    WHEN p.last_payment_amount >= (p.installment * 0.5) AND p.last_payment_amount < p.installment THEN ${spark.sql.bad_rated_pts} \
    WHEN p.last_payment_amount = p.installment THEN ${spark.sql.good_rated_pts} \
    WHEN p.last_payment_amount > p.installment AND p.last_payment_amount <= (p.installment * 1.50) THEN ${spark.sql.very_good_rated_pts} \
    ELSE ${spark.sql.unacceptable_rated_pts} \
    END  AS last_payment_pts, \
CASE \
    WHEN p.total_payment_recorded < (p.funded_amount_investor * 0.5) AND p.total_payment_recorded > 0 THEN ${spark.sql.good_rated_pts} \
    WHEN p.total_payment_recorded > (p.funded_amount_investor * 0.5) THEN ${spark.sql.very_good_rated_pts} \
    WHEN p.total_payment_recorded = 0 OR p.total_payment_recorded IS NULL THEN ${spark.sql.unacceptable_rated_pts} \
    END AS total_payment_pts \
FROM payment_details AS p \
INNER JOIN customer_details AS c \
ON p.member_id = c.member_id")
 
 
payment_pts_df.createOrReplaceTempView("payment_points")
spark.sql("SELECT * FROM payment_points").show()



defaulter_df.createOrReplaceTempView("defaulter_details")
# spark.conf.set("spark.sql.excellent_rated_pts",800)


defaulters_pts = spark.sql("SELECT p.*, \
CASE \
    WHEN d.defaulters_2yrs = 0 THEN ${spark.sql.excellent_rated_pts} \
    WHEN d.defaulters_2yrs BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} \
    WHEN d.defaulters_2yrs BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} \
    WHEN d.defaulters_2yrs > 5 OR d.defaulters_2yrs IS NULL THEN ${spark.sql.unacceptable_rated_pts} \
    END AS delinq_pts, \
CASE \
    WHEN d.public_records = 0 THEN ${spark.sql.excellent_rated_pts} \
    WHEN d.public_records BETWEEN 1 AND 2 THEN  ${spark.sql.bad_rated_pts} \
    WHEN d.public_records BETWEEN 3 AND 5 THEN  ${spark.sql.very_bad_rated_pts} \
    WHEN d.public_records > 5 OR d.public_records IS NULL THEN ${spark.sql.unacceptable_rated_pts} \
    END AS public_pts, \
CASE \
    WHEN d.public_records_bankruptcies = 0 THEN ${spark.sql.excellent_rated_pts} \
    WHEN d.public_records_bankruptcies BETWEEN 1 AND 2 THEN  ${spark.sql.bad_rated_pts} \
    WHEN d.public_records_bankruptcies BETWEEN 3 AND 5 THEN  ${spark.sql.very_bad_rated_pts} \
    WHEN d.public_records_bankruptcies > 5 OR d.public_records_bankruptcies IS NULL THEN ${spark.sql.unacceptable_rated_pts} \
    END AS public_bankruptcies_pts, \
CASE \
    WHEN d.enquiries_6mnths = 0 THEN ${spark.sql.excellent_rated_pts} \
    WHEN d.enquiries_6mnths BETWEEN 1 AND 2 THEN  ${spark.sql.bad_rated_pts} \
    WHEN d.enquiries_6mnths BETWEEN 3 AND 5 THEN  ${spark.sql.very_bad_rated_pts} \
    WHEN d.enquiries_6mnths > 5 OR d.enquiries_6mnths IS NULL THEN ${spark.sql.unacceptable_rated_pts} \
    END AS enq_pts, \
CASE \
    WHEN d.hardship_flag = 'N' THEN ${spark.sql.excellent_rated_pts} \
    WHEN d.hardship_flag = 'Y'  OR  d.hardship_flag IS NULL THEN ${spark.sql.bad_rated_pts} \
    END AS hardship_pts \
FROM defaulter_details AS d \
LEFT JOIN payment_points AS p \
ON d.member_id = p.member_id")
 
defaulters_pts.createOrReplaceTempView("loan_default_points")
spark.sql("SELECT * FROM loan_default_points LIMIT 5").show()



# Now we will give points to acoount_details & loan details :

finance_df = spark.sql("""SELECT a.*,
CASE 
    WHEN LOWER(b.loan_status) = "fully paid" THEN ${spark.sql.excellent_rated_pts} 
    WHEN LOWER(b.loan_status) = "current" THEN ${spark.sql.good_rated_pts}
    WHEN LOWER(b.loan_status) = "in_grace_period" THEN ${spark.sql.bad_rated_pts}
    WHEN LOWER(b.loan_status) = "Late(16-30)" OR LOWER(b.loan_status) = "Late(31-120)" THEN ${spark.sql.very_bad_rated_pts}
    WHEN LOWER(b.loan_status) = "charged-off" THEN ${spark.sql.unacceptable_rated_pts} 
    END AS loan_staus_pts, 
CASE 
    WHEN LOWER(c.home_ownership) = "own" THEN  ${spark.sql.excellent_rated_pts} 
    WHEN LOWER(c.home_ownership) = "rent" THEN ${spark.sql.good_rated_pts}
    WHEN LOWER(c.home_ownership) = "mortgage" THEN ${spark.sql.bad_rated_pts}
    WHEN LOWER(c.home_ownership) = "any" OR LOWER(c.home_ownership) IS NULL THEN ${spark.sql.very_bad_rated_pts}
    END as home_pts,
CASE 
    WHEN b.funded_amount <= (c.total_high_credit_limit * 0.10) THEN ${spark.sql.excellent_rated_pts}
    WHEN b.funded_amount > (c.total_high_credit_limit * 0.10) AND b.funded_amount <= (c.total_high_credit_limit * 0.20) THEN ${spark.sql.very_good_rated_pts}
    WHEN b.funded_amount > (c.total_high_credit_limit * 0.20) AND b.funded_amount <= (c.total_high_credit_limit * 0.30) THEN ${spark.sql.good_rated_pts}
    WHEN b.funded_amount > (c.total_high_credit_limit * 0.30) AND b.funded_amount <= (c.total_high_credit_limit * 0.50) THEN ${spark.sql.bad_rated_pts}
    WHEN b.funded_amount > (c.total_high_credit_limit * 0.50) AND b.funded_amount <= (c.total_high_credit_limit * 0.70) THEN ${spark.sql.very_bad_rated_pts}
    WHEN b.funded_amount > (c.total_high_credit_limit * 0.70) THEN ${spark.sql.unacceptable_rated_pts}
    END AS credit_limit_pts, 
CASE 
    WHEN  c.grade = "A" AND c.sub_grade = "A1" THEN  ${spark.sql.excellent_grade_pts}
    WHEN  c.grade = "A" AND c.sub_grade = "A2" THEN  (${spark.sql.excellent_grade_pts} * 0.80)
    WHEN  c.grade = "A" AND c.sub_grade = "A3" THEN  (${spark.sql.excellent_grade_pts} * 0.60)
    WHEN  c.grade = "A" AND c.sub_grade = "A4" THEN  (${spark.sql.excellent_grade_pts} * 0.40)
    WHEN  c.grade = "A" AND c.sub_grade = "A5" THEN  (${spark.sql.excellent_grade_pts} * 0.20)
    WHEN  c.grade = "B" AND c.sub_grade = "B1" THEN  ${spark.sql.very_good_grade_pts}
    WHEN  c.grade = "B" AND c.sub_grade = "B2" THEN  (${spark.sql.very_good_grade_pts} * 0.80)
    WHEN  c.grade = "B" AND c.sub_grade = "B3" THEN  (${spark.sql.very_good_grade_pts} * 0.60)
    WHEN  c.grade = "B" AND c.sub_grade = "B4" THEN  (${spark.sql.very_good_grade_pts} * 0.40)
    WHEN  c.grade = "B" AND c.sub_grade = "B5" THEN  (${spark.sql.very_good_grade_pts} * 0.20)
    WHEN  c.grade = "C" AND c.sub_grade = "C1" THEN  ${spark.sql.good_grade_pts}
    WHEN  c.grade = "C" AND c.sub_grade = "C2" THEN  (${spark.sql.good_grade_pts} *0.80)
    WHEN  c.grade = "C" AND c.sub_grade = "C3" THEN  (${spark.sql.good_grade_pts} *0.60)
    WHEN  c.grade = "C" AND c.sub_grade = "C4" THEN  (${spark.sql.good_grade_pts} *0.40)
    WHEN  c.grade = "C" AND c.sub_grade = "C5" THEN  (${spark.sql.good_grade_pts} *0.20)
    WHEN  c.grade = "D" AND c.sub_grade = "D1" THEN  ${spark.sql.bad_grade_pts}
    WHEN  c.grade = "D" AND c.sub_grade = "D2" THEN  (${spark.sql.bad_grade_pts} * 0.80)
    WHEN  c.grade = "D" AND c.sub_grade = "D3" THEN  (${spark.sql.bad_grade_pts} * 0.60)
    WHEN  c.grade = "D" AND c.sub_grade = "D4" THEN  (${spark.sql.bad_grade_pts} * 0.40)
    WHEN  c.grade = "D" AND c.sub_grade = "D5" THEN  (${spark.sql.bad_grade_pts} * 0.20)
    WHEN  c.grade = "E" AND c.sub_grade = "E1" THEN  ${spark.sql.very_bad_grade_pts}
    WHEN  c.grade = "E" AND c.sub_grade = "E2" THEN  (${spark.sql.very_bad_grade_pts} * 0.80)
    WHEN  c.grade = "E" AND c.sub_grade = "E3" THEN  (${spark.sql.very_bad_grade_pts} * 0.60)
    WHEN  c.grade = "E" AND c.sub_grade = "E4" THEN  (${spark.sql.very_bad_grade_pts} * 0.40)
    WHEN  c.grade = "E" AND c.sub_grade = "E5" THEN  (${spark.sql.very_bad_grade_pts} * 0.20)
    WHEN  c.grade IN ('F','G') AND c.sub_grade IN ('F1','F2','G1',"G2") THEN  ${spark.sql.unacceptable_grade_pts}
    WHEN  c.grade IN ('F','G') AND c.sub_grade IN ('F3','F4','G3',"G4") THEN  (${spark.sql.unacceptable_grade_pts} * 0.80)
    WHEN  c.grade IN ('F','G') AND c.sub_grade IN ('F5','F6','G5',"G6") THEN  (${spark.sql.unacceptable_grade_pts} * 0.40)
    END AS grade_pts 
FROM loan_default_points AS a 
LEFT OUTER JOIN  loan_details AS b ON a.member_id = b.member_id
LEFT OUTER JOIN account_details AS c ON a.member_id = c.member_id""")
finance_df.printSchema()


finance_df.createOrReplaceTempView("loan_score_details")
loan_score_df = spark.sql("""
SELECT member_id, state, country,first_name,last_name,
((last_payment_pts+total_payment_pts) * 0.20) AS payment_history_pts,
((delinq_pts+public_pts+public_bankruptcies_pts+enq_pts+hardship_pts) * 0.35) AS default_history_pts,
((loan_staus_pts+home_pts+credit_limit_pts+grade_pts) * 0.45) AS financial_health_pts
FROM loan_score_details
          """)
loan_score_df.createOrReplaceTempView("loan_score_pts")
loan_score_dataframe = spark.sql("""
  SELECT  member_id, state, country,first_name,last_name,
  (payment_history_pts+default_history_pts+financial_health_pts) AS loan_score
  FROM loan_score_pts       
          """)
loan_score_dataframe.show()



loan_score_dataframe.createOrReplaceTempView("loan_grades")
 
final_df = spark.sql("SELECT member_id, state, country,first_name,last_name,loan_score, \
case \
WHEN loan_score > ${spark.sql.very_good_grade_pts} THEN '" + excellent_grade + "' \
WHEN loan_score <= ${spark.sql.very_good_grade_pts} AND loan_score > ${spark.sql.good_grade_pts} THEN '" + very_good_grade + "' \
WHEN loan_score <= ${spark.sql.good_grade_pts} AND loan_score > ${spark.sql.bad_grade_pts} THEN '" + good_grade + "' \
WHEN loan_score <= ${spark.sql.bad_grade_pts} AND loan_score > ${spark.sql.very_bad_grade_pts} THEN '" + bad_grade + "' \
WHEN loan_score <= ${spark.sql.very_bad_grade_pts} AND loan_score > ${spark.sql.unacceptable_grade_pts} THEN '" + very_bad_grade + "' \
WHEN loan_score <= ${spark.sql.unacceptable_grade_pts} THEN '" + unacceptable_grade + "' \
end as loan_grade \
from loan_grades")
final_df.createOrReplaceTempView("loan_score_final")
spark.sql("SELECT * FROM loan_score_final WHERE loan_grade = 'B' ").show()

%sql
CREATE EXTERNAL TABLE IF NOT EXISTS lending.loan_score
USING PARQUET
LOCATION "/mnt/financeaccountdatlake/processed-data/loan_score/"