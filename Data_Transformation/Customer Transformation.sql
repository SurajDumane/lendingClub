-- CREATE DATABASE lending;


-- We will create hive tables on cleaned data and do our business transformation in order to make it available for analytics team.
--customer_key|         ingest_date|customer_id|member_id|first_name|last_name|premium_status|age|         state|country

CREATE EXTERNAL TABLE IF NOT EXISTS lending.customer_external(
customer_key STRING,o
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
;



SELECT * FROM lending.customer_external;
Table
 

-- Query to find the total count of customers by state and country

SELECT country,state, count(*)as total_cusomters FROM lending.customer_external
GROUP BY state,country;



-- Query to find the count of customers having premium membership falling in different age buckets

SELECT country,(CASE 
             WHEN age BETWEEN 18 AND 25 THEN "Young"
             WHEN age BETWEEN 26 AND 45 THEN "Working"
             WHEN age BETWEEN 46 AND 60 THEN "Old"
            ELSE "Senior"
            END) as age_group,
count(*) as premium_count
FROM lending.customer_external
WHERE premium_status= "TRUE"
GROUP BY country, age_group;


-- Query to find the percentage of customers in each state that are premium customers, grouped by country

WITH prm_customers AS (
  SELECT country,state,count(*) as prm_count FROM lending.customer_external
  WHERE premium_status = "TRUE"
  GROUP BY country,state
),
total_customers AS (
  SELECT country,state, count(*) total_count FROM lending.customer_external
  GROUP BY country,state
)

SELECT prm_customers.country,prm_customers.state,round(prm_customers.prm_count/total_customers.total_count * 100,2) as premium_percentage FROM prm_customers 
JOIN total_customers
ON prm_customers.country = total_customers.country AND prm_customers.state = total_customers.state;





-- We want to write the premium_percentage table to processed region
CREATE EXTERNAL TABLE IF NOT EXISTS lending.customer_premium_status
USING PARQUET
LOCATION "/mnt/financeaccountdatlake/processed-data/customers_premium_status/"

WITH prm_customers AS (
  SELECT country,state,count(*) as prm_count FROM lending.customer_external
  WHERE premium_status = "TRUE"
  GROUP BY country,state
),
total_customers AS (
  SELECT country,state, count(*) total_count FROM lending.customer_external
  GROUP BY country,state
)

SELECT prm_customers.country,prm_customers.state,round(prm_customers.prm_count/total_customers.total_count * 100,2) as premium_percentage FROM prm_customers 
JOIN total_customers
ON prm_customers.country = total_customers.country AND prm_customers.state = total_customers.state;





#Query to find the number of customers with a premium status of "true" in each country, grouped by age range using pyspark dataframe:

from pyspark.sql.functions import col,avg,when,count,round

customer_df = spark.read \
.format("parquet") \
.option("header",True) \
.option("path","/mnt/financeaccountdatlake/cleaned-data/customer_details/") \
.load()


prm_df  = customer_df.filter("premium_status = 'TRUE'") \
.withColumn("age_group", when((col("age") >= 18) & (col("age") <= 25), "Young") \
                         .when((col("age") >=26) & (col("age") <= 45),"Working") \
                         .when((col("age") >= 46) & (col("age") <= 60),"Middle") \
                         .otherwise("Senior")) \
.groupBy("country", "age_group") \
.agg(count("*"))

                
                                    
prm_df.show()                                  


#Query to find the average age of customers by state and country using pyspark dataframe
from pyspark.sql.functions import col,avg,when,count,round

avg_customer_age = customer_df.groupBy("country","state") \
    .agg(round(avg("age"),2).alias("avg_age"))

avg_customer_age.show()


 
avg_customer_age.createOrReplaceTempView("avg_age_cutomers")
 
-- Write avg age of customers to proecessed layer in datalke
 
CREATE EXTERNAL TABLE IF NOT EXISTS average_age_customers
USING PARQUET
LOCATION "/mnt/financeaccountdatlake/processed-data/avg_customer_age"
 
 
 
SELECT * FROM avg_age_cutomers;