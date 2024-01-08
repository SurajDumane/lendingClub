# Data Cleaning Process - Account Data

# MAGIC ### Step 01:
# MAGIC 1. Study and analyze the data
# MAGIC 2. Come up with scope for cleaning the data
# MAGIC 3. Bring out the cleaning techniques to be applied
# MAGIC 4. Include the new columns to be added

# MAGIC %md
# MAGIC ### Step 02:
# MAGIC 1. Infer the schema of the tables
# MAGIC 2. Read the data from the files
# MAGIC 3. Query the data
# MAGIC 4. Data cleaning techniques 
# MAGIC 5. New column additions
# MAGIC 6. Write the data to data lake




# Read account data :

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,FloatType,DateType
 
from pyspark.sql.functions import col,regexp_replace,sha2,current_timestamp,replace,concat,when
 
 
# acc_id    mem_id    loan_id    grade    sub_grade    emp_title    emp_length    home_ownership    annual_inc    verification_status    tot_hi_cred_lim    application_type    annual_inc_joint    verification_status_joint
 
account_schema = StructType([
StructField("acc_id",StringType()),
StructField("mem_id",StringType()),
StructField("loan_id",StringType()),
StructField("grade",StringType()),
StructField("sub_grade",StringType()),
StructField("emp_title",StringType()),
StructField("emp_length",StringType()),
StructField("home_ownership",StringType()),
StructField("annual_inc",DoubleType()),
StructField("verification_status",StringType()),
StructField("tot_hi_cred_lim",DoubleType()),
StructField("application_type",StringType()),
StructField("annual_inc_joint",DoubleType()),
StructField("verification_status_joint",StringType())
])
 
rawFiles_file_path = "/mnt/financeaccountdatlake/raw-data/" 
 
 
account_df = spark.read \
.format("csv") \
.schema(account_schema) \
.option("header",True) \
.option("path",f"{rawFiles_file_path}account_details.csv") \
.load()


# Cleaning of Data :
#====================
 
 
# account_df.createOrReplaceTempView("account")
# spark.sql("SELECT DISTINCT  emp_length FROM account").show()
# spark.sql("SELECT * FROM account WHERE verification_status_joint = 'null'").show()
 
# remove n/a with 'null' :
#===========================
refined_df = account_df.withColumn("emp_length", when(col("emp_length") == 'n/a', 'null').otherwise(col("emp_length")))
 
refined_df.createOrReplaceTempView("acc")
spark.sql("SELECT DISTINCT emp_length FROM acc").show()





# Make emp_legth column in proper format :

def remove_years(text):
    if "<" in text:
        return int(text.split("<")[1].split()[0])
    elif "+" in text:
        return int(text.split("+")[0])
    elif text == 'null':
        return text
 
    else:
        return int(text.split()[0])
    
 
 
remove_years_udf = udf(remove_years, IntegerType())
 
acc_df = refined_df.withColumn("emp_length", remove_years_udf(col("emp_length")))
 
acc_df.select("emp_length").distinct().show()
acc_df.printSchema()



# Convert null sting from data to actual NULL values:

refined_df = acc_df.replace('null', None)
refined_df.createOrReplaceTempView("tbl")
 
spark.sql("SELECT * from tbl where emp_length IS NULL").show()



# Rename Columns to proper names :

renamed_df = refined_df.withColumnRenamed("acc_id", "account_id") \
.withColumnRenamed("mem_id","member_id") \
.withColumnRenamed("emp_title","employee_designation") \
.withColumnRenamed("emp_length","employee_expreince") \
.withColumnRenamed("annual_inc","annual_income") \
.withColumnRenamed("tot_hi_cred_lim","total_high_credit_limit") \
.withColumnRenamed("annual_inc_joint","annual_income_joint")
 
renamed_df.show()



# Add ingest date & Surrogate Key :

# Adding ingets date for analysys
 
ingest_date_df = renamed_df.withColumn("ingest_date", current_timestamp())
 
# Adding Surrogate Key :
 
surrogate_df = ingest_date_df.withColumn("account_key", sha2(concat(col("account_id"),col("member_id"),col("loan_id")),256))
 
surrogate_df.show()

# Spark Sql to query datframe :

surrogate_df.createOrReplaceTempView("account_details")
 
final_df = spark.sql("SELECT account_key,ingest_date,account_id,member_id,loan_id,grade,sub_grade,employee_designation,employee_expreince,home_ownership,annual_income,verification_status,total_high_credit_limit,application_type,annual_income_joint,verification_status_joint FROM account_details")
 
final_df.show()



# Write cleaned data to datalke using Spark Writer API :

cleanedFiles_file_path= "/mnt/financeaccountdatlake/cleaned-data/"
 
 
 
final_df.write \
.format("parquet") \
.option("header",True) \
.mode("append") \
.option("path",f"{cleanedFiles_file_path}account_details/") \
.save()
 

dbutils.notebook.exit("executed account job")
