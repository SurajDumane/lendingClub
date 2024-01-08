# Data Cleaning Process - Loan Data


# ### Step 01:
# MAGIC 1. Study and analyze the data
# MAGIC 2. Come up with scope for cleaning the data
# MAGIC 3. Bring out the cleaning techniques to be applied
# MAGIC 4. Include the new columns to be added


# ### Step 02:
# MAGIC 1. Infer the schema of the tables
# MAGIC 2. Read the data from the files
# MAGIC 3. Query the data
# MAGIC 4. Data cleaning techniques 
# MAGIC 5. New column additions
# MAGIC 6. Write the data to data lake




from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,FloatType,DateType

from pyspark.sql.functions import col,regexp_replace,sha2,current_timestamp,replace,concat

# loan_id	mem_id	acc_id	loan_amt	fnd_amt	term	interest	installment	issue_date	loan_status	purpose	title	disbursement_method

loan_schema = StructType([
StructField("loan_id",StringType()),
StructField("mem_id",StringType()),
StructField("acc_id",StringType()),
StructField("loan_amt",DoubleType()),
StructField("fnd_amt",DoubleType()),
StructField("term",StringType()),
StructField("interest",StringType()),
StructField("installment",FloatType()),
StructField("issue_date",DateType()),
StructField("loan_status",StringType()),
StructField("purpose",StringType()),
StructField("title",StringType()),
StructField("disbursement_method",StringType())
])



loan_df = spark.read \
.format("csv") \
.option("header",True) \
.schema(loan_schema) \
.option("path",f"{rawFiles_file_path}loan_details.csv") \
.load()

loan_df.show()


# Remane the columns :

loan_renamed_df = loan_df.withColumnRenamed("mem_id","member_id") \
.withColumnRenamed("acc_id","account_id") \
.withColumnRenamed("loan_amt","loan_amount") \
.withColumnRenamed("fnd_amt","funded_amount")

# loan_renamed_df.show()


# Get proper format of column : term, interest 
from pyspark.sql.functions import replace,expr,cast
# For term Column :

string_to_remove = "months"
new_df = loan_renamed_df.withColumn("term", regexp_replace(col("term"), string_to_remove, ""))

refined_df = new_df.withColumn("term",col("term").cast(IntegerType()))



int_string_to_remove = "%"
modified_df = refined_df.withColumn("interest", regexp_replace(col("interest"),int_string_to_remove, ""))
new_modified_df = modified_df.withColumn("interest", col("interest").cast(IntegerType()))



# Add ingest_date and surrogate key
# add ingest date
ingest_date_df =  new_modified_df.withColumn("ingest_date", current_timestamp())

# add surrogate key
final = ingest_date_df.withColumn("loan_key", sha2(concat(col("loan_id"),col("member_id"),col("loan_amount")), 256))

final.show()

# Convert null Strings to Actual NULL values :
output_df =final.replace("null",None)

output_df.show()


# Spark Sql to query data


output_df.createOrReplaceTempView("loan")

spark.sql("Select * FROM loan WHERE  interest = 'NULL' ")

output_df.createOrReplaceTempView("loan_details")

cleaned_df  = spark.sql("Select loan_key,ingest_date,loan_id,member_id,account_id,loan_amount,funded_amount,term,interest,installment,issue_date,loan_status,purpose,title,disbursement_method FROM loan_details")

cleaned_df.show()
cleaned_df.printSchema()


# Write cleaned loan data to datalake
#Write df to datalake :
cleanedFiles_file_path= "/mnt/financeaccountdatlake/cleaned-data/"


cleaned_df.write \
.format("parquet") \
.option("header",True) \
.mode("append") \
.option("path",f"{cleanedFiles_file_path}loan_details/") \
.save()



dbutils.notebook.exit("exceuted loan job")


