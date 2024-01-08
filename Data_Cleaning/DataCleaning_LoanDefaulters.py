# Data Cleaning Process - Loan Defaulters Data


# ### Step 01:
# MAGIC 1. Study and analyze the data
# MAGIC 2. Come up with scope for cleaning the data
# MAGIC 3. Bring out the cleaning techniques to be applied
# MAGIC 4. Include the new columns to be added


# MAGIC ### Step 02:
# MAGIC 1. Infer the schema of the tables
# MAGIC 2. Read the data from the files
# MAGIC 3. Query the data
# MAGIC 4. Data cleaning techniques 
# MAGIC 5. New column additions
# MAGIC 6. Write the data to data lake


from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,FloatType,DateType
 
from pyspark.sql.functions import col,regexp_replace,sha2,current_timestamp,replace,concat,when
 
 
# loan_id    mem_id    def_id    delinq_2yrs    delinq_amnt    pub_rec    pub_rec_bankruptcies    inq_last_6mths    total_rec_late_fee    hardship_flag    hardship_type    hardship_length    hardship_amount
 
defaulter_schema = StructType([
StructField("loan_id",StringType()),
StructField("mem_id",StringType()),
StructField("def_id",StringType()),
StructField("delinq_2yrs",IntegerType()),
StructField("delinq_amnt",IntegerType()),
StructField("pub_rec",IntegerType()),
StructField("pub_rec_bankruptcies",IntegerType()),
StructField("inq_last_6mths",IntegerType()),
StructField("total_rec_late_fee",IntegerType()),
StructField("hardship_flag",StringType()),
StructField("hardship_type",StringType()),
StructField("hardship_length",StringType()),
StructField("hardship_amount",StringType())
])

 
# Read data using DataFrame API :
 
defaulters_df = spark.read \
.format("csv") \
.schema(defaulter_schema) \
.option("header",True) \
.option("path",f"{rawFiles_file_path}loan_defaulters.csv") \
.load()
 
defaulters_df.show()


# Rename the columns

 
loan_df_rename=defaulters_df.withColumnRenamed("mem_id", "member_id") \
.withColumnRenamed("def_id", "loan_default_id") \
.withColumnRenamed("delinq_2yrs", "defaulters_2yrs") \
.withColumnRenamed("delinq_amnt", "defaulters_amount") \
.withColumnRenamed("pub_rec", "public_records") \
.withColumnRenamed("pub_rec_bankruptcies", "public_records_bankruptcies") \
.withColumnRenamed("inq_last_6mths", "enquiries_6mnths") \
.withColumnRenamed("total_rec_late_fee", "late_fee")
 
loan_df_rename.show()


# Replace the NULL strings into NULL values

remove_null_df = loan_df_rename.replace('null',None)
remove_null_df.createOrReplaceTempView("null_tabl")
spark.sql("Select * FROM null_tabl WHERE defaulters_2yrs IS NULL ").show()


# Add the ingestion date to the dataframe
 
ingest_date_df = remove_null_df.withColumn("ingest_date", current_timestamp())
 
ingest_date_df.show()



# Add a surrogate key to the dataframe
 
loan_default_key = ingest_date_df.withColumn("loan_default_key", sha2(concat(col("loan_id"),col("member_id"),col("loan_default_id")), 256))
 
display(loan_default_key)



# Spark SQL to query Data :
 
loan_default_key.createOrReplaceTempView("defaulters_details")
 
 
 
 
final_df = spark.sql("""SELECT loan_default_key,ingest_date,loan_id,member_id,loan_default_id,defaulters_2yrs,defaulters_amount,public_records,public_records_bankruptcies,enquiries_6mnths,late_fee,hardship_flag,hardship_type,hardship_length,hardship_amount
FROM defaulters_details """)
 
display(final_df)


# Write the cleaned dataframe into data lake
 
cleanedFiles_file_path= "/mnt/financeaccountdatlake/cleaned-data/"
 
final_df.write \
.format("parquet") \
.option("header",True) \
.mode("append") \
.option("path",f"{cleanedFiles_file_path}defaulters_details/") \
.save()
 
dbutils.notebook.exit("executed defaulters job")
