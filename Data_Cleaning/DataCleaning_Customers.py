# Data Cleaning Process - Customers Data




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


from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from pyspark.sql.functions import col, concat, current_timestamp, sha2

# Infer schema using StructType :

customer_schema = StructType([
    StructField("cust_id",StringType()),
    StructField("mem_id",StringType()),
    StructField("fst_name",StringType()),
    StructField("lst_name",StringType()),
    StructField("prm_status",StringType()),
    StructField("age",IntegerType()),
    StructField("state",StringType()),
    StructField("country",StringType())

])
rawFiles_file_path = "/mnt/financeaccountdatlake/raw-data/"




# cust_id	mem_id	fst_name	lst_name	prm_status	age	state	country

# Spark Dataframe Reader API to read the data :

customer_df = spark.read \
.format("csv") \
.option("header","True") \
.schema(customer_schema) \
.option("path", f"{rawFiles_file_path}loan_customer_data.csv") \
.load()




# Rename the columns with proper names

customer_renamed_df = customer_df.withColumnRenamed("cust_id","customer_id") \
.withColumnRenamed("mem_id","member_id") \
.withColumnRenamed("fst_name","first_name") \
.withColumnRenamed("lst_name","last_name") \
.withColumnRenamed("prm_status","premium_status")

customer_renamed_df.show()



# Add ingestion date to dataframe
# Add the ingestion date column to dataframe 
# date is important for further analysys

customer_ingest_date_df = customer_renamed_df.withColumn("ingest_date", current_timestamp())


# Add Surogate Key
# To adentify each record uniquely, we will add surrogate key
# Surrogate key is system generated key 
# Sytem generated Key : 
#SHA-2 (Secure Hash Algorithm 2) is a set of cryptographic hash functions. It produces a 256-bit (32-byte) hash value and is generally considered to be a more secure.

customer_final_df = customer_ingest_date_df.withColumn("customer_key", sha2(concat(col("member_id"),col("age"),col("state")),256))

customer_final_df.show()




# Spark SQL to query cleaned data
# Create temp view to query like a table :

customer_final_df.createOrReplaceTempView("customer_details")

final_df = spark.sql("Select customer_key,ingest_date,customer_id,member_id,first_name,last_name,premium_status, age, state, country FROM customer_details")

final_df.show()



# Write cleaned data to datalake
# Spark Dataframe writer API to write data to datalake:



final_df.write \
.format("parquet") \
.mode("append") \
.option("header",True) \
.option("path",f"{cleanedFiles_file_path}customer_details/") \
.save()


dbutils.notebook.exit("executed Customer job")

