# Data Cleaning Process - Investors Data

## Step 01:
# MAGIC 1. Study and analyze the data
# MAGIC 2. Come up with scope for cleaning the data
# MAGIC 3. Bring out the cleaning techniques to be applied
# MAGIC 4. Include the new columns to be added

## Step 02:
# MAGIC 1. Infer the schema of the tables
# MAGIC 2. Read the data from the files
# MAGIC 3. Query the data
# MAGIC 4. Data cleaning techniques 
# MAGIC 5. New column additions
# MAGIC 6. Write the data to data lake

from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DoubleType
from pyspark.sql.functions import col, concat, current_timestamp,sha2
 
#Infer the schema of customer's data
investor_schema = StructType([
StructField("investor_loan_id", StringType(), True),
StructField("loan_id", StringType(), True),
StructField("investor_id", StringType(), False),
StructField("loan_funded_amt", DoubleType(), False),
StructField("investor_type", StringType(), False),
StructField("age", IntegerType(), False),
StructField("state", StringType(), False),
StructField("country", StringType(), False)
])
 
 
# Read the csv file into a dataframe:

#create a reader dataframe for investor's data
 
investor_df = spark.read \
.format("csv") \
.schema(investor_schema) \
.option("header",True) \
.option("path",f"{rawFiles_file_path}loan_investors.csv") \
.load()

investor_df.createOrReplaceTempView("investor_data")
spark.sql("select * from investor_data").show()

# Add the ingestion date to the dataframe:

#Include a ingest date column to signify when it got ingested into our data lake
investor_df_ingestDate=investor_df.withColumn("ingest_date", current_timestamp())
display(investor_df_ingestDate)

# Add a surrogate key to the dataframe:
#Include a customer_key column which acts like a surrogate key in the table
investor_df_key=investor_df_ingestDate.withColumn("investor_loan_key", sha2(concat(col("investor_loan_id"),col("loan_id"),col("investor_id")), 256))
display(investor_df_key)


# Use Spark SQL to query the data

#Using spark SQL to query the tables
investor_df_key.createOrReplaceTempView("investor_temp_table")
display_df=spark.sql("select investor_loan_key,ingest_date,investor_loan_id,loan_id,investor_id,loan_funded_amt,investor_type,age,state,country from investor_temp_table")
display(display_df)

# Write the cleaned dataframe into data lake :
 
#write the final cleaned customers data to data lake 
 
display_df.write \
.format("parquet") \
.options(header='True') \
.mode("append") \
.option("path",f"{cleanedFiles_file_path}investor_loan_details/") \
.save()
 
#List the files inside the investor_loan_detail folder
dbutils.fs.ls("/mnt/financeaccountdatlake/cleaned-data/investor_loan_details/")

dbutils.notebook.exit("executed invertors job")