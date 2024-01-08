# Data Cleaning : Payments

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,FloatType,DateType
from pyspark.sql.functions import col, concat, current_timestamp,regexp_replace,lit,to_date,sha2


# Infer Schema : 
payment_schema = StructType(fields=[StructField("loan_id", StringType(), False),
StructField("mem_id", StringType(), False),
StructField("latest_transaction_id", StringType(), False),
StructField("funded_amnt_inv", DoubleType(), True),
StructField("total_pymnt_rec", FloatType(), True),
StructField("installment", FloatType(), True),
StructField("last_pymnt_amnt", FloatType(), True),
StructField("last_pymnt_d", DateType(), True),
StructField("next_pymnt_d", DateType(), True),
StructField("pymnt_method", StringType(), True)
])
 


# Read Payments :

payment_df = spark.read \
.format("csv") \
.schema(payment_schema) \
.option("header",True) \
.option("path",f"{rawFiles_file_path}loan_payment.csv") \
.load()
 
payment_df.show()

payment_df.createOrReplaceTempView("payment_table")
payment_sql=spark.sql("select * from payment_table where last_pymnt_d < '2022-09-18' ")
display(payment_sql)


# Add Ingest Date :
payment_df_ingestDate=payment_df.withColumn("ingest_date", current_timestamp())
display(payment_sql)


# Add Surrogate Key :
payment_df_key=payment_df_ingestDate.withColumn("payment_key", sha2(concat(col("loan_id"),col("mem_id"),col("latest_transaction_id")), 256))
display(payment_df_key)

# Make null string to acutal NULL:
null_df=payment_df_key.replace("null",None)

null_df.createOrReplaceTempView("payment_table")
payment_sql=spark.sql("select * from payment_table where last_pymnt_d is null")
display(payment_sql)

# Rename Columns:
payment_df_rename=null_df.withColumnRenamed("mem_id","member_id") \
.withColumnRenamed("funded_amnt_inv","funded_amount_investor") \
.withColumnRenamed("total_pymnt_rec","total_payment_recorded") \
.withColumnRenamed("last_pymnt_amnt","last_payment_amount") \
.withColumnRenamed("last_pymnt_d","last_payment_date") \
.withColumnRenamed("next_pymnt_d","next_payment_date") \
.withColumnRenamed("pymnt_method","payment_method")

display(payment_df_rename)



payment_df_rename.createOrReplaceTempView("temp_table")
final_df=spark.sql("select payment_key,ingest_date,loan_id,member_id,latest_transaction_id,funded_amount_investor,total_payment_recorded, installment,last_payment_amount,last_payment_date,next_payment_date,payment_method from temp_table")
display(final_df)



# Write to datalake :

 
final_df.write \
.format("parquet") \
.option("header",True) \
.mode("append") \
.option("path",f"{cleanedFiles_file_path}payment_details/") \
.save()
 
dbutils.notebook.exit("executed payments job")