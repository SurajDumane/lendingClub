# Pre-Prod Environment Vairables

rawFiles_file_path = "/mnt/financeaccountdatlake/raw-data/"


processed_file_path="/mnt/financeaccountdatlake/processed-data/"


#cleaned File path
cleanedFiles_file_path= "/mnt/financeaccountdatlake/cleaned-data/"

cleanedScripts_folder_path = "/Users/xxxxxxxx@gmail.com/Cleaning-Data/"


dbfs_file_path="/FileStore/tables/"
from pyspark.sql.functions import current_timestamp
 
def ingestDate(input_df):
    date_df=input_df.withColumn("ingest_date",current_timestamp())
    return date_df