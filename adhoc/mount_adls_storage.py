
# Databricks notebook source


storage_account_name = "financeaccountdatlake"
 
 
client_id = ""
tenant_id = ""
client_secret = ""
 
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
 
def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)
 
mount_adls("raw-data")
 
mount_adls("cleaned-data")
 
mount_adls("raw-data-temp")
mount_adls("processed-data")


# # Command to unmount 
dbutils.fs.unmount("/mnt/financestoragebig2023/raw-data")
dbutils.fs.unmount("/mnt/financestoragebig2023/cleaned-data")
dbutils.fs.unmount("/mnt/financestoragebig2023/raw-data-temp")
dbutils.fs.unmount("/mnt/financestoragebig2023/processed-data")

#listing files inside container
display(dbutils.fs.ls(f"/mnt/{storage_account_name}/cleaned-data"))