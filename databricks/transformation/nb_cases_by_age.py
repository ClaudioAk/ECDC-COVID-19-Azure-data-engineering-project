# Databricks notebook source
# Mount ADLS Gen2 to Databricks

storage_account_name = "adlscovid19de"
storage_account_key = "ADD your Key"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",storage_account_key
    )

# Define paths
raw_path = f"abfss://raw@{storage_account_name}.dfs.core.windows.net"
processed_path = f"abfss://processed@{storage_account_name}.dfs.core.windows.net"

print("Configuration done!")


# COMMAND ----------

df_case_by_age = spark.read.csv(f"{raw_path}/cases_by_age.csv", header = True, inferSchema = True)

print(f"rows:{df_case_by_age.count()}")
print (f"columns:{len(df_case_by_age.columns)}")
df_case_by_age.printSchema()

# COMMAND ----------


# Filter nulls on critical columns
from pyspark.sql.functions import *

df_case_by_age_clean = df_case_by_age.filter(
    (col("new_cases") != "NA") &
    (col("rate_14_day_per_100k") != "NA")&
    col("country").isNotNull()&
    col("age_group").isNotNull()&
    col("year_week").isNotNull()
)

# Write to processed zone
df_case_by_age_clean.write.mode("overwrite").parquet(f"{processed_path}/cases_by_age")
 #verify
df_case_by_age_verify = spark.read.parquet(f"{processed_path}/cases_by_age")
df_case_by_age_verify.show(10)
df_case_by_age_verify.printSchema()