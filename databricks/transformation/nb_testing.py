# Databricks notebook source
# Mount ADLS Gen2 to Databricks
storage_account_name = "adlscovid19de"
storage_account_key = "ADD your Key"  

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",storage_account_key
)
# Define paths
raw_path= f"abfss://raw@{storage_account_name}.dfs.core.windows.net/"
processed_path= f"abfss://processed@{storage_account_name}.dfs.core.windows.net/"


# COMMAND ----------

df_testing= spark.read.csv(f"{raw_path}/testing.csv", header=True, inferSchema= True)
from pyspark.sql.functions import *
df_testing.show(10)

df_testing= df_testing.replace("NA",None)

df_testing_clean= df_testing.filter(col("new_cases").isNotNull() &
                                    col("tests_done").isNotNull() &
                                    col("testing_rate").isNotNull() &
                                    col("positivity_rate").isNotNull() &
                                    col("testing_data_source").isNotNull()
)
#adjust year-week formate
df_testing_clean = df_testing_clean.withColumn(
    "year_week",
    concat(
        split(translate(col("year_week"), "W", ""), "-")[0],  # "2025"
        lit("-"),
        lpad(split(translate(col("year_week"), "W", ""), "-")[1], 2, "0")  # "01"
    )
)
df_testing_clean.show(10)

# COMMAND ----------

# Write to processed zone

df_testing_clean.write.mode("overwrite").parquet(f"{processed_path}/testing")

#verify
df_testing_clean=spark.read.parquet(f"{processed_path}/testing")
df_testing_clean.show(10)

