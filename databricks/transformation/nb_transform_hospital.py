# Databricks notebook source
# Mount ADLS Gen2 to Databricks

storage_account_name = "adlscovid19de"
storage_account_key = "ADD your Key"  

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# Define paths
raw_path       = f"abfss://raw@{storage_account_name}.dfs.core.windows.net"
processed_path = f"abfss://processed@{storage_account_name}.dfs.core.windows.net"


df_hospital = spark.read.csv(
    f"{raw_path}/hospital_admissions.csv", header=True, inferSchema=True
)



print(f"rows:{df_hospital.count()}")
print(f"columns:{len(df_hospital.columns)}")
df_hospital.printSchema()
df_hospital.show(10)

# COMMAND ----------

from pyspark.sql.functions import *
df_hospital = df_hospital.select(
    col("country"),
    col("indicator"),
    col("date"),
    col("year_week"),
    col("value"),
    col("source")
)

df_hospital_clean = df_hospital.filter(
    col("country").isNotNull() &
    col("indicator").isNotNull() &
    col("value").isNotNull()
)
#adjust year-week formate

df_hospital_clean = df_hospital_clean.withColumn(
    "year_week",
    concat(
        split(translate(col("year_week"), "W", ""), "-")[0],  # "2025"
        lit("-"),
        lpad(split(translate(col("year_week"), "W", ""), "-")[1], 2, "0")  # "01"
    )
)

print(f"Clean rows: {df_hospital_clean.count()}")
df_hospital_clean.show(10)

# COMMAND ----------

#Write to Processed Zone
df_hospital_clean.write.mode("overwrite").parquet(f"{processed_path}/hospital_admissions")
print(" Written to Processed zone!")

#verify
df_verify = spark.read.parquet(f"{processed_path}/hospital_admissions")
print(f"processed_rows:{df_verify.count()}")
df_verify.printSchema()
df_verify.show(10)