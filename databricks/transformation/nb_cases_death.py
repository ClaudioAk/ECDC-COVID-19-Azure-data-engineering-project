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

print("Configuration done!")

# COMMAND ----------

df_cases = spark.read.csv(f"{raw_path}/cases_deaths.csv",header = True, inferSchema= True)

print(f"Rows: {df_cases.count()}")
print(f"Columns: {len(df_cases.columns)}")
df_cases.printSchema()

# COMMAND ----------

df_cases.show(10)

# COMMAND ----------

#tranformations 

from pyspark.sql.functions import *

df_cases_clean = df_cases.select(col("dateRep").alias("Reporting_date"),
                                 col("day"),
                                 col("month"),
                                 col("year"),
                                 col("cases").alias("Confirmed_cases"),
                                 col("deaths"),
                                 col("countriesAndTerritories").alias("Country"),
                                 col("geoId").alias("country_code_2_digit"),
                                 col("countryterritoryCode").alias("country_code_3_digit"),
                                 col("popData2019").alias("population"),
                                 col("continentExp").alias("continent")
)
df_cases_clean.show(10)




# COMMAND ----------

#Adjusting date formate
df_cases_clean= df_cases_clean.withColumn("Reporting_date", to_date(col("Reporting_date"),"dd/MM/yyyy"))

# Remove rows with null country or date and confirmed cases

df_cases_clean= df_cases_clean.filter(col("Country").isNotNull()&
                                       col("Reporting_date").isNotNull()&
                                       col("Confirmed_cases").isNotNull())

print(f"Clean rows: {df_cases_clean.count()}")


# COMMAND ----------

#Write to Processed Zone
df_cases_clean.write.mode("overwrite").parquet(f"{processed_path}/cases_deaths")

#verify
df_verify = spark.read.parquet(f"{processed_path}/cases_deaths")
print(f"processed_rows: {df_verify.count()}")
df_verify.show(10)