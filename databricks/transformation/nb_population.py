# Databricks notebook source
# Mount ADLS Gen2 to Databricks

storage_account_name = "adlscovid19de"
storage_account_key = "ADD your Key"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# Define paths

raw_path = f"abfss://raw@{storage_account_name}.dfs.core.windows.net/population/"
processed_path= f"abfss://processed@{storage_account_name}.dfs.core.windows.net/population/"


# COMMAND ----------

df_population = spark.read .option("sep", "\t") .option("compression", "gzip") .csv(f"{raw_path}demo_pjangroup.tsv.gz", header=True, inferSchema=True)

df_population.printSchema()
print(f"Row count: {df_population.count()}")
df_population.show(5)



from pyspark.sql.functions import *

#  Split the first column into 5 separate columns
df_population = df_population.withColumn("freq", split(col("freq,unit,sex,age,geo\\TIME_PERIOD"), ",")[0]) \
    .withColumn("unit", split(col("freq,unit,sex,age,geo\\TIME_PERIOD"), ",")[1]) \
    .withColumn("sex",  split(col("freq,unit,sex,age,geo\\TIME_PERIOD"), ",")[2]) \
    .withColumn("age",  split(col("freq,unit,sex,age,geo\\TIME_PERIOD"), ",")[3]) \
    .withColumn("geo",  split(col("freq,unit,sex,age,geo\\TIME_PERIOD"), ",")[4])

#  Keep only relevant columns (geo + 2019 population)
df_population = df_population.select(
    trim(col("geo")).alias("country_code"),
    trim(col("age")).alias("age_group"),
    trim(col("sex")).alias("sex"),
    trim(col("2019 ")).alias("population_2019")   # note the space in "2019 "
)

#  Filter: keep only TOTAL sex, TOTAL age, remove ":" missing values
df_population_clean = df_population.select(
    col("country_code"),
    col("age_group"),
    col("population_2019")).filter(
    (col("age_group") == "TOTAL") &
    (col("population_2019") != ":") &
    col("population_2019").isNotNull()
)



# Write to processed zone
df_population.write.mode("overwrite").parquet(
    "abfss://processed@adlscovid19de.dfs.core.windows.net/population/"
)
# Verify
df_verify = spark.read.parquet(f"{processed_path}population")
print(f"Row count: {df_verify.count()}")

df_population_clean.show(10)
