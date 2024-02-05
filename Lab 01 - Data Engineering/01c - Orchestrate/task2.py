# Databricks notebook source
dbutils.widgets.text("source_catalog", "tfnsw_bootcamp_catalog", "source_catalog")
dbutils.widgets.text("target_catalog", "tfnsw_bootcamp_catalog", "target_catalog")

# COMMAND ----------

source_catalog = dbutils.widgets.get("source_catalog")
target_catalog = dbutils.widgets.get("target_catalog")
schema_name = "opal"
table_name = "opal_card_transactions"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{schema_name};")

# COMMAND ----------

query = f"SELECT * from {source_catalog}.opal_bronze.opal_card_transactions WHERE location != '-1' AND transaction_type = 'journey'"

df = spark.sql(
    query
)

df.write.mode("overwrite").saveAsTable(f"{target_catalog}.{schema_name}.{table_name}")
