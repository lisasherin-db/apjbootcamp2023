# Databricks notebook source
dbutils.widgets.text("source_catalog", "tfnsw_bootcamp_catalog", "source_catalog")
dbutils.widgets.text("target_catalog", "tfnsw_bootcamp_catalog", "target_catalog")
dbutils.widgets.text("schema", "opal", "schema")

# COMMAND ----------

source_catalog = dbutils.widgets.get("source_catalog")
target_catalog = dbutils.widgets.get("target_catalog")
schema_name = dbutils.widgets.get("schema")
table_name = "opal_card_transactions"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{schema_name};")

# COMMAND ----------

query = "SELECT * from {source_catalog}.{schema_name}.{table_name} WHERE location != '-1' AND transaction_type = 'journey'"

df = spark.sql(
    query,
    source_catalog=source_catalog,
    schema_name=schema_name,
    table_name=table_name
)

df.write.mode("overwrite").saveAsTable(f"{target_catalog}.{schema_name}.{table_name}")
