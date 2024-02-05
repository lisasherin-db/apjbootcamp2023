# Databricks notebook source
dbutils.widgets.text("raw_data_path", "f'/Workspace/tmp/current_user_id/datasets/", "raw_data_path")
dbutils.widgets.text("target_catalog", "tfnsw_bootcamp_catalog", "target_catalog")

# COMMAND ----------

raw_path = dbutils.widgets.get("raw_data_path")
bronze_catalog = dbutils.widgets.get("target_catalog")
schema_name = "opal"
table_name = "opal_card_transactions"

spark.sql(f"USE CATALOG {bronze_catalog};")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
spark.sql(f"USE SCHEMA {schema_name};")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create opal table
# MAGIC CREATE OR REPLACE TABLE opal_card_transactions (
# MAGIC     transaction_id STRING PRIMARY KEY COMMENT "Unique identifier for each transaction",
# MAGIC     card_number STRING COMMENT "Opal card number associated with the transaction",
# MAGIC     transaction_type STRING COMMENT "Type of transaction (e.g., top-up, journey, balance inquiry)",
# MAGIC     amount DECIMAL(10, 2) COMMENT "Amount of the transaction (if applicable)",
# MAGIC     mode STRING COMMENT "Mode of transport (e.g., Train, Bus, Ferry, Light Rail)",
# MAGIC     tap_type STRING COMMENT "Type of tap (on or off)",
# MAGIC     created_at DATE COMMENT "Time of the transaction based on the Opal system time",
# MAGIC     location STRING COMMENT "Location details (including -1 for unknown, 4-digit postcode, or named locations)",
# MAGIC     card_type STRING COMMENT "Type of Opal card (e.g., concession, child, employee)"
# MAGIC );

# COMMAND ----------

opal_card_schema = spark.read.table(table_name).schema
df = spark.read.json(raw_path, schema=opal_card_schema)

df.write.mode("overwrite").saveAsTable(table_name)

# COMMAND ----------


