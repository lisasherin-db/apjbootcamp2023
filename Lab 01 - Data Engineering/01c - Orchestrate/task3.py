# Databricks notebook source
dbutils.widgets.text("source_catalog", "tfnsw_bootcamp_catalog", "source_catalog")
dbutils.widgets.text("target_catalog", "tfnsw_bootcamp_catalog", "target_catalog")

# COMMAND ----------

source_catalog = dbutils.widgets.get("source_catalog")
target_catalog = dbutils.widgets.get("target_catalog")
schema_name = "opal"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{schema_name};")

# COMMAND ----------

transport_modes = ["Ferry", "Train", "Light Rail", "Bus"]

# create a table for each mode of transport
for mode in transport_modes:
    table_name = f"monthly_{mode.lower().replace(' ', '_')}_trips"
    query = "SELECT DATE_FORMAT(created_at, 'yyyy-MM') year_month, mode, card_type, COUNT(transaction_id) as trips, SUM(amount) as fares from {source_catalog}.{schema_name}.opal_card_transactions WHERE mode = {mode} GROUP BY 1, 2, 3"

    df = spark.sql(query, mode=mode, catalog=source_catalog, schema=schema_name)
    df.write.mode("overwrite").saveAsTable(
        f"{target_catalog}.{schema_name}.{table_name}"
    )
