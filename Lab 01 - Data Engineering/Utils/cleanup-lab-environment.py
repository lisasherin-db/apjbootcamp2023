# Databricks notebook source
# pass catalog var as notebook widget
dbutils.widgets.text(name="catalog", defaultValue="tfnsw_bootcamp_catalog", label="catalog") 

# COMMAND ----------

current_user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
datasets_location = f'/Workspace/tmp/{current_user_id}/datasets/'

dbutils.fs.rm(datasets_location, True)
print(f'Deleted data files from location: %s' %datasets_location)

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog") # get catalog name from widget
spark.sql(f'create catalog if not exists {catalog_name};')
spark.sql(f'use catalog {catalog_name}')
database_name = current_user_id.split('@')[0].replace('.','_')+'_bootcamp'
spark.sql(f'drop database if exists {database_name} cascade;')
print(f'Deleted database: %s' %database_name)
