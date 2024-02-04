# Databricks notebook source
# MAGIC %md
# MAGIC ## Tags
# MAGIC
# MAGIC Tags are attributes containing keys and optional values that you can apply to different securable objects in Unity Catalog. Tagging is useful for organizing and categorizing different securable objects within a metastore. Using tags also simplifies search and discovery of your data assets.
# MAGIC
# MAGIC Read the documentation to see more details on tagging syntax: https://docs.databricks.com/en/data-governance/unity-catalog/tags.html

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Hands On Task!
# MAGIC
# MAGIC Below we will add tags to our tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- tag source on table
# MAGIC ALTER TABLE employees SET TAGS ("source" = "ERP", "classification" = "confidential"); 
# MAGIC
# MAGIC -- Apply PII tags to table columns
# MAGIC ALTER TABLE employees ALTER COLUMN Email SET TAGS ("PII"="email"); -- e.g. tag email, DoB, etc
# MAGIC ALTER TABLE employees ALTER COLUMN ContactNumber SET TAGS ("PII"="phone number"); 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Comments
# MAGIC
# MAGIC You can further enrich your data and enable discovery with comments on your different securables. See our docs for the DDL sytax: https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-comment.html
# MAGIC
# MAGIC If you want to add an **AI-generated** comment for a table or table column managed by Unity Catalog, see [Add AI-generated comments](https://docs.databricks.com/en/catalog-explorer/ai-comments.html) to a table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- add documentation to the table
# MAGIC COMMENT ON TABLE employees IS "This is a table containing TfNSW employee information sourced from the ERP system";

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Hands On Task!
# MAGIC
# MAGIC Choose a column and add some comments

# COMMAND ----------

# MAGIC %sql
# MAGIC -- add comments to columns
# MAGIC ALTER TABLE employees ALTER COLUMN <colname> COMMENT "<add comments here>";

# COMMAND ----------

# MAGIC %sql
# MAGIC -- now let's view documentation that we added
# MAGIC DESCRIBE TABLE EXTENDED employees; 

# COMMAND ----------

# MAGIC %run "../Lab 01 - Data Engineering/Utils/cleanup-lab-environment" $catalog="<add gold catalog name>"
