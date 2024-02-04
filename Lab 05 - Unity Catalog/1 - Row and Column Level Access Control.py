# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Introduction
# MAGIC
# MAGIC In this Notebook we will see how to work with Unity Catalog. 
# MAGIC
# MAGIC Some of the things we will look at are:
# MAGIC * Create Column Masks and Row Filters for fine grained access controls
# MAGIC * Add documentation to Delta Tables and Columns
# MAGIC * Create a mapping table for more dynamic access rules
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Prepare your lab
# MAGIC
# MAGIC Run the next 2 cells to generate some tables we will be using for this lab.

# COMMAND ----------

# MAGIC %run "../Lab 01 - Data Engineering/Utils/prepare-lab-environment"

# COMMAND ----------

# This will take up to 2min to run
generate_employee_dataset()

generate_cost_center_dataset()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- view table contents
# MAGIC SELECT * FROM employees

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Row level access control 
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/acls/table_uc_rls.png?raw=true" width="200" style="float: right; margin-top: 20; margin-right: 20" alt="databricks-demos"/>
# MAGIC
# MAGIC Row-level security allows you to automatically hide a subset of your rows based on who is attempting to query it, without having to maintain any seperate copies of your data.
# MAGIC
# MAGIC A typical use-case would be to filter out rows based on your country or Business Unit : you only see the data (financial transactions, orders, customer information...) pertaining to your region, thus preventing you from having access to the entire dataset.
# MAGIC
# MAGIC 💡 While this filter can be applied at the user / principal level, it is recommended to implement access policies using groups instead.
# MAGIC <br style="clear: both"/>
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/acls/table_uc_cls.png?raw=true" width="200" style="float: right; margin-top: 20; margin-right: 20; margin-left: 20" alt="databricks-demos"/>
# MAGIC
# MAGIC ## Column Level access control 
# MAGIC
# MAGIC Similarly, column-level access control helps you mask or anonymise the data that is in certain columns of your table, depending on the user or service principal that is trying to access it. This is typically used to mask or remove sensitive PII informations from your end users (email, SSN...).

# COMMAND ----------

# MAGIC %md
# MAGIC To capture the current user and check their membership to a particular group, Databricks provides you with 2 built-in functions: 
# MAGIC - `current_user()` 
# MAGIC - and `is_account_group_member()` respectively.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user();

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Hands On Task!
# MAGIC
# MAGIC Below we will browse the groups in the workspace and check our membership. 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show all groups in the workspace
# MAGIC SHOW GROUPS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- see what groups you are a member of
# MAGIC SHOW GROUPS WITH USER `<fill your email>`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create the Row Filter
# MAGIC
# MAGIC We will create a row filter and apply it on our employee table to see it in action! See the Row Filter syntax here: https://docs.databricks.com/en/data-governance/unity-catalog/row-and-column-filters.html#row-filter-syntax

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's create an access rule
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION cost_centre_filter(cc_param STRING) 
# MAGIC RETURN 
# MAGIC   is_account_group_member('admins') or -- admin can access all cost_centres
# MAGIC   cc_param like "NSW";  -- everybody can access regions containing NSW
# MAGIC
# MAGIC ALTER FUNCTION cost_centre_filter OWNER TO `account users`; -- grant access to all user to the function for the demo - don't do it in production

# COMMAND ----------

# MAGIC %sql
# MAGIC -- country will be the column send as parameter to our SQL function (country_param)
# MAGIC ALTER TABLE employees SET ROW FILTER bu_filter ON (bu);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check filter works
# MAGIC select * from employees

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Column Mask
# MAGIC We will create a column mask and apply it on our employee table to see it in action! See the Column Mask syntax here: https://docs.databricks.com/en/data-governance/unity-catalog/row-and-column-filters.html

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION simple_mask(column_value STRING)
# MAGIC    RETURN IF(is_account_group_member('admins'), column_value, "****"); -- admin can see raw value
# MAGIC    
# MAGIC ALTER FUNCTION simple_mask OWNER TO `account users`; -- grant access to all user to the function for the demo
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE employees ALTER COLUMN address SET MASK simple_mask;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Dynamic access rules with lookup data
# MAGIC Unity Catalog give us the flexibility to overwrite the definition of an access rule but also combine multiple rules on a single table to ultimately implement complex multi-dimensional access control on our data.
# MAGIC
# MAGIC Let's take a step further by adding an intermediate table describing a permission model. We'll use this table to lookup pre-defined mappings of users to their corresponding data, on which we'll base the behavior of our access control function.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Hands On Task!
# MAGIC
# MAGIC First, let's create this mapping table.
# MAGIC
# MAGIC In an organization where we have a user group for each department, we went ahead and mapped in this table each of these groups to their corresponding cost centres.
# MAGIC
# MAGIC The members of the `syd_trains_analysts` are thus mapped to cost centre data of the Sydney Trains cost centre, the members of the `syd_metro_analysts` are mapped to data for Sydney Metro, and so on.
# MAGIC
# MAGIC In our case, we belong to neither so we'll fall back on the all users condition (account users)
# MAGIC
# MAGIC **Note:** Groups can be created in your Identity Provider and synced to the Databricks workspace via the account console if you have the permissions to do so.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS map_costcentre_group (
# MAGIC   identity_group STRING,
# MAGIC   cost_centre ARRAY<STRING>
# MAGIC );
# MAGIC ALTER TABLE map_costcentre_group OWNER TO `account users`; -- for the demo only, allow all users to edit the table - don't do that in production!
# MAGIC
# MAGIC INSERT OVERWRITE map_costcentre_group (identity_group, cost_centre) VALUES
# MAGIC   ('syd_trains_analysts', Array("10001", "10002", "10003")),
# MAGIC   ('syd_metro_analysts',  Array("20001","20002","20003")),
# MAGIC   ('rds_analysts', Array("50001","50002"));
# MAGIC
# MAGIC SELECT * FROM map_costcentre_group;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION cost_centre_filter(cc_param STRING)
# MAGIC  RETURN 
# MAGIC  is_account_group_member('admins') or -- the current user is super admin, we can see everything
# MAGIC  exists (
# MAGIC   -- current user is in a group and the group array contains the region. You could also do it with more advanced control, joins etc.
# MAGIC   -- Spark optimizer will execute that as an efficient JOIN between the map_country_group table and your main table - you can check the query execution in Spark SQL UI for more details.  
# MAGIC    SELECT 1 FROM map_costcentre_group WHERE is_account_group_member(identity_group) AND array_contains(cost_centres, cc_param)
# MAGIC  );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- check the current rows visible
# MAGIC select * from employees

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Advanced Task
# MAGIC
# MAGIC Unity Catalog integrates with PowerBI and Tableau to easily share data and enforce governance on your data. 
# MAGIC
# MAGIC Navigate to your Catalog Explorer, find your tables and click Use with BI Tools to choose you preferred viz tool. Download your connection file and open it to view your datasets in your BI tool.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- remove mask from table
# MAGIC ALTER TABLE employees ALTER COLUMN address DROP MASK;
