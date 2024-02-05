# Databricks notebook source
# MAGIC %md
# MAGIC # Create multi task jobs using the Databricks Python SDK
# MAGIC Creating and maintaining jobs programatically is interesting when working with multiple environments, building up disaster recovery structures and even versioning your Databricks workflows.
# MAGIC
# MAGIC Using the Databricks SDK, we can create multi-task jobs without having to worry about the underlying Databricks APIs and their payload structures, leveraging the pre-built assets packed within the SDK.
# MAGIC

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.18.0 --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "../Utils/prepare-lab-environment"

# COMMAND ----------

# wait up to 2mins
generate_opal_transactions(500000)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, compute

w = WorkspaceClient()

my_email = w.current_user.me().user_name
my_name = w.current_user.me().display_name

# find your cluster id
for c in w.clusters.list():
    if c.cluster_name.startswith(my_name):
        cluster_id = c.cluster_id
        print(f"Cluster Name: {c.cluster_name}, Cluster ID: {c.cluster_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Hands On Task!
# MAGIC
# MAGIC Now let's create a job by scheduling the notebooks from the transformation section.
# MAGIC - **Task 1** is going to ingest the csv data from cloud storage into the bronze catalog
# MAGIC - **Task 2** is going to process bronze data, clean and store in silver catalog
# MAGIC - **Task 3** is going to transform the silver data into aggregated data and store in gold catalog

# COMMAND ----------

notebook_path = (
    "/Workspace/Repos/lisa.sherin@databricks.com/apjbootcamp2023/Lab 01 - Data Engineering/01c - Orchestrate"
)

# list of emails to notify on job status
email_notification_lst = [my_email]

# schedule daily at 6pm
quartz_cron_expression = "0 0 16 * * ?"

task1_params = {
    "raw_data_path": f"{datasets_location}opal-card-transactions/",
    "target_catalog": "tfnsw_bootcamp_catalog", # TODO replace catalog name
}

task2_params = {
    "source_catalog": "tfnsw_bootcamp_catalog", # TODO replace catalog name
    "target_catalog": "tfnsw_bootcamp_catalog" # TODO replace catalog name
}

task3_params = {
    "source_catalog": "silver_catalog", # TODO replace catalog name
    "target_catalog": "gold_catalog" # TODO replace catalog name
}

# let's create a multi-task job 
job = w.jobs.create(
    name=f"{my_name}-job",
    tasks=[
        # task 1
        jobs.Task(
            task_key="ingest-to-bronze",
            notebook_task=jobs.NotebookTask(notebook_path=f"{notebook_path}/task1", base_parameters=task1_params), 
            existing_cluster_id=cluster_id,
            email_notifications=jobs.JobEmailNotifications(on_failure=email_notification_lst)
        ),
        # task 2
        jobs.Task(
            task_key="bronze-to-silver",
            notebook_task=jobs.NotebookTask(notebook_path=f"{notebook_path}/task2", base_parameters=task2_params),
            existing_cluster_id=cluster_id,
            depends_on=[jobs.TaskDependency(task_key="ingest-to-bronze")], # adding dependency on previous task - now this won't run until previous task finished successfully
            email_notifications=jobs.JobEmailNotifications(on_failure=email_notification_lst)   
        ),
        # task 3
        jobs.Task(
            task_key="silver-to-gold",
            notebook_task=jobs.NotebookTask(notebook_path=f"{notebook_path}/task3", base_parameters=task3_params), # TODO fix to task3
            existing_cluster_id=cluster_id,
            depends_on=[jobs.TaskDependency(task_key="bronze-to-silver")], # adding dependency on previous task - now this won't run until previous task finished successfully
            email_notifications=jobs.JobEmailNotifications(on_failure=email_notification_lst)
        ),
    ],
    schedule=jobs.CronSchedule(
        quartz_cron_expression=quartz_cron_expression, timezone_id="Australia/Sydney"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Now, head to Workflows and run your job to see the pipeline in action!

# COMMAND ----------


