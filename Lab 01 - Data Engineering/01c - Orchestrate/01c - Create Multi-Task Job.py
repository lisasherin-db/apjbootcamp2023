# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks SDK
# MAGIC The Databricks SDK is like having a nitro boost for Python development in the Databricks Lakehouse. It's your secret sauce to turbocharge your coding abilities. Just like a sports car can handle all kinds of terrain, this SDK covers all public Databricks REST API operations. It's your all-access pass to the entire spectrum of Databricks features and functionalities. Imagine having an autopilot system that can handle unexpected twists and turns effortlessly. The SDK's internal HTTP client is as robust as your sports car's suspension, ensuring smooth handling even when things get bumpy. It's got your back with intelligent retries, so you can keep moving forward without a hiccup. Much like the precision engineering behind a high-performance vehicle, this SDK is meticulously crafted to provide you with the utmost control and accuracy over your Databricks projects. This SDK isn't just fast; it's fuel-efficient too. It streamlines your development process, making it sleek and efficient, so you can get more done in less time. You can always find the latest documentation at https://databricks-sdk-py.readthedocs.io
# MAGIC
# MAGIC Whether you're scripting like a code ninja in the shell, orchestrating seamless CI/CD production setups, or conducting symphonies of data from the Databricks Notebook, this SDK is your all-in-one, full-throttle, programmatic powerhouse. For now, Databricks Runtime may have outdated versions of Python SDK, so until further notice, make sure to always install the latest version 
# MAGIC
# MAGIC ## Let's create a Workflow!

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
# MAGIC ## Workflow Created! Now let's run it..
# MAGIC Head to Workflows and run your job to see the pipeline in action!

# COMMAND ----------

# MAGIC %md ## More Code Examples
# MAGIC
# MAGIC Please checkout [OAuth with Flask](https://github.com/databricks/databricks-sdk-py/tree/main/examples/flask_app_with_oauth.py), 
# MAGIC [Last job runs](https://github.com/databricks/databricks-sdk-py/tree/main/examples/last_job_runs.py), 
# MAGIC [Starting job and waiting](https://github.com/databricks/databricks-sdk-py/tree/main/examples/starting_job_and_waiting.py) examples. You can also dig deeper into different services, like
# MAGIC [alerts](https://github.com/databricks/databricks-sdk-py/tree/main/examples/alerts), 
# MAGIC [billable_usage](https://github.com/databricks/databricks-sdk-py/tree/main/examples/billable_usage), 
# MAGIC [catalogs](https://github.com/databricks/databricks-sdk-py/tree/main/examples/catalogs), 
# MAGIC [cluster_policies](https://github.com/databricks/databricks-sdk-py/tree/main/examples/cluster_policies), 
# MAGIC [clusters](https://github.com/databricks/databricks-sdk-py/tree/main/examples/clusters), 
# MAGIC [credentials](https://github.com/databricks/databricks-sdk-py/tree/main/examples/credentials), 
# MAGIC [current_user](https://github.com/databricks/databricks-sdk-py/tree/main/examples/current_user), 
# MAGIC [dashboards](https://github.com/databricks/databricks-sdk-py/tree/main/examples/dashboards), 
# MAGIC [data_sources](https://github.com/databricks/databricks-sdk-py/tree/main/examples/data_sources), 
# MAGIC [databricks](https://github.com/databricks/databricks-sdk-py/tree/main/examples/databricks), 
# MAGIC [encryption_keys](https://github.com/databricks/databricks-sdk-py/tree/main/examples/encryption_keys), 
# MAGIC [experiments](https://github.com/databricks/databricks-sdk-py/tree/main/examples/experiments), 
# MAGIC [external_locations](https://github.com/databricks/databricks-sdk-py/tree/main/examples/external_locations), 
# MAGIC [git_credentials](https://github.com/databricks/databricks-sdk-py/tree/main/examples/git_credentials), 
# MAGIC [global_init_scripts](https://github.com/databricks/databricks-sdk-py/tree/main/examples/global_init_scripts), 
# MAGIC [groups](https://github.com/databricks/databricks-sdk-py/tree/main/examples/groups), 
# MAGIC [instance_pools](https://github.com/databricks/databricks-sdk-py/tree/main/examples/instance_pools), 
# MAGIC [instance_profiles](https://github.com/databricks/databricks-sdk-py/tree/main/examples/instance_profiles), 
# MAGIC [ip_access_lists](https://github.com/databricks/databricks-sdk-py/tree/main/examples/ip_access_lists), 
# MAGIC [jobs](https://github.com/databricks/databricks-sdk-py/tree/main/examples/jobs), 
# MAGIC [libraries](https://github.com/databricks/databricks-sdk-py/tree/main/examples/libraries), 
# MAGIC [local_browser_oauth.py](https://github.com/databricks/databricks-sdk-py/tree/main/examples/local_browser_oauth.py), 
# MAGIC [log_delivery](https://github.com/databricks/databricks-sdk-py/tree/main/examples/log_delivery), 
# MAGIC [metastores](https://github.com/databricks/databricks-sdk-py/tree/main/examples/metastores), 
# MAGIC [model_registry](https://github.com/databricks/databricks-sdk-py/tree/main/examples/model_registry), 
# MAGIC [networks](https://github.com/databricks/databricks-sdk-py/tree/main/examples/networks), 
# MAGIC [permissions](https://github.com/databricks/databricks-sdk-py/tree/main/examples/permissions), 
# MAGIC [pipelines](https://github.com/databricks/databricks-sdk-py/tree/main/examples/pipelines), 
# MAGIC [private_access](https://github.com/databricks/databricks-sdk-py/tree/main/examples/private_access), 
# MAGIC [queries](https://github.com/databricks/databricks-sdk-py/tree/main/examples/queries), 
# MAGIC [recipients](https://github.com/databricks/databricks-sdk-py/tree/main/examples/recipients), 
# MAGIC [repos](https://github.com/databricks/databricks-sdk-py/tree/main/examples/repos), 
# MAGIC [schemas](https://github.com/databricks/databricks-sdk-py/tree/main/examples/schemas), 
# MAGIC [secrets](https://github.com/databricks/databricks-sdk-py/tree/main/examples/secrets), 
# MAGIC [service_principals](https://github.com/databricks/databricks-sdk-py/tree/main/examples/service_principals), 
# MAGIC [storage](https://github.com/databricks/databricks-sdk-py/tree/main/examples/storage), 
# MAGIC [storage_credentials](https://github.com/databricks/databricks-sdk-py/tree/main/examples/storage_credentials), 
# MAGIC [tokens](https://github.com/databricks/databricks-sdk-py/tree/main/examples/tokens), 
# MAGIC [users](https://github.com/databricks/databricks-sdk-py/tree/main/examples/users), 
# MAGIC [vpc_endpoints](https://github.com/databricks/databricks-sdk-py/tree/main/examples/vpc_endpoints), 
# MAGIC [warehouses](https://github.com/databricks/databricks-sdk-py/tree/main/examples/warehouses), 
# MAGIC [workspace](https://github.com/databricks/databricks-sdk-py/tree/main/examples/workspace), 
# MAGIC [workspace_assignment](https://github.com/databricks/databricks-sdk-py/tree/main/examples/workspace_assignment), 
# MAGIC [workspace_conf](https://github.com/databricks/databricks-sdk-py/tree/main/examples/workspace_conf), 
# MAGIC and [workspaces](https://github.com/databricks/databricks-sdk-py/tree/main/examples/workspaces).
# MAGIC
# MAGIC And, of course, https://databricks-sdk-py.readthedocs.io/.

# COMMAND ----------


