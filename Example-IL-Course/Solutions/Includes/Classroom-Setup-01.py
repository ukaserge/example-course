# Databricks notebook source
# MAGIC %run "./_databricks-academy-helper" $lesson="01"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

DA.cleanup()            # Remove the existing database and files
DA.init(create_db=True) # True is the default

# Execute any special scripts we need for this lesson
create_magic_table()
# init_mlflow_as_job()

DA.conclude_setup()

