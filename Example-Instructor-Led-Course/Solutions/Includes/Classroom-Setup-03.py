# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="03"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

DA.cleanup()            # Remove the existing database and files
DA.init(create_db=True) # True is the default

# Execute any special scripts we need for this lesson
create_magic_table()

DA.conclude_setup()

