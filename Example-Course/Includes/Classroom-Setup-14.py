# Databricks notebook source
# MAGIC %run ./_utility-functions

# COMMAND ----------

# MAGIC %run ./_multi-task-jobs-config

# COMMAND ----------

DA = DBAcademyHelper()      # Create the DA object with the specified lesson
DA.cleanup(validate=False)  # Remove the existing database and files
DA.init(create_db=True)     # True is the default
DA.install_datasets()       # Install (if necissary) and validate the datasets

# The location that the DLT databases should be written to
DA.paths.storage_location = f"{DA.paths.working_dir}/storage_location"

# One of the common patters for streaming data is to use 
# a DataFactory that loads one-batch at a time, on demand
DA.dlt_data_factory = DataFactory()
DA.dlt_data_factory.load()  # We need at least one batch to get started.

DA.conclude_setup()         # Conclude the setup by printing the DA object's final state

