# Databricks notebook source
# MAGIC %run ./_utility-functions

# COMMAND ----------

# MAGIC %run ./_multi-task-jobs-config

# COMMAND ----------

DA = DBAcademyHelper(**helper_arguments) # Create the DA object
DA.reset_environment()                   # Reset by removing databases and files from other lessons
DA.init(install_datasets=True,           # Initialize, install and validate the datasets
        create_db=True)                  # Continue initialization, create the user-db

# The location that the DLT databases should be written to
DA.paths.storage_location = f"{DA.paths.working_dir}/storage_location"

# One of the common patters for streaming data is to use 
# a DataFactory that loads one-batch at a time, on demand
DA.dlt_data_factory = DataFactory()

DA.dlt_data_factory.load()               # We need at least one batch to get started.
DA.conclude_setup()                      # Conclude setup by advertising environmental changes

