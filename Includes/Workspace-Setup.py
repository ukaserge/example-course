# Databricks notebook source
# MAGIC %run ./_utility-functions

# COMMAND ----------

# Define only so that we can reference known variables, 
# not actually invoking anything other functions.
from dbacademy.dbhelper import DBAcademyHelper, Paths
DA = DBAcademyHelper(**helper_arguments)

# Install the datasets, but don't forece a reinstall so as
# to keep it idempotent, but it will repair if issues are found.
DA.install_datasets(reinstall_datasets=False)

