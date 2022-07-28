# Databricks notebook source
# MAGIC %run ./_utility-functions

# COMMAND ----------

# Define only so that we can reference known variables, 
# not actually invoking anything other functions.
DA = DBAcademyHelper()
DA.install_datasets(reinstall=True)

