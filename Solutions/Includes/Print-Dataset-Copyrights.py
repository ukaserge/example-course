# Databricks notebook source
# MAGIC %run ./_utility-functions

# COMMAND ----------

from dbacademy.dbhelper import DBAcademyHelper

DA = DBAcademyHelper(**helper_arguments)
DA.init(install_datasets=True, create_db=False)

# COMMAND ----------

DA.print_copyrights()

