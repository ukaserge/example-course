# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./Includes/Common

# COMMAND ----------

# MAGIC %md
# MAGIC # Manage Datasets
# MAGIC The purpose of this notebook is to download and install datasets used by each course

# COMMAND ----------

import json, time
from dbacademy.dbrest.sql.endpoints import *
dbutils.widgets.removeAll()

all_courses = [""]
all_courses.extend(courses_map.keys())
dbutils.widgets.combobox("course", "", all_courses, "Course")
course = dbutils.widgets.get("course")
assert len(course) > 0, "Please select a course"

data_source_base_uri = f"wasbs://courseware@dbacademy.blob.core.windows.net/{course}"
version_files = dbutils.fs.ls(data_source_base_uri)

if len(version_files) == 1:
    data_source_version = version_files[0].name[0:-1]
    data_source_uri = version_files[0].path[0:-1]
else:
    all_versions = [""]
    all_versions.extend([r.name[0:-1] for r in version_files])
    dbutils.widgets.dropdown("version", "", all_versions, "Version")
    data_source_version = dbutils.widgets.get("version")
    assert len(data_source_version) > 0, "Please select the dataset version"
    data_source_uri = f"{data_source_base_uri}/{data_source_version}"

course_name = courses_map[course].get("name")
course_code = courses_map[course].get("code")
target_dir = f"dbfs:/mnt/dbacademy-datasets/{course}/{data_source_version}"

print(f"Course Name: {course_name}")
print(f"Dataset Repo: {data_source_uri}")
print(f"Target Directory: {target_dir}")

# COMMAND ----------

dbutils.notebook.exit("Precluding Run-All")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task: Delete Existing Dataset
# MAGIC This should only be executed to "repair" a datsets.

# COMMAND ----------

dbutils.fs.rm(target_dir, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task: Install Dataset

# COMMAND ----------

files = dbutils.fs.ls(f"{data_source_uri}")
for i, file in enumerate(files):
    start = int(time.time())
    print(f"Installing dataset {i+1}/{len(files)}, {file.name}", end="...")
    dbutils.fs.cp(file.path, f"{target_dir}/{file.name}", True)
    print(f"({int(time.time())-start} seconds)")
    
display(dbutils.fs.ls(target_dir))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
