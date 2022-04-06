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
# MAGIC # Manage SQL Endpoints
# MAGIC The purpose of this notebook is to create, start, stop and delete endpoints for each user in a workspace

# COMMAND ----------

import json
from dbacademy.dbrest.sql.endpoints import *

all_courses = [""]
all_courses.extend(courses_map.keys())
dbutils.widgets.combobox("course", "", all_courses, "Course")
course = dbutils.widgets.get("course")

all_usernames = ["All Users"]
all_usernames.extend([r.get("userName") for r in client.scim().users().list()])
dbutils.widgets.multiselect("usernames", "All Users", all_usernames, "Users")
usernames = dbutils.widgets.get("usernames")

assert len(course) > 0, "Please select a course"
assert len(usernames) > 0, "Please select either All Users or a valid subset of users"

course_name = courses_map[course].get("name")
course_code = courses_map[course].get("code")
usernames = usernames.split(",")

print(f"Course Name: {course_name}")
print(f"Course Code: {course_code}")

users = client.scim().users().list() if "All Users" in usernames else client.scim().users().to_users_list(usernames)

naming_template="da-{course}-{da_name}@{da_hash}"
naming_params={"course": course_code}

print("\nThis notebook's tasks will be applied to the following users:")
for user in users:
    print(" ", user.get("userName"))

# COMMAND ----------

dbutils.notebook.exit("Precluding Run-All")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task: Create SQL Endpoints

# COMMAND ----------

client.sql().endpoints().create_user_endpoints(naming_template=naming_template,      # Required
                                               naming_params=naming_params,          # Required
                                               cluster_size = CLUSTER_SIZE_2X_SMALL, # Required
                                               enable_serverless_compute = False,    # Required
                                               tags = { "dbacademy": course_name },  # Prototyping Management
                                               users=users)                          # Restrict to the specified list of users
# See docs for more parameters
# print(client.sql().endpoints().create_user_endpoints.__doc__)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Task: List SQL Endpoints

# COMMAND ----------

print("Found the following endpoints:")
for endpoint in client.sql().endpoints().list():
    name = endpoint.get("name")
    size = endpoint.get("size")
    print(f"  {name}: {size}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Task: Start SQL Endpoint

# COMMAND ----------

client.sql().endpoints().start_user_endpoints(naming_template, naming_params, users)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Task: Stop SQL Endpoints

# COMMAND ----------

client.sql().endpoints().stop_user_endpoints(naming_template, naming_params, users)

# COMMAND ----------

# MAGIC %md ## Task: Delete SQL Endpoints

# COMMAND ----------

client.sql().endpoints().delete_user_endpoints(naming_template, naming_params, users)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
