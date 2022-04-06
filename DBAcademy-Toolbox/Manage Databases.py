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
# MAGIC # Manage Databases
# MAGIC The purpose of this notebook is to create and drop databases for each user in a workspace

# COMMAND ----------

import json
from dbacademy import dbgems
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
usernames = [u.get("userName") for u in users]

naming_template="da-{course}-{da_name}@{da_hash}"
naming_params={"course": course_code}

print("\nThis notebook's tasks will be applied to the following users:")
for username in usernames:
    print(f"  {username}")

# COMMAND ----------

dbutils.notebook.exit("Precluding Run-All")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task: Create Course-Specific Databases
# MAGIC This task creates one database per user.

# COMMAND ----------

for username in usernames:
    db_name = to_db_name(username, naming_template, naming_params)
    db_path = f"dbfs:/mnt/dbacademy-users/{username}/{course_code}/database.db"
    
    print(f"Creating the database \"{db_name}\"\n   for \"{username}\" \n   at \"{db_path}\"\n")
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE;")
    spark.sql(f"CREATE DATABASE {db_name} LOCATION '{db_path}';")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task: Grant Privileges
# MAGIC This tasks creates the Databricks SQL query which in turn is executed to grant each user access to their databases.

# COMMAND ----------

sql = ""
for username in usernames:
    db_name = to_db_name(username, naming_template, naming_params)
    sql += f"GRANT ALL PRIVILEGES ON DATABASE `{db_name}` TO `{username}`;\n"
    sql += f"GRANT ALL PRIVILEGES ON ANY FILE TO `{username}`;\n"
    sql += f"ALTER DATABASE {db_name} OWNER TO `{username}`;\n"    
    sql += "\n"
    
query_name = f"Instructor - Grant All Users - {course_name}"
query = client.sql().queries().get_by_name(query_name)
if query is not None:
    client.sql().queries().delete_by_id(query.get("id"))

query = client.sql().queries().create(name=query_name, 
                                      query=sql[0:-1], 
                                      description="Grants the required access for all users to the databases for the course {course}",
                                      schedule=None, options=None, data_source_id=None)
query_id = query.get("id")
displayHTML(f"""Query created - follow this link to execute the grants in Databricks SQL</br></br>
                <a href="/sql/queries/{query_id}/source?o={dbgems.get_workspace_id()}" target="_blank">{query_name}</a>""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task: Drop Course-Specific Databases
# MAGIC This task dropes each user's course-specific database.

# COMMAND ----------

for user in users:
    username = user.get("userName")
    db_name = to_db_name(username, naming_template, naming_params)
    
    print(f"Dropping the database \"{db_name}\"")
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE;")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
