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

# Update this list with the email addresses (usernames)
# that should be managed by this script (e.g. created)
usernames = [
        "andras.fulop@databricks.com",
        "brooke.wenig@databricks.com",
        "charles.harding@databricks.com",
        "chengyin.eng@databricks.com",
        "conor.murphy@databricks.com",
        "david.harris@databricks.com",
        "gray.gwizdz@databricks.com",
        "guenia.izquierdo@databricks.com",
        "joel.budgor@databricks.com",
        "joseph.kambourakis@databricks.com",
        "ken.jones@databricks.com",
        "kornel.kovacs@databricks.com",
        "lisa.ritcher@databricks.com",
        "mark.ott@databricks.com",
        "marygrace.moesta@databricks.com",
        "miklos.toth@databricks.com",
        "patrick.putnam@databricks.com",
        "richard.smith@databricks.com",
        "ronald.daskevich@databricks.com",
        "yinxi@databricks.com",
        "zoltan.toth@databricks.com",
]

# COMMAND ----------

dbutils.notebook.exit("Precluding Run-All")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task: List Users
# MAGIC List the users currently in the workspace

# COMMAND ----------

users = client.scim().users().list()

print("Found the following users in the workspace:")
for user in users:
    username = user.get("userName")
    print(f"  {username}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task: Create Users
# MAGIC Create each user specified in the usernames list

# COMMAND ----------

for username in usernames:
    user = client.scim().users().get_by_username(username)
    if user is None:
        try:
            print(f"Creating the user {username}")
            client.scim().users().create(username)
        except Exception as e:
            print(e)
    else:
        print(f"Skipping creation of the user {username}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
