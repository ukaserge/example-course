# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Build-Time Substitutions

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC We are not really doing anything special here short of documenting how to use replacements and showing off dbgems
# MAGIC <pre>
# MAGIC * cloud: AWS
# MAGIC * username: jacob.parr@databricks.com
# MAGIC * spark_version: 10.4.x-scala2.12
# MAGIC * instance_pool_id: 1117-212409-soars13-pool-6plxsi6q
# MAGIC * host_name: 1117-212444-7o693v9s-10-141-232-138
# MAGIC * username: jacob.parr@databricks.com
# MAGIC * notebook_path: /Repos/Examples/example-il-course-source/Build-Scripts/Publish-All
# MAGIC * notebook_dir: /Repos/Examples/example-il-course-source/Build-Scripts
# MAGIC * api_endpoint: https://oregon.cloud.databricks.com
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Here is a quick index
# MAGIC ## Lessons
# MAGIC * [Your First Lesson]($./EILC 01 - Your First Lesson)
# MAGIC * [Your Second Lesson]($./EILC 02 - Your Second Lesson)
# MAGIC * [Your Third Lesson]($./EILC 03 - Your Third Lesson)
# MAGIC * [Your Fourth Lesson]($./EILC 04 - You Fourth Lesson)
# MAGIC 
# MAGIC ## Labs
# MAGIC * [Your First Lab]($./Labs/EILC 01L - Your First Lab)
# MAGIC * [Your Second Lab]($./Labs/EILC 02L - Your Second Lab)
# MAGIC * [Your Third Lab]($./Labs/EILC 03L - Your Third Lab)
# MAGIC * [Your Fourth Lab]($./Labs/EILC 04L - You Fourth Lab)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This is a [MD Link]("https://example.com") that the publisher should catch.
# MAGIC 
# MAGIC This is a <a href="https://example.com" target="top">HTML Link</a> that is missing the **`target="\_blank"`** argument.
# MAGIC 
# MAGIC This is a <a href="https://example.com" target="_blank">HTML Link</a> that has the required **`target="\_blank"`** argument.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
