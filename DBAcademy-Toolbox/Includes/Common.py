# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
# MAGIC git+https://github.com/databricks-academy/dbacademy-rest \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

from dbacademy.dbrest import DBAcademyRestClient
client = DBAcademyRestClient()

courses_map = {
    "advanced-data-engineering-with-databricks": {
        "code": "adewd",
        "name": "Advanced Data Engineering with Databricks",
        "source-repo": "advanced-data-engineering-with-databricks-source"
    },
    "advanced-machine-learning-with-databricks": {
        "code": "amlwd",
        "name": "Advanced Machine Learning with Databricks",
        "source-repo": "advanced-machine-learning-with-databricks-source",
    },
    "apache-spark-programming-with-databricks": {
        "code": "aspwd",
        "name": "Apache Spark Programming with Databricks",
        "source-repo": "apache-spark-programming-with-databricks-source",
    },
    "data-analysis-with-databricks": {
        "code": "dawd",
        "name": "Data Analysis with Databricks",
        "source-repo": "data-analysis-with-databricks-source",
    },
    "data-engineering-with-databricks": {
        "code": "dewd",
        "name": "Data Engineering with Databricks",
        "source-repo": "data-engineering-with-databricks-source",
    },
    "deep-learning-with-databricks": {
        "code": "dlwd",
        "name": "Deep Learning with Databricks",
        "source-repo": "deep-learning-with-databricks-source",
    },
    "just-enough-python-for-spark": {
        "code": "jepfs",
        "name": "Just Enough Python For Spark",
        "source-repo": "just-enough-python-for-spark-source",
    },
    "ml-in-production": {
        "code": "mlip",
        "name": "ML in Production",
        "source-repo": "ml-in-production-source",
    },
    "scalable-machine-learning-with-apache-spark": {
        "code": "smlwas",
        "name": "Scalable Machine Learning with Apache Spark",
        "source-repo": "scalable-machine-learning-with-apache-spark-source",
    }
}

# COMMAND ----------

def to_db_name(username, naming_template, naming_params):
    import re
    if "{da_hash}" in naming_template:
        assert naming_params.get("course", None) is not None, "The template is employing da_hash which requires course to be specified in naming_params"
        course = naming_params["course"]
        da_hash = abs(hash(f"{username}-{course}")) % 10000
        naming_params["da_hash"] = da_hash

    naming_params["da_name"] = username.split("@")[0]
    db_name = naming_template.format(**naming_params)    
    return re.sub("[^a-zA-Z0-9]", "_", db_name)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
