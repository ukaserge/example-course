# Databricks notebook source
# MAGIC %run ./_utility-functions

# COMMAND ----------

# Defined here because it is used only by this lesson
# Not bound to DA and prefixed with an underscore to hide from the student.
def _create_magic_table():
    """
    This is a sample utility method that creates a table and insert some data. Because this is not user-facing, we do not monkey-patch it into DBAcademyHelper
    """
    DA.paths.magic_tbl = f"{DA.paths.working_dir}/magic"
    
    spark.sql(f"""
CREATE TABLE IF NOT EXISTS magic (some_number INT, some_string STRING)
USING DELTA
LOCATION '{DA.paths.magic_tbl}'
""")
    spark.sql("INSERT INTO magic VALUES (1, 'moo')")
    spark.sql("INSERT INTO magic VALUES (2, 'baa')")
    spark.sql("INSERT INTO magic VALUES (3, 'wolf')")

# COMMAND ----------

DA = DBAcademyHelper()      # Create the DA object with the specified lesson
DA.cleanup(validate=False)  # Remove the existing database and files
DA.init(create_db=True)     # True is the default
DA.install_datasets()       # Install (if necissary) and validate the datasets

_create_magic_table()       # A lesson-specific utility method to create and load a table.

DA.conclude_setup()         # Conclude the setup by printing the DA object's final state

