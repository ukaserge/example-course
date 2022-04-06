# Databricks notebook source
# A utility method common used in streaming demonstrations.
# Note: it is best to show the students this code the first time
# so that they understand what it is doing and why, but from 
# that point forward, just call it via the DA object.
def _block_until_stream_is_ready(query, min_batches=2):
    import time
    while len(query.recentProgress) < min_batches:
        time.sleep(5) # Give it a couple of seconds

    print(f"The stream has processed {len(query.recentProgress)} batchs")
    
DA.block_until_stream_is_ready = _block_until_stream_is_ready

# COMMAND ----------

# Used to initialize MLflow with the job ID when ran under test
def init_mlflow_as_job():
    import mlflow
    
    job_experiment_id = sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
        dbutils.entry_point.getDbutils().notebook().getContext().tags())["jobId"]

    if job_experiment_id:
        mlflow.set_experiment(f"/Curriculum/Experiments/{job_experiment_id}")

    init_mlflow_as_job()

    None # Suppress output

# COMMAND ----------

# This is a utility method used by N different lessons.
# Note we are not registering it against DA because we 
# don't need the students to know about it.
def create_magic_table():
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

def _install_datasets(reinstall=False):
    import time

    DA.hidden.datasets = f"{DA.working_dir_prefix}/datasets"
    
    min_time = "3 min"
    max_time = "10 min"

    data_source_uri = "wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01"
    print(f"The source for this dataset is\n{data_source_uri}/\n")

    print(f"Your dataset directory is\n{DA.hidden.datasets}\n")
    existing = DA.paths.exists(DA.hidden.datasets)

    if not reinstall and existing:
        print(f"Skipping install of existing dataset.")
        print()
        validate_datasets()
        return 

    # Remove old versions of the previously installed datasets
    if existing:
        print(f"Removing previously installed datasets from\n{DA.hidden.datasets}")
        dbutils.fs.rm(DA.hidden.datasets, True)

    print(f"""Installing the datasets to {DA.hidden.datasets}""")

    print(f"""\nNOTE: The datasets that we are installing are located in Washington, USA - depending on the
          region that your workspace is in, this operation can take as little as {min_time} and 
          upwards to {max_time}, but this is a one-time operation.""")

    files = dbutils.fs.ls(data_source_uri)
    print(f"\nInstalling {len(files)} datasets: ")
    
    install_start = int(time.time())
    for f in files:
        start = int(time.time())
        print(f"Copying /{f.name[:-1]}", end="...")

        dbutils.fs.cp(f"{data_source_uri}/{f.name}", f"{DA.hidden.datasets}/{f.name}", True)
        print(f"({int(time.time())-start} seconds)")

    print()
    validate_datasets()
    print(f"""\nThe install of the datasets completed successfully in {int(time.time())-install_start} seconds.""")  

DA.install_datasets = _install_datasets

# COMMAND ----------

def _validate_datasets():
    pass # TODO - JACOB, implement a sample.

DA.validate_datasets = _validate_datasets

