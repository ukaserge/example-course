# Databricks notebook source
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
# MAGIC git+https://github.com/databricks-academy/dbacademy-rest \
# MAGIC git+https://github.com/databricks-academy/dbacademy-helper \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

# MAGIC %run ./_remote_files

# COMMAND ----------

from dbacademy.dbhelper import DBAcademyHelper, Paths

# The following attributes are externalized to make them easy
# for content developers to update with every new course.
helper_arguments = {
    "course_code" : "ec",             # The abreviated version of the course
    "course_name" : "example-course", # The full name of the course, hyphenated
    "data_source_name" : "example-course", # Should be the same as the course
    "data_source_version" : "v01",    # New courses would start with 01
    "enable_streaming_support": True, # This couse uses stream and thus needs checkpoint directories
    "install_min_time" : "3 min",     # The minimum amount of time to install the datasets (e.g. from Oregon)
    "install_max_time" : "10 min",    # The maximum amount of time to install the datasets (e.g. from India)
    "remote_files": remote_files,     # The enumerated list of files in the datasets
}

# COMMAND ----------

def block_until_stream_is_ready(self, query, min_batches=2):
    """
    A utility method used in streaming notebooks to block until the stream has processed n batches. This method serves one main purpose in two different usescase

    The purpose is to block the current command until the state of the stream is ready and thus allowing the next command to execute against the properly initialized stream.

    The first use case is in jobs where the stream is started in one cell but execution of subsequent cells start prematurely.

    The second use case is to slow down students who likewise attempt to execute subsequent cells before the stream is in a valid state either by invoking subsequent cells directly or by execute the Run-All Command

    Note: it is best to show the students this code the first time so that they understand what it is doing and why, but from that point forward, just call it via the DA object.
    """
    import time
    while len(query.recentProgress) < min_batches:
        time.sleep(5)  # Give it a couple of seconds

    print(f"The stream has processed {len(query.recentProgress)} batches")
    
DBAcademyHelper.monkey_patch(block_until_stream_is_ready)

