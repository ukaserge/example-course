# Databricks notebook source
# You may or may not want to use a Paths object, but it is a
# nice way to organize what would otherwise be dozens of hard-coded paths
class Paths():
    def __init__(self, working_dir):
        self.working_dir = working_dir
        self.people_with_dups = f"{working_dir}/datasets/people-with-dups.txt"


class DBAcademyHelper():

    def __init__(self, module_name, per_lesson, dataset_name, dataset_version, dataset_min, dataset_max):
        import re

        self.dataset_name = dataset_name
        self.dataset_version = dataset_version
        self.dataset_min = dataset_min
        self.dataset_max = dataset_max
        
        self.username = spark.sql(f"SELECT current_user()").first()[0]
        clean_username = re.sub(r"[^a-zA-Z0-9]", "-", self.username)
        for i in range(10): clean_username = clean_username.replace("--", "-")

        self.lesson_name = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None).split("/")[-1]
        clean_lesson_name = re.sub("[^a-zA-Z0-9]", "-", self.lesson_name).lower()
        for i in range(10): clean_lesson_name = clean_lesson_name.replace("--", "-")

        # You can decide if you want one user directory and/or one database per lesson
        # but this will give you the basic logic to create either one. 
        # Consider deleting the code that yo don't want

        if not per_lesson:
            self.working_dir = f"dbfs:/user/{self.username}/dbacademy/{module_name}"
            self.user_db = f"dbacademy_{clean_username}_{module_name}".replace("-", "_")
        else:
            self.working_dir = f"dbfs:/user/{self.username}/dbacademy/{module_name}/{clean_lesson_name}"
            self.user_db = f"dbacademy_{clean_username}_{module_name}_{clean_lesson_name}".replace("-", "_")

        # Create and use the database
        print(f"Creating the user-specific database\n{self.user_db}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.user_db}")
        spark.sql(f"USE {self.user_db}")

        self.paths = Paths(self.working_dir)

        # Inject into the spark config any variables you want available in SQL commands
        # The da is our standard namespace, referring to Databricsk Academy
        spark.conf.set("da.user_db", self.user_db)
        spark.conf.set("da.working_dir", self.working_dir)
        spark.conf.set("da.paths.people_with_dups", self.paths.people_with_dups)

    def path_exists(self, path):
        try: return len(dbutils.fs.ls(path)) >= 0
        except Exception:return False

    def _install_datasets(self, working_dir, course_name, version, min_time, max_time, reinstall=False):
        print(f"\nYour working directory is\n{working_dir}\n")

        # You can swap out the source_path with an alternate version during development
        # source_path = f"dbfs:/mnt/work-xxx/{course_name}"
        source_path = f"wasbs://courseware@dbacademy.blob.core.windows.net/{course_name}/{version}"
        print(f"The source of the datasets is\n{source_path}/\n")

        # Change the final directory to another name if you like, e.g. from "datasets" to "raw"
        target_path = f"{working_dir}/datasets"
        existing = self.path_exists(target_path)

        if not reinstall and existing:
            print(f"Skipping install of existing dataset to\n{target_path}")
            return

            # Remove old versions of the previously installed datasets
        if existing:
            print(f"Removing previously installed datasets from\n{target_path}")
            dbutils.fs.rm(target_path, True)

        print(f"""Installing the datasets to {target_path}""")

        print(f"""\nNOTE: The datasets that we are installing are located in Washington, USA - depending on the
        region that your workspace is in, this operation can take as little as {min_time} and 
        upwards to {max_time}, but this is a one-time operation.""")

        dbutils.fs.cp(source_path, target_path, True)
        print(f"""\nThe install of the datasets completed successfully.""")

    def install_datasets(self, reinstall=False):
        self._install_datasets(self.working_dir, self.dataset_name, self.dataset_version, self.dataset_min, self.dataset_max, reinstall=reinstall)

    def cleanup(self):
        if self.path_exists(self.working_dir):
            print(f"Removing the working directory \"{self.working_dir}\"")
            dbutils.fs.rm(self.working_dir, True)

        if spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{self.user_db}'").count() == 1:
            print(f"Dropping the database \"{self.user_db}\"")
            spark.sql(f"DROP DATABASE {self.user_db} CASCADE")

# "eilc" is short for "Example Instructor Led Course"
DBAcademy = DBAcademyHelper(module_name="eilc", 
                            per_lesson=False,
                            dataset_name="example-instructor-led-course",
                            dataset_version="v01",
                            dataset_min="5 seconds",
                            dataset_max="1 minute")

DA = DBAcademy # Shorter alias
DA.install_datasets()

