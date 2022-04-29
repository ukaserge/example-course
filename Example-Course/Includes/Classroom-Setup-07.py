# Databricks notebook source
# MAGIC %run ./_utility-functions

# COMMAND ----------

class DataFactory:
    def __init__(self):
        
        # Bind the stream-source to DA because we will use it again later.
        DA.paths.stream_source = f"{DA.paths.working_dir}/stream-source"
        
        self.source_dir = DA.hidden.datasets
        self.target_dir = DA.paths.stream_source
        
        # All three datasets *should* have the same count, but just in case,
        # We are going to take the smaller count of the three datasets
        orders_count = len(dbutils.fs.ls(f"{self.source_dir}/orders"))
        status_count = len(dbutils.fs.ls(f"{self.source_dir}/status"))
        customer_count = len(dbutils.fs.ls(f"{self.source_dir}/customers"))
        self.max_batch = min(min(orders_count, status_count), customer_count)
        
        self.current_batch = 0
        
    def load(self, continuous=False, delay_seconds=5):
        import time
        self.start = int(time.time())
        
        if self.current_batch >= self.max_batch:
            print("Data source exhausted\n")
            return False
        elif continuous:
            while self.load():
                time.sleep(delay_seconds)
            return False
        else:
            print(f"Loading batch {self.current_batch+1} of {self.max_batch}", end="...")
            self.copy_file("customers")
            self.copy_file("orders")
            self.copy_file("status")
            self.current_batch += 1
            print(f"{int(time.time())-self.start} seconds")
            return True
            
    def copy_file(self, dataset_name):
        file = f"{dataset_name}/{self.current_batch:02}.json"
        source_file = f"{self.source_dir}/{file}"
        target_file = f"{self.target_dir}/{file}"
        dbutils.fs.cp(source_file, target_file)

# COMMAND ----------

DA = DBAcademyHelper()   # Create the DA object with the specified lesson
DA.cleanup()             # Remove the existing database and files
DA.init(create_db=True)  # True is the default
DA.install_datasets()    # Install (if necissary) and validate the datasets
print()

# One of the common patters for streaming data is to use 
# a DataFactory that loads one-batch at a time, on demand
DA.streaming_data_factory = DataFactory()
DA.streaming_data_factory.load()  # We need at least one batch to get started.

DA.conclude_setup()               # Conclude the setup by printing the DA object's final state

