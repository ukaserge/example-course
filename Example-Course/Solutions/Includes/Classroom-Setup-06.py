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

# Defined here because it is used only by this lesson.

def get_pipeline_config(self):
    """
    Returns the configuration to be used by the student in configuring the pipeline.
    """
    path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    path = "/".join(path.split("/")[:-1])
    
    path_a = path + f"/EC 06.B - Pipelines/EC 06.B.1 - Orders Pipeline"
    path_b = path + f"/EC 06.B - Pipelines/EC 06.B.2 - Customers Pipeline"
    path_c = path + f"/EC 06.B - Pipelines/EC 06.B.3 - Status Pipeline"
    
    da_name, da_hash = DA.get_username_hash()
    pipeline_name = f"DA-{da_name}-{da_hash}-{self.course_code.upper()}-Example Pipeline"
    
    return pipeline_name, path_a, path_b, path_c, self.paths.stream_source

DBAcademyHelper.monkey_patch(get_pipeline_config)

# COMMAND ----------

# Defined here because it is used only by this lesson.

def print_pipeline_config(self):
    """
    Renders the configuration of the pipeline as HTML
    """
    pipeline_name, path_a, path_b, path_c, source = self.get_pipeline_config()

    width = "600px"
    
    displayHTML(f"""<table style="width:100%">
    <tr>
        <td style="white-space:nowrap; width:1em">Pipeline Name:</td>
        <td><input type="text" value="{pipeline_name}" style="width: {width}"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Source:</td>
        <td><input type="text" value="{source}" style="width: {width}"></td></tr>

        <td style="white-space:nowrap; width:1em">Target:</td>
        <td><input type="text" value="{self.db_name}" style="width: {width}"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Storage Location:</td>
        <td><input type="text" value="{self.paths.storage_location}" style="width: {width}"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Notebook #1 Path:</td>
        <td><input type="text" value="{path_a}" style="width: {width}"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Notebook #2 Path:</td>
        <td><input type="text" value="{path_b}" style="width: {width}"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Notebook #3 Path:</td>
        <td><input type="text" value="{path_c}" style="width: {width}"></td></tr>
    </table>""")

DBAcademyHelper.monkey_patch(print_pipeline_config)

# COMMAND ----------

# Defined here because it is used only by this lesson.

def create_pipeline(self):
    """
    Creates the prescribed pipline.
    """
    
    from dbacademy.dbrest import DBAcademyRestClient
    client = DBAcademyRestClient()

    pipeline_name, path_a, path_b, path_c, source = self.get_pipeline_config()
    print(f"Creating the pipeline {pipeline_name}")

    # Delete the existing pipeline if it exists
    client.pipelines().delete_by_name(pipeline_name)

    # Create the new pipeline
    pipeline = client.pipelines().create(
        name = pipeline_name, 
        storage = self.paths.storage_location, 
        target = self.db_name, 
        notebooks = [path_a, path_b, path_c],
        configuration = {"source": source})

    self.pipeline_id = pipeline.get("pipeline_id")
       
DBAcademyHelper.monkey_patch(create_pipeline)

# COMMAND ----------

# Defined here because it is used only by this lesson.

def start_pipeline(self):
    "Starts the pipline and then blocks until it has completed, failed or was canceled"

    import time
    from dbacademy.dbrest import DBAcademyRestClient
    client = DBAcademyRestClient()

    # Start the pipeline
    start = client.pipelines().start_by_id(self.pipeline_id)
    update_id = start.get("update_id")

    # Get the status and block until it is done
    update = client.pipelines().get_update_by_id(self.pipeline_id, update_id)
    state = update.get("update").get("state")

    done = ["COMPLETED", "FAILED", "CANCELED"]
    while state not in done:
        duration = 15
        time.sleep(duration)
        print(f"Current state is {state}, sleeping {duration} seconds.")    
        update = client.pipelines().get_update_by_id(self.pipeline_id, update_id)
        state = update.get("update").get("state")
    
    print(f"The final state is {state}.")    
    assert state == "COMPLETED", f"Expected the state to be COMPLETED, found {state}"

DBAcademyHelper.monkey_patch(start_pipeline)

# COMMAND ----------

DA = DBAcademyHelper()   # Create the DA object with the specified lesson
DA.cleanup()             # Remove the existing database and files
DA.init(create_db=True)  # True is the default
DA.install_datasets()    # Install (if necissary) and validate the datasets
print()

# The location that the DLT databases should be written to
DA.paths.storage_location = f"{DA.paths.working_dir}/storage_location"

# One of the common patters for streaming data is to use 
# a DataFactory that loads one-batch at a time, on demand
DA.dlt_data_factory = DataFactory()
DA.dlt_data_factory.load()  # We need at least one batch to get started.

DA.conclude_setup()         # Conclude the setup by printing the DA object's final state

