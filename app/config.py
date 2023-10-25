##  EXAMPLE CONFIG PAGE
from google.cloud import bigquery
import os

__version__ = '1.0.2'


# name of the project on GCP
project_id = 'gcp-project_id'
certificate_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'certificate_file.json')  # certification file for bigquery authentication

# job config for bigquery load job
'''
This is an example config for loading a table to bigquery.
Change the name of the timestamp column, and change TIMESTAMP to DATE according to the data.
Also change the partitoning to MONTH or YEAR according to size of the data.
This job config will create a bigquery table if it doesn't exist(dataset must exist already) and append data to the right partitions based on the timestamp column.
Its always better to use partitioned tables.
'''
job_config = bigquery.LoadJobConfig(schema = [bigquery.SchemaField('timestamp_column', 'TIMESTAMP'),],
                                        autodetect = True,
                                        time_partitioning=bigquery.TimePartitioning(
                                        type_=bigquery.TimePartitioningType.DAY,
                                        field='timestamp_column'
                                        ),)


'''
Define pcollections as a dictionary, this is just an example structure.
Each collection will be processed in parallel by dataflow.
Add more configurations as necessary.
'''
config_dict = {
     "config_1": {
        'read_table': 'gcp-project.bigquery-dataset.read-table-1',
        'write_table': 'gcp-project.bigquery-dataset.write-table-1',  
        'transform_parm': {'column_name': 'temperature'}
    },
   "config_2": {
        'read_table': 'gcp-project.bigquery-dataset.read-table-2',
        'write_table': 'gcp-project.bigquery-dataset.write-table-2',  
       'transform_parm': {'column_name': 'temperature'}
    },
    "config_3": {
        'read_table': 'gcp-project.bigquery-dataset.read-table-3',
        'write_table': 'gcp-project.bigquery-dataset.write-table-3',  
        'transform_parm': {'column_name': 'temperature'}
    },
}
