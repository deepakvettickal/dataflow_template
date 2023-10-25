from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from app import times as times
        

def client(cert_file_path=None):
    # If you are using a certificate file to authenticaticate bigquery, pass the path to the certificate file to argument cert_file_path.
    # Else, use application default credentials to authenticate bigquery. 
    if cert_file_path:
        credentials = service_account.Credentials.from_service_account_file(
            cert_file_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],)

        client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
    else:
        client = bigquery.Client()

    return client
    

def get_df_from_bigquery(query):
    # Function that will run the query at bigquery and return the data as a dataframe.
    return client().query(query).to_dataframe()


def write_df_to_bigquery(client, table_id: str, job_config,  df: pd.DataFrame):
    '''
    Function that appends rows in the dataframe df to the bigquery table with table_id as the identifier.
    
    Args:
        client: bigquery client to intereact with available datasets.
        table_id: identifier of the bigquery table to write to.
        job_config: schema and other configurations for the bigquery write job.
        df: dataframe to be appended to the bigquery table.
    '''
    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)  # Make an API request.
    load_job.result()  # Wait for the job to complete.