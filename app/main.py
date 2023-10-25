from app import config as cnf
from app import bigquery as bq
from app import times
from app import transform as tfm


def main(prod, config_dict=cnf.config_dict):
    '''
    Main function that does the following:
    1. Loops through pcollections specified in the configuration file.
    2. Performs authentication to intereact with Bigquery.
    3. Pulls data from bigquery table.
    4. Transforms and writes data to a Bigquery table.

    '''
    # get bigquery client
    print(f'Starting application, version: {cnf.__version__}')
    client = bq.client(cnf.certificate_file_path)

    date_today = times.utc_now().strftime("%Y-%m-%d")
    
    for config_name, config in config_dict.items():
        times.log(f'Running {config_name} for date: {date_today}')

        # read data from bigquery
        query =f"""
            SELECT *
            FROM 
            `{config['read_table']}` 
            WHERE
            DATE(timestamp) >= '{date_today}'
            """
        input_df = bq.get_df_from_bigquery(query)

        # transform the data
        output_df = tfm.transform_function(input_df, config['transform_param'])
        
        # write the data back to bigquery
        bq.write_df_to_bigquery(client, config['write_table'], cnf.job_config, output_df)
        times.log(f"added {len(output_df.index)} rows to {config['write_table']}")


if __name__ == "__main__":
    main(False)
