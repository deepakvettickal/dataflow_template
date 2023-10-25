import pandas as pd

def transform_function(df, params):
    column_name = params.get('column_name', 'value')  # Use 'value' as default
    if column_name in df.columns:
        df[column_name] = (df[column_name] - 32) * 5.0/9.0
    return df
