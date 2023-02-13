import os
import argparse
import parquet
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True, tags=["extract"],cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata_2021-01.csv.gz'
        # csv_name = 'output1.csv'
    else:
        csv_name = 'output.csv'
    
    os.system(f"wget {url} -O {csv_name}")
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=10000)
    df = next(df_iter)
    return df

@task(log_prints=True, retries=3)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df["passenger_count"] != 0]
    print(f"past: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df


@task(log_prints=True, retries=3)
def load_data(table_name, df):
    #engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    connection_block = SqlAlchemyConnector.load("postgres-connector") 
    with connection_block.get_connection(begin=False) as engine:
       df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
       df.to_sql(name=table_name, con=engine, if_exists='append')

@flow(name="subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging suflow for: {table_name}")


@flow(name="ingesting Taxi Data")
def main_flow(table_name: str = "yellow_taxi_trips"):
    # user = "postgres"
    # password = "trainings12"
    # host = "localhost"
    # port = "5432"
    # db = "ny_taxi"
    #table_name = "yellow_taxi_trips"
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    log_subflow(table_name)
    raw_data = extract_data(url)
    data = transform_data(raw_data)
    load_data(table_name, data)


if __name__ == '__main__':
    main_flow(table_name="yellow_taxi_trips")
    