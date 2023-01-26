import os
import pandas as pd
from sqlalchemy import create_engine
import argparse

csv_file = "output.csv"

def ingest_trips(table, engine):
    os.system(f"wget {file_url} -O {csv_file}.gz")
    os.system(f"gzip -d {csv_file}.gz")

    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100_000)
    while df_iter:
        try:
            print("Ingesting chunk of trips")
            df = next(df_iter)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.to_sql(table, engine, if_exists='append', index=False)
        except StopIteration:
            break

def ingest_zones(table, engine):
    os.system(f"wget {file_url} -O {csv_file}")

    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100_000)
    while df_iter:
        try:
            print("Ingesting chunk of zones")
            df = next(df_iter)
            df.to_sql(table, engine, if_exists='append', index=False)
        except StopIteration:
            break

parser = argparse.ArgumentParser(description='Ingest data into database')
parser.add_argument('--user', type=str, help='Database user')
parser.add_argument('--password', type=str, help='Database password')
parser.add_argument('--host', type=str, help='Database host')
parser.add_argument('--port', type=str, help='Database port')
parser.add_argument('--database', type=str, help='Database name')
parser.add_argument('--table', type=str, help='Table name')
parser.add_argument('--file_url', type=str, help='File url to ingest')

if __name__ == "__main__":
    params = parser.parse_args()
    user = params.user
    pwd = params.password
    host = params.host
    port = params.port
    db = params.database
    table = params.table
    file_url = params.file_url

    print("Received parameters: ")
    print("user: ", user)
    print("pwd: ", pwd)
    print("host: ", host)
    print("port: ", port)
    print("db: ", db)
    print("table: ", table)
    print("file_url: ", file_url)

    engine = create_engine(
        'postgresql://{user}:{pwd}@{host}:{port}/{db}'.format(user=user, pwd=pwd, host=host, port=port, db=db))

    if table == "taxi_trips":
        ingest_trips(table, engine)
    elif table == "zones":
        ingest_zones(table, engine)
