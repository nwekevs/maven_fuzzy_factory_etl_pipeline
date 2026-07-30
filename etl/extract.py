# etl/extract.py
import requests
import zipfile
import io
import duckdb
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()  # load variables from .env

def extract_and_stage():
    url = os.getenv("DATA_URL")
    db_path = os.getenv("DB_PATH", "etl_pipeline.duckdb")

    response = requests.get(url, stream=True)
    response.raise_for_status()

    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")

    for file_name in zip_file.namelist():
        if file_name.endswith(".csv"):
            table_name = file_name.replace(".csv", "").lower()
            with zip_file.open(file_name) as f:
                for chunk in pd.read_csv(f, chunksize=10000):
                    conn.register("chunk_df", chunk)
                    conn.execute(
                        f"CREATE TABLE IF NOT EXISTS staging.{table_name} AS SELECT * FROM chunk_df"
                    )
                    conn.execute(
                        f"INSERT INTO staging.{table_name} SELECT * FROM chunk_df"
                    )
            print(f"Loaded {file_name} into staging.{table_name}")

    conn.close()
