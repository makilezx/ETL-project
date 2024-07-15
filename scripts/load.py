"""
Load step

Purpose:
- gets the data from parquet files made during transformation step
- loads the data in Postgres DB according to predefined schema
"""

import pandas as pd
from sqlalchemy import create_engine
import logging
from sqlalchemy.exc import SQLAlchemyError
import os
import sys
import psycopg2


# Set-up logging
def setup_logger():
    log_file_path = "/opt/expdir/data/etl_load_process.log"
    logger = logging.getLogger("data_load")
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_file_path)
    console_handler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


logger = setup_logger()

# DB connection string
CONNECTION_STRING = "postgresql://myuser:mypassword@postgres_warehouse:5432/user_db"


def read_parquet(file_path):
    # Read a parquet file and return a df
    try:
        df = pd.read_parquet(file_path, engine="pyarrow")
        logger.info(f"Success: read files from {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error: reading files from {file_path}: {str(e)}")
        raise


def create_db_engine():
    # Create a DB engine
    try:
        engine = create_engine(CONNECTION_STRING)
        logger.info("Success: database engine created")
        return engine
    except Exception as e:
        logger.error(f"Error: creating database engine: {str(e)}")
        raise


def load_to_database(df, table_name, schema, engine):
    # Load a df to the Postgres SB
    try:
        df.to_sql(name=table_name, schema=schema, con=engine, if_exists="append", index=False)
        logger.info(f"Successfully loaded data to {schema}.{table_name}")
    except SQLAlchemyError as e:
        logger.error(f"Error loading data to {schema}.{table_name}: {str(e)}")
        raise


def load_data():
    # Function which handles data loading
    try:
        # Extract from parquet files
        user_df = read_parquet("/opt/expdir/data/user.parquet")
        earnings_df = read_parquet("/opt/expdir/data/earnings.parquet")
        jobs_df = read_parquet("/opt/expdir/data/jobs.parquet")
        geo_df = read_parquet("/opt/expdir/data/geo.parquet")

        # DB engine
        engine = create_db_engine()

        # Load
        with engine.begin() as connection:
            load_to_database(user_df, "user", "user_schema", connection)
            load_to_database(earnings_df, "earnings", "user_schema", connection)
            load_to_database(jobs_df, "jobs", "user_schema", connection)
            load_to_database(geo_df, "geo", "user_schema", connection)

        return True

    except Exception as e:
        logger.error(f"Error: data loading failed: {str(e)}")
        return False


"""
# Execute the loading process
try:
    logger.info("Load process started")
    success = load_data()
    if success:
        logger.info("Load process finished successfully")
    else:
        logger.error("Failed to load data")
except Exception as e:
    logger.error(f"Loading process failed: {e}")
"""
