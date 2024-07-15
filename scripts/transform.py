"""
Transformation step

Purpose:
- gets the JSON file made as the result of extraction process
- conducts inital data validation
- conducts series of transformations
- conducts the validation before saving the transformed data as parquet filess
"""

import pandas as pd
import os
import logging
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import json
import sys


# Set-up logging
def setup_logger():
    log_file_path = "/opt/expdir/data/etl_transformation_process.log"
    logger = logging.getLogger("data_transformation")
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


def load_staging_data(file_name="/opt/expdir/data/staging_df.json"):
    # Loads the staging data from previous (extract) step, saved as json file.
    try:
        df = pd.read_json(file_name, lines=True)
        logger.info("Success: loaded staging data from json file")
        return df
    except Exception as e:
        logger.error(f"Error: loading json file: {e}")
        return None


def clean_column_names(df):
    # Column names handling, replaces spaces in column names with underscores and converts column names to lowercase
    try:
        df.columns = df.columns.str.replace(" ", "_").str.lower()
        logger.info("Success: cleaned column names")
        return df
    except Exception as e:
        logger.error(f"Error: column name cleaning: {e}")
        return df


def rename_columns(df):
    # Renames specific columns to more clean, descriptive and conventional names
    try:
        column_mapping = {
            "pol": "gender",
            "ratings": "rating",
            "title": "job_title",
            "job_success": "job_success_perc",
            "earnings": "earnings_in_thousands",
        }
        df = df.rename(columns=column_mapping)
        logger.info("Success: renamed columns")
        return df
    except Exception as e:
        logger.error(f"Error: renaming columns: {e}")
        return df


def dropna_user_id(df):
    # Drops rows where user_id is NaN.
    # This is first layer of NaN handling, since we want to include only those users with existing ID
    # This is important because later transformations and identification of subject relies on this -ID and -ID derived from this column (see: generate_pid)
    try:
        df = df.dropna(subset=["user_id"])
        logger.info("Success: dropped rows with NaN user_id")
        return df
    except Exception as e:
        logger.error(f"Error: dropping NaN user_id: {e}")
        return df


def generate_pid(df):
    # Generates a unique -ID (named PID) by concatenating user_id and measure_code
    # Pid serves as additional identifier matching users with specific measurements and timestamps 
    # This is necessary due to seasonal nature of measurements and allows easier tracking the same users through time/measurements
    try:
        df["pid"] = df["user_id"].astype(str) + "_" + df["measure_code"].astype(str)
        logger.info("Success: generated PID")
        return df
    except Exception as e:
        logger.error(f"Error: generating PID: {e}")
        return df


def remove_duplicate_users(df):
    # Removes duplicates on basis of (already generated) pid values, keeping the first occurrence
    # Also, by this operation we allow the same users from different measurements/timestamps to be present in dataset (since PID is related to timestamps)
    try:
        df = df.drop_duplicates(subset=["pid"], keep="first")
        logger.info("Success: removed duplicate cases")
        return df
    except Exception as e:
        logger.error(f"Error: removing duplicated cases: {e}")
        return df


def fill_na(df, fill_values=None):
    # Fills NaN values in specific columns with predefined values
    try:
        if fill_values is None:
            fill_values = {
                "gender": "UNKNOWN",
                "earnings_in_thousands": "0",
                "job_success_perc": "0",
                "rating": "UNKNOWN",
                "total_hours": "0",
                "price_per_hour": "0",
                "main_profession": "UNKNOWN",
                "job_title": "UNKNOWN",
                "country": "UNKNOWN",
                "city": "UNKNOWN",
                "completed_jobs": "0",
                "region": "UNKNOWN",
            }

        for column, value in fill_values.items():
            if column in df.columns:
                df[column] = df[column].fillna(value)
        logger.info("Success: filled NaN values with specified values.")
        return df
    except Exception as e:
        logger.error(f"Error: filling NaNs with specified values: {e}")
        return df


def remove_symbols(df):
    # Removes specific symbols inherently present in the raw/staging data (e.g. $, %...)
    try:
        if "earnings_in_thousands" in df.columns:
            df["earnings_in_thousands"] = df["earnings_in_thousands"].astype(str).str.replace("[$+k]", "", regex=True)
        if "price_per_hour" in df.columns:
            df["price_per_hour"] = df["price_per_hour"].astype(str).str.replace("[$]", "", regex=True)
        if "job_success_perc" in df.columns:
            df["job_success_perc"] = df["job_success_perc"].astype(str).str.replace("[%]", "", regex=True)
        logger.info("Success: removed unwanted symbols from data")
        return df
    except Exception as e:
        logger.error(f"Error: removing unwanted symbols from dataset: {e}")
        return df


def extract_numbers(df):
    # Extracts numeric values from specific columns (addition to remove_symbols)
    try:
        numeric_cols = ["completed_jobs", "total_hours", "price_per_hour", "job_success_perc", "earnings_in_thousands"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.extract(r"(\d+)", expand=False)
        logger.info("Success: extracted numbers.")
        return df
    except Exception as e:
        logger.error(f"Error: extracting numbers: {e}")
        return df


def convert_to_numeric(df):
    # Explicitly converts specific columns/values from specific columns to numeric types
    # Further scales earnings_in_thousands so those cells represents appropriate values
    try:
        numeric_cols = ["job_success_perc", "earnings_in_thousands", "price_per_hour", "completed_jobs", "total_hours"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if "earnings_in_thousands" in df.columns:
            df["earnings_in_thousands"] *= 1000
        logger.info("Success: converted specific columns to numeric values")
        return df
    except Exception as e:
        logger.error(f"Error: converting to numeric values: {e}")
        return df


def convert_gender(df):
    # Converts initial gender values, coded as numbers, thus replacing them to female/male
    # Important due to later validation. We want to ensure only 3 possible values are allowed
    try:
        if "gender" in df.columns:
            df["gender"] = df["gender"].astype(int)
            gender_mapping = {0: "FEMALE", 1: "MALE"}
            df["gender"] = df["gender"].replace(gender_mapping)
        logger.info("Success: converted gender values")
        return df
    except Exception as e:
        logger.error(f"Error: replacing gender values: {e}")
        return df


def convert_to_uppercase(df):
    # Converts values to uppercase
    try:
        string_cols = [
            "user_id",
            "gender",
            "rating",
            "main_profession",
            "job_title",
            "country",
            "city",
            "country_code",
            "region",
            "measure_code",
            "pid",
        ]
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.upper()
        logger.info("Success: converted values to uppercase")
        return df
    except Exception as e:
        logger.error(f"Error: converting values to uppercase: {e}")
        return df


def define_data_types(df):
    # Explicitly defines the data types for each column
    try:
        data_types = {
            "user_id": str,
            "gender": str,
            "earnings_in_thousands": float,
            "job_success_perc": float,
            "rating": str,
            "total_hours": int,
            "price_per_hour": float,
            "main_profession": str,
            "job_title": str,
            "country": str,
            "city": str,
            "completed_jobs": int,
            "country_code": str,
            "region": str,
            "measure_code": str,
            "pid": str,
        }
        df = df.astype(data_types)
        logger.info("Success: defined data types for columns in dataset")
        return df
    except Exception as e:
        logger.error(f"Error: defining data types for columns in dataset: {e}")
        return df


def save_parquet_files(df):
    # Data is being separated into several dataframes, which are further saved as parquet files
    try:
        # Creation of the different dataframes according to DB schema
        # Those dataframes are aligned with schema used later in postgres db. In fact, each df/parquet file represents one table
        user_df = df[["pid", "user_id", "gender", "measure_code", "rating"]]
        earnings_df = df[["pid", "user_id", "earnings_in_thousands", "price_per_hour"]]
        jobs_df = df[
            [
                "pid",
                "user_id",
                "total_hours",
                "job_success_perc",
                "main_profession",
                "job_title",
                "completed_jobs",
            ]
        ]
        geo_df = df[["pid", "user_id", "country", "city", "region", "country_code"]]

        # Saving dataframes as separate parquet files
        user_df.to_parquet("/opt/expdir/data/user.parquet", engine="pyarrow")
        earnings_df.to_parquet("/opt/expdir/data/earnings.parquet", engine="pyarrow")
        jobs_df.to_parquet("/opt/expdir/data/jobs.parquet", engine="pyarrow")
        geo_df.to_parquet("/opt/expdir/data/geo.parquet", engine="pyarrow")

        # Saving transformed_df as a parquet file
        # It will be used for validation purposes
        df.to_parquet("/opt/expdir/data/transformed.parquet", engine="pyarrow")
        logger.info("Success: transformed dataframes are saved as parquet files")
    except Exception as e:
        logger.error(f"Error: saving transformed dataframes as parquet files: {e}")


def transform_data(df):
    # Applying a series of already defined transformations in predefined order
    try:
        df = (
            df.pipe(clean_column_names)
            .pipe(rename_columns)
            .pipe(dropna_user_id)
            .pipe(generate_pid)
            .pipe(remove_duplicate_users)
            .pipe(fill_na)
            .pipe(remove_symbols)
            .pipe(extract_numbers)
            .pipe(convert_to_numeric)
            .pipe(convert_gender)
            .pipe(convert_to_uppercase)
            .pipe(define_data_types)
            .pipe(save_parquet_files)
        )
        logger.info("Success: data is transformed")
        return df
    except Exception as e:
        logger.error(f"Error: main data transformation function failed: {e}")
        return df


""" 
# Execute the transformation process
try:
    logger.info("Transformation process started")
    staging_df = load_staging_data()
    if staging_df is not None:
        transformed_df = transform_data(staging_df)
        logger.info("Transformation process finished")
    else:
        logger.error("Failed to load staging data")
except Exception as e:
    logger.error(f"Transformation process failed: {e}")
"""
