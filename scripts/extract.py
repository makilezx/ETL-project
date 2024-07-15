"""
Extract step

Purpose:
- loads the data from the bunch of excel sheets
- performs necessary validation for columns of interest, handles their absence if needed
- concatenate the extracted dataframes on basis of separate sheets, making the monolitic dataframe as the result
- saves (staging) data as json file, ready for further transformations
"""

import pandas as pd
import os
import logging
import numpy as np
import json
import sys


# Set-up logging
def setup_logger():
    log_file_path = "/opt/expdir/data/etl_extraction_process.log"
    logger = logging.getLogger("data_extraction")
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

# Paths and required columns needed to extract data from excel files
paths = ["/opt/expdir/data/source.xlsx"]
required_columns = [
    "User ID",
    "Pol",
    "Earnings",
    "Job_Success",
    "Ratings",
    "Total_Hours",
    "Price_per_hour",
    "Main profession",
    "Title",
    "Country",
    "City",
    "Completed_Jobs",
    "country_code",
    "Region",
    "measure_code",
]

output_path = "/opt/expdir/data/staging_df.json"


def load_excel_sheets():
    # Loading sheets from excel files
    dfs = []
    try:
        for path in paths:
            xl = pd.ExcelFile(path)
            sheet_names = xl.sheet_names
            country_codes = [name[:2].lower() for name in sheet_names]
            filename = os.path.splitext(os.path.basename(path))[0]

            for name, code in zip(sheet_names, country_codes):
                df = pd.read_excel(path, sheet_name=name)
                df["country_code"] = code
                df["measure_code"] = filename
                dfs.append(df)
        logger.info(f"Success: loaded {len(dfs)} sheets from excel files")
    except Exception as e:
        logger.error(f"Error: loading excel sheets: {str(e)}")
        raise
    return dfs


def check_columns(df):
    # Checking if all required columns are present in given excel sheets/dataframes.
    # If not, we are adding them, maintaining the desired structure of output
    try:
        for col in required_columns:
            if col not in df.columns:
                df[col] = pd.NA
        logger.info(f"Success: all specified columns found in dataframe")
    except Exception as e:
        logger.error(f"Error: validating columns: {str(e)}")
        raise
    return df[required_columns]


def combine_dataframes(dfs):
    # Combining all extracted dataframes into one df
    try:
        combined_df = pd.concat(dfs, axis=0, ignore_index=True)
        logger.info(f"Success: dataframe combined")
    except Exception as e:
        logger.error(f"Error: combining dataframes extracted from excel: {str(e)}")
        raise
    return combined_df


def save_as_json(df):
    # Saving extracted data as json file
    try:
        df.to_json(output_path, orient="records", lines=True)
        logger.info(f"Success: dataframe saved as json")
    except Exception as e:
        logger.error(f"Error: saving dataframe as json file: {str(e)}")
        raise


def extract_data():
    # Function which handles all the operations mentioned above
    try:
        dfs = load_excel_sheets()
        validated_dfs = [check_columns(df) for df in dfs]
        combined_df = combine_dataframes(validated_dfs)
        save_as_json(combined_df)
        logger.info("Success: extraction completed")
        return combined_df
    except Exception as e:
        logger.error(f"Error: problem in extraction process: {str(e)}")
        raise


"""
# execute the extraction process
try:
    logger.info("Extraction process started")
    staging_df = extract_data()
    logger.info("Extraction process finished")
except Exception as e:
    logger.error(f"Extraction process failed: {str(e)}")
"""
