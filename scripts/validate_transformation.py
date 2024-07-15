import pandas as pd
import pyarrow.parquet as pq
import logging
import sys
import os


# Set-up logging
def setup_logger():
    log_file_path = "/opt/expdir/data/etl_transformation_validation_process.log"
    logger = logging.getLogger("data_transformation_validation")
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


def validate_transformed_data():
    parquet_path = "/opt/expdir/data/transformed.parquet"
    try:
        # Check if the parquet file exists
        assert os.path.exists(parquet_path), f"Validation failed: parquet file does not exist at: {parquet_path}"
        logger.info(f"Validation passed: parquet file exists at: {parquet_path}")

        # Load data from parquet file
        df = pd.read_parquet(parquet_path)

        # Assert the data is not empty
        assert not df.empty, "Validation failed: transformed data is empty"
        logger.info("Validation passed: transformed data is not empty")

        # Validate (expected) column names
        expected_columns = [
            "user_id",
            "gender",
            "earnings_in_thousands",
            "job_success_perc",
            "rating",
            "total_hours",
            "price_per_hour",
            "main_profession",
            "job_title",
            "country",
            "city",
            "completed_jobs",
            "country_code",
            "region",
            "measure_code",
            "pid",
        ]
        assert set(df.columns) == set(expected_columns), "Validation failed: expected columns are not present"
        logger.info("Validation passed: expected columns are present")

        # Validate (expected) data types
        expected_types = {
            "user_id": "object",
            "gender": "object",
            "earnings_in_thousands": "float64",
            "job_success_perc": "float64",
            "rating": "object",
            "total_hours": "int64",
            "price_per_hour": "float64",
            "main_profession": "object",
            "job_title": "object",
            "country": "object",
            "city": "object",
            "completed_jobs": "int64",
            "country_code": "object",
            "region": "object",
            "measure_code": "object",
            "pid": "object",
        }
        for col, expected_type in expected_types.items():
            assert (
                df[col].dtype == expected_type
            ), f"Validation failed: column '{col}' has type {df[col].dtype}, expected {expected_type}"
        logger.info("Validation passed: data types in specified columns are as expected")

        # Validate no null values
        assert df.isnull().sum().sum() == 0, "Validation failed: null values found in the dataset"
        logger.info("Validation passed: no null values found")

        # Validate gender values
        valid_gender_values = ["MALE", "FEMALE", "UNKNOWN"]
        assert df["gender"].isin(valid_gender_values).all(), "Validation failed: invalid gender values found"
        logger.info("Validation passed: gender values are valid")

        # Validate numeric columns (non-negative)
        numeric_columns = [
            "earnings_in_thousands",
            "job_success_perc",
            "price_per_hour",
            "completed_jobs",
            "total_hours",
        ]
        for col in numeric_columns:
            assert (df[col] >= 0).all(), f"Validation failed: negative values found in {col}"
        logger.info("Validation passed: numeric columns contain only non-negative values")

        # Validate rating values
        valid_ratings = ["TOP RATED", "UNKNOWN"]
        assert df["rating"].isin(valid_ratings).all(), "Validation failed: invalid rating values found"
        logger.info("Validation passed: rating values are valid")

        # Validate pid uniqueness
        assert df["pid"].is_unique, "Validation failed: duplicate pid values found"
        logger.info("Validation passed: PID values are unique")

        # Validate uppercase columns/values
        uppercase_columns = [
            "user_id",
            "gender",
            "rating",
            "job_title",
            "country",
            "city",
            "country_code",
            "region",
            "measure_code",
            "pid",
        ]
        for col in uppercase_columns:
            assert df[col].str.isupper().all(), f"Validation failed: non-uppercase values found in {col}"
        logger.info("Validation passed: specified columns contain only uppercase values")

        logger.info("All transformation validations passed successfully")
    except AssertionError as e:
        logger.error(f"Validation failed: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error during validation: {str(e)}")
        raise
