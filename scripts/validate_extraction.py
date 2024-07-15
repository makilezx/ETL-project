import pandas as pd
import os
import logging
import sys


# Set-up logging
def setup_logger():
    log_file_path = "/opt/expdir/data/etl_extraction_validation_process.log"
    logger = logging.getLogger("data_extraction_validation")
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

# List of required columns for validation
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


def validate_extracted_data():
    # Function which handles validation of extracted data
    json_path = "/opt/expdir/data/staging_df.json"
    try:
        # Check if the JSON file exists at given path
        assert os.path.exists(json_path), f"Validation failed: JSON file does not exist at path: {json_path}"
        logger.info(f"Validation passed: JSON file exists at path: {json_path}")

        # Load data from JSON at given path
        data = pd.read_json(json_path, lines=True)

        # Assert the data is not empty
        assert not data.empty, "Validation failed: extracted data is empty"
        logger.info("Validation passed: extracted data is not empty")

        # Assert if column count matches the required columns
        assert len(data.columns) == len(required_columns), "Column count mismatch"
        logger.info(f"Validation passed: column count matches the required columns: {len(required_columns)}")

        # Assert if all required columns are present
        for col in required_columns:
            assert col in data.columns, f"Validation failed: missing column: {col}"
            logger.info(f"Validation passed: column '{col}' is present")

        # Check for duplicate column names
        assert data.columns.duplicated().sum() == 0, "Validation failed: duplicate column names present"
        logger.info("Validation passed: no duplicate column names found")

        logger.info("All extraction validations passed successfully")
    except AssertionError as e:
        logger.error(f"Validation failed: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error during validation: {str(e)}")
        raise
