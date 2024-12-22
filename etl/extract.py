from pathlib import Path

import pandas as pd

from logging_config import logger


def extract(file_path: Path) -> pd.DataFrame:
    """
    Extract data from a CSV file and log key dataset information.

    Args:
        file_path (Path): Path to the CSV file.

    Returns:
        pd.DataFrame: Extracted data as a DataFrame.
    """
    try:
        # Read data from the specified file path
        data = pd.read_csv(file_path)

        # Log dataset details
        logger.info("Extracting data from %s", file_path)
        logger.info("Dataset contains %d rows and %d columns", data.shape[0], data.shape[1])
        logger.info("Column data types:\n%s", data.dtypes)

        return data

    except FileNotFoundError:
        # Log an error if the file is missing
        logger.error("File not found: %s", file_path)
        raise
    except Exception as e:
        # Log any other errors encountered during extraction
        logger.error("An error occurred while extracting data from %s: %s", file_path, str(e))
        raise
