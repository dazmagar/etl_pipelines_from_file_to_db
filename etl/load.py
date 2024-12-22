import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from logging_config import logger


def load(data: pd.DataFrame, table_name: str, db_connection_string: str) -> None:
    """
    Load a DataFrame into a specific table in the PostgreSQL database.

    Args:
        data (pd.DataFrame): DataFrame containing the data to load.
        table_name (str): Name of the table in the database.
        db_connection_string (str): Connection string for the PostgreSQL database.

    Returns:
        None
    """
    if not table_name.isidentifier():
        raise ValueError(f"Invalid table name: '{table_name}'")

    try:
        # Create database engine
        logger.info("Connecting to the database to load data into '%s'...", table_name)
        engine = create_engine(db_connection_string)

        # Load data into the specified table
        logger.info("Loading data into the '%s' table...", table_name)
        data.to_sql(table_name, engine, if_exists="replace", index=False)
        logger.info("Data successfully loaded into the '%s' table!", table_name)

    except SQLAlchemyError as e:
        logger.error("An error occurred while loading data into '%s': %s", table_name, e)
        raise
