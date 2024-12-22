from unittest.mock import patch

import pandas as pd
import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text

from etl.load import load


@pytest.mark.unit
class TestLoad:
    def test_load_table(self, sample_data: pd.DataFrame, db_connection: Engine) -> None:
        """
        Tests loading data into a PostgreSQL table.

        Args:
            sample_data (pd.DataFrame): Sample data for testing.
            db_connection (Engine): SQLAlchemy engine connected to a PostgreSQL database.

        Returns:
            None
        """
        table_name = "test_table"

        # Load data into the test database
        load(sample_data, table_name, db_connection.url)

        # Verify the data in the table
        with db_connection.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {table_name}")).fetchall()

            # Assertions
            assert len(result) == len(sample_data), "Row count mismatch"
            assert result[0][1] == "A", "Data mismatch in second column of the first row"

    def test_load_empty_dataframe(self, db_connection: Engine) -> None:
        """
        Tests loading an empty DataFrame into a PostgreSQL table.

        Args:
            db_connection (Engine): SQLAlchemy engine connected to a PostgreSQL database.

        Returns:
            None
        """
        table_name = "empty_table"
        empty_data = pd.DataFrame()

        # Load empty DataFrame
        load(empty_data, table_name, db_connection.url)

        # Verify the table exists but contains no data
        with db_connection.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            assert result == 0, "Empty table should have no rows"

    @patch("etl.load.create_engine", side_effect=SQLAlchemyError("Connection error"))
    def test_load_sqlalchemy_error(self, mock_engine: Engine, sample_data: pd.DataFrame) -> None:
        """
        Tests handling of SQLAlchemyError during data loading.

        Args:
            mock_engine: Mocked SQLAlchemy engine.
            sample_data (pd.DataFrame): Sample data for testing.

        Returns:
            None
        """
        with pytest.raises(SQLAlchemyError):
            load(sample_data, "error_table", "invalid_connection_string")

    def test_load_invalid_table_name(self, sample_data: pd.DataFrame, db_connection: Engine) -> None:
        """
        Tests loading data with an invalid table name.

        Args:
            sample_data (pd.DataFrame): Sample data for testing.
            db_connection (Engine): SQLAlchemy engine connected to a PostgreSQL database.

        Returns:
            None
        """
        invalid_table_name = "invalid-table-name!"  # Invalid table name

        with pytest.raises(ValueError):
            load(sample_data, invalid_table_name, db_connection.url)
