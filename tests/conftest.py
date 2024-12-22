import typing as t
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    """
    Creates a temporary CSV file for testing.

    Args:
        tmp_path (Path): Temporary directory for test files.

    Returns:
        Path: Path to the created CSV file.
    """
    file = tmp_path / "sample.csv"
    data = pd.DataFrame(
        {
            "Column1": [1, 2, 3],
            "Column2": ["A", "B", "C"],
        }
    )
    data.to_csv(file, index=False)
    return file


@pytest.fixture
def apps_data() -> pd.DataFrame:
    """
    Provides sample app data for testing.

    Returns:
        pd.DataFrame: DataFrame containing sample app data.
    """
    return pd.DataFrame(
        {
            "App": ["App1", "App2", "App3"],
            "Category": ["FOOD_AND_DRINK", "GAME", "FOOD_AND_DRINK"],
            "Rating": [4.5, 4.0, 3.5],
            "Reviews": [1500, 500, 200],
            "Installs": ["1,000+", "500+", "200+"],
        }
    )


@pytest.fixture
def reviews_data() -> pd.DataFrame:
    """
    Provides sample review data for testing.

    Returns:
        pd.DataFrame: DataFrame containing sample review data.
    """
    return pd.DataFrame(
        {
            "App": ["App1", "App2", "App3"],
            "Sentiment_Polarity": [0.8, 0.5, 0.3],
        }
    )


@pytest.fixture
def sample_data() -> pd.DataFrame:
    """
    Provides sample data for testing.

    Returns:
        pd.DataFrame: DataFrame containing sample data.
    """
    return pd.DataFrame(
        {
            "Column1": [1, 2, 3],
            "Column2": ["A", "B", "C"],
        }
    )


@pytest.fixture(scope="session")
def setup_test_db() -> t.Generator[None, None, None]:
    """
    Drops the test database before starting the tests.

    Returns:
        Generator[None, None, None]: Ensures the database setup is performed for the test session.
    """
    engine = create_engine("postgresql://dev_user:dev_password@localhost:5432/postgres")

    with engine.connect() as conn:
        conn.execute(text("COMMIT"))  # Commit is required for DROP DATABASE
        conn.execute(text("DROP DATABASE IF EXISTS test_db"))
        conn.execute(text("CREATE DATABASE test_db"))
    yield
    # Optional cleanup logic could go here


@pytest.fixture
def db_connection(setup_test_db: pytest.fixture) -> Engine:
    """
    Provides a connection to the test database.

    Args:
        setup_test_db (None): Ensures the test database is prepared before connecting.

    Returns:
        Engine: SQLAlchemy engine connected to the test database.
    """
    engine = create_engine("postgresql://dev_user:dev_password@localhost:5432/test_db")
    return engine
