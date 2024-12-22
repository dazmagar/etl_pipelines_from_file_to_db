from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from etl.extract import extract


@pytest.mark.unit
class TestExtract:
    def test_extract_valid_file(self, sample_csv: Path) -> None:
        """
        Tests extracting data from a valid file.

        Args:
            sample_csv (Path): Path to the sample CSV file.

        Returns:
            None
        """
        df = extract(sample_csv)
        assert not df.empty
        assert list(df.columns) == ["Column1", "Column2"]

    def test_extract_missing_file(self) -> None:
        """
        Tests handling a missing file scenario.

        Returns:
            None
        """
        invalid_file = Path("nonexistent.csv")
        with pytest.raises(FileNotFoundError):
            extract(invalid_file)

    @patch("etl.extract.logger")
    def test_extract_logging(self, mock_logger: MagicMock, sample_csv: Path) -> None:
        """
        Tests that logging works as expected during data extraction.

        Args:
            mock_logger (MagicMock): Mocked logger.
            sample_csv (Path): Path to the sample CSV file.

        Returns:
            None
        """
        extract(sample_csv)
        mock_logger.info.assert_any_call("Extracting data from %s", sample_csv)

    @patch("etl.extract.pd.read_csv", side_effect=Exception("Test exception"))
    @patch("etl.extract.logger")
    def test_extract_general_error(self, mock_logger: MagicMock, mock_read_csv: MagicMock, sample_csv: Path) -> None:
        """
        Tests handling a general exception during data extraction.

        Args:
            mock_logger (MagicMock): Mocked logger.
            mock_read_csv (MagicMock): Mocked pandas.read_csv to raise an exception.
            sample_csv (Path): Path to the sample CSV file.

        Returns:
            None
        """
        with pytest.raises(Exception, match="Test exception"):
            extract(sample_csv)

        # Check the logger.error call
        mock_logger.error.assert_any_call(
            "An error occurred while extracting data from %s: %s",
            sample_csv,
            "Test exception",  # String representation of the exception
        )
