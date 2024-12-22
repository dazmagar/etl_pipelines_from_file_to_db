import pandas as pd
import pytest

from etl.transform import transform


@pytest.mark.unit
class TestTransform:
    # Base transformation tests
    def test_transform_basic(self, apps_data: pd.DataFrame) -> None:
        """
        Tests basic transformation without additional filters.
        """
        transformed = transform(apps=apps_data)
        assert "App" in transformed.columns
        assert not transformed.empty

    def test_transform_empty_dataframe(self) -> None:
        """
        Tests transformation with an empty DataFrame.
        """
        empty_apps = pd.DataFrame()
        transformed = transform(apps=empty_apps)
        assert transformed.empty

    def test_transform_missing_columns(self, apps_data: pd.DataFrame) -> None:
        """
        Tests transformation with missing columns in the input DataFrame.
        """
        incomplete_apps = apps_data.drop(columns=["Rating", "Reviews"])
        transformed = transform(apps=incomplete_apps)
        assert "App" in transformed.columns
        assert "Rating" not in transformed.columns
        assert "Reviews" not in transformed.columns

    # Transformation with reviews
    def test_transform_filter_reviews(self, apps_data: pd.DataFrame, reviews_data: pd.DataFrame) -> None:
        """
        Tests filtering reviews by matching apps.
        """
        # Test without aggregation
        transformed = transform(
            apps=apps_data,
            reviews=reviews_data,
            filter_reviews=True,
            aggregate_reviews=False,
        )
        assert "Sentiment_Polarity" in transformed.columns
        assert not transformed["Sentiment_Polarity"].isna().all()

        # Test with aggregation
        transformed_agg = transform(
            apps=apps_data,
            reviews=reviews_data,
            filter_reviews=True,
            aggregate_reviews=True,
        )
        assert "Sentiment_Polarity" in transformed_agg.columns
        assert not transformed_agg["Sentiment_Polarity"].isna().all()

    def test_transform_aggregate_reviews(self, apps_data: pd.DataFrame, reviews_data: pd.DataFrame) -> None:
        """
        Tests aggregating reviews and joining them with apps.
        """
        transformed = transform(apps=apps_data, reviews=reviews_data, aggregate_reviews=True)
        assert "Sentiment_Polarity" in transformed.columns
        assert transformed["Sentiment_Polarity"].isna().sum() == 0

    def test_transform_empty_reviews(self, apps_data: pd.DataFrame) -> None:
        """
        Tests transformation with an empty reviews DataFrame.
        """
        empty_reviews = pd.DataFrame()
        transformed = transform(apps=apps_data, reviews=empty_reviews, aggregate_reviews=True)
        assert "Sentiment_Polarity_agg" not in transformed.columns

    def test_transform_reviews_missing_app_column(self, apps_data: pd.DataFrame, reviews_data: pd.DataFrame) -> None:
        """
        Tests transformation when reviews DataFrame is missing the 'App' column.
        """
        reviews_data = reviews_data.drop(columns=["App"])
        transformed = transform(apps=apps_data, reviews=reviews_data, aggregate_reviews=True)
        assert "Sentiment_Polarity_agg" not in transformed.columns

    # Filtering and sorting tests
    def test_transform_with_conditions(self, apps_data: pd.DataFrame, reviews_data: pd.DataFrame) -> None:
        """
        Tests transformation with specific filters and conditions.
        """
        transformed = transform(
            apps=apps_data,
            reviews=reviews_data,
            category="FOOD_AND_DRINK",
            min_rating=4.0,
            min_reviews=1000,
        )
        assert len(transformed) == 1
        assert transformed.iloc[0]["App"] == "App1"

    def test_transform_min_rating_only(self, apps_data: pd.DataFrame) -> None:
        """
        Tests filtering by min_rating only.
        """
        transformed = transform(apps=apps_data, min_rating=4.0)
        assert all(transformed["Rating"] > 4.0)

    def test_transform_min_reviews_only(self, apps_data: pd.DataFrame) -> None:
        """
        Tests filtering by min_reviews only.
        """
        transformed = transform(apps=apps_data, min_reviews=1000)
        assert all(transformed["Reviews"] > 1000)

    def test_transform_no_sort(self, apps_data: pd.DataFrame) -> None:
        """
        Tests transformation without specifying sort_by.
        """
        transformed = transform(apps=apps_data)
        assert transformed.equals(apps_data)  # Order should remain the same

    def test_transform_sorting(self, apps_data: pd.DataFrame) -> None:
        """
        Tests transformation with sorting logic.
        """
        transformed = transform(apps=apps_data, sort_by=["Rating", "Reviews"])
        assert transformed.iloc[0]["Rating"] >= transformed.iloc[-1]["Rating"]

    # Columns selection tests
    def test_transform_columns_to_keep(self, apps_data: pd.DataFrame) -> None:
        """
        Tests selecting specific columns to keep in the output DataFrame.
        """
        columns_to_keep = ["App", "Rating"]
        transformed = transform(apps=apps_data, columns_to_keep=columns_to_keep)
        assert list(transformed.columns) == columns_to_keep

    def test_transform_invalid_columns(self, apps_data: pd.DataFrame) -> None:
        """
        Tests transformation with invalid columns to keep.
        """
        with pytest.raises(KeyError):
            transform(apps=apps_data, columns_to_keep=["NonexistentColumn"])

    # Duplicates handling tests
    def test_transform_drop_duplicates(self, apps_data: pd.DataFrame, reviews_data: pd.DataFrame) -> None:
        """
        Tests dropping duplicates in both apps and reviews DataFrames.
        """
        duplicated_apps = pd.concat([apps_data, apps_data])
        duplicated_reviews = pd.concat([reviews_data, reviews_data])
        transformed = transform(apps=duplicated_apps, reviews=duplicated_reviews, drop_duplicates=True)
        assert transformed.shape[0] == apps_data.shape[0]

    # Error handling tests
    def test_transform_invalid_column(self, apps_data: pd.DataFrame) -> None:
        """
        Tests transformation with an invalid column specified in kwargs.
        """
        with pytest.raises(KeyError):
            transform(apps=apps_data, columns_to_keep=["NonexistentColumn"])

    def test_transform_no_sort_specified(self, apps_data: pd.DataFrame) -> None:
        """
        Tests transformation without specifying sort_by.
        """
        transformed = transform(apps=apps_data)
        assert transformed.equals(apps_data)
