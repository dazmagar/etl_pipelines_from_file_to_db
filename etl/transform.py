import typing as t

import pandas as pd

from logging_config import logger


def transform(apps: pd.DataFrame, reviews: pd.DataFrame = None, **kwargs: t.Any) -> pd.DataFrame:
    """
    Transform data to curate a dataset with apps and optional reviews.

    Args:
        apps (pd.DataFrame): DataFrame containing app information.
        reviews (pd.DataFrame, optional): DataFrame containing review information. Defaults to None.
        **kwargs (t.Any): Additional parameters for transformation logic.

    Returns:
        pd.DataFrame: Transformed DataFrame based on the provided parameters.
    """
    try:
        logger.info("Starting data transformation...")

        # Drop duplicates if enabled
        if kwargs.get("drop_duplicates", False):
            logger.info("Dropping duplicates...")
            apps = apps.drop_duplicates(subset=["App"]).copy()
            if reviews is not None:
                reviews = reviews.drop_duplicates().copy()

        # Convert 'Rating' and 'Reviews' columns to numeric
        if "Rating" in apps.columns:
            logger.info("Converting 'Rating' column to numeric...")
            apps["Rating"] = pd.to_numeric(apps["Rating"], errors="coerce")
        if "Reviews" in apps.columns:
            logger.info("Converting 'Reviews' column to numeric...")
            apps["Reviews"] = pd.to_numeric(apps["Reviews"], errors="coerce")

        # Filter by category if provided
        category = kwargs.get("category")
        if category:
            logger.info("Filtering apps by category: '%s'", category)
            apps = apps.loc[apps["Category"] == category, :]

        # Filter and aggregate reviews if provided
        if reviews is not None:
            logger.info("Processing reviews...")
            if "App" not in reviews.columns:
                logger.warning("The 'reviews' DataFrame does not contain an 'App' column. Skipping review processing.")
            else:
                filtered_reviews = reviews.loc[reviews["App"].isin(apps["App"]), ["App", "Sentiment_Polarity"]]
                if kwargs.get("aggregate_reviews", False):
                    logger.info("Aggregating reviews by app...")
                    aggregated_reviews = filtered_reviews.groupby("App").mean()
                    logger.info("Joining aggregated reviews with apps data...")
                    apps = apps.join(aggregated_reviews, on="App", how="left", rsuffix="_agg")
                else:
                    logger.info("Merging reviews with apps without aggregation...")
                    apps = apps.merge(filtered_reviews, on="App", how="left", suffixes=("", "_review"))

        # Keep only specified columns
        columns_to_keep = kwargs.get("columns_to_keep")
        if columns_to_keep:
            logger.info("Selecting relevant columns...")
            apps = apps.loc[:, columns_to_keep]

        # Apply filtering conditions (e.g., min_rating, min_reviews)
        min_rating = kwargs.get("min_rating")
        min_reviews = kwargs.get("min_reviews")
        if min_rating is not None or min_reviews is not None:
            logger.info("Filtering apps by min_rating=%s and min_reviews=%s...", min_rating, min_reviews)
            if min_rating is not None and min_reviews is not None:
                apps = apps.loc[(apps["Rating"] > min_rating) & (apps["Reviews"] > min_reviews), :]
            elif min_rating is not None:
                apps = apps.loc[apps["Rating"] > min_rating, :]
            elif min_reviews is not None:
                apps = apps.loc[apps["Reviews"] > min_reviews, :]

        # Sort and reset index
        sort_by = kwargs.get("sort_by")
        if sort_by:
            logger.info("Sorting apps by %s...", sort_by)
            apps.sort_values(by=sort_by, ascending=False, inplace=True)
            apps.reset_index(drop=True, inplace=True)

        logger.info("Transformation completed successfully. Result: %d rows, %d columns.", apps.shape[0], apps.shape[1])
        return apps

    except Exception as e:
        logger.error("An error occurred during data transformation: %s", e)
        raise
