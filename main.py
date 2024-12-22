import sys
from pathlib import Path

from etl.extract import extract
from etl.load import load
from etl.transform import transform
from logging_config import logger


def main() -> None:
    """
    Main function to orchestrate the ETL pipeline.
    """
    try:
        logger.info("Starting ETL pipeline...")

        # File paths (relative to the project root)
        apps_file = Path("raw_data/apps_data.csv")
        reviews_file = Path("raw_data/review_data.csv")
        db_connection_string = "postgresql://dev_user:dev_password@localhost:5432/etl_db"

        # Extract data from source files
        logger.info("Starting data extraction...")
        apps_data = extract(apps_file)
        reviews_data = extract(reviews_file)

        # Transformation with filtering conditions
        logger.info("Starting filtered transformation of apps_data...")
        filtered_apps_data = transform(
            apps=apps_data,
            reviews=reviews_data,
            drop_duplicates=True,
            category="FOOD_AND_DRINK",
            min_rating=4.0,
            min_reviews=1000,
            aggregate_reviews=True,
            filter_reviews=True,
            columns_to_keep=["App", "Rating", "Reviews", "Installs"],
            sort_by=["Rating", "Reviews"],
        )

        # Load transformed data into the database
        logger.info("Starting data loading...")
        load(apps_data, "apps_data", db_connection_string)
        load(filtered_apps_data, "filtered_apps_data", db_connection_string)
        load(reviews_data, "reviews_data", db_connection_string)

        logger.info("ETL pipeline completed successfully!")

    except Exception as e:
        logger.error("ETL pipeline failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
