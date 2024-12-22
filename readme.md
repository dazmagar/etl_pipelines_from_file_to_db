# ETL Data Pipeline Project

## Overview

This project is an example of an ETL (Extract, Transform, Load) data pipeline built using Python and PostgreSQL. The pipeline is designed to process app and review datasets, transform the data based on custom requirements, and load the processed data into a PostgreSQL database.

## Features

- **Extraction**: Reads data from CSV files using Pandas.
- **Transformation**: Cleans and filters data based on provided criteria.
- **Loading**: Inserts the transformed data into a PostgreSQL database using SQLAlchemy.
- **Testing**: Includes pytest-based tests for the ETL components.

## Project Structure
```r
|   .flake8                           # Configuration file for the flake8 linter to ensure code style consistency.
|   .gitignore
|   logging_config.py                 # Centralized logging configuration for the entire project.
|   main.py                           # Entry point of the project that orchestrates the ETL pipeline execution.
|   pyproject.toml                    # Python project configuration file, including dependencies and scripts.
|   readme.md
|       
+---database
|   |   .env                          # Environment variables for database configuration (e.g., user, password).
|   |   docker-sompose.yml            # Docker Compose configuration for running a PostgreSQL database.
|   |   
|   +---config
|           pg_hba.conf               # PostgreSQL access control configuration file.
|           postgresql.conf           # PostgreSQL general settings file.
|           
+---etl
|       extract.py                    # Module for extracting data from CSV files.
|       load.py                       # Module for loading data into a database.
|       transform.py                  # Module for transforming and cleaning data.
|       
+---raw_data
|       apps_data.csv                 # Source data file containing app details for analysis.
|       review_data.csv               # Source data file containing user reviews for analysis.
|       
+---scripts                           # Shell scripts for manage the database in docker container.
|       clean_db.sh
|       restart_db.sh
|       start_db.sh
|       stop_db.sh
|       
+---tests
        conftest.py                   # Configurations and fixtures for testing.
        test_extract.py               # Unit tests for the `extract` module.
        test_load.py                  # Unit tests for the `load` module.
        test_transform.py             # Unit tests for the `transform` module.
```

## Setup Instructions

1. Create a virtual environment:
```bash
   python -m venv venv
```
2. Activate the virtual environment:<br>
On Windows: `.\venv\Scripts\activate`<br>
On Linux/Mac: `source ./venv/bin/activate`
3. Upgrade pip and setuptools:
```bash
   python -m pip install --upgrade pip setuptools
```
4. Install dependencies:
```bash
   pip install -e .
   pip install -e .[flake8]
```
## Running Tests

Run the following command to execute tests and generate a coverage report:
```bash
pytest --tb=short --cov=etl
```
What the command does:<br>
`--tb=short` - Provides shorter traceback output for easier debugging.<br>
`--cov=etl` - Generates a test coverage report for the etl module.

### Current coverage
```r
Name               Stmts   Miss  Cover
--------------------------------------
etl\extract.py        16      0   100%
etl\load.py           16      0   100%
etl\transform.py      57      0   100%
--------------------------------------
TOTAL                 89      0   100%
```