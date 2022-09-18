# Generic Buy Now, Pay Later Project
Groups should generate their own suitable `README.md`.

Note to groups: Make sure to read the `README.md` located in `./data/README.md` for details on how the weekly datasets will be released.

# Pipeline
1. `download.py`: Downloads external datasets
2. `ETL_basic.py`: Performs ETL on transaction and merchant data (Output: `joined_data.parquet`)
3. `external_connection.py`: Joins data with external datasets (Input: `joined_data.parquet`, Output: `external_data.parquet`, `external_joined_data.parquet`)
4. `null_imputation.py`: Performs imputation on null values (Input: `external_joined_data.parquet`,  Output: `final_data.parquet` )

5. analysis.ipynb: Performs analysis and outlier removal (Input: `final_data.parquet`,  `external_data.parquet`,  Output: `cleaned_data.parquet` )
