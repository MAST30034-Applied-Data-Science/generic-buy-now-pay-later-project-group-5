# Generic Buy Now, Pay Later Project
Groups should generate their own suitable `README.md`.

Note to groups: Make sure to read the `README.md` located in `./data/README.md` for details on how the weekly datasets will be released.

# Pipeline
1. `download.py`: Downloads external datasets
2. `ETL_basic.py`: Performs ETL on transaction and merchant data (Output: `joined_data.parquet`, `consumer_fraud.parquet`, `merchant_fraud.parquet`)
3. `external_connection.py`: Joins data with external datasets (Input: `joined_data.parquet`, Output: `external_data.parquet`, `external_joined_data.parquet`)
4. `null_imputation.py`: Performs imputation on null values (Input: `external_joined_data.parquet`,  Output: `final_data.parquet` )

5. analysis.ipynb: Performs analysis and outlier removal (Input: `final_data.parquet`,  `external_data.parquet`,  Output: `cleaned_data.parquet` )

6. `fraud_regression.ipynb`: Produces predictions for consumer and merchant fraud given the known fraud cases, highlighting suspicious fraud cases, (Input: `merchant_fraud.parquet`, `consumer_fraud.parquet`, Output: `merchant_fraud_rate.csv`, `consumer_fraud_rate.csv`)

7. `ranking_features.ipynb`: Produces feature table to be used for ranking merchants, (Input: `cleaned_data.parquet`, `merchant_fraud_rate.csv`, `consumer_fraud_rate.csv`, Output: `merchant_ranking_properties.csv`)
8. `merchant_ranking.ipynb`: Produces ranking model and uses it to rank merchants, (Input: `merchant_ranking_properties.csv`)
