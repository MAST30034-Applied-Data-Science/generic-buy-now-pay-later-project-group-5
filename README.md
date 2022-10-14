# Generic Buy Now, Pay Later Project
**MAST30034 Applied Data Science - Group 5**

Group members:
- John Melluish ()
- Rebekah Wu ()
- Seen Yan Lee (1183422)
- Sen Turner ()
- Yi Xiang Chee (1165917)



# Pipeline
1. `download.py`: Downloads external datasets
    - requires argument --output: the path from current directory to the desired output file (the external data folder)
    
2. `ETL_basic.py`: Performs ETL on transaction and merchant data (Output: `joined_data.parquet`, `consumer_fraud.parquet`, `merchant_fraud.parquet`)
    - requires argument --curatedpath: that path from current directory to the curated folder & --tablespath: the path from current directory to the tables folder

3. `external_connection.py`: Joins data with external datasets (Input: `joined_data.parquet`, Output: `external_data.parquet`, `external_joined_data.parquet`)
    - requires argument --curatedpath: that path from current directory to the curated folder & --externalpath: the path from current directory to the external folder

4. `null_imputation.py`: Performs imputation on null values (Input: `external_joined_data.parquet`,  Output: `final_data.parquet`)
    - requires argument --curatedpath: the path from current directory to the curated folder

5. `analysis.ipynb`: Performs outlier removal and analysis (Input: `final_data.parquet`,  `external_data.parquet`,  Output: `cleaned_data.parquet`)

6. `merchant_forecast.ipynb`: Produces forecasts on merchants' performance for the next 365 days (Input: `cleaned_data.parquet`, Output: `future_predictions.csv`)

7. `fraud_regression.ipynb`: Produces predictions for consumer and merchant fraud given the known fraud cases, highlighting suspicious fraud cases (Input: `merchant_fraud.parquet`, `consumer_fraud.parquet`, Output: `merchant_fraud_rate.csv`, `consumer_fraud_rate.csv`)

8. `ranking_features.ipynb`: Produces feature table to be used for ranking merchants, (Input: `cleaned_data.parquet`, `merchant_fraud_rate.csv`, `consumer_fraud_rate.csv`, `future_predictions.csv`, Output: `merchant_ranking_properties.csv`)

9. `one_hot.ipynb`: Performs clustering on merchants based on given tags to produce merchant segments (Input: `tbl_merchant.parquet`, Output: `segmented_merchants.csv`)

10. `merchant_ranking.ipynb`: Produces ranking model and uses it to rank merchants (Input: `merchant_ranking_properties.csv`) *Note: this notebook is also to be used for Checkpoint 6*
