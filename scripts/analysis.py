from pyspark.sql import SparkSession, functions as f
import geopandas as gpd

# Read in transactions data
transactions = spark.read.parquet('../data/tables/transactions_20210228_20210827_snapshot')

transactions.createOrReplaceTempView("transactions")

# Aggregate statistics of transactions data by merchant
sdf_trans = spark.sql(
    """
    SELECT merchant_abn, 
        COUNT(*) AS n_transaction, 
        MAX(dollar_value) AS max_dollar, 
        MIN(dollar_value) AS min_dollar, 
        AVG(dollar_value) AS avg_dollar
    FROM transactions
    GROUP BY merchant_abn
    """
)

# Aggregate statistics of transactions data by consumers


# Geospatial visualisation by SA2