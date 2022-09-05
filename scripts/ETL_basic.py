from functools import reduce 
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("ETL")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

# Using INNER JOIN for now, may want to change this eventually

JOIN_TYPE = "inner"

transactions = spark.read.parquet('../data/tables/transactions_20210228_20210827_snapshot')
cons_user_details = spark.read.parquet('../data/tables/consumer_user_details.parquet')
tbl_merchants = spark.read.parquet('../data/tables/tbl_merchants.parquet')
tbl_consumers = spark.read.options(delimiter='|').csv('../data/tables/tbl_consumer.csv', header = True)

joined_data = transactions.join(tbl_merchants,['merchant_abn'],JOIN_TYPE)\
            .join(cons_user_details, ['user_id'],JOIN_TYPE)\
            .join(tbl_consumers.withColumnRenamed('name', 'consumer_name'), ['consumer_id'],JOIN_TYPE)
    
joined_data.write.mode('overwrite').parquet('../data/curated/joined_data.parquet')