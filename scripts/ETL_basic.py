from functools import reduce 
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = (
    SparkSession.builder.appName("ETL")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

# Using OUTER JOIN for now, may want to change this eventually

JOIN_TYPE = "outer"

transactions = spark.read.parquet('../data/tables/transactions_20210228_20210827_snapshot')
cons_user_details = spark.read.parquet('../data/tables/consumer_user_details.parquet')
tbl_merchants = spark.read.parquet('../data/tables/tbl_merchants.parquet')
tbl_consumers = spark.read.options(delimiter='|').csv('../data/tables/tbl_consumer.csv', header = True)

# Convert tags to lowercase, remove unwanted characters, and split text into array


tbl_merchants = tbl_merchants.withColumn('tags', f.lower(f.col('tags')))
tbl_merchants = tbl_merchants.withColumn('tags', f.regexp_replace('tags', r'[\(\)\[\]]', ''))
tbl_merchants = tbl_merchants.withColumn('tags', f.split(f.col('tags'), ','))

tbl_merchants = tbl_merchants.withColumn('take rate', f.element_at(f.col('tags'), -1))
tbl_merchants = tbl_merchants.withColumn('revenue level', f.element_at(f.col('tags'), -2))
tbl_merchants = tbl_merchants.withColumn('tags', f.slice(tbl_merchants.tags,1,f.size('tags')-2))

tbl_merchants = tbl_merchants.withColumn('take rate', f.regexp_replace('take rate', r'[a-zA-Z: ]', ''))
tbl_merchants = tbl_merchants.withColumn('revenue level', f.regexp_replace('revenue level', r'[ ]', ''))

joined_data = transactions.join(tbl_merchants,['merchant_abn'],JOIN_TYPE)\
            .join(cons_user_details, ['user_id'],JOIN_TYPE)\
            .join(tbl_consumers.withColumnRenamed('name', 'consumer_name'), ['consumer_id'],JOIN_TYPE)

joined_data.show(1, vertical = True, truncate = False)
    
joined_data.write.mode('overwrite').parquet('../data/curated/joined_data.parquet')