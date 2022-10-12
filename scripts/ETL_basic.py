from functools import reduce 
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import argparse

spark = (
    SparkSession.builder.appName("ETL")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

# Using LEFT JOIN so that only data with orders is used

JOIN_TYPE = "left"

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--curatedpath', help = 'the path to the curated data folder ex. data/curated/',type = str)
    parser.add_argument('--tablespath', help = 'the path to the tables data folder ex. data/tables/')
    args = parser.parse_args()

    transactions = spark.read.parquet(args.tablespath + 'transactions_20210228_20210827_snapshot')
    transactions2 = spark.read.parquet(args.tablespath + 'transactions_20210828_20220227_snapshot')
    transactions3 = spark.read.parquet(args.tablespath + 'transactions_20220228_20220828_snapshot')
    cons_user_details = spark.read.parquet(args.tablespath + 'consumer_user_details.parquet')
    tbl_merchants = spark.read.parquet(args.tablespath + 'tbl_merchants.parquet')
    tbl_consumers = spark.read.options(delimiter='|').csv(args.tablespath + 'tbl_consumer.csv', header = True)

    # Convert tags to lowercase, remove unwanted characters, and split text into array

    tbl_merchants = tbl_merchants.withColumn('tags', f.lower(f.col('tags')))
    tbl_merchants = tbl_merchants.withColumn('tags', f.regexp_replace('tags', r'[\(\)\[\]]', ''))
    tbl_merchants = tbl_merchants.withColumn('tags', f.split(f.col('tags'), ','))

    tbl_merchants = tbl_merchants.withColumn('take rate', f.element_at(f.col('tags'), -1))
    tbl_merchants = tbl_merchants.withColumn('revenue level', f.element_at(f.col('tags'), -2))
    tbl_merchants = tbl_merchants.withColumn('tags', f.slice(tbl_merchants.tags,1,f.size('tags')-2))

    tbl_merchants = tbl_merchants.withColumn('take rate', f.regexp_replace('take rate', r'[a-zA-Z: ]', ''))
    tbl_merchants = tbl_merchants.withColumn('revenue level', f.regexp_replace('revenue level', r'[ ]', ''))


    # Group all transactions and join

    all_transactions = transactions.union(transactions2).union(transactions3)

    joined_data = all_transactions.join(tbl_merchants,['merchant_abn'],JOIN_TYPE)\
                .join(cons_user_details, ['user_id'],JOIN_TYPE)\
                .join(tbl_consumers.withColumnRenamed('name', 'consumer_name'), ['consumer_id'],JOIN_TYPE)

    """
    Add fraud detection
    rename fraud_probability columns from fraud merchant and fraud consumer into merchang_fraud_prob and cosumer_fraud_prob
    """
    fraud_merchant_probs = spark.read.csv(args.tablespath + 'merchant_fraud_probability.csv', header = True)
    fraud_merchant_probs = fraud_merchant_probs.withColumnRenamed('fraud_probability', 'merchant_fraud_prob')
    fraud_consumer_probs = spark.read.csv(args.tablespath + 'consumer_fraud_probability.csv', header = True)
    fraud_consumer_probs = fraud_consumer_probs.withColumnRenamed('fraud_probability', 'consumer_fraud_prob')


    #create new feature transaction_count for both the fraud consumer and fraud merchant
    fraud_consumer = joined_data.groupBy(['user_id', 'order_datetime']).agg(f.count("*").alias("transaction_count"), f.avg("dollar_value").alias("avg_transaction_amt")).withColumn("is_fraud", f.lit(False))
    fraud_merchant = joined_data.groupBy(['merchant_abn', 'order_datetime']).agg(f.count("*").alias("transaction_count"), f.avg("dollar_value").alias("avg_transaction_amt")).withColumn("is_fraud", f.lit(False))

    # left join the fraud probability for merchant data and consumer data with selected columns
    fraud_merchant = fraud_merchant.join(fraud_merchant_probs,['merchant_abn', 'order_datetime'], JOIN_TYPE)
    fraud_consumer = fraud_consumer.join(fraud_consumer_probs,['user_id', 'order_datetime'], JOIN_TYPE)

    # Combine so only the one probability

    fraud_merchant = fraud_merchant.withColumn("fraud_probability",\
                                            f.when(f.col('merchant_fraud_prob').isNotNull(), f.col("merchant_fraud_prob")))\
                                            .drop("merchant_fraud_prob")
    fraud_consumer = fraud_consumer.withColumn("fraud_probability",\
                                            f.when(f.col('consumer_fraud_prob').isNotNull(), f.col("consumer_fraud_prob")))\
                                            .drop("consumer_fraud_prob")

    # Add new field denoting fraud

    fraud_merchant = fraud_merchant.withColumn("is_fraud",\
                                                f.when(f.col("fraud_probability").isNotNull(), True)\
                                                .when(f.col("fraud_probability").isNull(), False))

    fraud_consumer = fraud_consumer.withColumn("is_fraud",\
                                                f.when(f.col("fraud_probability").isNotNull(), True)\
                                                .when(f.col("fraud_probability").isNull(), False))

    # Impute non-fraud values as having 0.01 probaility of fraud

    fraud_merchant = fraud_merchant.withColumn("fraud_probability",\
                                            f.when(~ f.col('is_fraud'), 0.01)\
                                                .when(f.col('is_fraud'), f.col('fraud_probability')))
    fraud_consumer = fraud_consumer.withColumn("fraud_probability",\
                                            f.when(~ f.col('is_fraud'), 0.01)\
                                                .when(f.col('is_fraud'), f.col('fraud_probability')))

    joined_data.show(1, vertical = True, truncate = False)
    fraud_merchant.show(1, vertical = True, truncate = False)
    fraud_consumer.show(1, vertical = True, truncate = False)

    # Write the relevant files

    fraud_merchant.write.mode('overwrite').parquet(args.curatedpath + 'merchant_fraud.parquet')

    fraud_consumer.write.mode('overwrite').parquet(args.curatedpath + 'consumer_fraud.parquet')
        
    joined_data.write.mode('overwrite').parquet(args.curatedpath + 'joined_data.parquet')

if __name__ == "__main__":
    main()