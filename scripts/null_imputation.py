from functools import reduce 
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from zipfile import ZipFile
from pyspark import SparkFiles
import pyspark.sql.functions as F
from pyspark.sql.functions import col

spark = (
    SparkSession.builder.appName("Null_Imputation")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.executor.memory", "6g")
    .config("spark.driver.memory", "10g")
    .getOrCreate()
)

# download data

data = spark.read.parquet('../data/curated/external_joined_data.parquet')

# Account for postcodes without matching SA2 (using average of state)

data = data.withColumn(
    'state',
    F.when(((col('postcode') >= 1000) & (col('postcode') <= 1999)) | ((col('postcode') >= 2000) & (col('postcode') <= 2599)) | ((col('postcode') >= 2619) & (col('postcode') < 2899)) | ((col('postcode') >= 2921) & (col('postcode') < 2999)), 'NSW')\
    .when(((col('postcode') >= 200) & (col('postcode') <= 299)) | ((col('postcode') >= 2600) & (col('postcode') <= 2618)) | ((col('postcode') >= 2900) & (col('postcode') < 2920)), 'ACT')\
    .when(((col('postcode') >= 3000) & (col('postcode') <= 3999)) | ((col('postcode') >= 8000) & (col('postcode') <= 8999)), 'VIC')\
    .when(((col('postcode') >= 4000) & (col('postcode') <= 4999)) | ((col('postcode') >= 9000) & (col('postcode') <= 9999)), 'QLD')\
    .when(((col('postcode') >= 5000) & (col('postcode') <= 5999)), 'SA')\
    .when(((col('postcode') >= 6000) & (col('postcode') <= 6999)), 'WA')\
    .when(((col('postcode') >= 7000) & (col('postcode') <= 7999)), 'TAS')\
    .otherwise('NT')
)
data = data.withColumn(
    'SA2_NAME_2021',
    F.when( (col('state') == 'VIC') & (col('SA2_NAME_2021').isNull()), 'Victoria')\
    .when( (col('state') == 'NSW') & (col('SA2_NAME_2021').isNull()), 'New South Wales')\
    .when( (col('state') == 'QLD') & (col('SA2_NAME_2021').isNull()), 'Queensland')\
    .when( (col('state') == 'NT') & (col('SA2_NAME_2021').isNull()), 'Northern Territory')\
    .when( (col('state') == 'WA') & (col('SA2_NAME_2021').isNull()), 'Western Austraia')\
    .when( (col('state') == 'SA') & (col('SA2_NAME_2021').isNull()), 'South Australia')\
    .when( (col('state') == 'TAS') & (col('SA2_NAME_2021').isNull()), 'Tasmania')\
    .when( (col('state') == 'ACT') & (col('SA2_NAME_2021').isNull()), 'Australian Capital Territory')\
    .otherwise(col('SA2_NAME_2021'))
)

# Mean imputation by state for null postcodes

columns = ["Median_tot_prsnl_inc_weekly", "Median_rent_weekly", "Median_mortgage_repay_monthly", "Median_age_persons", "Median_tot_hhd_inc_weekly",\
           "Average_household_size", "Year_12_Highest_Level_of_School", "Did_Not_Attend_School", "TOT_P_P"]

for column in columns:
    data = data.withColumn(
        column,
        F.when( (col('SA2_NAME_2021') == 'Northern Territory'), data.groupBy('state').mean(column).collect()[0][1])\
        .when( (col('SA2_NAME_2021') == 'Australian Capital Territory'), data.groupBy('state').mean(column).collect()[1][1])\
        .when( (col('SA2_NAME_2021') == 'South Australia'), data.groupBy('state').mean(column).collect()[2][1])\
        .when( (col('SA2_NAME_2021') == 'Tasmania'), data.groupBy('state').mean(column).collect()[3][1])\
        .when( (col('SA2_NAME_2021') == 'Western Austraia'), data.groupBy('state').mean(column).collect()[4][1])\
        .when( (col('SA2_NAME_2021') == 'Queensland'), data.groupBy('state').mean(column).collect()[5][1])\
        .when( (col('SA2_NAME_2021') == 'Victoria'), data.groupBy('state').mean(column).collect()[6][1])\
        .when( (col('SA2_NAME_2021') == 'New South Wales'), data.groupBy('state').mean(column).collect()[7][1])\
        .otherwise(col(column))
)

print(data.where(data.postcode == 820))
    
data.write.mode('overwrite').parquet('../data/curated/final_data.parquet')