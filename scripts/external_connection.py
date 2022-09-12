from functools import reduce 
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from zipfile import ZipFile
from pyspark import SparkFiles
import pyspark.sql.functions as F
from pyspark.sql.functions import col

# Create a spark session (which will run spark jobs)
spark = (
    SparkSession.builder.appName("External Connection")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

ZipFile('../data/external/AUS_2021.zip').extractall('../data/external')

# G01 SELECTED PERSON CHARACTERISTICS BY SEX
pop_age_sex = spark.read.csv('../data/external/2021 Census GCP All Geographies for AUS/SA2/AUS/2021Census_G01_AUST_SA2.csv', header = True)

# AUSTRALIAN BUREAU OF STATISTICS 2021 Census of Population and Housing
personal_income = spark.read.csv('../data/external/2021 Census GCP All Geographies for AUS/SA2/AUS/2021Census_G02_AUST_SA2.csv', header = True)

# selecting columns from G01 table
# this table contains the SA2 code and population by age
age_pop_G01 = pop_age_sex['SA2_CODE_2021', 'TOT_P_P']
# this table contains the SA2 code and education background by age
edu_age_G01 = pop_age_sex['SA2_CODE_2021', 'High_yr_schl_comp_Yr_12_eq_P', 'High_yr_schl_comp_D_n_g_sch_P']
edu_age_G01 = edu_age_G01.withColumnRenamed('High_yr_schl_comp_Yr_12_eq_P', 'Completed Year 12')\
                         .withColumnRenamed('High_yr_schl_comp_D_n_g_sch_P', 'Did Not Attend School')
# this table contains income and related stats by SA2 code
incom_stats = personal_income["SA2_CODE_2021","Median_tot_prsnl_inc_weekly", "Median_rent_weekly", "Median_mortgage_repay_monthly", "Median_age_persons", "Median_tot_hhd_inc_weekly", "Average_household_size"]

# join all interesting external data together
external_data = incom_stats.join(edu_age_G01,["SA2_CODE_2021"],"outer")\
                           .join(age_pop_G01,["SA2_CODE_2021"],"outer")

joined_data = spark.read.parquet('../data/curated/joined_data.parquet')

# converts 2016 SA2 to 2021 SA2
correspondence = spark.read.csv('../data/external/sa2_correspondence.csv', header = True)

# converts postcodes to 2016 SA2
index = spark.read.csv('../data/external/2019 Locality to 2016 SA2 Coding Index.csv', header = True)

# make appropriate conversions
joined_data_sa2 = joined_data.join(index,['postcode'],"left")
df_sa2 = joined_data_sa2.join(correspondence,correspondence.SA2_MAINCODE_2016== joined_data_sa2.SA2_MAINCODE, "outer")

# drop unecessary columns
drop_columns = ("LOCALITY_ID", "LOCALITY_NAME", "LOCALITY_TYPE", "STATE", "SA2_MAINCODE", "SA2_NAME", \
               "SA2_MAINCODE_2016", 'SA2_NAME_2016', 'RATIO_FROM_TO', 'INDIV_TO_REGION_QLTY_INDICATOR', \
               'OVERALL_QUALITY_INDICATOR', 'BMOS_NULL_FLAG')
df_sa2 = df_sa2.drop(*drop_columns)

data = df_sa2.join(external_data, ["SA2_CODE_2021"], "outer")

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

# Cast data types

data = data.withColumn("Median_tot_prsnl_inc_weekly", data.Median_tot_prsnl_inc_weekly.cast('float'))
data = data.withColumn("Median_rent_weekly", data.Median_rent_weekly.cast('float'))
data = data.withColumn("Median_mortgage_repay_monthly",data.Median_mortgage_repay_monthly.cast('float'))
data = data.withColumn("Median_age_persons",data.Median_age_persons.cast('float'))
data = data.withColumn("Median_tot_hhd_inc_weekly",data.Median_tot_hhd_inc_weekly.cast('float'))
data = data.withColumn("Average_household_size",data.Average_household_size.cast('float'))
data = data.withColumnRenamed("Completed Year 12", "Completed_Year_12")
data = data.withColumn("Completed_Year_12",data.Completed_Year_12.cast('float'))
data = data.withColumnRenamed("Did Not Attend School", "Did_Not_Attend_School")
data = data.withColumn("Did_Not_Attend_School",data.Did_Not_Attend_School.cast('float'))
data = data.withColumn("TOT_P_P",data.TOT_P_P.cast('float'))

columns = ["Median_tot_prsnl_inc_weekly", "Median_rent_weekly", "Median_mortgage_repay_monthly", "Median_age_persons", "Median_tot_hhd_inc_weekly",\
           "Average_household_size", "Completed_Year_12", "Did_Not_Attend_School", "TOT_P_P"]

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
    

# Write data

external_data.write.mode('overwrite').parquet('../data/curated/external_data.parquet')

data.write.mode('overwrite').parquet('../data/curated/final_data.parquet')




