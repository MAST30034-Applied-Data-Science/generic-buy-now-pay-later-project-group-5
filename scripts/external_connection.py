from functools import reduce 
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from zipfile import ZipFile
from pyspark import SparkFiles

# Create a spark session (which will run spark jobs)
spark = (
    SparkSession.builder.appName("External Connection")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.executor.memory", "2g")
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
age_pop_G01 = pop_age_sex['SA2_CODE_2021', 'TOT_P_P', 'Age_0_4_yr_P', 'Age_5_14_yr_P', 'Age_15_19_yr_P', 'Age_20_24_yr_P', 'Age_25_34_yr_P', 'Age_35_44_yr_P', 'Age_45_54_yr_P', 'Age_55_64_yr_P', 'Age_65_74_yr_P', 'Age_75_84_yr_P', 'Age_85ov_P']
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

final_data = df_sa2.join(external_data, ["SA2_CODE_2021"], "outer")

external_data.write.mode('overwrite').parquet('../data/curated/external_data.parquet')

final_data.write.mode('overwrite').parquet('../data/curated/final_data.parquet')




