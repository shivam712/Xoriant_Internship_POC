import findspark
findspark.init()

import data_preprocessing

from pyspark import SparkContext, SparkConf
from pyspark.sql import*
from pyspark.sql.functions import *
import sys

conf = SparkConf().setMaster("local").setAppName("bigdatapoc")
sc = SparkContext.getOrCreate(conf=conf)
#sc = SparkContext(conf=conf) 
sc.setLogLevel("INFO")
hc = HiveContext(sc)

hc.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.info("pyspark script logger initialized")

file_cases_deaths_tests = '/home/hdoop/csv/Covid_Cases_Deaths_Tests_Data.csv'
file_vacc_population = '/home/hdoop/csv/Covid_Vacc_Population_Data.csv'

df_cases_deaths_tests = hc.read.csv(file_cases_deaths_tests, header=True)
df_vacc_population = hc.read.csv(file_vacc_population, header=True)

df_cases_deaths_tests = df_cases_deaths_tests.withColumn('date_in_dateFormat',to_date(unix_timestamp(col('date'), 'MM/dd/yyyy').cast("timestamp"))).drop('date')
df_vacc_population = df_vacc_population.withColumn('date_in_dateFormat',to_date(unix_timestamp(col('date'), 'MM/dd/yyyy').cast("timestamp"))).drop('date')

df_cases_deaths_tests = data_preprocessing.replace_null_with_updated(df_cases_deaths_tests,'total_cases','total_deaths','total_tests')
df_cases_deaths_tests = data_preprocessing.replace_null_with_zero(df_cases_deaths_tests,'total_cases','new_cases','total_deaths','new_deaths','total_tests','new_tests')

df_vacc_population = data_preprocessing.replace_null_with_updated(df_vacc_population,'total_vaccinations','people_vaccinated','people_fully_vaccinated','population')
df_vacc_population = data_preprocessing.replace_null_with_zero(df_vacc_population,'total_vaccinations','people_vaccinated','people_fully_vaccinated','new_vaccinations')

df_combined = df_cases_deaths_tests.join(df_vacc_population, on=['location','date_in_dateFormat'], how = 'inner')

#--7-DAY-AVERAGE----
df_combined = data_preprocessing.calculate_7_day_average(df_combined,'new_cases','average_new_cases')
df_combined = data_preprocessing.calculate_7_day_average(df_combined,'new_deaths','average_new_deaths')
df_combined = data_preprocessing.calculate_7_day_average(df_combined,'new_vaccinations','average_new_vaccinations')

#--CASE-FATALITY-RATIO----
df_combined = df_combined.withColumn('Case_Fatality_Ratio',((col('total_deaths')/col('total_cases'))*100))

#--TOTAL-VACCINATIONS-PER-MILLION----
df_combined = df_combined.withColumn('total_vaccinations_per_million',(col('total_vaccinations') * 100000) / col('population'))

df_combined = df_combined.orderBy(asc('location'),desc('date_in_dateFormat'))
#print(df_combined.count())

#df_combined.write.mode("overwrite").saveAsTable("covid_db.covid_data_table")

#df_combined.coalesce(1).write.csv('/home/swaraj/Desktop/BigData/covid_data.csv')

# hc.sql("use covid_db;")
#hc.sql("show tables;").show()
#df_combined.show(200)

LOGGER.info("pyspark script logger initialized !!!")