import pyspark
import pandas
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f
from datetime import datetime

schema = StructType([
    StructField('dispatching_base_num', StringType(), True),
    StructField('pickup_datetime', TimestampType(), True),
    StructField('dropoff_datetime', TimestampType(), True),
    StructField('PULocationID', StringType(), True),
    StructField('DOLocationID', StringType(), True),
    StructField('SR_Flag', StringType(), True),
    StructField('Affiliated_base_number', StringType(), True)
])


def ride_len_calc(start, stop):
    start = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    stop = datetime.strptime(stop, '%Y-%m-%d %H:%M:%S')
    duration = stop - start
    hours = round(duration.total_seconds()/3600, 4)
    return hours


custom_udf = f.udf(ride_len_calc, returnType=DoubleType())

# spark  instance
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read.parquet('./fhvhv/2021/06/*')


df = df. \
    withColumn('pickup_date', f.to_date(df.pickup_datetime)) \
    .withColumn('len_ride', custom_udf(df.pickup_datetime, df.dropoff_datetime)) \
    .select('pickup_date','len_ride', 'PULocationID', 'DOLocationID')

df.createOrReplaceTempView('temp_df')
spark.sql(sqlQuery="select max(len_ride) AS max_len_ride from temp_df;").show()



# How many taxi trips were there on June 15?
# df_res = spark.sql(sqlQuery="select count(1) from temp_df where pickup_date = '2021-06-15';")
# print(df_res.show())
