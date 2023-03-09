import pyspark
import pandas
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f
from datetime import datetime


# spark  instance
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df_rides = spark.read.parquet('./fhvhv/2021/06/*')
df_zones = spark.read.parquet('./zones/*')

merged_df = df_rides.join(df_zones, df_rides.PULocationID == df_zones.LocationID, how = 'outer')
merged_df.createOrReplaceTempView('temp_df')
spark.sql(sqlQuery="select count(1) as count, Zone from temp_df group by Zone order by count(1) desc;").show()

merged_df.show()