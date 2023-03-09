import pyspark
import pandas
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f
# spark  instance
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read.parquet('./fhvhv/2021/01/')
print(df.printSchema())

print(
df\
    .withColumn('pickup_date', f.to_date(df.pickup_datetime))\
    .select('pickup_date', 'PULocationID', 'DOLocationID')\
    .show())

def custom_fn(id):
    return f'asdf{id}'

custom_udf = f.udf(custom_fn, returnType=StringType())

print(
df\
    .withColumn('custom_id', custom_udf(df.hvfhs_license_num))\
    .select('custom_id', 'PULocationID', 'DOLocationID')\
    .show())
