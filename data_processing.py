# load june data
# wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .csv('fhvhv_tripdata_2021-06.csv.gz')

df.repartition(12).write.parquet('fhvhv/2021/06')

# file size in mb in the folder
# stat * --terse |awk '{print $2/1000000}'