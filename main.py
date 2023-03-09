import pyspark
from pyspark.sql import SparkSession
print(pyspark.__version__)
print(pyspark.__file__)

# how we connect to spark
# which spark cluster we will use (local) on all available cpus [*]
# set app name
# get or create session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .csv('taxi+_zone_lookup.csv')

df.show()

df.write.parquet('zones')