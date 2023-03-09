from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f
# spark  instance
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read.parquet('./fhvhv/2021/01/*')
df.createOrReplaceTempView('temp_df')
df_res = spark.sql(sqlQuery="select * from temp_df limit 10;")
print(df_res)
df_res.write.parquet('./fhvhv/res/', mode='overwrite')
