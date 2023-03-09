import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyarrow.parquet import ParquetFile
import pyarrow as pa

pf = ParquetFile("./fhvhv/2021/06/part-00000-85b2da46-a73c-471d-b6b9-680166181f7c-c000.snappy.parquet")
first_ten_rows = next(pf.iter_batches(batch_size=1000))

schema_df = pa.Table.from_batches(batches=[first_ten_rows]).to_pandas()
schema_df.pickup_datetime = schema_df.pickup_datetime.astype('datetime64')
schema_df.dropoff_datetime = schema_df.dropoff_datetime.astype('datetime64')

print(schema_df.dtypes)
# spark  instance

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

print(spark.createDataFrame(schema_df).schema)

"""
StructType([
StructField('dispatching_base_num', StringType(), True),
 StructField('pickup_datetime', TimestampType(), True), 
 StructField('dropoff_datetime', TimestampType(), True),
  StructField('PULocationID', StringType(), True),
   StructField('DOLocationID', StringType(), True), 
   StructField('SR_Flag', StringType(), True), 
   StructField('Affiliated_base_number', StringType(), True)
   ])
"""