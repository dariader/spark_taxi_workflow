import pandas as pd
import pyspark
from pyspark.sql import SparkSession
#https://nyc-tlc.s3.amazonaws.com/trip+data/fhvhv_tripdata_2021-01.csv
#https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-01.parquet
from pyarrow.parquet import ParquetFile
import pyarrow as pa

pf = ParquetFile("fhvhv_tripdata_2021-01.parquet")
first_ten_rows = next(pf.iter_batches(batch_size = 1000))

schema_df = pa.Table.from_batches([first_ten_rows]).to_pandas()

print(schema_df.dtypes)
# spark  instance
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

print(spark.createDataFrame(schema_df).schema)