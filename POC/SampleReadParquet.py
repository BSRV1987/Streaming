from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Read Parquet Files") \
    .getOrCreate()

parquet_path = "./SourceFiles/data/parquet_files/*.parquet"

parquet_df = spark.read.parquet(parquet_path)

parquet_df.show()
