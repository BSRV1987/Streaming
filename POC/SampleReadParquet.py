from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Read Parquet Files") \
    .getOrCreate()

# Specify the path to the Parquet files
parquet_path = "./SourceFiles/data/parquet_files/*.parquet"

# Read Parquet files into a DataFrame
parquet_df = spark.read.parquet(parquet_path)

# Display the DataFrame contents
parquet_df.show()
