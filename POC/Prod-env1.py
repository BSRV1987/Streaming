from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os
import shutil

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("JSON to CSV Streaming Pipeline") \
    .getOrCreate()

# Delete the checkpoint location for testing again and again.
checkpoint_location = "/tmp/checkpoint_location_1"
if os.path.exists(checkpoint_location):
    shutil.rmtree(checkpoint_location)

# Define json structure which is important for readstream
json_schema = StructType([
    StructField("datetime", StringType()),
    StructField("sales", StructType([
        StructField("quantity", IntegerType()),
        StructField("total_price", FloatType())
    ])),
    StructField("analytics", StructType([
        StructField("clicks", IntegerType()),
        StructField("impressions", IntegerType())
    ]))
])

# Define input and output directories. Can be any cloudstorages like s3, GS or Azure blob
input_path = "./SourceFiles/data/json_files"
output_path = "./SourceFiles/data/csv_files"

# Clear output for reruns
if os.path.exists(output_path):
    shutil.rmtree(output_path)

# Read JSON files as a stream
json_stream_df = spark.readStream \
    .schema(json_schema) \
    .json(input_path)

# Flatten the JSON structure
flattened_df = json_stream_df.select(
    "datetime",
    "sales.quantity",
    "sales.total_price",
    "analytics.clicks",
    "analytics.impressions"
)

# Define query to write the flattened data as CSV. Used append mode to write increments
query = flattened_df.writeStream \
    .format("csv") \
    .outputMode("append") \
    .option("header", "true") \
    .option("checkpointLocation", checkpoint_location) \
    .option("path", output_path) \
    .start()

# block until query is terminated, with stop() or with error
query.awaitTermination()