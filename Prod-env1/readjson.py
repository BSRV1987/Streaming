from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("JSON to CSV Streaming Pipeline") \
    .getOrCreate()

# Define schema for JSON data
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

# Define input and output directories
input_path = "file:///SourceFiles/data/json_files"
output_path = "file:///SourceFiles/data/csv_files"

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

# Define query to write the flattened data as CSV
query = flattened_df.writeStream \
    .format("csv") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_location") \
    .option("path", output_path) \
    .start()

# Wait for the streaming query to finish
query.awaitTermination()
