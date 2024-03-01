from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType,TimestampType
import os
import shutil

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Streaming CSV to Parquet Transformation Pipeline") \
    .getOrCreate()

# Delete the checkpoint location for testing again and again.
checkpoint_location = "/tmp/checkpoint_location_2"
if os.path.exists(checkpoint_location):
    shutil.rmtree(checkpoint_location)

# Define schema for CSV files
csv_schema = StructType([
    StructField("datetime", TimestampType()),
    StructField("quantity", IntegerType()),
    StructField("total_price", FloatType()),
    StructField("clicks", IntegerType()),
    StructField("impressions", IntegerType())
])

# Storage Can be any cloudstorage like s3, GS or Azure blob
input_path = "./SourceFiles/data/csv_files/*.csv"
output_path = "./SourceFiles/data/parquet_files"

# Clear output path for rerun
if os.path.exists(output_path):
    shutil.rmtree(output_path)

# Read CSV files as a stream watermark with 5 seconds instead of 5 minutes
csv_stream_df = spark.readStream \
    .schema(csv_schema) \
    .csv(input_path) \
    .withWatermark("datetime", "5 seconds")  

# Debug what is coming out of CSV
"""
console_query = csv_stream_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
"""

# Apply windowing to aggregate values every 5 seconds instead of 5 minutes
aggregated_df = csv_stream_df.groupBy(window("datetime", "5 seconds")).agg(sum("quantity").alias("total_quantity"),
                                                                           sum("total_price").alias("total_price"),
                                                                           sum("clicks").alias("total_clicks"),
                                                                           sum("impressions").alias("total_impressions"))

"""
# Debug what is written to parquet
console_query = aggregated_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
"""

# Write the aggregated data to Parquet files. Used append to write only incremental data. Parquet doesnt support complete -- An FYI
query = aggregated_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", checkpoint_location) \
    .option("path", output_path) \
    .start()

# block until query is terminated, with stop() or with error
query.awaitTermination()
