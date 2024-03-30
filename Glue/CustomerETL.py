%%configure
{
"--conf":"spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
"--datalake-formats":"delta"
}
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
from pyspark.sql.functions import lit, current_timestamp, date_add
import boto3
import os

# Define S3 paths for input CSV files and output Delta table
bucket_name = "products-bucket-12"
input_path = "CustomerData/raw/"
output_path = 'CustomerData/output/delta_tables/customer_data/'
archive_path = 'CustomerData/archive/'

# Read customer data from CSV files
customer_df = spark.read.option("header", "true").csv(f"s3://{bucket_name}/{input_path}")
customer_df.show()
processed_files = []

customer_df.printSchema()
# response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=output_path)
# print(response)
# if not 'Contents' in response:
#     print("Not Exists")
from pyspark.sql.functions import , lit
# Create a Boto3 S3 client
s3_client = boto3.client('s3')
merged_df = existing_data.union(customer_df)
merged_df.show()
from pyspark.sql.functions import desc

try:
    # List objects with the specified prefix
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=output_path)

    # Check if any objects are returned
    if not 'Contents' in response:
        # Perform full load (initial load) if the Delta table directory does not exist
        customer_df.withColumn("effective_date", current_timestamp()) \
                   .write.format("delta").mode("overwrite").save(f"s3://{bucket_name}/{output_path}")
        print("Initial Load Done. Output Folders Created.")
    else:
        # Delta table exists, perform incremental load (CDC or incremental updates)
        existing_data = spark.read.format("delta").load(f"s3://{bucket_name}/{output_path}")
        latest_effective_date = existing_data.selectExpr("MAX(effective_date) AS max_effective_date").collect()[0]["max_effective_date"]

        # Add effective date to new records
        customer_df = customer_df.withColumn("effective_date", date_add(lit(latest_effective_date), 1))

          # Merge new data into Delta table
        existing_data.createOrReplaceTempView("existing_data")
        customer_df.createOrReplaceTempView("customer_df")
        merged_df = existing_data.union(customer_df)
        merged_df = merged_df.orderBy("customer_id", desc("effective_date")).dropDuplicates(["customer_id"])
        merged_df.write.format("delta").mode("overwrite").save(f"s3://{bucket_name}/{output_path}")
        print("Incremental/CDC Load Done. Output Folders Updated.")

  
except Exception as e:
    print("An error occurred:", e)

# df= spark.read.option("header", "true").csv(f"s3://{bucket_name}/{archive_path}")
# df.show()
# df.withColumn("effective_date", current_timestamp()) \
#                    .write.format("delta").mode("overwrite").save(f"s3://{bucket_name}/{output_path}")
# print("Initial Load Done. Output Folders Created.")
try:
    # Get a list of objects in the source prefix
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=input_path)

    # Move each object to the archive location
    for obj in response.get('Contents', []):
        # Extract the key (file path) of the object
        source_key = obj['Key']

        # Check if the object is a file within the raw folder
        if source_key.endswith('/'):
            print(f"Skipping folder: {source_key}")
            continue

        # Generate the destination key by replacing the source prefix with the archive prefix
        destination_key = source_key.replace(input_path, archive_path, 1)

        # Copy the object to the archive location
        s3_client.copy_object(
            Bucket=bucket_name,
            Key=destination_key,
            CopySource={'Bucket': bucket_name, 'Key': source_key}
        )

        # Delete the object from the source location
        s3_client.delete_object(Bucket=bucket_name, Key=source_key)
        print(f"Moved to archive: {source_key}")

except Exception as e:
    print("An error occurred:", e)
data = spark.read.format("delta").load(f"s3://{bucket_name}/{output_path}")
data.show()

# Stop Spark context
sc.stop()
job.commit()