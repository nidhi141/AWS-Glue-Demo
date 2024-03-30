
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
s3_path = "s3://products-bucket-12/customer_raw/Customers_Data_29032024010101.csv"

# Create a DynamicFrame from the CSV file
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path]},
    format="csv",
    format_options={"separator": ",", "withHeader":True,"optimizePerformance":True}  # Optional: specify options like delimiter, header, etc.
)
dynamic_frame.toDF().show()
s3_path = "s3://products-bucket-12/customer_raw/Customers_Data_29032024010101.csv"
from pyspark.sql.functions import when, col, lit


df = spark.read.csv(s3_path, header=True, inferSchema=True)

flagged_df = df.withColumn("isPkeyDataExist", when(col("customer_id").isNotNull() & col("customer_name").isNotNull(), lit(True)).otherwise(lit(False)))
flagged_df.show()


job.commit()