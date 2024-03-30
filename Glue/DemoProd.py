
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
# Define the input and output paths from the arguments
input_path = 's3://abcddemo-products/Validated/Products/DemoCDCIncrementalLoad/'
target_path = 's3://abcddemo-products/DemoTarget/'
json_data = spark.read.json(input_path + "*")
json_data.printSchema()
products = json_data.selectExpr("explode(products) as product")
exploded_data=products.selectExpr("product.*")
exploded_data = exploded_data.drop("reviews")
exploded_data.printSchema()

from pyspark.sql.functions import max

# Group the data by product ID and select the latest timestamp for each product
latest_timestamps = exploded_data.groupBy('id').agg(max('ProcessedTimestamp').alias('latest_timestamp'))
latest_timestamps.show()
# Join the original DataFrame with the latest timestamps to filter out older records
latest_data = exploded_data.join(
    latest_timestamps,
    (exploded_data['id'] == latest_timestamps['id']) & (exploded_data['ProcessedTimestamp'] == latest_timestamps['latest_timestamp']),
    'inner'
).drop(latest_timestamps['id']).drop(latest_timestamps['latest_timestamp'])

# Now, latest_data DataFrame contains the latest records for each product based on the processed timestamp.
latest_data.printSchema()
latest_data.show()
from pyspark.sql.functions import col

# demo_path = "s3://abcddemo-products/CSVTarget/Temp"
# Filter data based on category and write to separate folders

categories = latest_data.select("category").distinct().rdd.flatMap(lambda x: x).collect()
category = "Beauty"
# For each category, determine if data needs to be updated or inserted
category_data = latest_data.filter(col("category") == category)
category_output_path = f"{target_path}/{category}"

# Read existing data for the category
existing_data = spark.read.json(category_output_path)
new_ids = category_data.join(existing_data, 'id', 'left_anti')
new_ids.show()





# Identify the IDs that are new or updated compared to the existing data
category_data.show()
existing_data.show()
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

updated_data = existing_data.join(updated_ids, "id","leftanti").union(category_data)
updated_data.show()

job.commit()