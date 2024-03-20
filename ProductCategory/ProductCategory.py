import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col, explode, explode_outer

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Get the arguments passed to the job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path'])

# Create a Glue job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the input and output paths from the arguments
# input_path = "s3://abcddemo-products/Validated/Products/products.json"
# output_path = "s3://abcddemo-products/CSVTarget/"
input_path = args['input_path']
output_path = args['output_path']

# Read JSON data from S3
json_data = spark.read.json(input_path)

# Explode the 'products' array to access the nested 'category' column
exploded_data = json_data.selectExpr("explode(products) as product")

# Extract ratings data
ratings_data = exploded_data.select("product.id", explode_outer("product.reviews").alias("review"))
# Flatten the nested structure for ratings data
flattened_ratings_data = ratings_data.select("id", "review.*")
# Write to a separate CSV file
ratings_csv_path = f"{output_path}/ratings"
flattened_ratings_data.write.mode("overwrite").csv(ratings_csv_path, header=True)

# Filter data based on category and write to separate folders
categories = exploded_data.select("product.category").distinct().rdd.flatMap(lambda x: x).collect()

# Flatten the imgs array
flattened_data = exploded_data.select(col("product.*"))
flattened_data = flattened_data.drop("imgs")
flattened_data = flattened_data.drop("reviews")
flattened_data.printSchema()

# Filter data based on category and write to separate folders
for category in categories:
    category_data = flattened_data.filter(col("category") == category)
    category_output_path = f"{output_path}/{category}"
    category_data.write.mode("overwrite").json(category_output_path)

# Commit the job
job.commit()
