
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
input_path = "s3://abcddemo-products/Validated/Products/"
output_path = "s3://abcddemo-products/CSVTarget/"

json_data = spark.read.json(input_path + "*")
json_data.printSchema()
print(json_data)
exploded_data = json_data.selectExpr("explode(products) as product")
exploded_data.printSchema()
exploded_data.show(20)
# Extract ratings data and write to a separate CSV file
ratings_data = exploded_data.select("product.id", explode_outer("product.reviews").alias("review"))
ratings_data.printSchema()
# Flatten the nested structure for ratings data
flattened_ratings_data = ratings_data.select("id", "review.*")
flattened_ratings_data.printSchema()
# Write the flattened ratings data to CSV
ratings_csv_path = f"{output_path}/ratings"
flattened_ratings_data.write.mode("overwrite").csv(ratings_csv_path, header=True)
from pyspark.sql.functions import col

flattened_data = exploded_data.select(col("product.*"))
flattened_data.printSchema()
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

 
job.commit()