# Processing Products JSON files and writing as Delta files in S3 in this job

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col, explode, explode_outer
from pyspark.sql.types import StructType, StructField
from pyspark.sql.functions import desc
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create a GlueContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Get the arguments passed to the job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'metadata_input_path' ,'output_path'])
# Define the new job parameter

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

input_path = args['input_path']
metadata_input_path = args['metadata_input_path']
output_path = args['output_path']


'''
# Define the input and output paths from the arguments
input_path = "s3://products-bucket-12/validated/Products/"
metadata_input_path = "s3://products-bucket-12/validated/Metadata/"
output_path = "s3://products-bucket-12/target/"
'''

print(input_path)
print(metadata_input_path)
print(output_path)
logger.info(f"loaded parameters ")

# Read JSON data from S3
json_data = spark.read.json(input_path)

json_data.printSchema()
#Explode the json data and filter out only valid records to process
exploded_data = json_data.select(explode("Products").alias("Products"))
exploded_data = exploded_data.filter(exploded_data.Products['isCategoryValid'] == True)
logger.info(f"explodded JSON data ")

# Processing Reviews data
ratings_data = exploded_data.select("Products.id", explode_outer("Products.reviews").alias("review"))
flattened_ratings_data = ratings_data.select("id", "review.*")
# Write to a separate CSV file
ratings_csv_path = f"{output_path}/ratings"
flattened_ratings_data.write.mode("overwrite").csv(ratings_csv_path, header=True)
logger.info(f"loaded review data into S3 ")
exploded_data.printSchema()
#Read metadata file and create lists of required columns for all categories

metadata_df = spark.read.json(metadata_input_path)
schema = metadata_df.schema
Fixed_Columns = ['category','eta','id','inStock','popular','price','rating','specs','title','ProcessedTimestamp']

all_dataframes = {}

for field in schema.fields:
    new_list = []  
    #new_list.append(field.name)
    for inner_field in field.dataType.fields:
        if isinstance(inner_field.dataType,StructType):
            for k in inner_field.dataType.fields:
                new_list.append(k.name)
                #print(new_list)
    new_list = new_list + Fixed_Columns
    #new_list.pop(0)
    all_dataframes[f"{field.name}"] = new_list
    #print(new_list)
    logger.info(f"Seggregated dataframe schema category wise for :{field.name}")

#Create dataframes dynamically for multiple categories

dfs = {}
for key,value in all_dataframes.items():
    #print(type(i))
    temp = exploded_data.filter(exploded_data.Products['category'] == key)
    temp = temp.select("Products.*")
    temp = temp.drop("img","isCategoryValid","reviews")
    temp = temp.select([ c for c in temp.columns if c in value])
    temp = temp.fillna(False,"popular")
    dfs[f"{key}_df"] = temp.orderBy("id",desc("ProcessedTimestamp")).dropDuplicates(['id'])


logger.info(f"created category wise dataframe ")




for category in dfs.keys():
    try:
        prefix = category.split('_')[0]
        path = f"{output_path}/{prefix}/"
        existing_df = spark.read.format("delta").load(path)
        delta_df = dfs[f"{category}"]
        df = existing_df.union(delta_df)
        upsertDataFrame = df.orderBy("id", desc("ProcessedTimestamp")).dropDuplicates(["id"])
        upsertDataFrame.write.format("delta").mode("overwrite").save(path)
        logger.info(f"Upserted the Data into S3 at {path}")
    
    except Exception as e:
        if "Path does not exist" in str(e):
            print(e)
            print("New files created", prefix)
            upsertDataFrame = dfs[f"{category}"]
            upsertDataFrame.write.format("delta").mode("overwrite").save(path)
            logger.info(f"Inserted the Data fir first time into S3 at {path}")
            #print(type(e))
        else:
            print(e)
    

upsertDataFrame.show()
job.commit()