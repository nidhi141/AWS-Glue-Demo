import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_flatten

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Raw:Products
RawProducts_node1710757747134 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://abcddemo-products/Validated/Products/"],
        "recurse": True,
    },
    transformation_ctx="RawProducts_node1710757747134",
)

# Script generated for node Flatten
Flatten_node1710920029064 = RawProducts_node1710757747134.gs_flatten(maxLevels=0)

# Script generated for node Amazon S3
AmazonS3_node1710758580598 = glueContext.write_dynamic_frame.from_options(
    frame=Flatten_node1710920029064,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://abcddemo-products/raw/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1710758580598",
)

job.commit()
