import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1710745255233 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": False,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://glue-demo-nidhi/demo/demoNoHeader.csv"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1710745255233",
)

# Script generated for node Amazon S3
additional_options = {
    "path": "s3://abcddemo-products/deltademo/target/",
    "write.parquet.compression-codec": "snappy",
}
AmazonS3_node1710745321871_df = AmazonS3_node1710745255233.toDF()
AmazonS3_node1710745321871_df.write.format("delta").options(**additional_options).mode(
    "append"
).save()

job.commit()
