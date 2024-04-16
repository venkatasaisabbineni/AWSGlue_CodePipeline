
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data directly from S3
source_path = "s3://bobbycodepipeline/input/customer_data.csv"
input_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    connection_options={"paths": [source_path]},
    format_options={"withHeader": True}
)
#Remove null values from the data
df = input_df.toDF()
df = df.withColumn("Credit_Card_Number", df["Credit_Card_Number"].cast("double"))
transformed_df = DynamicFrame.fromDF(df.dropna(),glueContext,"transformed_df")

# Convert to csv format
csv_path = "s3://bobbycodepipeline/output/transformed_customer_data.csv"
glueContext.write_dynamic_frame.from_options(
    frame=transformed_df,
    connection_type="s3",
    connection_options={"path": csv_path},
    format="csv"
)

job.commit()
