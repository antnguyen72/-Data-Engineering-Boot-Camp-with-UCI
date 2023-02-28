import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database = "my_crawl_db", 
    table_name = "big_data_projects_bucket", 
    transformation_ctx = "datasource0"
    )

applymapping1 = ApplyMapping.apply(
    frame = datasource0, 
    mappings = [
    ("invoiceno", "string", "invoiceno", "string"), 
    ("stockcode", "string", "stockcode", "string"), 
    ("description", "string", "description", "string"), 
    ("quantity", "long", "quantity", "long"), 
    ("invoicedate", "string", "invoicedate", "string"), 
    ("unitprice", "double", "unitprice", "double"), 
    ("customerid", "long", "customerid", "long"), 
    ("country", "string", "country", "string")
    ], 
    transformation_ctx = "applymapping1"
    )

resolvechoice2 = ResolveChoice.apply(
    frame = applymapping1, 
    choice = "make_struct", 
    transformation_ctx = "resolvechoice2"
    )

dropnullfields3 = DropNullFields.apply(
    frame = resolvechoice2, 
    transformation_ctx = "dropnullfields3"
    )

datasink4 = glueContext.write_dynamic_frame.from_options(
    frame = dropnullfields3, 
    connection_type = "s3", 
    connection_options = {"path": "s3://big-data-projects-bucket/output"}, 
    format = "parquet", 
    transformation_ctx = "datasink4"
    )

job.commit()