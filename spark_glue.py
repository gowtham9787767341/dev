import sys
import time
import datetime
from datetime import datetime
import pandas as pd
import json
import time
import boto3
import pandas as pd
import s3fs
import io
import pyarrow as pa
import string
import pyarrow.parquet as pq
from io import BytesIO
from io import StringIO
from json import dumps
from datetime import datetime
import json 
from pyspark.context import SparkContext
import pyspark.sql.functions as f
from pyspark.sql.functions import * 
from pyspark.sql.functions import year, month, dayofmonth,lit
from pyspark.sql.functions import date_format
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql import Window
from pyspark.sql.types import *
from dateutil import parser
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.window import Window as W
from pyspark.sql.functions import year, month,dayofmonth,rand
from awsglue.transforms import ResolveChoice, DropNullFields
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import ceil, col
import math
from awsglue.dynamicframe import DynamicFrame
import logging
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
hadoop_conf = sc._jsc.hadoopConfiguration()
# hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
job = Job(glueContext)
# Set up logging
logger = logging.getLogger()
# logger.setLevel(logging.INFO)
logger.setLevel(logging.DEBUG)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
s3_resource = boto3.resource('s3')
job = Job(glueContext)
# Set up logging
logger = logging.getLogger()
# logger.setLevel(logging.INFO)
logger.setLevel(logging.DEBUG)
print('testing')
bucket_name='inputtestvechichle'
read_location ="s3://inputtestvechichle/input"
write_location=r's3://inputtestvechichle/output/temp/audit/'
print(write_location)
df=spark.read.parquet(read_location)
df.show()
print(df.columns)
gdf1=df.select('year','month','day').distinct()
gdf2 =gdf1.withColumn("id",f.row_number().over(Window.orderBy(f.monotonically_increasing_id())))
gdf2.show()
totalcount=gdf2.count()
rcount=10000
noOfPartitions=math.ceil(totalcount/rcount)
print(noOfPartitions)
target_dff=gdf2.repartition(noOfPartitions)
print('writing')
target_dff.write.mode('append').csv(write_location)
print('written')
# ###For changing the name of the file created via spark"
PREFIX='blueprintip/output/temp/audit/'
bucket_name='tripelr'
client = boto3.client('s3')
response = client.list_objects(Bucket=bucket_name,Prefix=PREFIX)
name = response["Contents"][0]["Key"]
copy_source = {'Bucket':bucket_name, 'Key': name}
# copy_key = PREFIX + '-new-name.parquet'
copy_key = PREFIX + 'auditdata.csv'
client.copy(CopySource=copy_source, Bucket=bucket_name, Key=copy_key)
client.delete_object(Bucket=bucket_name, Key=name)
print('Successfully changed the  name')