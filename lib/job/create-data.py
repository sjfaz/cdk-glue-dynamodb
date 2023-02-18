import sys
import random
import string
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType

## TODO: Check if this is able to be distributed well

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME", "table_name", "item_size_kb", "number_of_items"])
print('Starting job with args:')
print(args)
print("Init datetime:", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
table_name = args["table_name"]
item_size_kb = int(args["item_size_kb"])
number_of_items = int(args["number_of_items"])

print(table_name, item_size_kb, number_of_items);

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print('Creating data for table:', datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

attributes = {}
totalattributes = (item_size_kb-1)*10 + 8

## Create dummy data for table
schema = StructType([
    StructField("pk", StringType()),
    StructField("sk", StringType()),
])
for y in range(totalattributes):
    schema.add(StructField("attribute_{0}".format(y), StringType()))

print (schema)

def generate_data(count):
    data = []
    for i in range(count):
        item_data = {}
        item_data['pk'] = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        item_data['sk'] = ''.join(random.choices(string.ascii_letters, k=10))
        for z in range(totalattributes):
            item_data['attribute_{0}'.format(z)] = ''.join(random.choices(string.ascii_letters, k=85))
        data.append(item_data)
    return data
    
data = generate_data(number_of_items)
print(data)
df = spark.createDataFrame(data, schema)
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

print('Writing data to table:', datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

glueContext.write_dynamic_frame_from_options(
frame=dynamic_frame,
connection_type="dynamodb",
connection_options={
"dynamodb.region": 'eu-west-1',
"dynamodb.output.tableName": table_name,
"dynamodb.throughput.write.percent": "1.0"
}
)

print('Finished Write DDB:', datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

job.commit()