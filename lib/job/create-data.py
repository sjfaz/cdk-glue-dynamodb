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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

## TODO: Implement item_size_kb using parameter input
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

## Create dummy data for table

schema = StructType([
    StructField("pk", StringType()),
    StructField("sk", StringType()),
    StructField("attribute_1", IntegerType()),
    StructField("attribute_2", StringType()),
    StructField("attribute_3", StringType()),
    StructField("attribute_4", StringType()),
    StructField("attribute_5", StringType()),
    StructField("attribute_6", StringType()),
    StructField("attribute_7", StringType()),
    StructField("attribute_8", StringType())
])

def generate_data(count):
    data = []
    for i in range(count):
        pk = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        sk = ''.join(random.choices(string.ascii_letters, k=10))
        attribute_1 = random.randint(18, 99)
        attribute_2 = ''.join(random.choices(string.ascii_letters, k=100))
        attribute_3 = ''.join(random.choices(string.ascii_letters, k=100))
        attribute_4 = ''.join(random.choices(string.ascii_letters, k=100))
        attribute_5 = ''.join(random.choices(string.ascii_letters, k=100))
        attribute_6 = ''.join(random.choices(string.ascii_letters, k=100))
        attribute_7 = ''.join(random.choices(string.ascii_letters, k=100))
        attribute_8 = ''.join(random.choices(string.ascii_letters, k=100))
        data.append({
            'pk': pk, 'sk': sk, 'attribute_1': attribute_1, 'attribute_2': attribute_2,
            'attribute_3': attribute_3, 'attribute_4': attribute_4,
            'attribute_5': attribute_5, 'attribute_6': attribute_6,
            'attribute_7': attribute_7, 'attribute_8': attribute_8,
        })
    return data
    
data = generate_data(number_of_items)
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