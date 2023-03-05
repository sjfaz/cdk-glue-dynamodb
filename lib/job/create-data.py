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

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME", "table_name", "item_size_kb", "number_of_items", "partition_count", "pk_index"])
print('Starting job with args:')
print(args)
print("Init datetime:", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
table_name = args["table_name"]
item_size_kb = int(args["item_size_kb"])
number_of_items = int(args["number_of_items"])
partition_count = int(args["partition_count"])
pk_index = int(args["pk_index"])

print(table_name, item_size_kb, number_of_items, partition_count, pk_index);

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
    # StructField("sk", StringType()),
])
for y in range(totalattributes):
    schema.add(StructField("attribute_{0}".format(y), StringType()))

def generate_data(partition_index):
    data = []
    items_per_partition = number_of_items // partition_count
    start_index = partition_index * items_per_partition
    end_index = start_index + items_per_partition
    print('p:{0} s:{1} e:{2}'.format(items_per_partition, start_index, end_index))
    for i in range(start_index, end_index):
        item_data = {}
        item_data['pk'] = 'p#000{0}'.format(i+pk_index)
        for z in range(totalattributes):
            item_data['attribute_{0}'.format(z)] = ''.join(random.choices(string.ascii_letters, k=85))
        data.append(item_data)
    return data

# Parallelize the data generation across multiple partitions
data_rdd = sc.parallelize(range(partition_count), partition_count).flatMap(generate_data)

# Convert RDD to DataFrame and then to a Glue DynamicFrame
df = data_rdd.toDF(schema=schema)
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

print('Writing data to table:', datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

# Repartition the DynamicFrame to increase the number of concurrent writes to DynamoDB
dynamic_frame = dynamic_frame.repartition(partition_count)

glueContext.write_dynamic_frame_from_options(
    frame=dynamic_frame,
    connection_type="dynamodb",
    connection_options={
        "dynamodb.region": 'eu-west-1',
        "dynamodb.output.tableName": table_name,
        "dynamodb.throughput.write.percent": 1 / partition_count
    }
)

print('Finished Write DDB:', datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

job.commit()
