import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join, array_intersect, size, array, lit

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import array_contains
from functools import reduce

# FROM FILES
tedx_dataset_path = "s3://tedx-2025-data-mp-30072025/final_list.csv"

# READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# START JOB CONTEXT AND JOB
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# READ INPUT FILES TO CREATE AN INPUT DATASET
tedx_dataset = spark.read \
    .option("header", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(tedx_dataset_path)

tedx_dataset.printSchema()

# FILTER ITEMS WITH NULL POSTING KEY
count_items = tedx_dataset.count()
count_items_null = tedx_dataset.filter("id is not null").count()

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")

# READ THE DETAILS
details_dataset_path = "s3://tedx-2025-data-mp-30072025/details.csv"
details_dataset = spark.read \
    .option("header", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(details_dataset_path)

details_dataset = details_dataset.select(col("id").alias("id_ref"), col("description"), col("duration"), col("publishedAt"))

# READ THE IMAGES
images_dataset_path = "s3://tedx-2025-data-mp-30072025/images.csv"
images_dataset = spark.read.option("header", "true").option("quote", "\"").option("escape", "\"").csv(images_dataset_path)
images_dataset = images_dataset.select(col("id").alias("id_ref"), col("url"))

# BUILD THE MAIN DATASET BY JOINING DETAILS AND IMAGES
tedx_dataset_main = tedx_dataset.join(details_dataset, tedx_dataset.id == details_dataset.id_ref, "left").drop("id_ref")
tedx_dataset_main = tedx_dataset_main.join(images_dataset, tedx_dataset_main.id == images_dataset.id_ref, "left").drop("id_ref")
tedx_dataset_main.printSchema()

# READ TAGS DATASET
tags_dataset_path = "s3://tedx-2025-data-mp-30072025/tags.csv"
tags_dataset = spark.read.option("header", "true").csv(tags_dataset_path)

# CREATE THE AGGREGATE MODEL FOR TAGS
tags_dataset_agg = tags_dataset.groupBy("id").agg(collect_list("tag").alias("tags"))

# FILTER FOR 'science' TAG
tags_dataset_agg = tags_dataset_agg.where("array_contains(tags, 'science')")

# READ WATCH_NEXT
watch_next_dataset_path = "s3://tedx-2025-data-mp-30072025/related_videos.csv" 
watch_next_dataset = spark.read.option("header", "true").csv(watch_next_dataset_path)
watch_next_dataset = watch_next_dataset.dropDuplicates()

# ADD WATCH_NEXT per ID TO AGGREGATE MODEL
watch_next_dataset_agg = watch_next_dataset.groupBy(col("id").alias("id_watch_next")).agg(collect_list("related_id").alias("WatchNext_id"), collect_list("title").alias("WatchNext_title"))

# JOIN EVERYTHING TOGETHER TO CREATE THE FINAL AGGREGATE DATASET
tedx_dataset_agg = tedx_dataset_main.join(tags_dataset_agg, tedx_dataset_main.id == tags_dataset_agg.id, "left") \
    .drop(tags_dataset_agg.id) \
    .join(watch_next_dataset_agg, tedx_dataset_main.id == watch_next_dataset_agg.id_watch_next, "left") \
    .drop(watch_next_dataset_agg.id_watch_next) \
    .withColumnRenamed("id", "_id")

tedx_dataset_agg.printSchema()

# FILTERING FOR BeyondTEDx
filtered_dataset = tedx_dataset_agg.where(size(array_intersect(col("tags"), array(lit("science"), lit("space"), lit("aliens"), lit("asteroid"), lit("astronomy"), lit("Big Bang"), lit("dark matter"), lit("exploration"), lit("evolution"), lit("innovation"), lit("Mars"), lit("Moon"), lit("Mission Blue"), lit("planets"), lit("NASA"), lit("quantum"), lit("rocket science"), lit("Solar System"), lit("Sun"), lit("String Theory"), lit("Universe")))) > 0)

# PRINT THE FILTERED DATASET SCHEMA 
filtered_dataset.printSchema()

write_mongo_options = {
    "connectionName": "BeyondTEDx",
    "database": "unibg_tedx_2025",
    "collection": "tedx_data",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(filtered_dataset, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)