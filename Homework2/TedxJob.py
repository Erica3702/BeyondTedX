import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_intersect, size, array, lit

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

S3_BUCKET_PATH = "s3://tedx-2025-data-mp05082025/"

# Parametri e Inizializzazione del Job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. LETTURA DATASET DI BASE

tedx_dataset = spark.read \
    .option("header", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(S3_BUCKET_PATH + "final_list.csv")

details_dataset = spark.read \
    .option("header", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(S3_BUCKET_PATH + "details.csv")

tedx_dataset_main = tedx_dataset.join(details_dataset, tedx_dataset.id == details_dataset.id, "left") \
    .drop(details_dataset.id)

# 2. AGGIUNTA TAGS

tags_dataset_path = S3_BUCKET_PATH + "tags.csv"
tags_dataset = spark.read.option("header", "true").csv(tags_dataset_path)

tags_dataset_agg = tags_dataset.groupBy("id").agg(collect_list("tag").alias("tags"))

tedx_dataset_agg = tedx_dataset_main.join(tags_dataset_agg, tedx_dataset_main.id == tags_dataset_agg.id, "left") \
    .drop(tags_dataset_agg.id)

# 3. AGGIUNTA VIDEO SUGGERITI (WATCH NEXT)


watch_next_dataset_path = S3_BUCKET_PATH + "related_videos.csv"
watch_next_dataset = spark.read.option("header", "true").csv(watch_next_dataset_path)
watch_next_dataset = watch_next_dataset.dropDuplicates()

watch_next_dataset_agg = watch_next_dataset.groupBy(col("id").alias("id_watch_next")) \
    .agg(collect_list("related_id").alias("WatchNext_id"), collect_list("title").alias("WatchNext_title"))

tedx_dataset_agg = tedx_dataset_agg.join(watch_next_dataset_agg, tedx_dataset_agg.id == watch_next_dataset_agg.id_watch_next, "left") \
    .drop("id_watch_next")

# 4. FILTRAGGIO TEMATICO

beyond_tedx_keywords = [
    "space", "aliens", "asteroid", "astronomy", "Big Bang", "dark matter",
    "exploration", "evolution", "innovation", "Mars", "Moon", "planets", "NASA",
    "quantum", "rocket science", "Solar System", "Sun", "String Theory", "Universe"
]

# il filtro  richiede almeno 3 tag in comune
filtered_dataset = tedx_dataset_agg.where(
    size(array_intersect(col("tags"), array([lit(x) for x in beyond_tedx_keywords]))) > 2
)

print("Schema finale del dataset filtrato:")
final_dataset = filtered_dataset.withColumnRenamed("id", "_id")
final_dataset.printSchema()


# 5. SCRITTURA SU MONGODB

write_mongo_options = {
    "connectionName": "Mongodbatlas connection",
    "database": "unibg_tedx_2025",
    "collection": "tedx_data",
    "ssl": "true",
    "ssl.domain_match": "false"
}

tedx_dynamic_frame = DynamicFrame.fromDF(final_dataset, glueContext, "tedx_dynamic_frame")
glueContext.write_dynamic_frame.from_options(
    frame=tedx_dynamic_frame,
    connection_type="mongodb",
    connection_options=write_mongo_options
)

job.commit()