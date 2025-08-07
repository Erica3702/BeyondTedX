###### Job per il caricamento iniziale degli utenti da S3 a MongoDB

import sys
from pyspark.sql.functions import col
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# 1. Inizializzazione e Configurazione
print("INIZIALIZZAZIONE JOB")
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 2. Definizione dei Percorsi e Nomi
s3_bucket_path = "s3://tedx-2025-data-mp05082025/"
input_csv_file = s3_bucket_path + "log.csv"
mongo_connection_name = "Mongodbatlas connection" 
mongo_database_name = "unibg_tedx_2025"
mongo_collection_name = "users"

# 3. Lettura del file CSV da S3 
print(f"Lettura del file di input: {input_csv_file}")
user_dataset = spark.read \
    .option("header", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("delimiter", ",") \
    .csv(input_csv_file)

print("Schema del file CSV letto:")
user_dataset.printSchema()

# 4. Scrittura su MongoDB
print(f"Inizio scrittura sulla collezione '{mongo_collection_name}' in MongoDB...")
write_mongo_options = {
    "connectionName": mongo_connection_name,
    "database": mongo_database_name,
    "collection": mongo_collection_name,
    "ssl": "true",
    "ssl.domain_match": "false"
}

user_dynamic_frame = DynamicFrame.fromDF(user_dataset, glueContext, "user_dynamic_frame")

glueContext.write_dynamic_frame.from_options(
    frame=user_dynamic_frame,
    connection_type="mongodb",
    connection_options=write_mongo_options
)

# 5. Completamento del Job
job.commit()
print(f"JOB COMPLETATO: Dati caricati con successo nella collezione '{mongo_collection_name}'.")