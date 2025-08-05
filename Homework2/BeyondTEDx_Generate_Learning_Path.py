import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, size, array, lit, sum as _sum, round as _round, concat, initcap, expr, array_contains
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


# 1. INIZIALIZZAZIONE E CONFIGURAZIONE
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

S3_BUCKET_PATH = "s3://tedx-2025-data-mp05082025/"


# 2. LETTURA E PREPARAZIONE DEI DATI

# Lettura dataset principali
tedx_dataset = spark.read.option("header", "true").option("quote", "\"").option("escape", "\"").csv(S3_BUCKET_PATH + "final_list.csv")
details_dataset = spark.read.option("header", "true").option("quote", "\"").option("escape", "\"").csv(S3_BUCKET_PATH + "details.csv")
tags_dataset = spark.read.option("header", "true").csv(S3_BUCKET_PATH + "tags.csv")

# Join iniziali per avere i dati necessari (id, titolo, durata, data, tags) 
tedx_dataset_main = tedx_dataset.join(details_dataset, tedx_dataset.id == details_dataset.id, "left").drop(details_dataset.id)

# Aggregazione dei tag
tags_dataset_agg = tags_dataset.groupBy("id").agg(collect_list("tag").alias("tags"))

# Creazione del DataFrame di base con tutti i dati necessari 
base_talks_df = tedx_dataset_main.join(tags_dataset_agg, tedx_dataset_main.id == tags_dataset_agg.id, "left").drop(tags_dataset_agg.id)

print("Schema del DataFrame di base pronto per la generazione dei percorsi:")
base_talks_df.printSchema()


# 3. LOGICA DI GENERAZIONE DEI PERCORSI FORMATIVI

# temi principali dei path
main_tags_for_paths = ["Mars", "dark matter", "rocket science", "innovation", "exploration", "NASA", "Universe"]
all_paths_dfs = []

for tag in main_tags_for_paths:
    print(f"--> Elaborazione percorsi per il tag: '{tag}'")
    
    talks_for_path = base_talks_df.filter(array_contains(col("tags"), tag))
    
    # Crea un percorso solo se ci sono almeno 3 talk sull'argomento
    if talks_for_path.count() >= 3:
        path_agg_df = talks_for_path.sort("publishedAt") \
            .agg(
                collect_list("id").alias("talk_ids"),
                _sum(col("duration").cast("long")).alias("total_duration_sec")
            )
        
        # Arricchisce il percorso con altri dati (titolo, descrizione, badge, ...)
        path_details_df = path_agg_df \
            .withColumn("path_title", concat(lit("Explore: "), initcap(lit(tag)))) \
            .withColumn("talks_count", size(col("talk_ids"))) \
            .withColumn("total_duration_minutes", _round(col("total_duration_sec") / 60)) \
            .withColumn("badge_to_unlock", concat(lit("Expert in "), initcap(lit(tag)))) \
            .withColumn("path_description", expr(f"concat('A path of ', talks_count, ' talk (', total_duration_minutes, ' min) to explore the theme of ''{tag}''.')")) \
            .withColumn("main_tag", lit(tag)) \
            .withColumn("_id", col("main_tag")) # Crea un _id univoco basato sul tag
            
        all_paths_dfs.append(path_details_df.drop("total_duration_sec"))
        print(f"    Percorso per '{tag}' creato con {talks_for_path.count()} talk.")

# Unisce tutti i DataFrame dei percorsi in un unico DataFrame finale 
if all_paths_dfs:
    # Part dal primo DataFrame e unisce gli altri
    final_paths_df = all_paths_dfs[0]
    for i in range(1, len(all_paths_dfs)):
        final_paths_df = final_paths_df.union(all_paths_dfs[i])
    
    print("\nSchema finale dei percorsi generati:")
    final_paths_df.printSchema()


# 4. SCRITTURA DEI PERCORSI SU MONGODB
    write_mongo_options = {
        "connectionName": "Mongodbatlas connection",
        "database": "unibg_tedx_2025",
        "collection": "learning_paths",  # <-- NOME DELLA NUOVA COLLEZIONE
        "ssl": "true",
        "ssl.domain_match": "false"
    }

    # Conversione e scrittura (con controllo duplicati nella colonna _id) 
    _id_counts = final_paths_df.groupBy("_id").count().filter(col("count") > 1)
    if _id_counts.count() > 0:
        print("ATTENZIONE: Trovati valori _id duplicati. La scrittura su MongoDB fallirà.")
        _id_counts.show()
    else:
        print("Nessun valore _id duplicato trovato. Procedo con la scrittura.")
        paths_dynamic_frame = DynamicFrame.fromDF(final_paths_df, glueContext, "paths_dynamic_frame")
        glueContext.write_dynamic_frame.from_options(
            frame=paths_dynamic_frame,
            connection_type="mongodb",
            connection_options=write_mongo_options
        )
        print(f"\n{final_paths_df.count()} percorsi formativi sono stati scritti con successo sulla collezione 'learning_paths'.")

else:
    print("\nNessun percorso formativo è stato generato (nessun tag ha prodotto abbastanza talk).")

# 5. COMPLETAMENTO JOB
job.commit()