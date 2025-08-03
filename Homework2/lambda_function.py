import json
import logging
import boto3
import os

# 1. CONFIGURAZIONE

# Imposta il logger per visualizzare informazioni utili nei log di CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Inizializza il client per il servizio AWS Glue.
try:
    client = boto3.client('glue')
except Exception as e:
    logger.error("Errore durante l'inizializzazione del client boto3 per Glue.")
    logger.error(e)
    # Gestione dell'errore di inizializzazione
    
# NOME DEL JOB DA AVVIARE
GLUE_JOB_NAME = "BeyondTEDx_Generate_Learning_Path" 


# 2. GESTORE DELLA LAMBDA

def lambda_handler(event, context):
    logger.info(f"## TRIGGER RICEVUTO ##")
    logger.info(f"Tentativo di avvio del job Glue: '{GLUE_JOB_NAME}'")
    
    try:
        # Avvia l'esecuzione del job specificato.
        response = client.start_job_run(JobName=GLUE_JOB_NAME)
        
        run_id = response['JobRunId']
        
        logger.info(f"## SUCCESSO ##")
        logger.info(f"Job '{GLUE_JOB_NAME}' avviato con successo.")
        logger.info(f"Job Run ID: {run_id}")
        
        # Restituisce una risposta positiva.
        return {
            'statusCode': 200,
            'body': json.dumps(f"Job {GLUE_JOB_NAME} avviato. Run ID: {run_id}")
        }
        
    except Exception as e:
        # Se qualcosa va storto durante l'avvio del job (es. job non trovato, ruolo con permessi mancanti),
        # logga l'errore in modo dettagliato.
        logger.error(f"## FALLIMENTO ##")
        logger.error(f"Impossibile avviare il job '{GLUE_JOB_NAME}'. Dettagli errore:")
        logger.error(str(e))
        
        raise e