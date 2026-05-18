"""
MAIN PIPELINE - Proyecto Integración de Datos del Oro

Arquitectura:
CSV + yfinance
        ↓
Kafka Producer
        ↓
Kafka Topics
        ↓
Kafka Consumer
        ↓
AWS S3 (RAW)
        ↓
AWS Glue (ETL)
        ↓
S3 Processed
        ↓
MongoDB Atlas (opcional)

Requisitos:
- Kafka levantado en localhost:9092
- Topics creados:
    - gold_csv
    - gold_yfinance
- Bucket S3 existente
- Glue crawler y Glue job creados manualmente una vez

Autor: Proyecto Final
"""

# ─────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────
import os
import json
import time
import boto3

from dotenv import load_dotenv
from confluent_kafka import Producer, Consumer

import pandas as pd
import yfinance as yf
import numpy as np

from datetime import datetime, timezone
from pymongo import MongoClient

# ─────────────────────────────────────────────
# VARIABLES ENTORNO
# ─────────────────────────────────────────────
load_dotenv()

# AWS
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
SESSION_TOKEN = os.getenv("SESSION_TOKEN")
REGION = os.getenv("REGION")

BUCKET_NAME = os.getenv("S3_BUCKET")

# GLUE
GLUE_DATABASE = os.getenv("GLUE_DATABASE")
GLUE_CRAWLER = os.getenv("GLUE_CRAWLER")
GLUE_JOB = os.getenv("GLUE_JOB")

# KAFKA
KAFKA_BROKER = "localhost:9092"

TOPIC_CSV = "gold_csv"
TOPIC_YF = "gold_yfinance"

# MONGO
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

# CSV
CSV_PATH = os.getenv("CSV_PATH")

# ─────────────────────────────────────────────
# AWS SESSION
# ─────────────────────────────────────────────
session = boto3.session.Session(
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    aws_session_token=SESSION_TOKEN,
    region_name=REGION
)

s3 = session.client("s3")
glue = session.client("glue")

# ─────────────────────────────────────────────
# KAFKA PRODUCER
# ─────────────────────────────────────────────
producer = Producer({
    "bootstrap.servers": KAFKA_BROKER
})

# ─────────────────────────────────────────────
# KAFKA CONSUMER
# ─────────────────────────────────────────────
consumer = Consumer({
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": "gold-consumer-group",
    "auto.offset.reset": "earliest"
})

consumer.subscribe([TOPIC_CSV, TOPIC_YF])

# ─────────────────────────────────────────────
# LIMPIEZA JSON
# ─────────────────────────────────────────────
def clean_record(record):
    clean = {}

    for k, v in record.items():

        if isinstance(k, tuple):
            k = "_".join(map(str, k))

        if isinstance(v, (np.integer, np.floating)):
            v = float(v)

        if "Timestamp" in str(type(v)):
            v = str(v)

        clean[str(k)] = v

    return clean

# ─────────────────────────────────────────────
# PRODUCER CSV
# ─────────────────────────────────────────────
def publish_csv():

    print("\n[CSV] Publicando datos CSV...")

    df = pd.read_csv(CSV_PATH, sep=';')

    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    for _, row in df.iterrows():

        record = row.to_dict()

        record["source"] = "csv"
        record["ingested_at"] = datetime.now(
            timezone.utc
        ).isoformat()

        producer.produce(
            TOPIC_CSV,
            key="csv",
            value=json.dumps(record)
        )

    producer.flush()

    print("[CSV] Datos enviados a Kafka")

# ─────────────────────────────────────────────
# PRODUCER YFINANCE
# ─────────────────────────────────────────────
def publish_yfinance():

    print("\n[yfinance] Descargando datos del oro...")

    df = yf.download(
        "GC=F",
        start="2004-01-01",
        interval="1d"
    )

    if df.empty:
        print("[yfinance] No se obtuvieron datos")
        return

    # flatten columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            "_".join(col) if isinstance(col, tuple) else col
            for col in df.columns
        ]

    df = df.reset_index()

    for _, row in df.iterrows():

        record = row.to_dict()

        record = clean_record(record)

        record["source"] = "yfinance"
        record["ticker"] = "GC=F"

        record["ingested_at"] = datetime.now(
            timezone.utc
        ).isoformat()

        producer.produce(
            TOPIC_YF,
            key="yfinance",
            value=json.dumps(record)
        )

    producer.flush()

    print("[yfinance] Datos enviados a Kafka")

# ─────────────────────────────────────────────
# GUARDAR EN S3
# ─────────────────────────────────────────────
def save_to_s3(topic, message):
    timestamp = datetime.now(timezone.utc).isoformat()

    key = f"{topic}/{timestamp}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(message)
    )

    print(f"[S3] Guardado: {key}")

# ─────────────────────────────────────────────
# CONSUMER LOOP
# ─────────────────────────────────────────────
def run_consumer():

    print("\n[Consumer] Escuchando Kafka...")

    try:

        TOTAL_CSV = 5531
        TOTAL_YF = 5620

        # contadores
        csv_count = 0
        yf_count = 0

        while True:

            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"[ERROR] {msg.error()}")
                continue

            topic = msg.topic()

            value = json.loads(
                msg.value().decode("utf-8")
            )

            # contar mensajes
            if topic == "gold_csv":
                csv_count += 1

            elif topic == "gold_yfinance":
                yf_count += 1

            print(
                f"[KAFKA] recibido de {topic} | "
                f"CSV={csv_count} | YF={yf_count}"
            )

            save_to_s3(topic, value)

            # condición de parada
            # cambia los números por la cantidad real esperada
            if csv_count >= TOTAL_CSV and yf_count >= TOTAL_YF:
                print("\n[INFO] Todos los datos procesados")
                break

        consumer.close()

    except KeyboardInterrupt:
        print("\n[INFO] Cerrando consumer...")

    finally:
        consumer.close()

# ─────────────────────────────────────────────
# GLUE CRAWLER
# ─────────────────────────────────────────────
def run_glue_crawler():

    print("\n[Glue] Ejecutando crawler...")

    try:
        glue.start_crawler(Name=GLUE_CRAWLER)
        print("[Glue] Crawler iniciado")

    except Exception as e:
        print(f"[Glue] Error crawler: {e}")


# ─────────────────────────────────────────────
# GLUE JOB + WAIT
# ─────────────────────────────────────────────
def wait_for_glue_job(job_run_id):

    print("\n[Glue] Esperando finalización...")

    while True:

        response = glue.get_job_run(
            JobName=GLUE_JOB,
            RunId=job_run_id
        )

        status = response["JobRun"]["JobRunState"]

        print("[Glue] Estado:", status)

        if status == "SUCCEEDED":
            print("[Glue] ETL OK")
            break

        if status in ["FAILED", "STOPPED", "TIMEOUT"]:
            raise Exception(f"Glue falló: {status}")

        time.sleep(15)


def run_glue_job():

    print("\n[Glue] Ejecutando ETL Job...")

    try:

        response = glue.start_job_run(JobName=GLUE_JOB)
        job_run_id = response["JobRunId"]

        print("[Glue] Job ID:", job_run_id)

        wait_for_glue_job(job_run_id)

    except Exception as e:
        print("[Glue] Error job:", e)


# ─────────────────────────────────────────────
# S3 → MONGODB
# ─────────────────────────────────────────────
def upload_processed_to_mongodb():

    print("\n[MongoDB] Cargando datos desde S3...")

    try:

        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]

        collection.delete_many({})

        prefix = "processed/gold_unified/"

        response = s3.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=prefix
        )

        if "Contents" not in response:
            print("[MongoDB] No hay datos procesados")
            return

        total = 0

        for obj in response["Contents"]:

            key = obj["Key"]

            if key.endswith("/"):
                continue

            print("[MongoDB] leyendo:", key)

            file_obj = s3.get_object(
                Bucket=BUCKET_NAME,
                Key=key
            )

            content = file_obj["Body"].read().decode("utf-8")

            lines = content.strip().split("\n")

            docs = []

            for line in lines:
                try:
                    docs.append(json.loads(line))
                except:
                    pass

            if docs:
                collection.insert_many(docs)
                total += len(docs)

        print(f"[MongoDB] Insertados: {total}")

    except Exception as e:
        print("[MongoDB] Error:", e)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":

    print("=" * 60)
    print("PIPELINE ORO - FINAL")
    print("=" * 60)

    # 1. PRODUCER
    # publish_csv()
    # publish_yfinance()

    # 2. CONSUMER -> S3
    # run_consumer()

    # 3. GLUE CRAWLER
    run_glue_crawler()
    time.sleep(10)

    # 4. GLUE ETL
    run_glue_job()

    # 5. S3 → MONGODB
    upload_processed_to_mongodb()

    print("\n[INFO] PIPELINE COMPLETADO")