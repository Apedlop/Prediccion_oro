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

import mysql.connector
from botocore.exceptions import ClientError

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
rds = session.client("rds")

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
# AWS RDS
# ─────────────────────────────────────────────

def create_rds_instance():
    try:
        print("Comprobando si la instancia RDS ya existe...")
        info = rds.describe_db_instances(
            DBInstanceIdentifier=os.getenv("DB_INSTANCE_ID")
        )
        print("Instancia encontrada.")
    except ClientError as e:
        if "DBInstanceNotFound" in str(e):
            print("Creando instancia RDS...")

            rds.create_db_instance(
                DBInstanceIdentifier=os.getenv("DB_INSTANCE_ID"),
                AllocatedStorage=20,
                DBInstanceClass="db.t4g.micro",
                Engine="mariadb",
                MasterUsername=os.getenv("DB_USER"),
                MasterUserPassword=os.getenv("DB_PASSWORD"),
                DBName=os.getenv("DB_NAME"),
                PubliclyAccessible=True
            )
        else:
            raise e

    waiter = rds.get_waiter('db_instance_available')
    waiter.wait(DBInstanceIdentifier=os.getenv("DB_INSTANCE_ID"))

    info = rds.describe_db_instances(
        DBInstanceIdentifier=os.getenv("DB_INSTANCE_ID")
    )

    endpoint = info['DBInstances'][0]['Endpoint']['Address']

    print(f"RDS disponible en endpoint: {endpoint}")
    return endpoint

def connect_to_rds(endpoint):
    # Ha estado dando error la conexión, asi que se ha hecho un tunel con EC2 para conectar el RDS localmente 
    config = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": "127.0.0.1",
        "port": 3306,
        "database": os.getenv("DB_NAME")
    }

    DB_NAME = os.getenv("DB_NAME")

    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor(dictionary=True)

    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    cursor.execute(f"USE {DB_NAME}")

    print(f"Conectado a la base de datos {DB_NAME}")

    return cnx, cursor

def create_gold_table(cursor, cnx):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold_prices (
            id            INT PRIMARY KEY AUTO_INCREMENT,
            date          VARCHAR(50),
            open          FLOAT,
            high          FLOAT,
            low           FLOAT,
            close         FLOAT,
            volume        FLOAT,
            source        VARCHAR(20),
            ingested_at   VARCHAR(50)
        )
    """)
    cnx.commit()
    print("[RDS] Tabla gold_prices lista")

def drop_table(cursor, cnx):

    cursor.execute("""
        DROP TABLE IF EXISTS gold_prices
    """)

    cnx.commit()

    print("[RDS] Tabla gold_prices eliminada")

def ingest_s3_data():

    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix="gold_csv/"
    )

    if "Contents" not in response:
        print("[S3] No hay archivos en gold_csv/")
        return []

    records = []

    for obj in response["Contents"]:

        key = obj["Key"]

        # Saltar carpetas
        if key.endswith("/"):
            continue

        print(f"[S3] Procesando: {key}")

        try:

            file_obj = s3.get_object(
                Bucket=BUCKET_NAME,
                Key=key
            )

            content = file_obj["Body"].read().decode("utf-8")

            data = json.loads(content)

            if "date;open;high;low;close;volume" in data:

                raw = data["date;open;high;low;close;volume"]

                values = raw.split(";")

                if len(values) != 6:
                    print(f"[S3] Formato CSV inválido: {key}")
                    continue

                record = {
                    "date": values[0],
                    "open": float(values[1]),
                    "high": float(values[2]),
                    "low": float(values[3]),
                    "close": float(values[4]),
                    "volume": float(values[5]),
                    "source": data.get("source"),
                    "ingested_at": data.get("ingested_at")
                }

            elif "date" in data:

                record = {
                    "date": data.get("date"),
                    "open": float(data.get("open", 0)),
                    "high": float(data.get("high", 0)),
                    "low": float(data.get("low", 0)),
                    "close": float(data.get("close", 0)),
                    "volume": float(data.get("volume", 0)),
                    "source": data.get("source"),
                    "ingested_at": data.get("ingested_at")
                }

            else:
                print(f"[S3] Formato desconocido: {key}")
                continue

            records.append(record)

        except Exception as e:
            print(f"[S3] Error procesando {key}: {e}")

    print(f"[S3] {len(records)} registros preparados")

    return records

def ingest_s3_data():

    paginator = s3.get_paginator("list_objects_v2")

    pages = paginator.paginate(
        Bucket=BUCKET_NAME,
        Prefix="gold_csv/"
    )

    records = []

    total_files = 0

    for page in pages:

        if "Contents" not in page:
            continue

        for obj in page["Contents"]:

            key = obj["Key"]

            # Saltar carpetas
            if key.endswith("/"):
                continue

            total_files += 1

            print(f"[S3] Procesando: {key}")

            try:

                file_obj = s3.get_object(
                    Bucket=BUCKET_NAME,
                    Key=key
                )

                content = file_obj["Body"].read().decode("utf-8")

                data = json.loads(content)

                if "date;open;high;low;close;volume" in data:

                    raw = data["date;open;high;low;close;volume"]

                    values = raw.split(";")

                    if len(values) != 6:
                        print(f"[S3] Formato inválido: {key}")
                        continue

                    record = {
                        "date": values[0],
                        "open": float(values[1]),
                        "high": float(values[2]),
                        "low": float(values[3]),
                        "close": float(values[4]),
                        "volume": float(values[5]),
                        "source": data.get("source"),
                        "ingested_at": data.get("ingested_at")
                    }

                elif "date" in data:

                    record = {
                        "date": data.get("date"),
                        "open": float(data.get("open", 0)),
                        "high": float(data.get("high", 0)),
                        "low": float(data.get("low", 0)),
                        "close": float(data.get("close", 0)),
                        "volume": float(data.get("volume", 0)),
                        "source": data.get("source"),
                        "ingested_at": data.get("ingested_at")
                    }

                else:
                    print(f"[S3] Formato desconocido: {key}")
                    continue

                records.append(record)

            except Exception as e:
                print(f"[S3] Error procesando {key}: {e}")

    print(f"\n[S3] Archivos procesados: {total_files}")
    print(f"[S3] Registros preparados: {len(records)}")

    return records

def insert_data(cursor, cnx, records):

    print(f"\n[RDS] Insertando {len(records)} registros...")

    sql = """
        INSERT INTO gold_prices
        (
            date,
            open,
            high,
            low,
            close,
            volume,
            source,
            ingested_at
        )
        VALUES
        (
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
    """

    rows = []

    for record in records:

        rows.append((
            record["date"],
            record["open"],
            record["high"],
            record["low"],
            record["close"],
            record["volume"],
            record["source"],
            record["ingested_at"]
        ))

    cursor.executemany(sql, rows)

    cnx.commit()

    print(f"[RDS] {cursor.rowcount} filas insertadas correctamente")

def csv_to_rds():

    print("\n[RDS] Cargando S3 → RDS...")

    try:

        endpoint = create_rds_instance()

        cnx, cursor = connect_to_rds(endpoint)

        create_gold_table(cursor, cnx)

        # INGESTA DESDE S3
        records = ingest_s3_data()

        # INSERT EN RDS
        if records:
            insert_data(cursor, cnx, records)
        else:
            print("[RDS] No hay datos para insertar")

        cursor.close()
        cnx.close()

        print("[RDS] Proceso completado")

    except Exception as e:
        print(f"[RDS] Error: {e}")

# ─────────────────────────────────────────────
# MONGODB
# ─────────────────────────────────────────────

def ingest_yfinance_to_mongo():

    paginator = s3.get_paginator("list_objects_v2")

    pages = paginator.paginate(
        Bucket=BUCKET_NAME,
        Prefix="gold_yfinance/"
    )

    records = []

    total_files = 0

    for page in pages:

        if "Contents" not in page:
            continue

        for obj in page["Contents"]:

            key = obj["Key"]

            # Saltar carpetas
            if key.endswith("/"):
                continue

            total_files += 1

            print(f"[S3] Procesando: {key}")

            try:

                file_obj = s3.get_object(
                    Bucket=BUCKET_NAME,
                    Key=key
                )

                content = file_obj["Body"].read().decode("utf-8")

                data = json.loads(content)

                # =================================================
                # NORMALIZAR NOMBRES DE COLUMNAS
                # =================================================

                record = {

                    "date": data.get("Date"),

                    "open": float(
                        data.get("Open_GC=F") or 0
                    ),

                    "high": float(
                        data.get("High_GC=F") or 0
                    ),

                    "low": float(
                        data.get("Low_GC=F") or 0
                    ),

                    "close": float(
                        data.get("Close_GC=F") or 0
                    ),

                    "volume": float(
                        data.get("Volume_GC=F") or 0
                    ),

                    "source": data.get("source"),

                    "ticker": data.get("ticker"),

                    "ingested_at": data.get("ingested_at")
                }

                records.append(record)

            except Exception as e:

                print(f"[S3] Error procesando {key}: {e}")

    print(f"\n[S3] Archivos procesados: {total_files}")
    print(f"[S3] Registros preparados: {len(records)}")

    return records


def insert_to_mongo(records):

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    if not records:
        print("[MONGO] No hay datos para insertar")
        return

    result = collection.insert_many(records)

    print(f"[MONGO] Insertados {len(result.inserted_ids)} registros")

def drop_mongo_collection():

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    db.drop_collection("gold_yfinance")

    print("[MONGO] Colección gold_yfinance eliminada")

def yfinance_to_mongo():

    print("\n[MONGO] Iniciando ingesta S3 → MongoDB")

    # drop_mongo_collection()

    records = ingest_yfinance_to_mongo()
    print(f"[DEBUG] records recibidos: {len(records)}")

    insert_to_mongo(records)

    print("[MONGO] Proceso completado")

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

    # 3. RDS
    #csv_to_rds()

    # MONGODB
    yfinance_to_mongo()

    # 3. GLUE CRAWLER
    # run_glue_crawler()
    # time.sleep(10)

    # 4. GLUE ETL
    # run_glue_job()

    # 5. S3 → MONGODB
    # upload_processed_to_mongodb()

    print("\n[INFO] PIPELINE COMPLETADO")