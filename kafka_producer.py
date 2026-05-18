"""
PRODUCER - Ingesta de datos en Kafka
Fuentes:
  - CSV local con datos históricos del oro
  - yfinance con datos del oro (GC=F)

Topics:
  - gold_csv
  - gold_yfinance
"""

import json
import pandas as pd
import yfinance as yf
import numpy as np
from datetime import datetime, timezone
from confluent_kafka import Producer

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"
TOPIC_CSV = "gold_csv"
TOPIC_YF = "gold_yfinance"
CSV_PATH = r"data\gold_1d_data.csv"

# ─────────────────────────────────────────────
# PRODUCER
# ─────────────────────────────────────────────
producer = Producer({
    "bootstrap.servers": KAFKA_BROKER
})

print(f"[INFO] Conectado a Kafka en {KAFKA_BROKER}")

# ─────────────────────────────────────────────
# FUNCIÓN LIMPIEZA (CRÍTICA)
# ─────────────────────────────────────────────
def clean_record(record):
    clean = {}

    for k, v in record.items():

        # arreglar keys tipo tuple
        if isinstance(k, tuple):
            k = "_".join(map(str, k))

        # numpy types
        if isinstance(v, (np.integer, np.floating)):
            v = float(v)

        # pandas timestamp
        if "Timestamp" in str(type(v)):
            v = str(v)

        clean[str(k)] = v

    return clean

# ─────────────────────────────────────────────
# CSV
# ─────────────────────────────────────────────
def publish_csv():
    df = pd.read_csv(CSV_PATH)

    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    print(f"[CSV] {len(df)} registros")

    for _, row in df.iterrows():
        record = row.to_dict()

        record["source"] = "csv"
        record["ingested_at"] = datetime.now(timezone.utc).isoformat()

        producer.produce(
            TOPIC_CSV,
            key="csv",
            value=json.dumps(record)
        )

    producer.flush()
    print("[CSV] enviado a Kafka")

# ─────────────────────────────────────────────
# YFINANCE
# ─────────────────────────────────────────────
def publish_yfinance():
    df = yf.download(
        "GC=F",
        start="2004-01-01",
        interval="1d"
    )

    if df.empty:
        print("[yfinance] sin datos")
        return

    # flatten columns SIEMPRE
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            "_".join(col) if isinstance(col, tuple) else col
            for col in df.columns
        ]

    df = df.reset_index()

    print(f"[yfinance] {len(df)} registros")

    for _, row in df.iterrows():
        record = row.to_dict()

        record = clean_record(record)

        record["source"] = "yfinance"
        record["ticker"] = "GC=F"
        record["ingested_at"] = datetime.now(timezone.utc).isoformat()

        producer.produce(
            TOPIC_YF,
            key="yfinance",
            value=json.dumps(record)
        )

    producer.flush()
    print("[yfinance] enviado a Kafka")

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":

    publish_csv()
    publish_yfinance()

    print("[INFO] Producer finalizado correctamente")