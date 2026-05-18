import json
import boto3
import os
from dotenv import load_dotenv
from confluent_kafka import Consumer
from datetime import datetime, timezone

load_dotenv()

# ─────────────────────────────────────────────
# CONEXIÓN AWS (CORRECTA)
# ─────────────────────────────────────────────
session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION")
)

s3 = session.client("s3")
athena = session.client("athena")

BUCKET_NAME = os.getenv("S3_BUCKET")

# ─────────────────────────────────────────────
# KAFKA CONSUMER
# ─────────────────────────────────────────────
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "gold-consumer-group",
    "auto.offset.reset": "earliest"
})

TOPICS = ["gold_csv", "gold_yfinance"]
consumer.subscribe(TOPICS)

print("[INFO] Consumer conectado a Kafka")

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