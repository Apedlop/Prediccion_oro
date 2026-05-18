"""
AWS GLUE ETL SCRIPT
Unifica datos RAW del oro desde S3
y guarda resultado procesado en S3.

Entrada:
s3://BUCKET/raw/gold_csv/
s3://BUCKET/raw/gold_yfinance/

Salida:
s3://BUCKET/processed/gold_unified/
"""

from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit

# ─────────────────────────────────────────────
# SPARK / GLUE
# ─────────────────────────────────────────────
sc = SparkContext()
glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)
job.init("gold-etl-job", {})

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
BUCKET = "TU_BUCKET"

CSV_PATH = f"s3://{BUCKET}/raw/gold_csv/"
YF_PATH = f"s3://{BUCKET}/raw/gold_yfinance/"

OUTPUT_PATH = f"s3://{BUCKET}/processed/gold_unified/"

# ─────────────────────────────────────────────
# LEER JSON RAW
# ─────────────────────────────────────────────
csv_df = spark.read.json(CSV_PATH)

yf_df = spark.read.json(YF_PATH)

# ─────────────────────────────────────────────
# NORMALIZAR CSV
# ─────────────────────────────────────────────
csv_df = csv_df.select(

    col("date").alias("timestamp"),

    col("open").cast("double").alias("open"),

    col("high").cast("double").alias("high"),

    col("low").cast("double").alias("low"),

    col("close").cast("double").alias("close"),

    col("volume").cast("double").alias("volume"),

    col("source")
)

# ─────────────────────────────────────────────
# NORMALIZAR YFINANCE
# ─────────────────────────────────────────────
yf_df = yf_df.select(

    col("date").alias("timestamp"),

    col("open_gc=f").cast("double").alias("open"),

    col("high_gc=f").cast("double").alias("high"),

    col("low_gc=f").cast("double").alias("low"),

    col("close_gc=f").cast("double").alias("close"),

    col("volume_gc=f").cast("double").alias("volume"),

    col("source")
)

# ─────────────────────────────────────────────
# UNIFICAR
# ─────────────────────────────────────────────
final_df = csv_df.unionByName(yf_df)

# ─────────────────────────────────────────────
# ELIMINAR NULOS
# ─────────────────────────────────────────────
final_df = final_df.dropna()

# ─────────────────────────────────────────────
# ELIMINAR DUPLICADOS
# ─────────────────────────────────────────────
final_df = final_df.dropDuplicates()

# ─────────────────────────────────────────────
# MOSTRAR SCHEMA
# ─────────────────────────────────────────────
final_df.printSchema()

# ─────────────────────────────────────────────
# MOSTRAR EJEMPLOS
# ─────────────────────────────────────────────
final_df.show(5)

# ─────────────────────────────────────────────
# GUARDAR PROCESADO
# ─────────────────────────────────────────────
final_df.write \
    .mode("overwrite") \
    .json(OUTPUT_PATH)

print("\n[INFO] Datos procesados guardados en:")
print(OUTPUT_PATH)

# ─────────────────────────────────────────────
# FINALIZAR JOB
# ─────────────────────────────────────────────
job.commit()