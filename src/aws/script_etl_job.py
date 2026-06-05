import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

# Inicialización de Glue y Spark
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ─────────────────────────────────────────────
# 1. LEER DATOS DE AWS RDS
# ─────────────────────────────────────────────
rds_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://gold-db.c72o00exslb2.us-east-1.rds.amazonaws.com:3306/gold_db") \
    .option("dbtable", "gold_prices") \
    .option("user", "admin") \
    .option("password", "admin123") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("ssl", "true") \
    .load()

# Seleccionar y ordenar columnas de RDS
rds_clean = rds_df.select(
    col("date"),
    col("open"),
    col("high"),
    col("low"),
    col("close"),
    col("volume"),
    col("source"),
    col("ingested_at")
)

# ─────────────────────────────────────────────
# 2. LEER DATOS DE MONGODB ATLAS
# ─────────────────────────────────────────────
mongo_options = {
    "connection.uri": "mongodb+srv://user:user@cluster0.xjujdqn.mongodb.net/",
    "database": "gold_db",
    "collection": "gold_collection",
    "ssl": "true",
    "ssl.domain_match": "true"
}

# CORREGIDO: Ahora pasa los argumentos correctamente sin duplicar 'options'
mongo_df = spark.read.format("mongodb") \
    .options(**mongo_options) \
    .load()

# Seleccionar y ordenar columnas de Mongo
mongo_clean = mongo_df.select(
    col("date"),
    col("open"),
    col("high"),
    col("low"),
    col("close"),
    col("volume"),
    col("source"),
    col("ingested_at")
)

# ─────────────────────────────────────────────
# 3. UNIFICACIÓN DE LOS DATOS (UNION)
# ─────────────────────────────────────────────
unified_df = rds_clean.union(mongo_clean)

# ─────────────────────────────────────────────
# 4. GUARDAR EL RESULTADO EN S3 PROCESADO
# ─────────────────────────────────────────────
s3_output_path = "s3://bucket-gold-data/processed/"

unified_df.coalesce(1).write \
    .mode("overwrite") \
    .parquet(s3_output_path)

print("[GLUE JOB] Unión vertical (Union) completada con éxito en S3 Processed")
job.commit()