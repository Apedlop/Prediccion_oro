import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

# S3
csv_path = "s3://bucket-gold-data/gold_csv/"
yf_path ="s3://bucket-gold-data/gold_yfinance/"

# LEER JSON
df_csv = spark.read.json(csv_path)
df_yf = spark.read.json(yf_path)

# RENOMBRAR COLUMNAS CSV
df_csv = df_csv.select(
    col("Date").alias("date"),
    col("Open").alias("open"),
    col("High").alias("high"),
    col("Low").alias("low"),
    col("Close").alias("close"),
    col("Volume").alias("volume")
)

# RENOMBRAR COLUMNAS YFINANCE
df_yf = df_yf.select(
    col("Date").alias("date"),
    col("Open_GC=F").alias("open"),
    col("High_GC=F").alias("high"),
    col("Low_GC=F").alias("low"),
    col("Close_GC=F").alias("close"),
    col("Volume_GC=F").alias("volume")
)

# UNIÓN DATASETS
df_final = df_csv.unionByName(df_yf)

# eliminar duplicados
df_final = df_final.dropDuplicates()

# generar 1 solo archivo
df_final = df_final.coalesce(1)

# OUTPUT S3
output_path = "s3://bucket-gold-data/processed/gold_unified/"

df_final.write \
    .mode("overwrite") \
    .json(output_path)

print("ETL COMPLETADO")

job.commit()