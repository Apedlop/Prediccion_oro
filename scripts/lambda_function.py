import json
import boto3
import io
import pandas as pd

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    BUCKET_NAME = "bucket-gold-data" 
    PREFIX = "processed/"
    
    try:
        # 1. Buscar el archivo Parquet dentro de la carpeta /processed/
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
        
        parquet_key = None
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                parquet_key = obj['Key']
                break
                
        if not parquet_key:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'No se encontro el archivo Parquet en S3. Revisa si el Glue Job termino.'})
            }
            
        # 2. Descargar y leer el archivo Parquet desde S3 hacia la memoria de Lambda
        s3_object = s3_client.get_object(Bucket=BUCKET_NAME, Key=parquet_key)
        parquet_data = s3_object['Body'].read()
        
        df = pd.read_parquet(io.BytesIO(parquet_data))
        
        # 3. Convertir los datos a formato JSON (texto) para que viajen por la URL
        json_data = df.to_json(orient='records')
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'  # Esto permite que tu PC local se conecte sin problemas de CORS
            },
            'body': json_data
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }