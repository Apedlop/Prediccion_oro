# **Guía de Despliegue del Pipeline**

Esta guía describe los pasos necesarios para desplegar y ejecutar el pipeline completo desde la configuiración del entorno local hasta la ejecución de los modelos de predicción.

## **Requisitos Previos**

#### **Software local**
- Python 3.12.6
- Apache Kafka levantado en `localhost:9092`
- Acceso a AWS
- Encaso de usar RDS, tener un túnel SSH activo hacia EC2

#### **Dependencias Python**

```bash
pip install -r requirements.txt
```

## **1. Configuración del Entorno**

Copia `.env.example` a `.env` y rellena los valores:
 
```bash
cp .env.example .env
```
 
Variables requeridas:
 
```
ACCESS_KEY=<AWS Access Key ID>
SECRET_KEY=<AWS Secret Access Key>
SESSION_TOKEN=<AWS Session Token>   # Solo si usas credenciales temporales (AWS Academy)
REGION=us-east-1
 
S3_BUCKET=bucket-gold-data
 
GLUE_DATABASE=db_gold
GLUE_CRAWLER=crawler-gold
GLUE_JOB=job-gold
 
MONGO_URI=mongodb+srv://<user>:<password>@cluster0.xxx.mongodb.net/
MONGO_DB=gold_db
MONGO_COLLECTION=gold_collection
 
CSV_PATH=datasets/raw/gold_1d_data.csv
 
DB_USER=admin
DB_PASSWORD=<contraseña RDS>
DB_INSTANCE_ID=gold-db
DB_NAME=gold_db
DB_ENDPOINT=gold-db.xxxx.us-east-1.rds.amazonaws.com
 
URL_LAMBDA=https://<id>.lambda-url.us-east-1.on.aws/
```
 
> **Nota de seguridad**: nunca subas el fichero `.env` real a un repositorio público. El `.gitignore` ya lo excluye.

## **2. Levantar Kafka**

```bash
# En la terminal
docker run -d \
  --name kafka \
  -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  apache/kafka:latest
```

El topic se crea solo una vez iniciamos el Pipeline.

En caso de que quesamos ver que los datos se están introduciendo correctamente:

- Entramos en el contenedor 
```bash
docker exec -it kafka bash
```

- Entramos en la carpeta
```bash
cd /opt/kafka/bin/
```

- Iniciamos comando para ver el consumer
```bash
# Topic gold_csv
./kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic gold_csv \
  --from-beginning

# Topic gold_yfinance
./kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic gold_yfinance \
  --from-beginning
```

## **3. Ejecutar Pipeline Completo**

El script `scripts/run_pipeline.py` orquesta todas las fases. Para ejecutar fases individuales, descomenta/comenta las líneas del bloque `__main__`:
 
```bash
python scripts/run_pipeline.py
```
 
### Fases disponibles (en orden):
 
| Orden | Función | Descripción |
|-------|---------|-------------|
| 1 | `publish_csv()` | Lee CSV y publica en topic `gold_csv` |
| 2 | `publish_yfinance()` | Descarga de yfinance y publica en `gold_yfinance` |
| 3 | `run_consumer()` | Consume ambos topics y guarda en S3 |
| 4 | `csv_to_rds()` | Lee `gold_csv/` de S3 e inserta en RDS MariaDB |
| 5 | `yfinance_to_mongo()` | Lee `gold_yfinance/` de S3 e inserta en MongoDB |
| 6 | *(manual en AWS)* | Ejecutar Glue Job `job-gold` en la consola AWS |
| 7 | *(automático)* | Lambda genera el JSON disponible en la URL |
 
> Las fases 1-5 se pueden ejecutar localmente. Las fases 6 y 7 requieren acción en la consola de AWS.

## **4. Configurar el Túnel SSH para RDS**

RDS no es directamente accesible desde fuera de la VPC. Usa un túnel SSH a través de EC2:

- Crea en AWS EC2 una nueva instancia.
- Creala normalmente y cuando te pidan una key selecciona crear una nueva, y esa la descargas.
- Confirma que la instancia está pública.
- En una terminal ve al directorio donde tengas guardado el key que anteriormente descargaste.
- Ejecuta ese comando:
```bash
ssh -i <clave.pem> -L 3306:<endpoint_rds>:3306 ec2-user@<ip_publica_ec2> -N
```

Mientras este túnel esté activo, el pipeline puede conectarse a `127.0.0.1:3306` como si fuera el RDS local.

## **5. Ejecutar el Glue Job en AWS**

1. Accede a la consola AWS → **AWS Glue** → **ETL jobs**
2. Selecciona el job `job-gold`
3. En la parte de 'Script' pega el codigo que hay en `src/aws/script_etl_job.py` de estre proyecto
4. Pulsa *Run job*
5. Verifica que existe un fichero .parquet en `s3://bucket-gold-data/processed/`
6. Si está significa que todo a funcionado.

## **6. Crear la URL con Lambda**

1. Crea una nueva función
2. Deja la opción *Crear desde cero (Author from scrath)*
    2.1. Configura estos tres campos:
        - Nombre de la función: traer_datos_oro
        - Lenguaje: Elige la misma versión que tengas de Python (Ej: Python 3.12 el que tengo yo)
        - Arquitectura: x86_64
    2.2. Despliega la pestaña que dice Change default execution role (Cambiar rol predeterminado)
    2.3. Selecciona Use an existing role (Usar un rol existente)
    2.4. En el desplegable que aparece, busca y selecciona LabRole (el rol del laboratorio)
    2.5. Creamos la función
3. En la pestaña **Code** pegamos el codigo que está en `src\aws\lambda_function.py` de este proyecto
4. Añadimos la capa de Pandas:
    4.1. En la sección llamada Layers o Capas, hacemos clic en **Añadir una capa**
    4.2. Selecciona la opción 'AWS layers'
    4.3. En el desplegable de AWS layers, busca una que se llama `AWSSDKPandas-Python312` (o el número de Python que elegiste en el paso 1.1)
    4.4. En el desplegable de Version, elige la última versión (el número más alto que te aparezca)
    4.5. Añadimos la capa
5. En la pesaña **Configuración**, en la lista de opciones que hay a la izquierda, haz clic en Function URL
6. Crea una funcion URL
    6.1. En *Auth type* marna NONE, para que la URL sea pública
    6.2 Guarda
7. Ya tendrás el enlace creado

Para comprobar que el codigo funcione:
```python
import requests, pandas as pd
 
URL = "https://<id>.lambda-url.us-east-1.on.aws/"
df = pd.DataFrame(requests.get(URL).json())
print(df.shape)
```

## **7. Ejecutar los Notebooks de Modelado**

Los notebooks deben ejecutarse en orden:
 
```
notebooks/hito1.ipynb   → Visión del problema y contexto
notebooks/hito2.ipynb   → Obtención y almacenamiento de datos
notebooks/hito3.ipynb   → EDA y preparación de datos
notebooks/hito4.ipynb   → Modelado y validación (LSTM / BiLSTM)
```