# **Arquitectura del sistema**

## **Introducción**

Esta arquitectura tiene como objetivo integrar datos históricos y datos en tiempo real relacionados con el precio del oro, procesarlos y generar un conjunto de datos unificado, para poder usarlo en tareas de análisis y predicción.

El sistema se compone por diferenctes servicios distribuidos que permiten la ingesta, almacenamiento, procesamiento e integración de datos utilizando tecnologías de streaming, almacenamiento en la nube y servicios gestionados en AWS.

## **Flujo de Datos**

El flujo de procesamiento comienza con dos fuentes de datos:

- Un conjunto de datos histórico (2004 - 2026) en formato CSV.
- Datos financieros obtenidos dinámicamente mediente la librería yFinance.

Ambas fuentes son enviadas a Apache Kafka mediante un productor (Producer), que publica la información en dos tópicos independientes:

- *gold_csv*
- *gold_yfinance*

Posteriormente, un consumidor (Consumer) lee los mensajes publicados para verificar que la información ha sido generada y distribuida correctamente.

Una vez validada la recepción de los datos, éstos se almacenan en Amazon S3, manteniendo la separación entre ambas funtes mediante dos directorios independientes: 

- *gold_csv/*
- *gold_yfinance/*

## **Almacenamiento de Datos**

Tras la carga inicial en Amazon S3, cada conjunto de datos se almacena en una base de datos distinta: 

- **CSV:** Los datos procedentes del archivo CSV son almacenados en Amazon RDS utilizando MariaDB como sistema gestor de bases de datos relacional.
- **yFinance:** Los datos obtenidos mediante la yFinance son almacenando en MongoDB, una base de datos no relacional.

## **Integración y Procesamiento**

La integración de ambas fuentes se realiza mediante AWS Glue. 

Este servio se encarga de:

1. Leer los datos almacenados en MariaDB
2. Leer los datos almacenados en MongoDB.
3. Aplicar las trnasformaciones necesarias.
4. Unificar ambas fuentes en un único conjunto de datos.
5. Almacenar el reusltado final en Amazon S3.

El resultado se guarda dentro de un directorio de S3: *processed/*

## **Exposición de Datos**

Una función de AWS Lambda se encarga de acceder al archivo procesado almacenado en S3.

La función realiza las siguientes tareas: 

1. Localizar el archivo geenrado por AWS Glue.
2. Generar URL de acceso.
3. Permitir consumo del dataset unificado desde aplicaciones externar o procesos de análisis.

De esta manera se separa el almacenamiento del consumo de datos, facilitando la escalabilidad y el acceso controlado a la infromación procesada.

## **Diagraa de la Arquitectura**

```
 [CSV local]         [yfinance (GC=F)]
      │                     │
      └────────┬────────────┘
               ▼
       ┌───────────────┐
       │ Kafka Producer│  (confluent_kafka)
       └───────┬───────┘
               │
       ┌───────▼────────┐
       │  Kafka Topics  │
       │  ┌───────────┐ │
       │  │ gold_csv  │ │
       │  │gold_yfinance│
       │  └───────────┘ │
       └───────┬────────┘
               │
       ┌───────▼───────┐
       │ Kafka Consumer│  (+ validación de recuento)
       └───────┬───────┘
               │
       ┌───────▼─────────┐
       │   AWS S3 RAW    │  s3://bucket-gold-data/
       │  ┌───────────┐  │
       │  │ gold_csv/ │  │   ← JSON por registro
       │  │gold_yfinance/│
       │  └───────────┘  │
       └──┬──────────┬───┘
          │          │
   ┌──────▼──┐   ┌───▼─────────┐
   │AWS RDS  │   │ MongoDB     │
   │(MariaDB)│   │  Atlas      │
   │gold_csv │   │gold_yfinance│
   └──────┬──┘   └───┬─────────┘
          │          │
          └────┬─────┘
               │
       ┌───────▼───────┐
       │   AWS Glue    │  (PySpark ETL)
       │  job-gold     │  Union vertical + limpieza
       └───────┬───────┘
               │
       ┌───────▼───────┐
       │  S3 Processed │  s3://bucket-gold-data/processed/
       │  (Parquet)    │
       └───────┬───────┘
               │
       ┌───────▼───────┐
       │  AWS Lambda   │  Lee Parquet → devuelve    JSON por URL
       └───────┬───────┘
               │
       ┌───────▼───────┐
       │  Cliente local│  url_lambda.py / notebooks
       │ (pandas + URL)│
       └───────────────┘
               │
       ┌───────▼───────┐
       │ Modelos ML    │  LSTM / BiLSTM (Keras)
       └───────────────┘
```

## **Tecnologías Utilizadas**

| Componente | Tecnología | 
|------------|------------|
| Streaming de datos | Apache Kafka |
| Productor de eventos | Kafka Producer |
| Consumidor de eventos | Kafka Consumer | 
| Alacenamiento en la nube | Amazon S3 |
| Base de datos relacional | MariaDB (Amazon RDS) |
| Base de datos NoSQL | MongoDB | 
| Procesamiento ETL | AWS Glue |
| Computación Serverless | AWS Lambda |
| Obtención de datos financieros | yFinace |
| Lenguaje principal | Python |
