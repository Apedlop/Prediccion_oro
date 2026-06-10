# **Decisiones Técnicas**

## **1. Fuentes de datos**

Se han utilizado dos fuentes de datos complementarias: un conjunto de datos histórico obtenido de Kaggle y datos financieros obtenidos mediante la librería yFinance. La combinación de ambas fuentes permite disponer de un conjunto de datos más completo, aprovechando el histórico del CSV y la actualización continua proporcionada por yFinance.

## **2. Apache Kafka como mensajería**

Se ha utilizado Apache Kafka como sistema de mensajería para desacoplar la generación y el procesamiento de datos. Además, se han creado dos topics independientes (gold_csv y gold_yfinance) para mantener separadas ambas fuentes y facilitar la trazabilidad de la información durante todo el flujo de procesamiento.

## **3. Serialización JSON por registro en S3**

Los mensajes consumidos desde Kafka se almacenan en Amazon S3 en formato JSON. Esta decisión simplifica la implementación del sistema y permite conservar los datos originales antes de aplicar cualquier transformación o proceso de integración.

## **4. Doble almacenamiento (RDS + MongoDB)**

Se ha optado por utilizar dos bases de datos diferentes para trabajar con distintos modelos de almacenamiento. Los datos del CSV se almacenan en MariaDB debido a su estructura fija y tabular, mientras que los datos procedentes de yFinance se almacenan en MongoDB, cuya estructura documental ofrece una mayor flexibilidad para gestionar información dinámica.

## **5. Túnel SSH vía EC2 para conectar con RDS**

Para acceder a Amazon RDS desde el entorno local se ha utilizado un túnel SSH a través de una instancia EC2. Esta solución permitió establecer una conexión segura con la base de datos y facilitar la carga inicial de los datos.

## **6. AWS Glue para la unificación (ETL)**

AWS Glue se ha utilizado como herramienta ETL para extraer los datos almacenados en MariaDB y MongoDB, transformarlos y unificarlos en un único conjunto de datos. El resultado se almacena posteriormente en Amazon S3 para su consumo por el resto de componentes del sistema.

## **7. AWS Lambda para exponer los datos**

Se ha implementado una función AWS Lambda encargada de leer los datos procesados almacenados en S3 y exponerlos mediante una URL accesible a través de HTTP. Esto permite consultar la información sin necesidad de acceder directamente a los servicios de almacenamiento de AWS.

## **8. LSTM y BiLSTM para los modelos**

Para la predicción del precio del oro se han utilizado redes neuronales LSTM y BiLSTM, ya que son modelos especialmente adecuados para el análisis de series temporales. Estas arquitecturas permiten capturar relaciones temporales complejas en los datos y mejorar la capacidad predictiva frente a modelos tradicionales.