# **Preparación de Datos**

## **1. Fuentes Originales**
 
#### **1.1 CSV Histórico**
 
- **Origen**: Kaggle
- **Formato**: semicolon-separated (`;`), cabecera en la primera fila
- **Periodo**: 11/06/2004 – presente (~5531 registros)
- **Columnas originales**: `Date`, `Open`, `High`, `Low`, `Close`, `Volume`
- **Particularidades**:
  - Separador decimal: punto (`.`)
  - Fecha con formato `YYYY.MM.DD HH:MM`
  - `Volume` en unidades de contratos (no dólares)

#### **1.2 yFinance**
 
- **Origen**: Yahoo Finance vía librería `yfinance`
- **Periodo**: 01/01/2004 – fecha de ejecución (~5620 registros)
- **Columnas originales**: MultiIndex con sufijo del ticker: `Open_GC=F`, `High_GC=F`, `Low_GC=F`, `Close_GC=F`, `Volume_GC=F`, `Date`
- **Particularidades**:
  - El MultiIndex de columnas debe aplanarse antes de serializar a JSON
  - Algunos registros tienen `Volume = 0` (días sin actividad registrada)
 
## **2. Limpieza en el Producer (Kafka)**
 
Antes de publicar en Kafka, el producer aplica la función `clean_record()`:
 
1. **Claves tipo tupla**: las columnas MultiIndex de yfinance se convierten a string (`"open_gc=f"`)
2. **Tipos numpy**: `np.integer` y `np.floating` se convierten a `float` nativo de Python para permitir la serialización JSON
3. **Timestamps pandas**: se convierten a string ISO-8601
4. **Metadatos añadidos**: campo `source` (`"csv"` o `"yfinance"`) y `ingested_at` (timestamp UTC en el momento de publicación)
 
## **3. Normalización en AWS Glue**
 
El job PySpark (`script_etl_job.py`) normaliza los esquemas antes de unificar:
 
| Campo final | Origen CSV | Origen yfinance |
|-------------|-----------|-----------------|
| `date` | `date` | `date` |
| `open` | `open` (DOUBLE) | `open_gc=f` (DOUBLE) |
| `high` | `high` (DOUBLE) | `high_gc=f` (DOUBLE) |
| `low` | `low` (DOUBLE) | `low_gc=f` (DOUBLE) |
| `close` | `close` (DOUBLE) | `close_gc=f` (DOUBLE) |
| `volume` | `volume` (DOUBLE) | `volume_gc=f` (DOUBLE) |
| `source` | `"csv"` | `"yfinance"` |
| `ingested_at` | timestamp ingestión | timestamp ingestión |
 
## **4. Dataset Procesado Final**
 
- **Fichero**: `datasets/processed/datos_precio_oro.csv`
- **Registros**: ~11501 filas (unión de ambas fuentes)
- **Columnas**: `date`, `open`, `high`, `low`, `close`, `volume`, `source`, `ingested_at`
- **Rango temporal**: 2004-01-26 hasta 2026
 
## **5. Preparación para Modelado**
 
#### **5.1 Selección de Variable Objetivo**
 
Se utiliza `close` como variable objetivo de predicción.
 
#### **5.2 Transformación (recomendada)**

Para abordar la no-estacionariedad del precio del oro se recomienda transformar el cierre a **retornos logarítmicos**:
 
```python
df['log_return'] = np.log(df['close'] / df['close'].shift(1))
df = df.dropna()
```
 
Esto convierte la serie de niveles absolutos (no estacionaria) en una serie de cambios relativos (aproximadamente estacionaria), lo que mejora la convergencia y generalización de los modelos LSTM.
 
#### **5.3 Escalado**
 
Se aplica `MinMaxScaler` (rango [0, 1]) sobre la variable transformada:
 
```python
from sklearn.preprocessing import MinMaxScaler
scaler = MinMaxScaler()
data_scaled = scaler.fit_transform(df[['log_return']])
```
 
El scaler se persiste en `datasets/train_test/scaler.pkl` para poder invertir la transformación al evaluar predicciones.
 
#### **5.4 Creación de Secuencias**
 
Se construyen secuencias de longitud `window_size` (típicamente 60 días) para alimentar las capas LSTM:
 
```python
X, y = [], []
for i in range(window_size, len(data_scaled)):
    X.append(data_scaled[i-window_size:i, 0])
    y.append(data_scaled[i, 0])
X, y = np.array(X), np.array(y)
X = X.reshape(X.shape[0], X.shape[1], 1)
```
 
#### **5.5 Split Temporal**
 
La división train/test es estrictamente temporal (sin shuffle) para evitar data leakage:
 
- **Train**: 80% de los registros más antiguos
- **Test**: 20% de los registros más recientes
Los arrays resultantes (`X_train`, `X_test`, `y_train`, `y_test`) y las fechas correspondientes (`dates_train`, `dates_test`) se guardan en `datasets/train_test/`.
 
## **6. Artefactos Generados**
 
| Fichero | Descripción |
|---------|-------------|
| `datasets/raw/gold_1d_data.csv` | CSV original sin modificar |
| `datasets/processed/datos_precio_oro.csv` | Dataset unificado descargado desde Lambda |
| `datasets/train_test/X_train.npy` | Secuencias de entrenamiento |
| `datasets/train_test/X_test.npy` | Secuencias de test |
| `datasets/train_test/y_train.npy` | Targets de entrenamiento |
| `datasets/train_test/y_test.npy` | Targets de test |
| `datasets/train_test/dates_train.npy` | Fechas asociadas al train |
| `datasets/train_test/dates_test.npy` | Fechas asociadas al test |
| `datasets/train_test/scaler.pkl` | MinMaxScaler ajustado |
| `datasets/train_test/data_scaled.pkl` | Serie escalada completa |