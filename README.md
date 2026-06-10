# **Predicción del Precio del Oro**

## **Introducción**

El precio del oro es un indicador económico ampliamente utilizado en mercados financieros, ya que suele considerarse un valor refugio en momentos de incertidumbre económica. Por este motivo, analizar su comportamiento y tratar de anticipar su evolución resulta un problema de gran interés tanto a nivel académico como profesional.

Este proyecto nace con el objetivo de construir un sistema completo capaz de recopilar datos desde distintas fuentes, integrarlos y transformarlos en información útil para su análisis y predicción. Más allá del modelo de Machine Learning, el foco principal del trabajo está en el diseño de toda la infraestructura de datos necesaria para que la información sea fiable, coherente y fácilmente accesible.

De esta forma, se busca simular un entorno real de ingeniería de datos, donde la información no proviene de un único origen, sino de múltiples sistemas que deben coordinarse entre sí para generar un dataset final de calidad.

## **Objetivos del sistema**

- Integrar datos de múltiples fuentes (CSV histórico y yFinance).
- Garantizar trazabilidad de los datos desde su origen.
- Almacenar datos en diferentes sistemas según su naturaleza.
- Unificar datos heterogéneos en una única fuente analítica.
- Exponer los datos procesados de forma accesible mediante API.
- Preparar un dataset final para modelos de Machine Learning.

## **Documentación técnica**
 
- [`docs/arquitectura.md`](docs/arquitectura.md) — Diagrama completo del sistema y descripción de cada componente
- [`docs/decisiones_tecnicas.md`](docs/decisiones_tecnicas.md) — Por qué se eligió cada tecnología y qué alternativas se descartaron
- [`docs/preparacion_datos.md`](docs/preparacion_datos.md) — Limpieza, normalización, feature engineering y partición train/test
- [`docs/despliegue.md`](docs/despliegue.md) — Guía detallada de despliegue con comandos listos para copiar