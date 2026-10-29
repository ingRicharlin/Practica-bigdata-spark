# Tarea 3 - Procesamiento de Datos con Apache Spark

Curso Big Data (202016911) - UNAD. Procesamiento en batch con PySpark y procesamiento en tiempo real con Kafka + Spark Streaming.

## 1. Descripcion del problema y del dataset

- **Problema:** Analizar el comportamiento de los hurtos en Colombia para identificar las modalidades más frecuentes, las zonas o departamentos con mayor incidencia delictiva y la evolución     temporal de los casos reportados.
- **Dataset:** Reporte de Hurtos
- **Fuente:** Portal de Datos Abiertos de Colombia  https://www.datos.gov.co/Seguridad-y-Defensa/HURTO-PERSONAS/4rxi-8m8d/about_data
- **Tamano y columnas principales:** Conjunto de datos en formato CSV con miles de registros. Las columnas principales incluyen la fecha del hecho, la modalidad del hurto, el departamento, el municipio y las variables cuantitativas asociadas al conteo de casos
### Ejecución del procesamiento Batch

1. Asegúrate de crear el directorio necesario para almacenar las evidencias gráficas del EDA:
   ```bash
   mkdir -p graficas
## 2. Procesamiento en batch (Spark + Python)

Archivo: `batch_analysis.py` (recibe opcionalmente la ruta del CSV; por defecto lee `hdfs://localhost:9000/Practica/hurtos.csv`)

Que hace:
1. Carga el CSV desde HDFS.
2. Limpieza: [nulos, tipos de datos, duplicados, ...]
3. Analisis exploratorio: [estadisticas, agrupaciones, top N, ...]
4. Visualizacion: [graficas generadas y donde se guardan]
5. EN CASO DE USAR LA API LOS DATOS SE GUARDAN EN JASON

### Requisitos
- Ubuntu Server 22.04 con Java 11
- Hadoop 3.3.6 y Apache Spark 3.5.2
- Python 3 con `pyspark==3.5.2` (debe coincidir con la version de Spark instalada)
- `matplotlib` para las graficas: `pip3 install --user matplotlib`

### Como ejecutarlo
```bash
# 1. Iniciar Hadoop
start-all.sh

# 2. Subir el dataset a HDFS
hdfs dfs -mkdir -p /Tarea3
hdfs dfs -put LODATOSAQUI.csv /Tarea3

# 3. Ejecutar el analisis
python3 batch_analysis.py
```

Las graficas (PNG) y el resumen del analisis (`resumen_eda.txt`) quedan en la carpeta `graficas/`, y el dataset limpio se guarda en HDFS en formato Parquet (`/Practica/limpio`).

## 3. Procesamiento en tiempo real (Kafka + Spark Streaming)

Archivos: `kafka_producer.py` y `spark_streaming_consumer.py`

- El productor genera lecturas simuladas de sensores cada segundo y las publica en el topic `sensor_data`.
- El consumidor lee el topic con Spark Structured Streaming y calcula promedios por sensor en ventanas de 1 minuto.

### Como ejecutarlo
```bash
# Instalar la libreria de Kafka para Python
pip3 install --user kafka-python

# Iniciar ZooKeeper y Kafka (Kafka 3.9.x descomprimido en ~/kafka)
~/kafka/bin/zookeeper-server-start.sh -daemon ~/kafka/config/zookeeper.properties
sleep 10
~/kafka/bin/kafka-server-start.sh -daemon ~/kafka/config/server.properties
sleep 10

# Crear el topic
~/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --replication-factor 1 --partitions 1 --topic sensor_data

# Terminal 1: productor
python3 kafka_producer.py

# Terminal 2: consumidor el paquete debe coincidir con tu version de Spark
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 spark_streaming_consumer.py
```

El estado del trabajo se puede ver en `http://localhost:4040` Spark UI.

## 4. Autor

Ricardo Linares - Ingenieria de Sistemas, UNAD
