"""Consumidor con Spark Structured Streaming: lee el topic 'sensor_data' de Kafka
y calcula temperatura y humedad promedio por sensor en ventanas de 1 minuto."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType

spark = SparkSession.builder.appName("SensorDataAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Esquema de los mensajes JSON que envia el productor
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", FloatType()),
    StructField("humidity", FloatType()),
    StructField("timestamp", TimestampType()),
])

# Lectura continua desde Kafka
df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "sensor_data")
      .load())

# El valor llega como bytes: se convierte a texto y se parsea el JSON
parsed_df = (df.select(from_json(col("value").cast("string"), schema).alias("data"))
               .select("data.*"))

# Promedios por ventana de 1 minuto y por sensor
windowed_stats = (parsed_df
    .groupBy(window(col("timestamp"), "1 minute"), "sensor_id")
    .agg({"temperature": "avg", "humidity": "avg"}))

# Salida por consola
query = (windowed_stats.writeStream
         .outputMode("complete")
         .format("console")
         .start())

query.awaitTermination()
