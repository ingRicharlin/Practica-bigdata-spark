"""
Tarea 3 - Procesamiento Batch con Apache Spark (PySpark)
Estudiante: Ricardo Linares
Curso: Big Data - UNAD
"""

import matplotlib
matplotlib.use("Agg")  # Para guardar imágenes sin interfaz gráfica
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# 1. Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("Tarea3_Batch_Hurtos") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Ruta del archivo en HDFS
ruta_archivo = "hdfs://localhost:9000/Tarea3/hurtos.csv"

print("=" * 60)
print("1. CARGA DEL DATASET (COMO JSON)")
print("=" * 60)

# Cargar correctamente como JSON ya que el archivo tiene formato JSON
df = spark.read \
    .option("multiLine", "true") \
    .json(ruta_archivo)

print(f"Total de registros iniciales: {df.count()}")
print("Columnas detectadas:", df.columns)
df.printSchema()


print("=" * 60)
print("2. LIMPIEZA DE DATOS")
print("=" * 60)

df_limpio = df.dropna(how="all").dropDuplicates()
print(f"Total de registros después de limpiar nulos y duplicados: {df_limpio.count()}")


print("=" * 60)
print("3. ANÁLISIS EXPLORATORIO (EDA)")
print("=" * 60)

# 3.1 Top 10 Departamentos con más registros de hurtos
print("\n--- Top 10 Departamentos con más registros ---")
top_deptos = df_limpio.groupBy("departamento").count().orderBy(F.desc("count")).limit(10)
top_deptos.show(truncate=False)

pdf_deptos = top_deptos.toPandas()
fig, ax = plt.subplots(figsize=(10, 5))
ax.barh(pdf_deptos['departamento'], pdf_deptos['count'], color='#2E75B6')
ax.set_title("Top 10 Departamentos con Mayor Cantidad de Hurtos")
ax.set_xlabel("Número de Registros")
ax.set_ylabel("Departamento")
ax.invert_yaxis()
plt.tight_layout()
plt.savefig("graficas/top_departamentos.png")
plt.close()
print("   -> Gráfica guardada en graficas/top_departamentos.png")


# 3.2 Top 10 Municipios con más registros de hurtos
print("\n--- Top 10 Municipios con más registros ---")
top_muni = df_limpio.groupBy("municipio", "departamento").count().orderBy(F.desc("count")).limit(10)
top_muni.show(truncate=False)

pdf_muni = top_muni.toPandas()
fig, ax = plt.subplots(figsize=(10, 5))
ax.barh(pdf_muni['municipio'] + " (" + pdf_muni['departamento'] + ")", pdf_muni['count'], color='#C55A11')
ax.set_title("Top 10 Municipios con Mayor Cantidad de Hurtos")
ax.set_xlabel("Número de Registros")
ax.set_ylabel("Municipio")
ax.invert_yaxis()
plt.tight_layout()
plt.savefig("graficas/top_municipios.png")
plt.close()
print("   -> Gráfica guardada en graficas/top_municipios.png")


print("=" * 60)
print("4. GUARDADO DEL DATASET LIMPIO")
print("=" * 60)

ruta_salida = "hdfs://localhost:9000/Tarea3/limpio"
df_limpio.write.mode("overwrite").parquet(ruta_salida)
print(f"Dataset limpio guardado exitosamente en: {ruta_salida}")

spark.stop()
print("Proceso batch finalizado correctamente.")
