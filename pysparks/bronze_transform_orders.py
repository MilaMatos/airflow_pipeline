from pyspark.sql import SparkSession
import os

# Inicializar uma sessão Spark
spark = SparkSession.builder.appName("BronzeTransform_Orders").getOrCreate()

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#caminho do arquivo .json 
orders_path = os.path.join(base_path, "lakehouse/landing/orders.json")
#caminho do arquivo para salvar o arquivo parquet
orders_bronze_path = os.path.join(base_path, "lakehouse/bronze/orders")

orders_bronze = spark.read.json(orders_path)
orders_bronze.write.mode("overwrite").parquet(orders_bronze_path)

spark.stop()