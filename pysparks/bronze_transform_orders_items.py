import os
from pyspark.sql import SparkSession

# Inicializar uma sessão Spark
spark = SparkSession.builder.appName("BronzeTransform_OrdersItems").getOrCreate()

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#caminho do arquivo .json 
orders_items_path = os.path.join(base_path, "lakehouse/landing/order_items.json")
#caminho do arquivo para salvar o arquivo parquet
bronze_path_orders_items = os.path.join(base_path, "lakehouse/bronze/orders_items")

orders_items_bronze = spark.read.json(orders_items_path)
orders_items_bronze.write.mode("overwrite").parquet(bronze_path_orders_items)

spark.stop()
