from pyspark.sql import SparkSession
import os

# Inicializar uma sessão Spark
spark = SparkSession.builder.appName("SilverTransform_Orders").getOrCreate()

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

orders_path_bronze = os.path.join(base_path, "lakehouse/bronze/orders")
orders_path_silver = os.path.join(base_path, "lakehouse/silver/orders")


silver_orders = spark.read.parquet(orders_path_bronze)

orders_prefix = "order_"
silver_orders = silver_orders.toDF(*[col.replace(orders_prefix, "") for col in silver_orders.columns])

silver_orders.write.mode("overwrite").parquet(orders_path_silver)

#silver_orders.show(truncate=False)

spark.stop()

