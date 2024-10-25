from pyspark.sql import SparkSession
import os

# Inicializar uma sessão Spark
spark = SparkSession.builder.appName("SilverTransform_OrdersItems").getOrCreate()

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

orders_items_path_bronze = os.path.join(base_path, "lakehouse/bronze/orders_items")
orders_items_path_silver = os.path.join(base_path, "lakehouse/silver/orders_items")


silver_orders_items = spark.read.parquet(orders_items_path_bronze)

orders_items_prefix = "order_item_"
silver_orders_items = silver_orders_items.toDF(*[col.replace(orders_items_prefix, "") for col in silver_orders_items.columns])

silver_orders_items.write.mode("overwrite").parquet(orders_items_path_silver)

#silver_orders_items.show(truncate=False)

spark.stop()


