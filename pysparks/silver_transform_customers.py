from pyspark.sql import SparkSession
import os

# Inicializar uma sessão Spark
spark = SparkSession.builder.appName("SilverTransform_Customers").getOrCreate()

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

customers_path_bronze = os.path.join(base_path, "lakehouse/bronze/customers")
customers_path_silver = os.path.join(base_path, "lakehouse/silver/customers")


silver_customers = spark.read.parquet(customers_path_bronze)

customer_prefix = "customer_"
silver_customers = silver_customers.toDF(*[col.replace(customer_prefix, "") for col in silver_customers.columns])

silver_customers.write.mode("overwrite").parquet(customers_path_silver)

#silver_customers.show(truncate=False)

spark.stop()
