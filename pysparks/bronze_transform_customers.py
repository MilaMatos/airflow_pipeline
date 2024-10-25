import os
from pyspark.sql import SparkSession

# Inicializar uma sessão Spark
spark = SparkSession.builder.appName("BronzeTransform_Customers").getOrCreate()

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#base_path = os.path.dirname(os.path.abspath(__file__))

#caminho do arquivo .json 
customers_path = os.path.join(base_path, "lakehouse/landing/customers.json")
#caminho do arquivo para salvar o arquivo parquet
customers_bronze_path = os.path.join(base_path, "lakehouse/bronze/customers")

customers_bronze = spark.read.json(customers_path)
customers_bronze.write.mode("overwrite").parquet(customers_bronze_path)

spark.stop()