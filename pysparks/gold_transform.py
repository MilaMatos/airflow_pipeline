import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Inicializar uma sessão Spark
spark = SparkSession.builder.appName("GoldTransform").getOrCreate()

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Definir os caminhos para os arquivos da camada "silver"
silver_paths = {
    "customers": (os.path.join(base_path, "lakehouse/silver/customers")),
    "orders": (os.path.join(base_path, "lakehouse/silver/orders")),
    "order_items": (os.path.join(base_path, "lakehouse/silver/orders_items"))
}

# Ler os arquivos Parquet da camada "silver"
customers_df = spark.read.parquet(silver_paths["customers"])
orders_df = spark.read.parquet(silver_paths["orders"])
order_items_df = spark.read.parquet(silver_paths["order_items"])

# Juntando os DataFrames
# Juntando customers com orders via id do cliente
customers_orders_df = customers_df.join(orders_df, customers_df.id == orders_df.customer_id, "inner")

# Juntando com order_items via id do pedido
full_df = customers_orders_df.join(order_items_df, orders_df.id == order_items_df.order_id, "inner")

# Agrupando os dados por cidade, estado e contando a quantidade de pedidos
summary_df = full_df.groupBy('city', 'state') \
    .agg(
        F.countDistinct(orders_df.id).alias('qtd_de_pedidos'),  # Quantidade de pedidos por cidade e estado
        F.round(F.sum(order_items_df.subtotal), 2).alias('valor_total_pedidos')  # Valor total dos pedidos com 2 casas decimais
    )

# Exibir o resultado, ordenando pelo valor total
summary_df.select("city", "state", "qtd_de_pedidos", "valor_total_pedidos") \
    .orderBy(F.desc("valor_total_pedidos")) \
    .show(truncate=False)

# Escrever os dados na camada "gold" em formato Parquet
output_path = os.path.join(base_path, "lakehouse/gold/orders_summary")
summary_df.write.mode("overwrite").parquet(output_path)

# Parar a sessão Spark
spark.stop()
