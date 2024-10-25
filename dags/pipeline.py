from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

# Definindo o caminho base
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Definindo argumentos padrão para o DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),  # Defina a data de início conforme necessário
    'retries': 1,
}

# Criando o DAG
dag = DAG(
    'lakehouse_pipeline',
    default_args=default_args,
    description='Pipeline para transformação de dados no Lakehouse',
    schedule_interval= None,  # Defina a frequência de execução
)

# Tarefas para a camada Bronze
bronze_customers = BashOperator(
    task_id='bronze_transform_customers',
    bash_command=f'python "{os.path.join(base_path, "pysparks/bronze_transform_customers.py")}"',
    dag=dag,
)

bronze_orders = BashOperator(
    task_id='bronze_transform_orders',
    bash_command=f'python "{os.path.join(base_path, "pysparks/bronze_transform_orders.py")}"',
    dag=dag,
)

bronze_order_items = BashOperator(
    task_id='bronze_transform_order_items',
    bash_command=f'python "{os.path.join(base_path, "pysparks/bronze_transform_orders_items.py")}"',
    dag=dag,
)

# Tarefas para a camada Silver
silver_customers = BashOperator(
    task_id='silver_transform_customers',
    bash_command=f'python "{os.path.join(base_path, "pysparks/silver_transform_customers.py")}"',
    dag=dag,
)

silver_orders = BashOperator(
    task_id='silver_transform_orders',
    bash_command=f'python "{os.path.join(base_path, "pysparks/silver_transform_orders.py")}"',
    dag=dag,
)

silver_order_items = BashOperator(
    task_id='silver_transform_orders_items',
    bash_command=f'python "{os.path.join(base_path, "pysparks/silver_transform_orders_items.py")}"',
    dag=dag,
)

# Tarefa para a camada Gold
gold_transform = BashOperator(
    task_id='gold_transform',
    bash_command=f'python "{os.path.join(base_path, "pysparks/gold_transform.py")}"',
    dag=dag,
)

# Definindo as dependências entre as tarefas
bronze_customers >> silver_customers
bronze_orders >> silver_orders
bronze_order_items >> silver_order_items

silver_customers >> gold_transform
silver_orders >> gold_transform
silver_order_items >> gold_transform
