from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

# ==============================================================================
# CONFIGURACIÓN DEL DAG - ALZHEIMER CLUSTER
# ==============================================================================
# Este DAG está diseñado para validar la conectividad y visualización en la UI
# de Airflow 3, saludando a los 4 futuros workers que se integrarán al cluster.
# ==============================================================================

def say_hello(worker_id, greeting_index):
    """
    Función que imprime el saludo.
    """
    worker_name = f"Worker-{worker_id}"
    print(f"========================================")
    print(f"SALUDO OFICIAL DEL SISTEMA ALZHEIMER")
    print(f"Destinatario: {worker_name}")
    print(f"Número de saludo: {greeting_index} de 3")
    print(f"¡Hola {worker_name}! ¡Bienvenido al cluster!")
    print(f"========================================")

default_args = {
    'owner': 'antigravity_admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_saludo_4_workers_x3',
    default_args=default_args,
    description='DAG de bienvenida: 3 saludos para cada uno de los 4 futuros workers',
    schedule_interval='@once',
    start_date=datetime(2024, 5, 1),
    catchup=False,
    tags=['alzheimer', 'infrastructure', 'welcome'],
) as dag:

    # Generamos la lógica para los 4 futuros workers
    for w_id in range(1, 5):
        # Usamos TaskGroups para una visualización premium en la UI de Airflow
        with TaskGroup(group_id=f'Saludos_Futuro_Worker_{w_id}') as tg:
            
            tasks = []
            for g_id in range(1, 4):
                t = PythonOperator(
                    task_id=f'saludo_nro_{g_id}',
                    python_callable=say_hello,
                    op_kwargs={
                        'worker_id': w_id, 
                        'greeting_index': g_id
                    },
                )
                tasks.append(t)
            
            # Encadenamos los saludos para que se vean en orden (opcional, pero se ve mejor en el grafo)
            tasks[0] >> tasks[1] >> tasks[2]

    # Nota: Los TaskGroups se ejecutarán en paralelo por defecto a menos que se defina orden entre ellos.
    # En este caso, saludamos a todos los workers simultáneamente.
