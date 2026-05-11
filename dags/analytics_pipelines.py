from airflow.decorators import dag, task # propio de airflow
from datetime import datetime, timedelta # nativo de python
import logging # similar a console.log en JS

# 1 configuracion de argumentos por defecto
default_args = {
    'owner': 'draco',
    'depend_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# 2. definicion del DAG usando el decorador @dag
@dag(
    dag_id = 'analytics_pipeline_v8',
    default_args = default_args,
    description = 'ejercicio 8',
    schedule = None,
    start_date = datetime(2026, 5, 11),
    catchup = False,
    tags = ['ejercicio 8', 'etl']
)
def defanalytics_pipeline():

    # ---- Fase de extracion
    @task
    def extract_data_from_api():
        """
        Simulacion extraccion datos desde una api
        """
        logging.info("Paso 1: Incicando extraccion de datos desde la api...")
        # datos
        data = {
            "source": "api_externa",
            "status": "ok",
            "payload": [10, 20, 30]
        }
        return data
    
    # --- fase de tranformacion
    @task
    def tranformation_data(raw_data: dict):
        """
        Recibe los datos de la tarea anterior y los procesa
        """
        logging.info(f'paso 2: tranformando datos de {raw_data["source"]}...')
                # Calculamos una métrica simple: la suma de los números en el payload
        total = sum(raw_data['payload'])
        
        # Devolvemos un nuevo diccionario con el resultado procesado
        return {
            "total_sum": total, 
            "processed_at": str(datetime.now())
        }

    # ---- Fase de Carga ----
    @task
    def load_to_postgres(processed_data: dict):
        """
        Recibe los datos procesados y simula la carga en Postgres
        """
        logging.info("Paso 3: Cargando datos en PostgreSQL en AWS...")
        # Por ahora solo imprimimos el resultado para confirmar que llegó bien
        print(f"¡Éxito! Se guardó el total: {processed_data['total_sum']}")

    # ---- DEFINICIÓN DEL FLUJO (ORQUESTACIÓN) ----
    # 1. Ejecutamos la extracción y guardamos su resultado en una variable
    data_cruda = extract_data_from_api()
    
    # 2. Pasamos el resultado de la extracción a la transformación
    data_procesada = tranformation_data(data_cruda)
    
    # 3. Pasamos el resultado procesado a la carga
    load_to_postgres(data_procesada)

# 3. Ejecutar la función principal para que Airflow detecte el DAG
defanalytics_pipeline()


