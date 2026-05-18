import time
import pandas as pd
import requests
import logging as log
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Configuración (Respetando tus variables)
api_url = "https://randomuser.me/api/"
batch_size = 5000  # Lote de descarga seguro
total_users = 162541
bucket_name = "draco-caso-3-users"
s3_key = "user/users_random.csv"
aws_conn_id = "my_s3_conn"

def users_from_api_to_s3():
    # 1. Validación dinámica: comprobar si el archivo ya existe en S3 antes de hacer peticiones a la API
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    if s3_hook.check_for_key(s3_key, bucket_name):
        log.info(f"[SKIP] El archivo s3://{bucket_name}/{s3_key} ya existe en S3. Omitiendo descarga e ingesta de la API para optimizar el clúster.")
        return

    log.info(f"[INFO] >> Iniciando descarga de {total_users} usuarios desde {api_url}")
    usuarios = []
    
    start_time = time.time()
    max_execution_time = 360  # Límite máximo de 6 minutos en total para toda la ingesta
    
    # 2. Descargar en lotes usando tu lógica de extracción (¡Súper rápido y seguro!)
    for i in range(0, total_users, batch_size):
        # Controlar el tiempo transcurrido total para fallar rápido si se vuelve eterno
        elapsed = time.time() - start_time
        if elapsed > max_execution_time:
            raise TimeoutError(f"[TIMEOUT] La ingesta excedió el límite máximo de {max_execution_time}s (Transcurrido: {elapsed:.1f}s). Abortando para liberar el worker.")

        current_batch = min(batch_size, total_users - i)
        log.info(f"Descargando lote de {current_batch} usuarios...")
        
        # Bucle de reintentos inteligente para evitar Rate Limit (429) o fallas de red
        retries = 5
        backoff = 3
        for attempt in range(1, retries + 1):
            try:
                response = requests.get(api_url, params={"results": current_batch}, timeout=30)
                # Si recibimos 429, esperamos y reintentamos con más calma
                if response.status_code == 429:
                    log.warning(f"[RATE LIMIT] Código 429 en intento {attempt}/{retries}. Esperando {backoff} segundos...")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                response.raise_for_status()
                usuarios.extend(response.json()['results'])
                break  # Éxito: salimos del bucle de reintentos
            except requests.exceptions.RequestException as e:
                log.warning(f"[ERROR] Intento {attempt}/{retries} falló: {e}")
                if attempt == retries:
                    raise e
                time.sleep(backoff)
                backoff *= 2
                
        # Pausa leve de 1.5 segundos entre lotes exitosos para ser respetuosos con el servidor
        time.sleep(1.5)
        
    # 3. Aplanar automáticamente con Pandas (¡Todo el anidamiento resuelto en 1 línea!)
    df = pd.json_normalize(usuarios)
    
    # 4. Filtrar y renombrar solo las columnas que nos interesan para la base de datos
    columns_mapping = {
        'login.uuid': 'user_id',
        'login.username': 'username',
        'gender': 'gender',
        'name.first': 'first_name',
        'name.last': 'last_name',
        'email': 'email',
        'dob.age': 'age',
        'dob.date': 'dob',
        'registered.date': 'registered_date',
        'phone': 'phone',
        'location.city': 'city',
        'location.state': 'state',
        'location.country': 'country',
        'location.postcode': 'postcode',
        'location.coordinates.latitude': 'latitude',
        'location.coordinates.longitude': 'longitude',
        'nat': 'nationality'
    }
    df = df[list(columns_mapping.keys())].rename(columns=columns_mapping)
    
    # 5. Convertir a CSV en memoria RAM y subir directo a S3 (¡Cero problemas de disco local!)
    log.info(f"Subiendo CSV directamente a S3 en: s3://{bucket_name}/{s3_key}")
    csv_data = df.to_csv(index=False)
    
    s3_hook.load_string(
        string_data=csv_data,
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )
    log.info(f"[SUCCESS] Ingesta completada. Total: {len(df)} usuarios subidos a S3.")
