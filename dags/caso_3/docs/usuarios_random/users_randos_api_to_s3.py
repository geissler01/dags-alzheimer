import time
import pandas as pd
import requests
import os
import boto3

from dotenv import load_dotenv

# Cargar variables de entorno (desde .env)
load_dotenv()

# Configuración idéntica a tus variables
api_url = "https://randomuser.me/api/"
batch_size = 5000
total_users = 162541
output_filename = "users_random.csv"

def generate_users_locally():
    start_time = time.time()
    existing_df = None
    
    # Intentar cargar datos existentes para no perder el progreso de lo que ya se descargó
    if os.path.exists(output_filename):
        try:
            existing_df = pd.read_csv(output_filename)
            print(f"Archivo local existente encontrado con {len(existing_df)} usuarios.")
        except Exception as e:
            print(f"No se pudo leer el archivo existente (se creará uno nuevo): {e}")

    if existing_df is not None and len(existing_df) >= total_users:
        print(f"¡El archivo ya está completo con {len(existing_df)} usuarios!")
        df = existing_df
    else:
        already_downloaded = len(existing_df) if existing_df is not None else 0
        missing_users = total_users - already_downloaded
        print(f"Iniciando descarga de los {missing_users} usuarios faltantes (para completar {total_users})...")
        
        usuarios = []
        for i in range(0, missing_users, batch_size):
            current_batch = min(batch_size, missing_users - i)
            print(f"Descargando lote de {current_batch} usuarios... (Progreso descarga: {len(usuarios)}/{missing_users})")
            
            # Bucle de reintentos robusto que no se salta lotes por 429
            attempt = 1
            backoff = 4
            while True:
                try:
                    response = requests.get(api_url, params={"results": current_batch}, timeout=30)
                    if response.status_code == 429:
                        print(f"[RATE LIMIT 429] API Saturada. Esperando {backoff} segundos...")
                        time.sleep(backoff)
                        backoff = min(backoff * 2, 60) # Espera máxima de 1 minuto
                        continue
                    response.raise_for_status()
                    usuarios.extend(response.json()['results'])
                    break
                except Exception as e:
                    print(f"[ERROR] Intento {attempt} falló: {e}")
                    attempt += 1
                    if attempt > 10:
                        raise e
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60)
            
            # Pausa saludable de 2.0 segundos entre lotes
            time.sleep(2.0)
            
        print("Aplanando estructura JSON con Pandas...")
        df_new = pd.json_normalize(usuarios)
        
        # Filtrado y renombrado de columnas
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
        df_new = df_new[list(columns_mapping.keys())].rename(columns=columns_mapping)
        
        if existing_df is not None:
            df = pd.concat([existing_df, df_new], ignore_index=True)
        else:
            df = df_new

    print(f"Guardando archivo final en: {output_filename}")
    df.to_csv(output_filename, index=False)
    
    # --- SUBIDA A S3 ---
    s3_bucket = os.getenv("S3_BUCKET")
    s3_key = f"raw/{output_filename}" # Ruta en tu bucket (ej. raw/users_random.csv)
    
    if s3_bucket:
        try:
            print(f"Iniciando subida a S3: s3://{s3_bucket}/{s3_key}...")
            # Boto3 buscará las credenciales automáticamente en las variables de entorno
            s3_client = boto3.client('s3')
            s3_client.upload_file(output_filename, s3_bucket, s3_key)
            print("[SUCCESS] Archivo subido exitosamente a AWS S3.")
        except Exception as e:
            print(f"[ERROR] Falló la subida a S3: {e}")
    else:
        print("[WARNING] S3_BUCKET no definido en el .env, se omite la subida a S3.")
    
    duration = time.time() - start_time
    print(f"\n[SUCCESS] ¡Proceso completado exitosamente!")
    print(f" - Registros generados: {len(df)}")
    # Muestra el tiempo en formato minutos:segundos
    print(f" - Tiempo transcurrido: {int(duration // 60)}m {int(duration % 60)}s")

if __name__ == "__main__":
    generate_users_locally()
