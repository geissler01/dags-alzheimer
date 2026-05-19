import os
import sys
import tempfile
import shutil
from pathlib import Path
from io import StringIO
import csv
import logging

# Agrega la carpeta dags al sys.path para permitir importacion absoluta
dag_dir = Path(__file__).resolve().parents[2]
if str(dag_dir) not in sys.path:
    sys.path.append(str(dag_dir))

# 1. Cargar credenciales de Airflow de forma dinamica primero
try:
    from airflow.models import Variable
    os.environ['KAGGLE_USERNAME'] = Variable.get("KAGGLE_USERNAME")
    os.environ['KAGGLE_KEY'] = Variable.get("KAGGLE_KEY")
    logging.info("[INFO] Credenciales de Kaggle cargadas desde Airflow UI Variables")
except Exception:
    pass

# 2. AHORA importamos las demas librerias despues de configurar las variables de entorno
import pandas as pd
import kaggle
from caso_3.tasks.services.db_conection import get_db_engine

def psql_insert_copy(table, conn, keys, data_iter):
    # Obtiene la conexion nativa del driver
    db_api_conn = conn.connection
    with db_api_conn.cursor() as cur:
        # Crea un buffer en memoria RAM
        s_buf = StringIO()
        writer = csv.writer(s_buf) 
        writer.writerows(data_iter)
        s_buf.seek(0)

        # Extrae el nombre del schema y la tabla
        column = ', '.join(f'"{k}"' for k in keys)
        if table.schema:
            table_name = f'"{table.schema}"."{table.name}"'
        else:
            table_name = f'"{table.name}"'

        # Construye el comando COPY
        sql = f'COPY {table_name}({column}) FROM STDIN WITH CSV'
        
        # Resiliencia hibrida psycopg2/psycopg3
        if hasattr(cur, 'copy'):
            # psycopg3 (Nativo en Airflow 3)
            with cur.copy(sql) as copy_op:
                copy_op.write(s_buf.getvalue())
        else:
            # psycopg2 (Local / Fallback)
            cur.copy_expert(sql=sql, file=s_buf)

def ingest_kaggle_to_postgres():
    logging.info("[INFO] Iniciando ingesta unificada desde Kaggle a Postgres")
    
    # Autenticacion en Kaggle
    kaggle.api.authenticate()
    
    # Crear carpeta temporal segura en el disco fisico (evitando RAM tmpfs de /tmp)
    temp_dir = Path(__file__).resolve().parents[2] / "temp_kaggle_data"
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    engine = get_db_engine()
    
    # 3. Validar si las tablas ya existen y tienen datos para evitar descargas e ingestas repetidas
    tables_to_check = ["netflix_titles", "spotify_artists", "spotify_dict_artists", "spotify_tracks"]
    all_loaded = True
    
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            for t in tables_to_check:
                # Verificar si existe la tabla en raw_layer
                check_exists = conn.execute(
                    text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'raw_layer' AND table_name = :t)"),
                    {"t": t}
                ).scalar()
                
                if not check_exists:
                    all_loaded = False
                    break
                    
                # Verificar si tiene al menos una fila
                count = conn.execute(text(f"SELECT COUNT(1) FROM raw_layer.{t}")).scalar()
                if count == 0:
                    all_loaded = False
                    break
    except Exception:
        # En caso de error (por ejemplo, si el schema raw_layer aun no esta creado), procedemos con la ingesta
        all_loaded = False
        
    if all_loaded:
        logging.info("[INFO] --> Las tablas de Netflix y Spotify ya existen y tienen datos en la capa 'raw_layer'. Omitiendo descarga e ingesta.")
        return
    
    try:
        # --- A. NETFLIX ---
        dataset_netflix = "shivamb/netflix-shows"
        netflix_temp = temp_dir / "netflix"
        netflix_temp.mkdir(parents=True, exist_ok=True)
        
        logging.info(f"[INFO] --> Descargando dataset de Netflix: {dataset_netflix}")
        kaggle.api.dataset_download_files(dataset_netflix, path=str(netflix_temp), unzip=True)
        
        netflix_file = netflix_temp / "netflix_titles.csv"
        if netflix_file.exists():
            logging.info("[INFO] --> Cargando netflix_titles.csv a Postgres...")
            df_netflix = pd.read_csv(netflix_file, dtype=str)
            df_netflix.to_sql(
                name="netflix_titles",
                con=engine,
                schema="raw_layer",
                if_exists="replace",
                index=False,
                method=psql_insert_copy
            )
            logging.info(f"[SUCCESS] --> Tabla raw_layer.netflix_titles cargada ({len(df_netflix)} filas)")
        else:
            logging.error("[ERROR] No se encontro netflix_titles.csv en la descarga.")

        # --- B. SPOTIFY ---
        dataset_spotify = "yamaerenay/spotify-dataset-19212020-600k-tracks"
        spotify_temp = temp_dir / "spotify"
        spotify_temp.mkdir(parents=True, exist_ok=True)
        
        logging.info(f"[INFO] --> Descargando dataset de Spotify: {dataset_spotify}")
        kaggle.api.dataset_download_files(dataset_spotify, path=str(spotify_temp), unzip=True)
        
        files_to_load = {
            "artists.csv": "spotify_artists",
            "dict_artists.json": "spotify_dict_artists",
            "tracks.csv": "spotify_tracks"
        }
        
        for file_name, table_name in files_to_load.items():
            full_path = spotify_temp / file_name
            if not full_path.exists():
                logging.warning(f"[WARNING] El archivo {file_name} no existe en la descarga.")
                continue
                
            if file_name.lower().endswith(".csv"):
                logging.info(f"[INFO] --> Cargando {file_name} a la tabla raw_layer.{table_name} en chunks...")
                first_chunk = True
                for chunk_df in pd.read_csv(full_path, dtype=str, chunksize=200000):
                    chunk_df.to_sql(
                        name=table_name,
                        con=engine,
                        schema="raw_layer",
                        if_exists="replace" if first_chunk else "append",
                        index=False,
                        method=psql_insert_copy
                    )
                    first_chunk = False
                logging.info(f"[SUCCESS] --> Tabla raw_layer.{table_name} cargada exitosamente.")
            else:
                try:
                    df = pd.read_json(full_path)
                except ValueError:
                    df = pd.read_json(full_path, typ='series').reset_index()
                    df.columns = ['artist_id', 'related_artists']
                    df = df.astype(str)
                
                logging.info(f"[INFO] --> Cargando {file_name} a la tabla raw_layer.{table_name}...")
                df.to_sql(
                    name=table_name,
                    con=engine,
                    schema="raw_layer",
                    if_exists="replace",
                    index=False,
                    method=psql_insert_copy
                )
                logging.info(f"[SUCCESS] --> Tabla raw_layer.{table_name} cargada ({len(df)} filas)")

        logging.info("[SUCCESS] >> Ingesta y carga completa de Kaggle finalizada exitosamente.")
        
    finally:
        # Limpieza de archivos temporales
        try:
            shutil.rmtree(temp_dir)
            logging.info("[INFO] Archivos temporales de Kaggle eliminados para liberar espacio.")
        except Exception as e:
            logging.warning(f"[WARNING] No se pudo limpiar la carpeta temporal: {e}")

if __name__ == "__main__":
    ingest_kaggle_to_postgres()
