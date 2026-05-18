import sys
import os
from pathlib import Path
from sqlalchemy import text

# Agrega la carpeta dags al sys.path para permitir importacion absoluta
dag_dir = Path(__file__).resolve().parents[2]
if str(dag_dir) not in sys.path:
    sys.path.append(str(dag_dir))

from caso_3.tasks.services.db_conection import get_db_engine

# Intenta importar los proveedores de S3 y Kaggle de forma segura
try:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    HAS_S3 = True
except ImportError:
    HAS_S3 = False

try:
    from kaggle.api.kaggle_api_extended import KaggleApi
    HAS_KAGGLE = True
except ImportError:
    HAS_KAGGLE = False

def validate_postgres():
    print("[INFO] Iniciando validacion de conexion a la base de datos Postgres")
    try:
        # Obtiene el motor de base de datos de nuestro servicio modular
        engine = get_db_engine()
        with engine.connect() as conn:
            # Ejecuta ping de validacion basica
            result = conn.execute(text("SELECT 1")).scalar()
            if result == 1:
                print("[SUCCESS] Conexion exitosa a PostgreSQL")
            # Muestra la version del motor analitico
            db_version = conn.execute(text("SELECT version()")).scalar()
            print(f"[INFO] Version de la base de datos: {db_version}")
    except Exception as e:
        print("[ERROR] Fallo la conexion a la base de datos Postgres")
        print(f"[DETAILS] {e}")

def validate_s3():
    print("[INFO] Iniciando validacion de conexion a AWS S3")
    if not HAS_S3:
        print("[ERROR] El proveedor de AWS no esta instalado (instale apache-airflow-providers-amazon)")
        return
    try:
        # Utiliza la conexion AWS configurada en la UI de Airflow como 'my_s3_conn'
        hook = S3Hook(aws_conn_id='my_s3_conn')
        buckets = hook.get_conn().list_buckets()
        bucket_count = len(buckets.get("Buckets", []))
        print(f"[SUCCESS] Conexion exitosa a AWS S3. Se encontraron {bucket_count} buckets.")
    except Exception as e:
        print("[ERROR] Fallo la conexion a AWS S3")
        print(f"[DETAILS] {e}")

def validate_kaggle():
    print("[INFO] Iniciando validacion de conexion a Kaggle")
    if not HAS_KAGGLE:
        print("[ERROR] El cliente de Kaggle no esta instalado (instale kaggle)")
        return
    try:
        # Intenta cargar credenciales de la UI de Airflow de forma dinamica
        try:
            from airflow.models import Variable
            os.environ['KAGGLE_USERNAME'] = Variable.get("KAGGLE_USERNAME")
            os.environ['KAGGLE_KEY'] = Variable.get("KAGGLE_KEY")
            print("[INFO] Credenciales de Kaggle cargadas desde Airflow UI Variables")
        except Exception:
            # Fallback local: Si no hay contexto de Airflow, usa las variables de entorno de la maquina
            print("[INFO] Usando credenciales de Kaggle del entorno local del sistema")
            
        # Inicializa y valida la autenticacion
        api = KaggleApi()
        api.authenticate()
        print(f"[SUCCESS] Autenticacion exitosa en Kaggle. Usuario activo: {api.config.username}")
    except Exception as e:
        print("[ERROR] Fallo la conexion o autenticacion en Kaggle")
        print(f"[DETAILS] {e}")

def main():
    print("=" * 60)
    print("[INFO] DIAGNOSTICO DE CONEXIONES Y SERVICIOS - CASO 3")
    print("=" * 60)
    
    validate_postgres()
    print("-" * 60)
    validate_s3()
    print("-" * 60)
    validate_kaggle()
    
    print("=" * 60)

if __name__ == "__main__":
    main()
