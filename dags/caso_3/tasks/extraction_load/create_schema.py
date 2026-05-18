import sys
import logging as log
from pathlib import Path
from sqlalchemy import text

# 1. ARREGLAR LA RUTA ABSOLUTA PRIMERO
# Agrega la carpeta dags al sys.path para permitir importación absoluta de forma segura
dag_dir = Path(__file__).resolve().parents[2]
if str(dag_dir) not in sys.path:
    sys.path.append(str(dag_dir))

# 2. AHORA IMPORTAMOS EL SERVICIO DE CONEXIÓN
from caso_3.tasks.services.db_conection import get_db_engine

# Configuración de logs
logging_format = '[%(asctime)s] %(levelname)s - %(message)s'
log.basicConfig(level=log.INFO, format=logging_format)

def create_analytical_schemas():
    log.info("Iniciando creación/verificación de los esquemas analíticos...")
    try:
        # Localizar y leer el archivo schemas.sql
        sql_file_path = Path(__file__).resolve().parents[2] / "sql" / "schemas.sql"
        log.info(f"Ejecutando SQL desde: {sql_file_path}")
        
        with open(sql_file_path, "r", encoding="utf-8") as f:
            sql_script = f.read()
            
        # Conectar a la DB y ejecutar todo el SQL completo de una vez (Sencillo con autocommit)
        engine = get_db_engine()
        with engine.begin() as conn:
            conn.execute(text(sql_script))
            
        log.info("[SUCCESS] Todos los esquemas definidos en schemas.sql fueron creados o verificados exitosamente.")
    except Exception as e:
        log.error(f"[ERROR] Error al crear los esquemas analíticos: {e}")
        raise e

# if __name__ == "__main__":
#     create_analytical_schemas()
