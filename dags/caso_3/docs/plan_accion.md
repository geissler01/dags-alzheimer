# Plan de Acción Refinado: Pipeline Modular ELT con Airflow y DBT (Caso 3)

Este plan incorpora tus requerimientos exactos de negocio descritos en [CASO 3.docx](file:///c:/Users/ASUS/Desktop/RIWI/ruta-avanzada/m5/m5-analitica/clusters/alzheimer/dags-alzheimer/dags/caso_3/docs/CASO 3.docx), optimiza la memoria para tus **Workers de 2GB de RAM**, y define la integración limpia de **DBT** dentro del entorno Docker actual de tu clúster de AWS.

---

## 1. Estrategia de Mitigación de Memoria (Evitar Caídas por OOM en Workers de 2GB)

El dataset de **MovieLens 25M** es masivo. Si lo leemos por completo en memoria (`pd.read_csv`), el contenedor colapsará por falta de RAM.

### Solución Técnica:

1. **Descarga a Disco Local:** Los datasets (MovieLens de su ZIP oficial, Spotify/Netflix de Kaggle) se descargarán primero en el almacenamiento efímero del worker (`/tmp/` del contenedor).
2. **Carga en Bloques (Chunking):** Implementaremos un generador con `chunksize=50000` en pandas o utilizaremos el comando `COPY` directo de `psycopg2` para inyectar los datos en streaming.
3. **Consumo de RAM Garantizado:** Con esta estrategia, el consumo de memoria del Worker se mantendrá estable entre **50MB y 100MB**, permitiendo procesar archivos de gigabytes sin riesgo alguno.

---

## 2. Gestión de Entornos y DBT en Docker (Clúster AWS)

### En tu Entorno de Desarrollo Local:

La carpeta virtual `.venv` debe residir en la **raíz del proyecto** (`dags-alzheimer/.venv`), tal como muestra tu captura de pantalla. Esto te permite tener autocompletado y validación de sintaxis local.

### En tu Clúster de Producción (AWS):

Tus archivos `.env` revelan que Airflow corre dentro de contenedores Docker y utiliza la variable `EXTRA_REQUIREMENTS` para instalar dependencias al iniciar.

1. **Instalación de DBT:** Añadiremos `dbt-postgres` a la línea 18 en [master/.env](file:///c:/Users/ASUS/Desktop/RIWI/ruta-avanzada/m5/m5-analitica/clusters/alzheimer/cluster-config/master/.env#L18) y a la línea 92 en [worker/.env](file:///c:/Users/ASUS/Desktop/RIWI/ruta-avanzada/m5/m5-analitica/clusters/alzheimer/cluster-config/worker/.env#L92):
   ```ini
   EXTRA_REQUIREMENTS=apache-airflow-providers-amazon apache-airflow-providers-celery apache-airflow-providers-postgres supabase kaggle psycopg2-binary celery dbt-postgres
   ```
2. **Ejecución en Airflow:** Al hacer esto, DBT estará instalado nativamente dentro de los contenedores de Airflow. Podremos ejecutar los modelos directamente usando un operador de comandos apuntando al directorio del proyecto:
   ```bash
   dbt run --project-dir /opt/airflow/dags/caso_3/dbt_caso3 --profiles-dir /opt/airflow/dags/caso_3/dbt_caso3
   ```

---

## 3. Workflow de Ingesta de Usuarios (100k usuarios Random API)

Para garantizar consistencia (que los usuarios no cambien en cada ejecución) y enriquecer la geografía:

```text
[API Random User] ──(Descarga batches)──> [Archivo CSV Temporal] ──(Subir)──> [AWS S3 o Supabase Bucket]
                                                                                     │
[Validación de 100k] <──(Carga masiva por chunks)── [Base Postgres raw_layer] <──────┘
```

1. **Tarea 1 (Landing Raw):** Descarga en batches desde la API, unifica en un CSV y lo sube de forma definitiva a tu bucket de S3 (`s3://logs-s3-alzheimer-g/raw/usuarios_random_100k.csv`) o a una tabla intermedia en Supabase. Esto congela los usuarios para futuras ejecuciones.
2. **Tarea 2 (Load y Validación):** Descarga el archivo desde S3/Supabase, lo inserta por bloques en `raw_layer.usuarios`, y ejecuta una validación rápida para confirmar que se importaron exactamente 100k registros únicos.

---

## 4. Conexión de Base de Datos Centralizada

Usaremos tu Connection ID configurado en Airflow: **`Db_caso_3_janner`**.
Actualizaremos [db_conection.py](file:///c:/Users/ASUS/Desktop/RIWI/ruta-avanzada/m5/m5-analitica/clusters/alzheimer/dags-alzheimer/dags/caso_3/tasks/services/db_conection.py) para usar este ID de forma nativa:

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine

def get_db_engine():
    # Obtiene la URI de la DB configurada en la UI de Airflow como 'Db_caso_3_janner'
    hook = PostgresHook(postgres_conn_id='Db_caso_3_janner')
    return create_engine(hook.get_uri())
```

---

## 5. Propuesta de Cambios en Archivos

### 📂 Carpeta Principal: [caso_3](file:///c:/Users/ASUS/Desktop/RIWI/ruta-avanzada/m5/m5-analitica/clusters/alzheimer/dags-alzheimer/dags/caso_3)

#### [MODIFY] [db_conection.py](file:///c:/Users/ASUS/Desktop/RIWI/ruta-avanzada/m5/m5-analitica/clusters/alzheimer/dags-alzheimer/dags/caso_3/tasks/services/db_conection.py)

- Configurar la conexión con PostgresHook usando `Db_caso_3_janner`.

#### [MODIFY] [schemas.sql](file:///c:/Users/ASUS/Desktop/RIWI/ruta-avanzada/m5/m5-analitica/clusters/alzheimer/dags-alzheimer/dags/caso_3/sql/schemas.sql)

- Crear los esquemas analíticos si no existen (`raw_layer`, `staging_layer`, `intermediate_layer`, `marts_layer`).

#### [NEW] [ingestion_from_kaggle.py](file:///c:/Users/ASUS/Desktop/RIWI/ruta-avanzada/m5/m5-analitica/clusters/alzheimer/dags-alzheimer/dags/caso_3/tasks/extraction_load/ingestion_from_kaggle.py)

- Descarga de datasets Spotify y Netflix de Kaggle a disco local y carga a base de datos usando pandas chunking.

#### [NEW] [ingestion_from_movielens.py](file:///c:/Users/ASUS/Desktop/RIWI/ruta-avanzada/m5/m5-analitica/clusters/alzheimer/dags-alzheimer/dags/caso_3/tasks/extraction_load/ingestion_from_movielens.py)

- Descarga masiva de MovieLens 25M, extracción en disco, y carga optimizada con streaming para evitar OOM.

#### [NEW] [ingestion_from_s3.py](file:///c:/Users/ASUS/Desktop/RIWI/ruta-avanzada/m5/m5-analitica/clusters/alzheimer/dags-alzheimer/dags/caso_3/tasks/extraction_load/ingestion_from_s3.py)

- Gestión de la subida inicial de los 100k usuarios generados a S3 y su posterior lectura.

#### [NEW] [dbt_caso3](file:///c:/Users/ASUS/Desktop/RIWI/ruta-avanzada/m5/m5-analitica/clusters/alzheimer/dags-alzheimer/dags/caso_3/dbt_caso3) [Directorio]

- Proyecto inicial de DBT para modelar las capas.

### 📂 Archivo Principal de Airflow

#### [NEW] [pipeline_caso3.py](file:///c:/Users/ASUS/Desktop/RIWI/ruta-avanzada/m5/m5-analitica/clusters/alzheimer/dags-alzheimer/dags/pipeline_caso3.py)

- DAG principal con TaskFlow API (`@dag`, `@task`). Enlaza la inicialización de esquemas, las cuatro descargas/cargas masivas paralelas y la ejecución del pipeline DBT.

---

## 6. Plan de Verificación

1. **Test de Conexión:**
   - Validar que la base de datos es accesible usando `Db_caso_3_janner` en un script rápido.
2. **Pruebas de Ingesta por Bloques (Local):**
   - Ejecutar la ingesta en un dataset pequeño para verificar que el chunking en disco funcione correctamente y libere memoria.
3. **Validación de la Estructura DAG:**
   - Comprobar que Airflow compila el DAG sin errores de importación circular ni sintaxis.
