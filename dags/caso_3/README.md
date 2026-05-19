# Plataforma de Analytics Unificada para Streaming (Caso 3)
## Orquestación ELT con Apache Airflow

Este directorio contiene la solución completa de Inteligencia de Negocio y Data Warehousing para el Caso 3 de la plataforma global de streaming multimedia. El objetivo de este proyecto es unificar los silos de información provenientes de MovieLens, Netflix y Spotify en un único Modelo Estrella (Kimball Star Schema).

---

## Estructura del Proyecto

La solución está diseñada de forma modular, separando la orquestación, las tareas de ingesta física y la transformación analítica:

* `pipeline_caso3.py`: Es el corazón del proyecto. Contiene el DAG de Apache Airflow que orquesta absolutamente todo el flujo ELT.
* `dbt_caso3/`: Contiene el proyecto de transformación de datos usando dbt (Data Build Tool). Revisa el `README.md` dentro de esa carpeta para detalles específicos de dbt.
* `tasks/`: Contiene los scripts de Python para extracción y carga (ingesta en crudo desde Kaggle y AWS S3 hacia Postgres).
* `docs/`: Documentación analítica y de requerimientos del negocio.

---

## Cómo Ejecutar el Proyecto en Producción

Toda la ejecución de este proyecto **debe realizarse a través de Apache Airflow** utilizando el archivo `pipeline_caso3.py`.

El pipeline automatiza los siguientes pasos en orden:
1. Validación de variables de entorno y pre-requisitos.
2. Ingesta de datos crudos desde Kaggle (MovieLens, Netflix, Spotify).
3. Ingesta de datos desde Amazon S3 (usuarios).
4. Ejecución del flujo de dbt (Staging, Intermediate y Marts) para transformar los datos crudos en el modelo dimensional final.

**Pasos para ejecutar:**
1. Accede a la interfaz web de tu instancia de Airflow.
2. Localiza el DAG generado por el archivo `pipeline_caso3.py`.
3. Activa el DAG (Turn On) y presiona "Trigger DAG" para iniciar la corrida completa.

---

## Solución de Problemas (Troubleshooting de Clúster/AWS)

### Entorno Virtual Corrupto o Error de Sincronización en Contenedores (.venv)
En entornos distribuidos o de AWS, es común que la sincronización de archivos o el reinicio de los contenedores corrompa los enlaces simbólicos (symlinks) internos de tu entorno virtual `.venv`. 

Si al ejecutar tu pipeline en Airflow recibes errores como "python bin not found" o fallas de importación de dependencias porque el sync rompió las referencias de tu entorno virtual, puedes forzar la recreación del enlace simbólico al binario de Python de tu sistema ejecutando este comando en la consola del clúster/contenedor:

```bash
# Recrear el enlace simbólico del binario de Python del sistema hacia el .venv
ln -sf /usr/bin/python3 /opt/airflow/.venv/bin/python
```

Esto reparará instantáneamente el direccionamiento de tu entorno virtual y permitirá que Airflow ejecute todas sus tareas sin colapsar.
