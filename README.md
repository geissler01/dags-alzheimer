# Repositorio DAGs Alzheimer

Este repositorio contiene los DAGs (Directed Acyclic Graphs), plugins, y utilidades de Airflow para el clúster Analítico "Alzheimer".

## Estructura de Carpetas

- `/dags/`: Contiene los scripts en Python que definen los pipelines de Airflow.
- `/dags/common/`: Scripts y constantes reutilizables en distintos DAGs.
- `/dags/sql/`: Consultas SQL y scripts de bases de datos.
- `/plugins/`: Hooks, operadores, y sensores personalizados para extender Airflow.
- `/spark_jobs/`: Scripts de Apache Spark para ser enviados al clúster de Spark remoto.
- `/tests/`: Pruebas de integridad para asegurar que los DAGs carguen sin errores de sintaxis.

## Despliegue Automatizado

Este repositorio debe clonarse en la ruta `/opt/dags-alzheimer` dentro de todos los nodos de Airflow (Master y Workers). Se puede integrar con n8n u otro CI/CD para automatizar los `git pull` tras cada actualización en la rama principal.
