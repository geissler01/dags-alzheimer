# Proyecto Analítico dbt (Caso 3)

Este subdirectorio contiene exclusivamente la configuración y los modelos analíticos de dbt (Data Build Tool) para el Caso 3. Aquí es donde ocurre la transformación de datos para pasar de registros crudos a un Modelo Estrella Kimball optimizado.

---

## Estructura de Capas Analíticas

El proyecto está diseñado bajo las mejores prácticas de ingeniería analítica y se divide en tres capas principales:

1. **Capa Staging (`models/staging/`):**
   Actúa como la primera línea de defensa. Lee los datos crudos de la base de datos (ingestados por Airflow), estandariza los tipos de datos, maneja valores nulos y aplica IDs numéricos secuenciales si las fuentes no los tienen. Todos los modelos aquí se materializan como vistas (`view`).

2. **Capa Intermediate (`models/intermediate/`):**
   Aquí ocurre la magia de limpieza pesada: expresiones regulares para limpiar títulos, cálculos de edad y fidelidad de los usuarios, la separación de arreglos de texto (como los géneros concatenados) y la estructuración del sentimiento de las calificaciones. Todos los modelos se materializan físicamente como tablas (`table`).

3. **Capa Marts (`models/marts/`):**
   Es el producto final de cara al negocio (Power BI). Contiene las dimensiones (`dim_users`, `dim_content`, `dim_genres`, `dim_date`), la tabla puente para los géneros (`bridge_content_genres`) y la tabla de hechos centralizada unificada (`fact_user_ratings`). Se materializan como tablas físicas (`table`) para garantizar tiempos de consulta ultrarrápidos.

---

## Cómo Ejecutar y Probar dbt en Local

Aunque en el entorno de producción este proyecto dbt es orquestado automáticamente por Airflow (ver el `README.md` en la raíz de `caso_3`), puedes correr este proyecto de forma aislada en tu entorno local para desarrollo o pruebas.

### 1. Entorno Virtual y Dependencias
En la carpeta raíz del proyecto, asegúrate de activar tu entorno virtual e instalar los conectores:

```bash
# Activar entorno (Ejemplo para Mac/Linux)
source ../.venv/bin/activate

# Instalar dbt para Postgres
pip install dbt-postgres
```

### 2. Configurar el Perfil de dbt (profiles.yml)
Para que dbt local se conecte a tu base de datos, debes crear o editar el archivo `profiles.yml` en esta misma carpeta (`dbt_caso3`) o en tu ruta global de usuario (`~/.dbt/profiles.yml`). Un ejemplo de configuración:

```yaml
dbt_caso3:
  outputs:
    dev:
      type: postgres
      host: localhost          # Cambia esto por la IP o endpoint de tu base de datos
      port: 5432
      user: tu_usuario
      password: tu_password
      dbname: postgres
      schema: public
      threads: 4
  target: dev
```

### 3. Comandos de Consola útiles

Una vez configurado tu perfil, puedes usar estos comandos dentro de la carpeta `dbt_caso3`:

```bash
# Probar que la conexión a la base de datos funciona
dbt debug

# Ejecutar todos los modelos en orden de dependencias
dbt run

# Ejecutar solo una capa específica (ejemplo: solo staging)
dbt run --select staging

# Probar la calidad de datos y las aserciones (primary keys, not nulls)
dbt test

# Borrar archivos temporales de compilación
dbt clean
```

---

## Mantenimiento y Reglas para Desarrolladores

* **Idempotencia:** Los modelos de dbt están diseñados usando hashes (MD5) para generar identificadores de tablas de hechos. Esto asegura que puedes correr `dbt run` 100 veces seguidas y no se generarán datos duplicados.
* **Manejo de Tablas Huérfanas:** Es importante recordar que si cambias el nombre de un archivo SQL (por ejemplo, renombrar `int_user_interactions` a `int_user_ratings`), dbt no irá automáticamente a la base de datos a borrar la tabla vieja de interacciones. Si haces reestructuraciones de nombres, deberás ir a Postgres y borrar la tabla vieja a mano o hacer un `DROP SCHEMA` completo para que dbt reconstruya todo desde cero, manteniéndolo libre de basura.
