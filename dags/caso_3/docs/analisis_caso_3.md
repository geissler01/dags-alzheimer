# ANÁLISIS ESTRATÉGICO Y DISEÑO DE INTELIGENCIA DE NEGOCIO (CASO 3)
## Plataforma Global de Streaming: Retención, Engagement y Catálogo Unificado

---

## 1. Introducción y Contexto de la Empresa

La organización objeto de este análisis es una **compañía internacional de entretenimiento digital dedicada al streaming de contenido multimedia**. Opera en un mercado altamente competitivo a escala global y atiende a millones de usuarios activos diarios distribuidos a través de un ecosistema multiplataforma que abarca:
* Smart TVs y consolas de videojuegos.
* Dispositivos móviles (Android/iOS) y tablets.
* Navegadores web y dispositivos domésticos inteligentes.

La oferta comercial de la plataforma es sumamente diversa, combinando múltiples formatos de entretenimiento: **películas, series, documentales, música en streaming, podcasts y transmisiones en vivo**, bajo un modelo de monetización híbrido que incluye suscripciones premium (SVOD) y publicidad segmentada (AVOD).

### El Desafío del Crecimiento
El éxito y la rápida expansión de la plataforma han provocado una **explosión en el volumen de datos generados**. Cada segundo, millones de usuarios realizan clics, pausas, búsquedas, reproducciones y asignan calificaciones. Esta avalancha de información semiestructurada e histórica se recolecta desde servidores globales, logs técnicos y pasarelas de pago, lo que ha generado una fragmentación extrema (silos de datos) que impide tener una visión unificada del negocio.

---

## 2. Contexto de la Problemática y Dolores Departamentales

Durante el último año fiscal, la alta dirección detectó tres métricas críticas en franco deterioro:
1. **Reducción en el Tiempo Promedio de Consumo (Engagement):** Los usuarios pasan menos horas activos por sesión en la plataforma.
2. **Aumento en la Tasa de Cancelación de Suscripciones (Churn):** Fuga de clientes hacia competidores.
3. **Baja Tasa de Conversión:** Dificultad para convertir a los usuarios gratuitos en suscriptores premium.

Dado que la información estaba fragmentada, cada departamento desarrolló su propia hipótesis del problema sin base científica, generando debates internos improductivos:

* **Marketing & Recomendaciones:** Argumenta que el algoritmo de recomendación personalizada es deficiente y no ofrece contenido relevante a los usuarios, provocando su desconexión inmediata.
* **Diseño y Producto:** Sostiene que el flujo de registro inicial (*onboarding*) es demasiado complejo y frustrante, lo que explica la alta tasa de abandono temprano de nuevos usuarios.
* **Comercial y Monetización:** Sospecha que la segmentación demográfica y por comportamiento es incorrecta, afectando la efectividad de los anuncios segmentados y la conversión premium.
* **Soporte y Tecnología (TI):** Sospecha de problemas técnicos reales como fallas en los servidores de video, retrasos (*buffering*) y lentitud en regiones específicas fuera de EE. UU.

---

## 3. Arquitectura dbt y Modelo Estrella Implementado

Para poner fin a la incertidumbre y dotar a la compañía de una **Única Fuente de la Verdad (Single Source of Truth)**, diseñamos y construimos un almacén de datos dimensional analítico (*Kimball Star Schema*) en PostgreSQL y dbt, orquestado de forma aislada mediante Apache Airflow.

El flujo procesa las fuentes crudas de **MovieLens** (comportamiento e historial de interacción de usuarios con películas), **Netflix** (catálogo enriquecido de series y shows) y **Spotify** (catálogo y metadata de pistas musicales).

### La Estructura de Capas en dbt

1. **Capa Staging (Limpieza e Identificación Única):**
   * Eliminamos duplicados y estandarizamos formatos inconsistentes.
   * **Innovación Técnica:** Generamos un `numericId` secuencial para Netflix y Spotify usando `ROW_NUMBER()`. Esto permitió estructurar claves sintéticas consistentes de texto (`movielens_123`, `netflix_45`, `spotify_99`) evitando cualquier tipo de colisión de IDs entre diferentes orígenes.
   * Aplicamos tipados estrictos, comillas dobles para columnas reservadas (`"type"`) y controlamos nulos en orígenes críticos.

2. **Capa Intermediate (Enriquecimiento y Modelado):**
   * **`int_users_enriched.sql`:** Clasificamos demográficamente a los usuarios por generación (Gen Z, Millennials, Gen X, Baby Boomers) y por nivel de lealtad comercial ('Extra', 'Protagonista', 'Leyenda') basado en su antigüedad acumulada en días.
   * **`int_movies_enriched.sql`:** Limpiamos y pulimos el título original eliminando el año entre paréntesis mediante expresiones regulares (`REGEXP_REPLACE`) y `TRIM`, aislando con precisión el año de estreno analítico (`releaseYear`).
   * **`int_unified_content.sql`:** **La joya de la corona del negocio**. Consolidó en una sola tabla lógica todo el catálogo multimedia de películas, series y música de la corporación.
   * **`int_user_ratings.sql`:** Estandarizamos las interacciones de calificaciones de MovieLens, conservando el ID original y el sintético unificado, y mapeamos las calificaciones a una métrica de sentimiento técnico (`ratingSentiment`: 'Crítica', 'Indiferente', 'Favorable', 'Sobresaliente').
   * **`int_content_genres_split.sql`:** Desenredamos las relaciones many-to-many de géneros usando `UNNEST(STRING_TO_ARRAY)` tanto para las barras de MovieLens (`|`) como para las comas de Netflix (`, `).

3. **Capa Marts (El Modelo Estrella Consolidado):**
   * **`dim_users`:** Perfil demográfico completo y lealtad.
   * **`dim_content`:** Catálogo analítico maestro y universal (Movies, Shows, Music).
   * **`dim_date`:** Dimensión temporal estática y precalculada para evitar lógica pesada de fechas en reportes.
   * **`dim_genres` & `bridge_content_genres`:** Modelo puente normalizado que permite realizar filtrados multi-género hiperrápidos y limpios en Power BI.
   * **`fact_user_ratings`:** La tabla de hechos central. Conecta las métricas de negocio directo con `dim_users`, `dim_content` y `dim_date`, exponiendo el score numérico, el sentimiento y el ID de película puro.

---

## 4. Guía Estratégica: Preguntas de Negocio vs. Visualizaciones en Power BI

Esta guía práctica detalla cómo usar el modelo estrella en Power BI para resolver los dolores de cada departamento, junto con recomendaciones específicas de visualización e interpretación de datos (*data storytelling*):

| Departamento | Pregunta Crítica del Negocio | Campos Necesarios del Modelo Estrella | Gráfico Sugerido en Power BI | Storytelling y Explicación para Directivos |
| :--- | :--- | :--- | :--- | :--- |
| **Alta Dirección / Negocio** | ¿Existe una pérdida real de engagement de usuarios en la plataforma a lo largo del tiempo? | `fact_user_ratings.rating_value`, `dim_date.year`, `dim_date.month_name` | **Gráfico de Líneas con Línea de Tendencia** | *"Observamos la evolución del rating promedio y volumen de actividad mes a mes. Una pendiente negativa confirma que los usuarios están perdiendo interés o que el contenido reciente no está rindiendo según las expectativas."* |
| **Comercial / Ventas** | ¿Cómo se distribuyen los usuarios más leales (Leyendas) frente a los nuevos (Extra) por generación? | `dim_users.generation`, `dim_users.loyalty_category`, `dim_users.user_id` (Count) | **Gráfico de Barras Agrupadas y Apiladas (100%)** | *"Muestra qué generaciones (ej. Gen Z vs. Millennials) representan el grupo con mayor retención ('Leyendas') y dónde debemos dirigir las campañas para evitar que los usuarios más nuevos ('Extra') abandonen."* |
| **Marketing** | ¿El algoritmo de recomendaciones es relevante para los usuarios o está fallando? | `fact_user_ratings.rating_sentiment`, `dim_content.source_platform` | **Gráfico de Anillo (Donut Chart)** | *"Al segmentar por el sentimiento de calificación, si el sector 'Crítica' o 'Indiferente' supera el 40% en recomendaciones, confirmamos científicamente que Marketing tiene razón: las sugerencias no están alineadas al gusto de la audiencia."* |
| **Producto** | ¿El perfil incompleto en el onboarding es causa directa de baja interacción de usuarios? | `dim_users.is_complete_profile`, `fact_user_ratings.rating_id` (Count) | **Gráfico de Columnas Clúster** | *"Comparamos la actividad total entre usuarios con perfil completo e incompleto. Si las cuentas incompletas muestran una caída drástica tras los primeros 7 días, se demuestra que el Onboarding inicial de Producto requiere simplificación."* |
| **Producto / Catálogo** | ¿Qué géneros de contenido tienen el mayor índice de aprobación y volumen de consumo en la plataforma? | `dim_genres.genre_name`, `fact_user_ratings.rating_value` (Average), `bridge_content_genres.bridge_id` (Count) | **Gráfico de Dispersión (Scatter Chart) o Treemap** | *"El eje X muestra la calificación promedio y el Y la cantidad de reproducciones/votos. Los géneros en el cuadrante superior derecho son nuestros éxitos absolutos (alta audiencia y amor). Los del cuadrante inferior izquierdo deben ser descontinuados."* |
| **TI / Soporte** | ¿Cómo correlacionamos la antigüedad del contenido con la experiencia del usuario? | `dim_content.release_year`, `fact_user_ratings.rating_sentiment` | **Gráfico de Columnas Apiladas** | *"Permite evaluar si el contenido retro o clásico tiene mejor recepción que los lanzamientos de estreno recientes, orientando las decisiones de compra de licencias basadas en la respuesta emocional real del usuario."* |

---

## 5. Recomendaciones Estratégicas para la Alta Dirección (Plan de Acción)

Con el nuevo modelo estrella en funcionamiento, la compañía está en posición de tomar decisiones basadas en datos científicos inmediatos. Recomendamos implementar el siguiente plan de acción corporativo:

### 1. Plan de Rescate del Onboarding (Producto)
* **Hallazgo:** Analizar con `dim_users.is_complete_profile` si los usuarios con perfiles vacíos o registros inconclusos tienen una retención significativamente menor en las primeras semanas.
* **Acción:** Si se comprueba la hipótesis, implementar un flujo de registro "social rápido" (Google/Apple login) y retrasar la solicitud de datos demográficos pesados hasta que el usuario haya consumido al menos 3 horas de video.

### 2. Segmentación Avanzada y Re-focalización Comercial (Comercial)
* **Hallazgo:** Cruzar `loyalty_category` y `generation` para descubrir a nuestro cliente ideal.
* **Acción:** Si los "Millennials" constituyen el 60% de nuestra categoría "Leyenda", y la "Gen Z" domina en la categoría "Extra" (abandono rápido), se debe reestructurar la pauta publicitaria. El equipo comercial debe enfocar campañas de conversión premium específicas para Gen Z utilizando música de Spotify (conecta mejor con ellos) y retener a los Millennials con películas nostálgicas en el catálogo de Netflix.

### 3. Auditoría Ciega al Algoritmo de Recomendación (Marketing)
* **Hallazgo:** Evaluar la proporción de `rating_sentiment` en películas sugeridas por el sistema.
* **Acción:** Si el sentimiento dominante es 'Indiferente' o 'Crítica', suspender temporalmente el algoritmo actual y realizar una prueba A/B en producción: un 50% de usuarios recibirá recomendaciones basadas en géneros altamente calificados de su mismo cohorte generacional (fácilmente consultables con nuestra estrella `dim_genres` y `dim_users`), y el otro 50% con el sistema antiguo.

### 4. Expansión a Telemetría de Red y Logs Técnicos (Recomendación de Infraestructura)
* **Hallazgo:** Confirmar o desmentir la sospecha del equipo de TI sobre los errores de buffering y caídas en regiones específicas.
* **Acción:** Dado que los datasets analíticos de catálogo no contienen logs de tráfico, se recomienda al equipo de tecnología capturar los logs HTTP de los reproductores móviles y Smart TVs a un bucket de Amazon S3, procesarlos con Apache Spark e integrarlos a dbt mediante una nueva tabla de hechos llamada **`fact_network_telemetry`**. Esta nueva tabla se unirá directamente con las dimensiones `dim_users` y `dim_date` que ya dejamos construidas, permitiendo mapear latencias y errores de buffering por país y tipo de dispositivo al instante.

---

*Reporte preparado para la Alta Dirección y Equipos de Analítica del Caso 3. El modelo analítico dimensional se encuentra completamente optimizado y documentado en el repositorio dbt corporativo.*
