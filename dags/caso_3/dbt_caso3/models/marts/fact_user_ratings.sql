-- =========================================================================
-- Tabla de Hechos: Calificaciones de Usuarios (Ratings)
-- =========================================================================
-- Esta es la tabla central de nuestro modelo estrella (Kimball).
-- Aquí guardamos cada vez que un usuario interactúa y califica un contenido.
-- La conectamos con dim_content, dim_users y dim_date.

WITH ratings AS (
    SELECT * FROM {{ ref('int_user_ratings') }}
),

fact_user_ratings AS (
    SELECT 
        -- Generamos un ID único (PK) con MD5 para garantizar que no se creen 
        -- registros duplicados si corremos el pipeline de Airflow varias veces (Idempotencia).
        MD5(CAST(userId AS VARCHAR) || CAST(contentId AS VARCHAR) || CAST(ratedAt AS VARCHAR)) AS rating_id,
        
        -- =====================
        -- Claves Foráneas (FK)
        -- =====================
        userId AS user_id,
        contentId AS content_id, -- FK directa al catálogo maestro (Netflix, Spotify, MovieLens)
        ratingDate AS date_id,   -- FK a dim_date (le cortamos la hora para que el cruce sea diario)
        
        -- Dejamos el ID original numérico por si necesitamos hacer un cruce rápido sin el string sintético
        original_movie_id AS movie_id,
        
        -- =====================
        -- Métricas del Negocio
        -- =====================
        rating AS rating_value,
        
        -- Clasificación en texto del score (Ej: 'Sobresaliente', 'Crítica') para graficar rápido en Power BI
        ratingSentiment AS rating_sentiment,
        
        -- Guardamos el timestamp exacto como atributo degenerado por si se ocupa
        ratedAt AS rated_at
        
    FROM ratings
)

SELECT * FROM fact_user_ratings
