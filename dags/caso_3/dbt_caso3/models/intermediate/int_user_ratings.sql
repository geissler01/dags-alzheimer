-- =========================================================================
-- Modelo Intermedio: Limpieza y Enriquecimiento de Calificaciones (Ratings)
-- =========================================================================
-- En este paso transformamos los registros crudos de calificaciones de MovieLens.
-- Preparamos los identificadores para la unificación y creamos la lógica de negocio
-- para el análisis de sentimiento (qué tan buena o mala le pareció la película al usuario).

WITH ratings AS (
    SELECT * FROM {{ ref('stg_movielens_ratings') }}
),

user_ratings AS (
    SELECT
        userId,
        
        -- ID sintético ('movielens_123') para poder conectarnos más tarde con dim_content
        'movielens_' || CAST(movieId AS VARCHAR) AS contentId,
        
        -- Conservamos el ID numérico puro por si el equipo de BI necesita hacer cruces manuales rápidos
        movieId AS original_movie_id,
        
        rating,
        ratedAt,
        
        -- Extraemos la fecha sin la hora exacta. 
        -- Esto es vital para poder cruzar con nuestra dimensión de tiempo (dim_date) a nivel diario.
        CAST(ratedAt AS DATE) AS ratingDate,

        -- Lógica de Negocio: Análisis de Sentimiento de la Calificación.
        -- Transformamos un simple número en una categoría accionable para el equipo de Marketing.
        CASE 
            WHEN rating <= 2.0 THEN 'Crítica'
            WHEN rating > 2.0 AND rating <= 3.5 THEN 'Indiferente'
            WHEN rating > 3.5 AND rating <= 4.5 THEN 'Favorable'
            ELSE 'Sobresaliente'
        END AS ratingSentiment

    FROM ratings
)

SELECT * FROM user_ratings
