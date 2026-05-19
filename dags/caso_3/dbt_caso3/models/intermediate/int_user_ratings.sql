WITH ratings AS (
    SELECT * FROM {{ ref('stg_movielens_ratings') }}
),

user_ratings AS (
    SELECT
        userId,
        -- ID sintético unificado para conectarse con dim_content
        'movielens_' || CAST(movieId AS VARCHAR) AS contentId,
        -- Conservamos el ID original numérico
        movieId AS original_movie_id,
        rating,
        ratedAt,
        
        -- Extraer fecha sin hora para cruce con dim_date
        CAST(ratedAt AS DATE) AS ratingDate,

        -- Clasificación técnica del sentimiento de la calificación
        CASE 
            WHEN rating <= 2.0 THEN 'Crítica'
            WHEN rating > 2.0 AND rating <= 3.5 THEN 'Indiferente'
            WHEN rating > 3.5 AND rating <= 4.5 THEN 'Favorable'
            ELSE 'Sobresaliente'
        END AS ratingSentiment

    FROM ratings
)

SELECT * FROM user_ratings
