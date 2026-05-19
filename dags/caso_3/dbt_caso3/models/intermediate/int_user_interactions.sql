WITH ratings AS (
    SELECT * FROM {{ ref('stg_movielens_ratings') }}
),

user_interactions AS (
    SELECT
        userId,
        -- Adaptamos el movieId al nuevo formato unificado de contentId
        'movielens_' || CAST(movieId AS VARCHAR) AS contentId,
        rating,
        ratedAt,
        
        -- Extraer fecha sin hora para cruce con dim_date
        CAST(ratedAt AS DATE) AS interactionDate,

        -- Banderas de engagement basadas en la calificación
        CASE WHEN rating >= 4.0 THEN TRUE ELSE FALSE END AS isHighlyRated,
        CASE WHEN rating <= 2.0 THEN TRUE ELSE FALSE END AS isLowRated

    FROM ratings
)

SELECT * FROM user_interactions
