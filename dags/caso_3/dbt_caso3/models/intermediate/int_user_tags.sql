WITH source_tags AS (
    SELECT * FROM {{ ref('stg_movielens_tags') }}
),

user_tags AS (
    SELECT
        userId,
        -- Adaptamos el movieId para que coincida con el catálogo unificado
        'movielens_' || CAST(movieId AS VARCHAR) AS contentId,
        
        -- Estandarizamos la etiqueta a minúsculas y sin espacios a los lados
        LOWER(TRIM(tag)) AS tag_name,
        
        taggedAt,
        
        -- Fecha sin hora para la dimensión de tiempo
        CAST(taggedAt AS DATE) AS interactionDate
    FROM source_tags
)

SELECT * FROM user_tags
