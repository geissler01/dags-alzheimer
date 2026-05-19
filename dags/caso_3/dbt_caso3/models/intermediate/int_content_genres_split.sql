WITH movies_genres AS (
    SELECT 
        'movielens_' || CAST(movieId AS VARCHAR) AS contentId,
        UNNEST(STRING_TO_ARRAY(
            CASE WHEN genres = '(no genres listed)' THEN 'Unknown' ELSE genres END, 
            '|'
        )) AS genre_name
    FROM {{ ref('stg_movielens_movies') }}
),

netflix_genres AS (
    SELECT 
        'netflix_' || CAST(numericId AS VARCHAR) AS contentId,
        TRIM(UNNEST(STRING_TO_ARRAY(categories, ','))) AS genre_name
    FROM {{ ref('stg_netflix_titles') }}
    WHERE categories IS NOT NULL
),

unified_genres AS (
    SELECT * FROM movies_genres
    UNION ALL
    SELECT * FROM netflix_genres
)

SELECT 
    contentId,
    -- Limpieza básica del texto para estandarizar
    TRIM(genre_name) AS genre_name
FROM unified_genres
WHERE genre_name IS NOT NULL AND genre_name != ''
