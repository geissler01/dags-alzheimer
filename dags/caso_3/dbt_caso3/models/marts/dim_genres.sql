WITH distinct_genres AS (
    SELECT DISTINCT genre_name
    FROM {{ ref('int_content_genres_split') }}
),

dim_genres AS (
    SELECT 
        -- Surrogate Key usando MD5 para el género
        MD5(genre_name) AS genre_id,
        genre_name
    FROM distinct_genres
)

SELECT * FROM dim_genres
