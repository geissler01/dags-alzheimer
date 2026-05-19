WITH split_genres AS (
    SELECT * FROM {{ ref('int_content_genres_split') }}
),

genres_dim AS (
    SELECT * FROM {{ ref('dim_genres') }}
),

bridge_content_genres AS (
    SELECT 
        -- PK de la tabla puente
        MD5(s.contentId || s.genre_name) AS bridge_id,
        s.contentId AS content_id,
        d.genre_id
    FROM split_genres s
    INNER JOIN genres_dim d ON s.genre_name = d.genre_name
)

SELECT * FROM bridge_content_genres
