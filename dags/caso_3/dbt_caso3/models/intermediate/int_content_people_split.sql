WITH netflix_cast AS (
    SELECT 
        'netflix_' || CAST(numericId AS VARCHAR) AS contentId,
        TRIM(UNNEST(STRING_TO_ARRAY(castMembers, ','))) AS person_name,
        'Actor' AS role
    FROM {{ ref('stg_netflix_titles') }}
    WHERE castMembers IS NOT NULL
),

netflix_directors AS (
    SELECT 
        'netflix_' || CAST(numericId AS VARCHAR) AS contentId,
        TRIM(UNNEST(STRING_TO_ARRAY(director, ','))) AS person_name,
        'Director' AS role
    FROM {{ ref('stg_netflix_titles') }}
    WHERE director IS NOT NULL
),

spotify_artists AS (
    SELECT 
        'spotify_' || CAST(numericId AS VARCHAR) AS contentId,
        -- Limpiamos los corchetes o comillas en caso de que vengan como array de texto
        TRIM(UNNEST(STRING_TO_ARRAY(
            REPLACE(REPLACE(REPLACE(artistNames, '[', ''), ']', ''), '''', ''), 
            ','
        ))) AS person_name,
        'Artist' AS role
    FROM {{ ref('stg_spotify_tracks') }}
    WHERE artistNames IS NOT NULL
),

unified_people AS (
    SELECT * FROM netflix_cast
    UNION ALL
    SELECT * FROM netflix_directors
    UNION ALL
    SELECT * FROM spotify_artists
)

SELECT 
    contentId,
    person_name,
    role
FROM unified_people
WHERE person_name IS NOT NULL AND person_name != ''
