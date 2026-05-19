-- =========================================================================
-- Modelo Intermedio: Desglose de Géneros (Unnesting)
-- =========================================================================
-- Los datasets originales traen los géneros concatenados en una sola columna.
-- Ejemplo MovieLens: "Action|Adventure|Sci-Fi"
-- Ejemplo Netflix: "Comedies, Romantic Movies"
-- Aquí utilizamos UNNEST para crear una fila individual por cada género, 
-- lo que nos permitirá construir la tabla puente más adelante.

WITH movies_genres AS (
    SELECT 
        'movielens_' || CAST(movieId AS VARCHAR) AS contentId,
        -- Separamos por el pipe (|) y convertimos a múltiples filas
        UNNEST(STRING_TO_ARRAY(
            CASE WHEN genres = '(no genres listed)' THEN 'Unknown' ELSE genres END, 
            '|'
        )) AS genre_name
    FROM {{ ref('stg_movielens_movies') }}
),

netflix_genres AS (
    SELECT 
        'netflix_' || CAST(numericId AS VARCHAR) AS contentId,
        -- Separamos por coma (,) y limpiamos los espacios en blanco
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
    -- Limpieza final del texto para estandarizar (evita duplicados por espacios)
    TRIM(genre_name) AS genre_name
FROM unified_genres
WHERE genre_name IS NOT NULL AND genre_name != ''
