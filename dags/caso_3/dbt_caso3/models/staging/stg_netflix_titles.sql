WITH sourceData AS (
    SELECT * FROM {{ source('raw_layer', 'netflix_titles') }}
),

renamedAndCasted AS (
    SELECT
        -- Generamos un ID numérico secuencial
        CAST(ROW_NUMBER() OVER (ORDER BY show_id) AS INTEGER) AS numericId,
        
        CAST(show_id AS VARCHAR) AS showId,
        CAST(type AS VARCHAR) AS type,
        CAST(title AS VARCHAR) AS title,
        CAST(director AS VARCHAR) AS director,
        CAST("cast" AS VARCHAR) AS castMembers,
        CAST(country AS VARCHAR) AS country,
        
        -- Estandarización numérica en camelCase
        CAST(release_year AS INTEGER) AS releaseYear,
        
        CAST(date_added AS VARCHAR) AS dateAdded,
        CAST(rating AS VARCHAR) AS contentRating,
        CAST(duration AS VARCHAR) AS duration,
        CAST(listed_in AS VARCHAR) AS categories,
        CAST(description AS VARCHAR) AS description
    FROM sourceData
)

SELECT * FROM renamedAndCasted
