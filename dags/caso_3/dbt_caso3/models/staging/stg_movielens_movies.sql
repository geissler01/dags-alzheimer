WITH sourceData AS (
    SELECT * FROM {{ source('raw_layer', 'movielens_movies') }}
),

renamedAndCasted AS (
    SELECT
        -- Identificadores y textos en camelCase tipados de forma simple
        CAST("movieId" AS INTEGER) AS movieId,
        CAST(title AS VARCHAR) AS title,
        CAST(genres AS VARCHAR) AS genres
    FROM sourceData
)

SELECT * FROM renamedAndCasted
