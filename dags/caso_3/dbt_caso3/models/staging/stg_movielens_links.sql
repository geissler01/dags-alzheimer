WITH sourceData AS (
    SELECT * FROM {{ source('raw_layer', 'movielens_links') }}
),

renamedAndCasted AS (
    SELECT
        CAST("movieId" AS INTEGER) AS movieId,
        CAST("imdbId" AS VARCHAR) AS imdbId,
        CAST("tmdbId" AS VARCHAR) AS tmdbId
    FROM sourceData
)

SELECT * FROM renamedAndCasted
