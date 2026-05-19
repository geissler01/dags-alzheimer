WITH sourceData AS (
    SELECT * FROM {{ source('raw_layer', 'movielens_genome_scores') }}
),

renamedAndCasted AS (
    SELECT
        CAST("movieId" AS INTEGER) AS movieId,
        CAST("tagId" AS INTEGER) AS tagId,
        CAST(relevance AS DOUBLE PRECISION) AS relevance
    FROM sourceData
)

SELECT * FROM renamedAndCasted
