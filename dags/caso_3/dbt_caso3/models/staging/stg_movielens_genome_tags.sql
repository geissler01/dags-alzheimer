WITH sourceData AS (
    SELECT * FROM {{ source('raw_layer', 'movielens_genome_tags') }}
),

renamedAndCasted AS (
    SELECT
        CAST("tagId" AS INTEGER) AS tagId,
        CAST(tag AS VARCHAR) AS tag
    FROM sourceData
)

SELECT * FROM renamedAndCasted
