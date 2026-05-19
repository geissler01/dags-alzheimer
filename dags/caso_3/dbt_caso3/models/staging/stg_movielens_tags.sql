WITH sourceData AS (
    SELECT * FROM {{ source('raw_layer', 'movielens_tags') }}
),

renamedAndCasted AS (
    SELECT
        -- 1. Identificadores en camelCase y tipo entero
        CAST("userId" AS INTEGER) AS userId,
        CAST("movieId" AS INTEGER) AS movieId,
        
        -- 2. Etiqueta de texto
        CAST(tag AS VARCHAR) AS tag,
        
        -- 3. Conversión del timestamp Unix a fecha normal en camelCase
        TO_TIMESTAMP(CAST("timestamp" AS BIGINT)) AS taggedAt
    FROM sourceData
)

SELECT * FROM renamedAndCasted
