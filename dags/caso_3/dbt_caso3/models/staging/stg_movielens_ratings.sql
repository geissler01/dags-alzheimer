WITH sourceData AS (
    SELECT * FROM {{ source('raw_layer', 'movielens_ratings') }}
),

renamedAndCasted AS (
    SELECT
        -- 1. Identificadores en camelCase y tipo entero
        CAST("userId" AS INTEGER) AS userId,
        CAST("movieId" AS INTEGER) AS movieId,
        
        -- 2. Calificación tipada como decimal (rango 0.5 a 5.0)
        CAST(rating AS DECIMAL(3, 1)) AS rating,
        
        -- 3. Convertimos los segundos Epoch UNIX a una fecha timestamp normal
        TO_TIMESTAMP(CAST("timestamp" AS BIGINT)) AS ratedAt
    FROM sourceData
)

SELECT * FROM renamedAndCasted
