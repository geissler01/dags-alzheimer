WITH sourceData AS (
    SELECT * FROM {{ source('raw_layer', 'spotify_artists') }}
),

renamedAndCasted AS (
    SELECT
        -- Identificadores y nombres en camelCase
        CAST(id AS VARCHAR) AS artistId,
        CAST(name AS VARCHAR) AS artistName,
        
        -- Datos numéricos tipados de forma correcta
        CAST(followers AS DOUBLE PRECISION) AS followers,
        CAST(popularity AS INTEGER) AS popularity,
        
        -- Géneros del artista como cadena de texto
        CAST(genres AS VARCHAR) AS genres
    FROM sourceData
)

SELECT * FROM renamedAndCasted
