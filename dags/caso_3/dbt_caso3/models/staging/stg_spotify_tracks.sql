WITH sourceData AS (
    SELECT * FROM {{ source('raw_layer', 'spotify_tracks') }}
),

renamedAndCasted AS (
    SELECT
        -- Generamos un ID numérico secuencial
        CAST(ROW_NUMBER() OVER (ORDER BY id) AS INTEGER) AS numericId,
        
        -- Identificadores y textos
        CAST(id AS VARCHAR) AS trackId,
        CAST(name AS VARCHAR) AS trackName,
        
        -- Datos numéricos de popularidad y duración
        CAST(popularity AS INTEGER) AS popularity,
        CAST(duration_ms AS INTEGER) AS durationMs,
        
        -- Campo explicito casteado como entero
        CAST(explicit AS INTEGER) AS isExplicit,
        
        -- Relación de artistas
        CAST(artists AS VARCHAR) AS artistNames,
        CAST(id_artists AS VARCHAR) AS artistIds,
        CAST(release_date AS VARCHAR) AS releaseDate,
        
        -- Métricas musicales y acústicas tipadas a punto flotante
        CAST(danceability AS DOUBLE PRECISION) AS danceability,
        CAST(energy AS DOUBLE PRECISION) AS energy,
        CAST(key AS INTEGER) AS keySignature,
        CAST(loudness AS DOUBLE PRECISION) AS loudness,
        CAST(mode AS INTEGER) AS mode,
        CAST(speechiness AS DOUBLE PRECISION) AS speechiness,
        CAST(acousticness AS DOUBLE PRECISION) AS acousticness,
        CAST(instrumentalness AS DOUBLE PRECISION) AS instrumentalness,
        CAST(liveness AS DOUBLE PRECISION) AS liveness,
        CAST(valence AS DOUBLE PRECISION) AS valence,
        CAST(tempo AS DOUBLE PRECISION) AS tempo,
        CAST(time_signature AS INTEGER) AS timeSignature
    FROM sourceData
)

SELECT * FROM renamedAndCasted
