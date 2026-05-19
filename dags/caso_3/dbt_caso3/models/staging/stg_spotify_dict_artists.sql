WITH sourceData AS (
    SELECT * FROM {{ source('raw_layer', 'spotify_dict_artists') }}
),

renamedAndCasted AS (
    SELECT
        CAST(artist_id AS VARCHAR) AS artistId,
        CAST(related_artists AS VARCHAR) AS relatedArtists
    FROM sourceData
)

SELECT * FROM renamedAndCasted
