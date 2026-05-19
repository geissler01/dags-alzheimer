WITH movies AS ( -- primera tabla temporal
    SELECT 
        'movielens_' || CAST(movieId AS VARCHAR) AS contentId,
        CAST(movieId AS VARCHAR) AS originalId, -- Guardamos el ID original puro
        title,
        'Movie' AS contentType, -- una columna 'contentType' donde todas las filas diran 'Movie'
        releaseYear, -- año que ya antes calculamos
        'MovieLens' AS sourcePlatform -- etiqueta forzada con el origen
    FROM {{ ref('int_movies_enriched') }} -- usamos una tabla ya de esta misma capa, todos los datos vienen de ahi
),

netflix AS (
    SELECT 
        'netflix_' || CAST(numericId AS VARCHAR) AS contentId, -- usamos el nuevo ID numerico para la union
        showId AS originalId, -- Guardamos el ID original puro (alfanumérico)
        title,
        "type" AS contentType, -- aqui si va a haver variedad de contenido (agregamos comillas por si acaso)
        releaseYear,
        'Netflix' AS sourcePlatform
    FROM {{ ref('stg_netflix_titles') }}
),

spotify AS (
    SELECT 
        'spotify_' || CAST(numericId AS VARCHAR) AS contentId, -- usamos el nuevo ID numerico para la union
        trackId AS originalId, -- Guardamos el ID original puro (hash)
        trackName AS title,
        'Track' AS contentType,
        CAST(SUBSTRING(releaseDate FROM 1 FOR 4) AS INTEGER) AS releaseYear,
        'Spotify' AS sourcePlatform
    FROM {{ ref('stg_spotify_tracks') }}
),

unified_content AS (  -- junta las tablas temprales en vertical
    SELECT * FROM movies
    UNION ALL
    SELECT * FROM netflix
    UNION ALL
    SELECT * FROM spotify
)

SELECT * FROM unified_content
