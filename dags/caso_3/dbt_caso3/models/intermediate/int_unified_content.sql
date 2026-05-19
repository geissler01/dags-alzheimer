-- =========================================================================
-- Modelo Intermedio: Unificación del Catálogo Maestro (Single Source of Truth)
-- =========================================================================
-- Aquí ocurre la magia principal del negocio. Unificamos verticalmente (UNION ALL) 
-- el contenido de MovieLens, Netflix y Spotify en una sola megatabla.
-- Para evitar que el ID 1 de Netflix se mezcle con el ID 1 de Spotify, 
-- creamos un 'contentId' sintético (Ej: 'netflix_1', 'spotify_1').

WITH movies AS ( 
    SELECT 
        -- Creamos la llave maestra agregando el prefijo
        'movielens_' || CAST(movieId AS VARCHAR) AS contentId,
        CAST(movieId AS VARCHAR) AS originalId, -- Guardamos el ID original puro por si acaso
        title,
        'Movie' AS contentType, -- Forzamos el tipo 'Movie' porque todo MovieLens son películas
        releaseYear, -- Año extraído analíticamente en int_movies_enriched
        'MovieLens' AS sourcePlatform -- Sello de procedencia
    FROM {{ ref('int_movies_enriched') }}
),

netflix AS (
    SELECT 
        'netflix_' || CAST(numericId AS VARCHAR) AS contentId, -- Usamos el ID secuencial numérico que generamos en staging
        showId AS originalId, -- Guardamos el ID original alfanumérico (ej: 's1', 's2')
        title,
        "type" AS contentType, -- Netflix ya nos dice si es 'Movie' o 'TV Show'
        releaseYear,
        'Netflix' AS sourcePlatform
    FROM {{ ref('stg_netflix_titles') }}
),

spotify AS (
    SELECT 
        'spotify_' || CAST(numericId AS VARCHAR) AS contentId, -- Igual, usamos nuestro ID numérico secuencial de staging
        trackId AS originalId, -- Guardamos el ID original (hash de spotify)
        trackName AS title,
        'Track' AS contentType, -- Forzamos el tipo porque son pistas musicales
        -- Extraemos solo los primeros 4 dígitos de la fecha para quedarnos con el año
        CAST(SUBSTRING(releaseDate FROM 1 FOR 4) AS INTEGER) AS releaseYear,
        'Spotify' AS sourcePlatform
    FROM {{ ref('stg_spotify_tracks') }}
),

unified_content AS (  
    -- Juntamos las tres plataformas en un solo inventario global
    SELECT * FROM movies
    UNION ALL
    SELECT * FROM netflix
    UNION ALL
    SELECT * FROM spotify
)

SELECT * FROM unified_content
