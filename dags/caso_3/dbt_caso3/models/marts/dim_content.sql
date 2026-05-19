-- =========================================================================
-- Dimensión de Contenido (Catálogo Unificado)
-- =========================================================================
-- Aquí centralizamos todo el inventario de Netflix, Spotify y MovieLens.
-- Esto es clave para que en Power BI podamos cruzar el contenido de todas
-- las plataformas en un solo lugar sin romper las visualizaciones.

WITH unified_content AS (
    SELECT * FROM {{ ref('int_unified_content') }}
),

dim_content AS (
    SELECT 
        -- PK Sintética que nos asegura que no haya colisión de IDs entre plataformas
        contentId AS content_id,
        
        -- Nombre limpio de la película/serie/canción (sin el año atravesado)
        title,
        
        -- Categorización (Movie, TV Show, Audio Track)
        contentType AS content_type,
        
        -- Año de estreno extraído analíticamente para tendencias de consumo
        releaseYear AS release_year,
        
        -- De dónde viene la data originalmente
        sourcePlatform AS source_platform
    FROM unified_content
)

SELECT * FROM dim_content
