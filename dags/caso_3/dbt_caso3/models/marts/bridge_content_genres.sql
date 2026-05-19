-- =========================================================================
-- Tabla Puente: Contenido - Géneros
-- =========================================================================
-- Esta tabla resuelve la relación de 'muchos a muchos' (Many-to-Many) 
-- entre el catálogo de contenido y los géneros.
-- Si una película tiene 3 géneros, aquí habrá 3 filas apuntando al mismo content_id.
-- Esto es fundamental para que Power BI pueda filtrar películas por género
-- sin duplicar los datos ni inflar artificialmente la tabla fact_user_ratings.

WITH split_genres AS (
    SELECT * FROM {{ ref('int_content_genres_split') }}
),

genres_dim AS (
    SELECT * FROM {{ ref('dim_genres') }}
),

bridge_content_genres AS (
    SELECT 
        -- Generamos un ID único MD5 para la tabla puente basado en el contenido y el género
        MD5(s.contentId || s.genre_name) AS bridge_id,
        
        -- Claves Foráneas (FK) para relacionar la dimensión de contenido con la de géneros
        s.contentId AS content_id,
        d.genre_id
        
    FROM split_genres s
    INNER JOIN genres_dim d ON s.genre_name = d.genre_name
)

SELECT * FROM bridge_content_genres
