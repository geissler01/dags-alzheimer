WITH all_tags AS (
    -- Tomamos las etiquetas maestras del genoma
    SELECT LOWER(TRIM(tag)) AS tag_name FROM {{ ref('stg_movielens_genome_tags') }}
    
    UNION -- UNION (sin ALL) elimina automáticamente los duplicados
    
    -- Agregamos cualquier etiqueta inventada por el usuario que no esté en el genoma
    SELECT LOWER(TRIM(tag)) AS tag_name FROM {{ ref('stg_movielens_tags') }}
),

dim_tags AS (
    SELECT 
        -- Creamos un ID seguro (MD5) basado en la palabra de la etiqueta
        MD5(tag_name) AS tag_id,
        tag_name
    FROM all_tags
    WHERE tag_name IS NOT NULL AND tag_name != ''
)

SELECT * FROM dim_tags
