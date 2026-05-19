WITH user_tags AS (
    SELECT * FROM {{ ref('int_user_tags') }}
),

dim_tags AS (
    SELECT * FROM {{ ref('dim_tags') }}
),

fact_user_tags AS (
    SELECT
        -- Surrogate Key: Creamos un ID único combinando quién, a qué película, cuándo y qué tag le puso
        MD5(CAST(u.userId AS VARCHAR) || CAST(u.contentId AS VARCHAR) || CAST(u.taggedAt AS VARCHAR) || u.tag_name) AS tag_interaction_id,
        
        -- Claves Foráneas (FK)
        u.userId AS user_id,
        u.contentId AS content_id,
        t.tag_id,
        u.interactionDate AS date_id,
        
        -- Fecha exacta (Timestamp)
        u.taggedAt AS tagged_at
        
    FROM user_tags u
    -- Usamos LEFT JOIN para asegurar que no se pierdan interacciones, aunque debería cruzar 100% perfecto
    LEFT JOIN dim_tags t ON u.tag_name = t.tag_name
)

SELECT * FROM fact_user_tags
