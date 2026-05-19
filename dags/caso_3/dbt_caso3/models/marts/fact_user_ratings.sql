WITH ratings AS (
    SELECT * FROM {{ ref('int_user_ratings') }}
),

fact_user_ratings AS (
    SELECT 
        -- PK de la tabla de hechos usando MD5
        MD5(CAST(userId AS VARCHAR) || CAST(contentId AS VARCHAR) || CAST(ratedAt AS VARCHAR)) AS rating_id,
        
        -- Claves Foráneas (FK) para Power BI
        userId AS user_id,
        contentId AS content_id, -- Conexión directa con la unificación (dim_content)
        ratingDate AS date_id,
        
        -- ID numérico de película puro (solicitado por el usuario)
        original_movie_id AS movie_id,
        
        -- Métricas
        rating AS rating_value,
        
        -- Atributos dimensionales degenerados
        ratingSentiment AS rating_sentiment,
        ratedAt AS rated_at
        
    FROM ratings
)

SELECT * FROM fact_user_ratings
