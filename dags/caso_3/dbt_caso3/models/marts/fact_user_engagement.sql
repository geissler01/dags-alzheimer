WITH interactions AS (
    SELECT * FROM {{ ref('int_user_interactions') }}
),

fact_user_engagement AS (
    SELECT 
        -- Surrogate key (PK) for the fact table using MD5
        MD5(CAST(userId AS VARCHAR) || CAST(contentId AS VARCHAR) || CAST(ratedAt AS VARCHAR)) AS engagement_id,
        
        -- Foreign Keys
        userId AS user_id,
        contentId AS content_id,
        interactionDate AS date_id,
        
        -- Timestamps
        ratedAt AS interaction_timestamp,
        
        -- Metrics / Facts
        rating AS engagement_score,
        
        -- Dimensions / Flags
        isHighlyRated AS is_highly_rated,
        isLowRated AS is_low_rated
        
    FROM interactions
)

SELECT * FROM fact_user_engagement
