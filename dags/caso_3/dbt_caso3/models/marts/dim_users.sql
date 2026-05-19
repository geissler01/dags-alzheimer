WITH enriched_users AS (
    SELECT * FROM {{ ref('int_users_enriched') }}
),

dim_users AS (
    SELECT 
        -- Surrogate key es preferible, pero usaremos el userId numérico para mantener la relación con interactions
        userId AS user_id,
        originalUuid AS original_uuid,
        username,
        gender,
        age,
        generation,
        
        -- Geografía
        city,
        state,
        country,
        nationality,
        latitude,
        longitude,
        
        -- Métricas / Flags
        accountAgeDays AS account_age_days,
        isCompleteProfile AS is_complete_profile,
        
        -- Fechas
        dateOfBirth AS date_of_birth,
        registeredAt AS registered_at
        
    FROM enriched_users
)

SELECT * FROM dim_users
