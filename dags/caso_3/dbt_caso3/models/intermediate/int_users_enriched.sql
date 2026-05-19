WITH staging_users AS (
    SELECT * FROM {{ ref('stg_users_generated') }} -- codigo dbt
),

enriched_users AS (
    SELECT 
        userId,
        originalUuid,
        username,
        gender,
        firstName,
        lastName,
        email,
        age,
        dateOfBirth,
        registeredAt,
        phone,
        city,
        state,
        country,
        postcode,
        latitude,
        longitude,
        nationality,

        -- 1. Clasificación por Generaciones
        CASE
            WHEN EXTRACT(YEAR FROM dateOfBirth) BETWEEN 1946 AND 1964 THEN 'Boomer'
            WHEN EXTRACT(YEAR FROM dateOfBirth) BETWEEN 1965 AND 1980 THEN 'Gen X'
            WHEN EXTRACT(YEAR FROM dateOfBirth) BETWEEN 1981 AND 1996 THEN 'Millennial'
            WHEN EXTRACT(YEAR FROM dateOfBirth) BETWEEN 1997 AND 2012 THEN 'Gen Z'
            WHEN EXTRACT(YEAR FROM dateOfBirth) >= 2013 THEN 'Gen Alpha'
            ELSE 'Unknown' -- por sin hay un matusalen
        END AS generation, -- nueva colimna de genraciones

        -- 2. Antigüedad de la cuenta en días (usando CURRENT_DATE para obtener el total de días)
        (CURRENT_DATE - CAST(registeredAt AS DATE)) AS accountAgeDays,

        -- 2.5 Nivel de Lealtad (Clasificación temática de cine basada en años)
        CASE 
            WHEN (CURRENT_DATE - CAST(registeredAt AS DATE)) < 365 THEN 'Extra (Nuevo)' -- Menos de 1 año
            WHEN (CURRENT_DATE - CAST(registeredAt AS DATE)) < 3650 THEN 'Protagonista (Frecuente)' -- Entre 1 y 10 años
            ELSE 'Leyenda de Hollywood (Veterano)' -- Más de 10 años
        END AS loyaltyCategory,

        -- 3. Bandera de Perfil Completo
        CASE 
            WHEN phone IS NOT NULL AND email IS NOT NULL AND city IS NOT NULL 
            THEN TRUE 
            ELSE FALSE 
        END AS isCompleteProfile

    FROM staging_users
)

SELECT * FROM enriched_users
