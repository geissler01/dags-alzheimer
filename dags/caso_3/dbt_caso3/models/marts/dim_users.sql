-- =========================================================================
-- Dimensión de Usuarios (User Profile)
-- =========================================================================
-- Esta dimensión guarda el perfil demográfico completo de nuestra audiencia.
-- Los datos se originaron de una API externa, pasaron por S3 y fueron limpiados aquí.
-- Fundamental para realizar segmentación de campañas de marketing.

WITH enriched_users AS (
    SELECT * FROM {{ ref('int_users_enriched') }}
),

dim_users AS (
    SELECT 
        -- PK: Usaremos el userId numérico para mantener la relación de alto rendimiento con fact_user_ratings
        userId AS user_id,
        
        -- ID original del sistema fuente (por si hay auditorías del equipo técnico)
        originalUuid AS original_uuid,
        
        -- Datos Demográficos
        username,
        gender,
        age,
        generation, -- Segmentación clave para el equipo Comercial (Gen Z, Millennial, etc.)
        
        -- Datos Geográficos para analizar latencias y consumo por región
        city,
        state,
        country,
        nationality,
        latitude,
        longitude,
        
        -- =====================
        -- Métricas del Perfil / Flags
        -- =====================
        -- Vital para análisis de Retención (Churn) y éxito del Onboarding
        accountAgeDays AS account_age_days,
        isCompleteProfile AS is_complete_profile,
        
        -- Auditoría de Tiempo
        dateOfBirth AS date_of_birth,
        registeredAt AS registered_at
        
    FROM enriched_users
)

SELECT * FROM dim_users
