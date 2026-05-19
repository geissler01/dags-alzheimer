WITH sourceData AS (
    SELECT * FROM {{ source('raw_layer', 'users_generated') }}
),

renamedAndCasted AS (
    SELECT
        -- 1. Generamos la clave numérica secuencial que calza 1-a-1 con ratings (1 a 162541)
        ROW_NUMBER() OVER (ORDER BY user_id) AS userId,
        
        -- 2. Guardamos la clave UUID original por si acaso
        CAST(user_id AS VARCHAR) AS originalUuid,
        
        -- 3. Datos demográficos y perfil en camelCase
        CAST(username AS VARCHAR) AS username,
        CAST(gender AS VARCHAR) AS gender,
        CAST(first_name AS VARCHAR) AS firstName,
        CAST(last_name AS VARCHAR) AS lastName,
        CAST(email AS VARCHAR) AS email,
        
        -- 4. Edad tipada como entero
        CAST(age AS INTEGER) AS age,
        
        -- 5. Fechas tipadas a tipo de tiempo normal
        CAST(dob AS TIMESTAMP) AS dateOfBirth,
        CAST(registered_date AS TIMESTAMP) AS registeredAt,
        
        -- 6. Contacto y Ubicación en camelCase
        CAST(phone AS VARCHAR) AS phone,
        CAST(city AS VARCHAR) AS city,
        CAST(state AS VARCHAR) AS state,
        CAST(country AS VARCHAR) AS country,
        CAST(postcode AS VARCHAR) AS postcode,
        
        -- 7. Coordenadas decimales
        CAST(latitude AS DOUBLE PRECISION) AS latitude,
        CAST(longitude AS DOUBLE PRECISION) AS longitude,
        
        CAST(nationality AS VARCHAR) AS nationality
    FROM sourceData
)

SELECT * FROM renamedAndCasted
