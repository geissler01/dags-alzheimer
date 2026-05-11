-- sql/crear_tabla_ejercicio_8.sql
CREATE TABLE IF NOT EXISTS enriched_users_poblacion (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender VARCHAR(20),
    email VARCHAR(150),
    city VARCHAR(100),
    country VARCHAR(100),
    age INT,
    population_count BIGINT,
    population_year INT,
    fecha_proceso TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
