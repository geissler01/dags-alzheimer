WITH movies AS (
    SELECT * FROM {{ ref('stg_movielens_movies') }}
), -- esta coma significa espera, primera taza

links AS (
    SELECT * FROM {{ ref('stg_movielens_links') }}
), -- segunda tabla temporal

enriched_movies AS ( --tasa principal, tabla principal
    SELECT 
        m.movieId, -- saca esto de la tabla movie
        m.title,
        m.genres,
        l.imdbId, -- de la tabla links
        l.tmdbId,
        
        -- Separar el año del título si es posible (formato común en MovieLens "Title (Year)")
        -- Extraemos usando regex básico.
        CASE 
            WHEN m.title ~ '\([0-9]{4}\)$' -- el titulo termina con 4 numeros encerrados entre parentesis?
            THEN CAST(SUBSTRING(m.title FROM '\(([0-9]{4})\)$') AS INTEGER) -- corta esos 4 numeros
            ELSE NULL -- pone null si la pelicula no tiene año en el titulo
        END AS releaseYear -- nueva tabla de años de peliculas
        
    FROM movies m -- tabla que habiamos hecho antes y alias
    LEFT JOIN links l ON m.movieId = l.movieId -- segunda tabla que habia hecho y alias, conservo todo lo que tiene movies
)

SELECT * FROM enriched_movies -- para que se materialice todo lo que hicimos y sea tomado por el nombre definitivo de la tabla.

-- El nombre definitivo de la tabla será dado por dtb con el nombre del sql, ene este caso -> int_movies_enriched
