-- =====================================================================================
-- TRINO: consultas sobre SILVER que replican la tabla GOLD (gold.cant_post_x_user_hist)
-- Catalogo: lakehouse   (Iceberg + Nessie)   |   Conexion JDBC: localhost:8082, usuario "admin"
-- Ejecutar con DBeaver (driver Trino) o con:  docker compose exec trino trino
-- =====================================================================================

-- 1) Las tablas de Silver son consultables desde Trino
SHOW SCHEMAS FROM lakehouse;
SHOW TABLES FROM lakehouse.silver;

SELECT 'users_hist' AS tabla, count(*) AS filas, min(fecha_cargue) AS fecha_cargue FROM lakehouse.silver.users_hist
UNION ALL
SELECT 'post_hist',           count(*),          min(fecha_cargue)                  FROM lakehouse.silver.post_hist;

-- 2) REPLICA de Gold: misma regla de negocio del job gold_agg, leyendo solo de Silver
SELECT
    u.id                       AS user_id,
    u.display_name,
    u.reputation,
    u.creation_date            AS fecha_registro,
    u.location,
    m.anio_post,
    m.total_posts,
    m.total_preguntas,
    m.total_respuestas,
    m.score_total,
    m.score_promedio,
    m.vistas_total,
    m.ultimo_post
FROM (
    SELECT
        owner_user_id,
        anio_post,
        count(*)                                           AS total_posts,
        sum(CASE WHEN post_type_id = 1 THEN 1 ELSE 0 END)  AS total_preguntas,
        sum(CASE WHEN post_type_id = 2 THEN 1 ELSE 0 END)  AS total_respuestas,
        sum(score)                                         AS score_total,
        avg(score)                                         AS score_promedio,
        sum(view_count)                                    AS vistas_total,
        max(creation_date)                                 AS ultimo_post
    FROM lakehouse.silver.post_hist
    WHERE owner_user_id IS NOT NULL
    GROUP BY owner_user_id, anio_post
) m
JOIN lakehouse.silver.users_hist u ON m.owner_user_id = u.id
ORDER BY m.total_posts DESC, u.id
LIMIT 20;

-- 3) VALIDACION: la replica debe tener las mismas filas que la tabla Gold
WITH replica AS (
    SELECT
        u.id AS user_id, u.display_name, u.reputation, u.creation_date AS fecha_registro, u.location,
        m.anio_post, m.total_posts, m.total_preguntas, m.total_respuestas,
        m.score_total, m.score_promedio, m.vistas_total, m.ultimo_post
    FROM (
        SELECT
            owner_user_id, anio_post,
            count(*)                                           AS total_posts,
            sum(CASE WHEN post_type_id = 1 THEN 1 ELSE 0 END)  AS total_preguntas,
            sum(CASE WHEN post_type_id = 2 THEN 1 ELSE 0 END)  AS total_respuestas,
            sum(score)                                         AS score_total,
            avg(score)                                         AS score_promedio,
            sum(view_count)                                    AS vistas_total,
            max(creation_date)                                 AS ultimo_post
        FROM lakehouse.silver.post_hist
        WHERE owner_user_id IS NOT NULL
        GROUP BY owner_user_id, anio_post
    ) m
    JOIN lakehouse.silver.users_hist u ON m.owner_user_id = u.id
),
gold AS (
    SELECT user_id, display_name, reputation, fecha_registro, location,
           anio_post, total_posts, total_preguntas, total_respuestas,
           score_total, score_promedio, vistas_total, ultimo_post
    FROM lakehouse.gold.cant_post_x_user_hist
)
SELECT
    (SELECT count(*) FROM replica)                                  AS filas_replica,
    (SELECT count(*) FROM gold)                                     AS filas_gold,
    (SELECT count(*) FROM (SELECT * FROM replica EXCEPT SELECT * FROM gold)) AS en_replica_y_no_en_gold,
    (SELECT count(*) FROM (SELECT * FROM gold EXCEPT SELECT * FROM replica)) AS en_gold_y_no_en_replica;
-- Resultado esperado: filas_replica = filas_gold  y  las dos diferencias en 0

-- 4) Un registro puntual, para compararlo uno a uno con lo que muestra Dremio
SELECT * FROM lakehouse.gold.cant_post_x_user_hist ORDER BY total_posts DESC, user_id LIMIT 5;
