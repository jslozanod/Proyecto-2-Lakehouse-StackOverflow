-- =====================================================================================
-- TRINO: consultas sobre SILVER que replican la tabla GOLD (gold.cant_post_x_user_hist)
-- Catalogo: lakehouse   (Iceberg + Nessie)   |   Conexion JDBC: localhost:8082, usuario "admin"
-- Ejecutar con DBeaver (driver Trino) o con:  docker compose exec trino trino
-- Nota: en los JOIN la tabla agregada (la chica) va a la derecha para que Trino arme en
-- memoria esa y no los 2,5 millones de usuarios.
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
FROM lakehouse.silver.users_hist u
JOIN (
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
) m ON m.owner_user_id = u.id
ORDER BY m.total_posts DESC, u.id
LIMIT 20;

-- 3) VALIDACION: se calcula la replica UNA sola vez y se compara contra la tabla Gold,
--    fila por fila (llave user_id + anio_post). IS DISTINCT FROM compara tambien los NULL.
WITH replica AS (
    SELECT
        u.id AS user_id, u.display_name, u.reputation, u.creation_date AS fecha_registro, u.location,
        m.anio_post, m.total_posts, m.total_preguntas, m.total_respuestas,
        m.score_total, m.score_promedio, m.vistas_total, m.ultimo_post
    FROM lakehouse.silver.users_hist u
    JOIN (
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
    ) m ON m.owner_user_id = u.id
)
SELECT
    count(r.user_id)                                                    AS filas_replica,
    count(g.user_id)                                                    AS filas_gold,
    count_if(g.user_id IS NULL)                                         AS solo_en_replica,
    count_if(r.user_id IS NULL)                                         AS solo_en_gold,
    count_if(r.user_id IS NOT NULL AND g.user_id IS NOT NULL AND (
           r.display_name     IS DISTINCT FROM g.display_name
        OR r.reputation       IS DISTINCT FROM g.reputation
        OR r.fecha_registro   IS DISTINCT FROM g.fecha_registro
        OR r.location         IS DISTINCT FROM g.location
        OR r.total_posts      IS DISTINCT FROM g.total_posts
        OR r.total_preguntas  IS DISTINCT FROM g.total_preguntas
        OR r.total_respuestas IS DISTINCT FROM g.total_respuestas
        OR r.score_total      IS DISTINCT FROM g.score_total
        OR r.score_promedio   IS DISTINCT FROM g.score_promedio
        OR r.vistas_total     IS DISTINCT FROM g.vistas_total
        OR r.ultimo_post      IS DISTINCT FROM g.ultimo_post))          AS filas_con_diferencias
FROM replica r
FULL OUTER JOIN lakehouse.gold.cant_post_x_user_hist g
    ON r.user_id = g.user_id AND r.anio_post = g.anio_post;
-- Resultado esperado: filas_replica = filas_gold  y  solo_en_replica, solo_en_gold, filas_con_diferencias en 0

-- 4) Un registro puntual, para compararlo uno a uno con lo que muestra Dremio
SELECT * FROM lakehouse.gold.cant_post_x_user_hist ORDER BY total_posts DESC, user_id LIMIT 5;
