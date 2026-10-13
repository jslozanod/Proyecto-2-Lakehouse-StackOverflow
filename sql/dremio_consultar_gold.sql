-- =====================================================================================
-- DREMIO: consulta de la tabla GOLD (gold.cant_post_x_user_hist)
-- Fuente: nessie  (Iceberg + Nessie)   |   UI: http://localhost:9047   |   usuario admin / admin12345
-- La fuente se crea con:  bash scripts/dremio_init.sh
-- Se ejecuta en el "SQL Runner" de Dremio o desde DBeaver (driver Arrow Flight SQL / JDBC de Dremio).
-- =====================================================================================

-- 1) La tabla Gold es consultable desde Dremio
SELECT count(*) AS filas, min(fecha_cargue) AS fecha_cargue
FROM nessie.gold.cant_post_x_user_hist;
-- Debe dar el mismo numero de filas que la replica calculada en Trino (sql/trino_replicar_gold.sql)

-- 2) Los 5 usuarios con mas posts (comparar uno a uno con la misma consulta en Trino)
SELECT user_id, display_name, anio_post, total_posts, total_preguntas, total_respuestas,
       score_total, score_promedio
FROM nessie.gold.cant_post_x_user_hist
ORDER BY total_posts DESC, user_id
LIMIT 5;

-- 3) Resumen por anio
SELECT anio_post,
       count(*)                          AS usuarios,
       sum(total_posts)                  AS posts,
       sum(total_preguntas)              AS preguntas,
       sum(total_respuestas)             AS respuestas,
       round(avg(score_promedio), 2)     AS score_promedio_medio
FROM nessie.gold.cant_post_x_user_hist
GROUP BY anio_post
ORDER BY anio_post;

-- 4) Un registro puntual
SELECT * FROM nessie.gold.cant_post_x_user_hist WHERE user_id = 12687061;
