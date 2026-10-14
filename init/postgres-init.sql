-- Esto se ejecuta una sola vez, cuando se crea el volumen de PostgreSQL.
-- La base "airflow" ya la crea la imagen con POSTGRES_DB. Acá creamos la de Nessie,
-- así el catálogo queda guardado y no se pierde aunque reiniciemos o recreemos los contenedores.
CREATE DATABASE nessie;
