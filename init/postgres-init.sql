-- Se ejecuta una sola vez, al crear el volumen de PostgreSQL.
-- La base "airflow" la crea la imagen (POSTGRES_DB). Aqui se crea la del catalogo Nessie,
-- asi el catalogo queda PERSISTENTE aunque se reinicien o recreen los contenedores.
CREATE DATABASE nessie;
