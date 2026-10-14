# %% [markdown]
# # Gold - agregación (PySpark + Iceberg)
# Esta es la Tarea 3 del DAG. Cruzamos las dos tablas históricas de Silver (`post_hist` y `users_hist`) y
# generamos la tabla de métricas `gold.cant_post_x_user_hist`: por usuario y por año, cuántos posts publicó,
# cuántas preguntas y respuestas, su puntaje y la fecha de su último post.
#
# Se escribe con **MERGE** (llave: `user_id` + `anio_post`) y tiene la columna `fecha_cargue`.

# %%
import os
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.appName("gold_agg")
    .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
# El lector vectorizado de Iceberg (Arrow) tumbaba la JVM de los ejecutores cuando leía tablas que ya tenían
# datos (SIGSEGV en netty). Lo desactivamos y Spark usa el lector normal de Parquet.
spark.conf.set("spark.sql.iceberg.vectorization.enabled", "false")
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

DESTINO = "nessie.gold.cant_post_x_user_hist"

# %%
# Creamos la tabla una sola vez, siempre en la misma carpeta del bucket gold
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DESTINO} (
        user_id BIGINT, display_name STRING, reputation INT, fecha_registro TIMESTAMP, location STRING,
        anio_post INT, total_posts BIGINT, total_preguntas BIGINT, total_respuestas BIGINT,
        score_total BIGINT, score_promedio DOUBLE, vistas_total BIGINT, ultimo_post TIMESTAMP,
        fecha_cargue TIMESTAMP
    )
    USING iceberg
    LOCATION 's3://gold/cant_post_x_user_hist'
    TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'snappy',
        'read.parquet.vectorization.enabled' = 'false'
    )
""")

# %%
posts = spark.table("nessie.silver.post_hist")
users = spark.table("nessie.silver.users_hist")
print(f"[gold] entradas: post_hist={posts.count():,} filas, users_hist={users.count():,} filas")

# Métricas por usuario y por año, calculadas a partir de los posts
metricas = (
    posts.where(F.col("owner_user_id").isNotNull())
    .groupBy("owner_user_id", "anio_post")
    .agg(
        F.count("*").alias("total_posts"),
        F.sum((F.col("post_type_id") == 1).cast("int")).cast("long").alias("total_preguntas"),
        F.sum((F.col("post_type_id") == 2).cast("int")).cast("long").alias("total_respuestas"),
        F.sum("score").cast("long").alias("score_total"),
        # No redondeamos el promedio: Spark y Trino redondean distinto los .xx5 exactos (1,025 da 1,03 en uno y 1,02 en el otro).
        # Si guardamos el promedio exacto da lo mismo en los dos, y el redondeo se hace solo al mostrarlo.
        F.avg("score").alias("score_promedio"),
        F.sum("view_count").cast("long").alias("vistas_total"),
        F.max("creation_date").alias("ultimo_post"),
    )
)

# Cruzamos con users_hist para agregarle a cada métrica los datos del usuario (nombre, reputación, etc.)
gold = (
    metricas.join(users, metricas.owner_user_id == users.id, "inner")
    .select(
        users.id.alias("user_id"),
        users.display_name,
        users.reputation,
        users.creation_date.alias("fecha_registro"),
        users.location,
        metricas.anio_post,
        metricas.total_posts,
        metricas.total_preguntas,
        metricas.total_respuestas,
        metricas.score_total,
        metricas.score_promedio,
        metricas.vistas_total,
        metricas.ultimo_post,
    )
    .withColumn("fecha_cargue", F.current_timestamp())
)

# %%
gold.createOrReplaceTempView("origen_merge")
spark.sql(f"""
    MERGE INTO {DESTINO} t
    USING origen_merge s
    ON t.user_id = s.user_id AND t.anio_post = s.anio_post
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print(f"[gold] {DESTINO}: {spark.table(DESTINO).count():,} filas despues del merge")

spark.stop()
