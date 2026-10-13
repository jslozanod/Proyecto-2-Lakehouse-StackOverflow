# %% [markdown]
# # Gold - agregacion (PySpark + Iceberg)
# Tarea 3 del DAG. Cruza las dos tablas historicas de Silver (`post_hist` y `users_hist`) y genera la
# tabla de metricas `gold.cant_post_x_user_hist`: por usuario y anio, cuantos posts publico, cuantas
# preguntas y respuestas, su puntaje y su ultimo post.
#
# Se escribe con **MERGE** (llave: `user_id` + `anio_post`) y lleva la columna `fecha_cargue`.

# %%
import os
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.appName("gold_agg")
    .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
# El lector vectorizado de Iceberg (Arrow) tumba la JVM de los ejecutores al leer tablas con datos
# (SIGSEGV en netty/directBufferAddress); se usa el lector Parquet normal.
spark.conf.set("spark.sql.iceberg.vectorization.enabled", "false")
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

DESTINO = "nessie.gold.cant_post_x_user_hist"

# %%
# La tabla se crea una sola vez, en una ubicacion fija del bucket gold
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

# Metricas por usuario y anio a partir de los posts
metricas = (
    posts.where(F.col("owner_user_id").isNotNull())
    .groupBy("owner_user_id", "anio_post")
    .agg(
        F.count("*").alias("total_posts"),
        F.sum((F.col("post_type_id") == 1).cast("int")).cast("long").alias("total_preguntas"),
        F.sum((F.col("post_type_id") == 2).cast("int")).cast("long").alias("total_respuestas"),
        F.sum("score").cast("long").alias("score_total"),
        # Sin redondear: Spark y Trino redondean distinto los .xx5 exactos (1,025 -> 1,03 vs 1,02).
        # El promedio exacto da el mismo valor en ambos motores; el redondeo se hace al mostrarlo.
        F.avg("score").alias("score_promedio"),
        F.sum("view_count").cast("long").alias("vistas_total"),
        F.max("creation_date").alias("ultimo_post"),
    )
)

# Cruce con users_hist: se agregan los datos del usuario a cada metrica
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
