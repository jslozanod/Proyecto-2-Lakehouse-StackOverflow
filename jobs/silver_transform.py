# %% [markdown]
# # Silver - transformación (PySpark + Iceberg)
# Acá leemos los Parquet de Bronze, los limpiamos (tipos, nulos y duplicados), les agregamos la columna
# `fecha_cargue` y los escribimos en tablas Iceberg del catálogo Nessie con **MERGE** (nunca override):
#
# * `silver.users_hist`: users 2020 + 2021. **Esta la procesa el pipeline de Airflow** (Tarea 2).
# * `silver.post_hist`: posts 2020 + 2021. La armamos a mano con este mismo script (`--tabla posts`),
#   como dice el enunciado.
#
# Cada tabla se crea una sola vez (`CREATE TABLE IF NOT EXISTS`, en una carpeta fija del bucket `silver`).
# Por eso, si corremos el script varias veces se actualiza la misma tabla y no aparecen tablas nuevas en MinIO.

# %%
import argparse
import os
from pyspark.sql import SparkSession, Window, functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--tabla", choices=["users", "posts", "all"], default="all",
                    help="que tabla historica construir (el DAG usa 'users')")
args, _ = parser.parse_known_args()
TABLA = os.getenv("SILVER_TABLA", args.tabla)

spark = (
    SparkSession.builder.appName("silver_transform")
    .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
# El lector vectorizado de Iceberg (Arrow) tumbaba la JVM de los ejecutores cuando leía tablas que ya tenían
# datos (SIGSEGV en netty). Lo desactivamos y Spark usa el lector normal de Parquet.
spark.conf.set("spark.sql.iceberg.vectorization.enabled", "false")
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

# %%
def crear_tabla(nombre, columnas, ubicacion):
    """Crea la tabla Iceberg solo si no existe, siempre en la misma carpeta (sin sufijos al azar)."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {nombre} ({columnas})
        USING iceberg
        LOCATION '{ubicacion}'
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'snappy',
            'read.parquet.vectorization.enabled' = 'false'
        )
    """)


def deduplicar(df, clave, orden):
    """Si una misma llave viene repetida, nos quedamos con el registro más reciente."""
    w = Window.partitionBy(clave).orderBy(F.col(orden).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def merge_en_iceberg(df, destino, condicion):
    """Hace el MERGE: actualiza las filas que ya están en la tabla e inserta las nuevas."""
    df.createOrReplaceTempView("origen_merge")
    spark.sql(f"""
        MERGE INTO {destino} t
        USING origen_merge s
        ON {condicion}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# %% [markdown]
# ## users_hist

# %%
def construir_users_hist():
    destino = "nessie.silver.users_hist"
    crear_tabla(destino, """
        id BIGINT, reputation INT, creation_date TIMESTAMP, display_name STRING,
        last_access_date TIMESTAMP, views BIGINT, up_votes BIGINT, down_votes BIGINT,
        location STRING, account_id BIGINT, anio_creacion INT, fecha_cargue TIMESTAMP
    """, "s3://silver/users_hist")

    bronze = spark.read.parquet("s3a://bronze/users/2020/users_2020.parquet",
                                "s3a://bronze/users/2021/users_2021.parquet")
    curado = (
        bronze.select(
            F.col("Id").cast("long").alias("id"),
            F.col("Reputation").cast("string").cast("int").alias("reputation"),   # en el Parquet viene como binary, lo pasamos a texto y luego a número
            F.col("CreationDate").alias("creation_date"),
            F.trim(F.col("DisplayName").cast("string")).alias("display_name"),
            F.col("LastAccessDate").alias("last_access_date"),
            F.col("Views").cast("long").alias("views"),
            F.col("UpVotes").cast("long").alias("up_votes"),
            F.col("DownVotes").cast("long").alias("down_votes"),
            F.trim(F.col("Location").cast("string")).alias("location"),
            F.col("AccountId").cast("long").alias("account_id"),
        )
        .where(F.col("id").isNotNull() & F.col("creation_date").isNotNull())   # regla de calidad: sin id ni fecha de creación no sirve la fila
        .withColumn("anio_creacion", F.year("creation_date"))
    )
    curado = deduplicar(curado, "id", "last_access_date").withColumn("fecha_cargue", F.current_timestamp())

    merge_en_iceberg(curado, destino, "t.id = s.id")
    total = spark.table(destino).count()
    print(f"[silver] {destino}: {total:,} filas despues del merge")

# %% [markdown]
# ## post_hist

# %%
def construir_post_hist():
    destino = "nessie.silver.post_hist"
    crear_tabla(destino, """
        id BIGINT, post_type_id INT, accepted_answer_id BIGINT, creation_date TIMESTAMP,
        score INT, view_count BIGINT, owner_user_id BIGINT, last_activity_date TIMESTAMP,
        tags STRING, answer_count INT, comment_count INT, favorite_count INT,
        parent_id BIGINT, closed_date TIMESTAMP, anio_post INT, fecha_cargue TIMESTAMP
    """, "s3://silver/post_hist")

    bronze = spark.read.parquet("s3a://bronze/post/2020/post_2020.parquet",
                                "s3a://bronze/post/2021/post_2021.parquet")
    # Quitamos Body y Title (son textos largos que no usamos en las métricas) y le ponemos tipo a cada columna
    curado = (
        bronze.select(
            F.col("Id").cast("long").alias("id"),
            F.col("PostTypeId").cast("int").alias("post_type_id"),
            F.col("AcceptedAnswerId").cast("long").alias("accepted_answer_id"),
            F.col("CreationDate").alias("creation_date"),
            F.col("Score").cast("int").alias("score"),
            F.col("ViewCount").cast("long").alias("view_count"),
            F.col("OwnerUserId").cast("long").alias("owner_user_id"),
            F.col("LastActivityDate").alias("last_activity_date"),
            F.col("Tags").cast("string").alias("tags"),
            F.col("AnswerCount").cast("int").alias("answer_count"),
            F.col("CommentCount").cast("int").alias("comment_count"),
            F.col("FavoriteCount").cast("int").alias("favorite_count"),
            F.col("ParentId").cast("string").cast("long").alias("parent_id"),   # en el Parquet viene como binary, lo pasamos a texto y luego a número
            F.col("ClosedDate").alias("closed_date"),
        )
        .where(F.col("id").isNotNull() & F.col("creation_date").isNotNull())
        .withColumn("anio_post", F.year("creation_date"))
    )
    curado = deduplicar(curado, "id", "last_activity_date").withColumn("fecha_cargue", F.current_timestamp())

    merge_en_iceberg(curado, destino, "t.id = s.id")
    total = spark.table(destino).count()
    print(f"[silver] {destino}: {total:,} filas despues del merge")

# %%
if TABLA in ("users", "all"):
    construir_users_hist()
if TABLA in ("posts", "all"):
    construir_post_hist()

spark.stop()
