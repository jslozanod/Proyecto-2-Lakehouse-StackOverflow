# %% [markdown]
# # Bronze - Parte A: carga manual
# Segun el enunciado, solo `users_2021` entra por el pipeline (DLT). El resto de Bronze se carga a mano:
# `post_2020`, `post_2021` y `users_2020`.
#
# Para no ahogar los equipos, cada Parquet original se recorta a un rango de meses (por defecto enero a
# junio) con DuckDB, que filtra archivos grandes con memoria acotada, y luego se sube a MinIO (bucket `bronze`).
# Rutas resultantes: `bronze/post/2020/post_2020.parquet`, `bronze/post/2021/post_2021.parquet`,
# `bronze/users/2020/users_2020.parquet`.

# %%
import os
import duckdb
import s3fs

RAW_DIR = os.getenv("RAW_DIR", "/data/raw")
OUT_DIR = os.getenv("PREPARED_DIR", "/data/prepared")
MES_DESDE = int(os.getenv("FECHA_DESDE_MES", "1"))
MES_HASTA = int(os.getenv("FECHA_HASTA_MES", "6"))
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
BUCKET = "bronze"

# (archivo origen, tabla en bronze, anio)
CARGAS_MANUALES = [
    ("posts_2020.parquet", "post", 2020),
    ("posts_2021.parquet", "post", 2021),
    ("users.parquet", "users", 2020),   # users.parquet trae todos los anios: se recorta a 2020
]

# %%
def rango_fechas(anio, mes_desde, mes_hasta):
    """Devuelve [inicio, fin) del rango de meses, como texto UTC para SQL."""
    fin_anio, fin_mes = (anio + 1, 1) if mes_hasta == 12 else (anio, mes_hasta + 1)
    return f"{anio}-{mes_desde:02d}-01 00:00:00+00", f"{fin_anio}-{fin_mes:02d}-01 00:00:00+00"


def recortar_parquet(origen, destino, anio, mes_desde, mes_hasta):
    """Deja en `destino` solo las filas de `origen` creadas en [anio, meses]. Devuelve el numero de filas."""
    inicio, fin = rango_fechas(anio, mes_desde, mes_hasta)
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='1GB'; SET threads=2; SET temp_directory='/tmp/duckdb_tmp'")
    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet('{origen}')
            WHERE CreationDate >= TIMESTAMPTZ '{inicio}' AND CreationDate < TIMESTAMPTZ '{fin}'
        ) TO '{destino}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    filas = con.execute(f"SELECT count(*) FROM read_parquet('{destino}')").fetchone()[0]
    con.close()
    return filas


def subir_a_minio(ruta_local, clave):
    """Sube el archivo al bucket bronze de MinIO (sobrescribe si ya existe: override)."""
    fs = s3fs.S3FileSystem(
        key=os.getenv("AWS_ACCESS_KEY_ID", "admin"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY", "password"),
        client_kwargs={"endpoint_url": S3_ENDPOINT, "region_name": "us-east-1"},
    )
    fs.put(ruta_local, f"{BUCKET}/{clave}")

# %%
for archivo, tabla, anio in CARGAS_MANUALES:
    origen = os.path.join(RAW_DIR, archivo)
    destino = os.path.join(OUT_DIR, f"{tabla}_{anio}.parquet")
    clave = f"{tabla}/{anio}/{tabla}_{anio}.parquet"
    filas = recortar_parquet(origen, destino, anio, MES_DESDE, MES_HASTA)
    subir_a_minio(destino, clave)
    print(f"[bronze manual] s3://{BUCKET}/{clave}: {filas:,} filas (meses {MES_DESDE}-{MES_HASTA} de {anio})")
