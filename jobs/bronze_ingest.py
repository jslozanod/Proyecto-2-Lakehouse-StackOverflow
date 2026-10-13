# %% [markdown]
# # Bronze - Parte B: ingesta con DLT (la que ejecuta Airflow)
# Tarea 1 del DAG. DLT lee la fuente `users` (Parquet de StackOverflow), se queda con los registros
# del anio indicado (por defecto 2021, de enero a junio) y los guarda en el bucket `bronze` de MinIO
# en formato Parquet: `bronze/users/2021/users_2021.parquet`.
#
# Escritura **override**: `write_disposition="replace"` y un nombre de archivo fijo, asi ejecutar el
# pipeline varias veces sobrescribe el mismo archivo y no genera duplicados.

# %%
import os
import datetime as dt
import dlt
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# Los nombres de columna de Bronze se dejan tal cual vienen en la fuente (datos crudos)
os.environ.setdefault("SCHEMA__NAMING", "direct")

ORIGEN = os.getenv("USERS_SOURCE", "/data/raw/users.parquet")
ANIO = int(os.getenv("USERS_YEAR", "2021"))
MES_DESDE = int(os.getenv("FECHA_DESDE_MES", "1"))
MES_HASTA = int(os.getenv("FECHA_HASTA_MES", "6"))

# %%
def rango_fechas(anio, mes_desde, mes_hasta):
    utc = dt.timezone.utc
    inicio = dt.datetime(anio, mes_desde, 1, tzinfo=utc)
    fin = dt.datetime(anio + 1, 1, 1, tzinfo=utc) if mes_hasta == 12 else dt.datetime(anio, mes_hasta + 1, 1, tzinfo=utc)
    return inicio, fin


@dlt.resource(name="users", write_disposition="replace")
def users():
    """Lee el Parquet de origen por lotes y entrega solo los usuarios creados en el rango."""
    inicio, fin = rango_fechas(ANIO, MES_DESDE, MES_HASTA)
    tipo = pa.timestamp("ms", tz="UTC")
    ini, fi = pa.scalar(inicio, type=tipo), pa.scalar(fin, type=tipo)
    pf = pq.ParquetFile(ORIGEN)
    for lote in pf.iter_batches(batch_size=250_000):
        t = pa.Table.from_batches([lote])
        sub = t.filter(pc.and_(pc.greater_equal(t["CreationDate"], ini), pc.less(t["CreationDate"], fi)))
        if sub.num_rows:
            yield sub

# %%
# DLT guarda en <bucket>/<dataset_name>/<layout>. Con dataset_name="users" y este patron
# el archivo queda en bronze/users/2021/users_2021.parquet (siempre el mismo nombre: override).
destino = dlt.destinations.filesystem(
    layout=str(ANIO) + "/{table_name}_" + str(ANIO) + ".{ext}",
    warn_unsafe_layout_separators=False,
)
pipeline = dlt.pipeline(
    pipeline_name="bronze_users",
    destination=destino,
    dataset_name="users",
    pipelines_dir=os.getenv("DLT_PIPELINES_DIR", "/tmp/dlt_pipelines"),
)
info = pipeline.run(users(), loader_file_format="parquet")
print(info)
print(f"[bronze dlt] users {ANIO} (meses {MES_DESDE}-{MES_HASTA}) cargado en s3://bronze/users/{ANIO}/users_{ANIO}.parquet")
