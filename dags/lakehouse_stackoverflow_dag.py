"""
DAG del Proyecto 2: arquitectura Lakehouse (Medallion) con el dataset de StackOverflow.

Un solo "Play" ejecuta las tres capas, en orden:

    bronze_ingest  ->  silver_transform  ->  gold_agg

1. bronze_ingest     DLT lee la fuente `users` y guarda users_2021 en Bronze (Parquet, override).
2. silver_transform  PySpark + Iceberg: consolida users_hist (2020 + 2021) con MERGE.
3. gold_agg          PySpark + Iceberg: cruza post_hist y users_hist y genera
                     gold.cant_post_x_user_hist con MERGE.

Las tareas de Spark se envian al cluster (spark://spark-master:7077) con SparkSubmitOperator.
La conexion "spark_default" la crea automaticamente el servicio airflow-init del docker-compose.
Los mismos tres scripts existen como notebooks (bronze_ingest, silver_transform, gold_agg) por si
hay que ejecutarlos a mano.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

JOBS = "/opt/airflow/jobs"

default_args = {
    "owner": "grupo3",
    "retries": 1,
}

with DAG(
    dag_id="lakehouse_stackoverflow",
    description="Bronze (DLT) -> Silver (Spark + Iceberg) -> Gold (Spark + Iceberg)",
    default_args=default_args,
    schedule=None,                      # se dispara a mano con el boton Play
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "medallion", "stackoverflow"],
) as dag:

    # Tarea 1 - Bronze: ingesta con DLT hacia MinIO (Parquet, override)
    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=f"python {JOBS}/bronze_ingest.py",
    )

    # Tarea 2 - Silver: users_hist en Iceberg con MERGE (Spark)
    silver_transform = SparkSubmitOperator(
        task_id="silver_transform",
        application=f"{JOBS}/silver_transform.py",
        application_args=["--tabla", "users"],
        conn_id="spark_default",
        name="silver_transform",
        verbose=True,
    )

    # Tarea 3 - Gold: metricas en Iceberg con MERGE (Spark)
    gold_agg = SparkSubmitOperator(
        task_id="gold_agg",
        application=f"{JOBS}/gold_agg.py",
        conn_id="spark_default",
        name="gold_agg",
        verbose=True,
    )

    bronze_ingest >> silver_transform >> gold_agg
