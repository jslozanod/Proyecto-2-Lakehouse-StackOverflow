"""
Genera los notebooks de respaldo a partir de los scripts de la carpeta jobs/ (los que tienen marcas "# %%").
Así el notebook y la tarea del DAG tienen exactamente la misma lógica.

Se corre desde la raíz del proyecto:
    python scripts/make_notebooks.py
"""
import json
import os

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOBS = os.path.join(RAIZ, "jobs")
NOTEBOOKS = os.path.join(RAIZ, "notebooks")

# notebook -> (título, descripción, [scripts de jobs que lo forman, en orden])
NOTEBOOKS_DEF = {
    "bronze_ingest": (
        "bronze_ingest",
        "Respaldo para correr a mano la **Tarea 1** del DAG. La Parte A carga los Parquet que no pasan por el "
        "pipeline y la Parte B hace la ingesta con DLT (la misma que ejecuta Airflow).",
        ["bronze_manual.py", "bronze_ingest.py"],
    ),
    "silver_transform": (
        "silver_transform",
        "Respaldo para correr a mano la **Tarea 2** del DAG. Arma `silver.users_hist` y `silver.post_hist` "
        "(en el DAG solo se ejecuta `users`).",
        ["silver_transform.py"],
    ),
    "gold_agg": (
        "gold_agg",
        "Respaldo para correr a mano la **Tarea 3** del DAG. Genera `gold.cant_post_x_user_hist`.",
        ["gold_agg.py"],
    ),
}


def a_celdas(ruta):
    """Corta un script en celdas usando las marcas '# %%' (código) y '# %% [markdown]' (texto)."""
    with open(ruta, encoding="utf-8") as f:
        lineas = f.read().splitlines()
    celdas, tipo, buf = [], None, []

    def cerrar():
        nonlocal buf
        while buf and not buf[-1].strip():
            buf.pop()
        if tipo and buf:
            if tipo == "markdown":
                texto = "\n".join(l[2:] if l.startswith("# ") else l.lstrip("#") for l in buf)
                celdas.append({"cell_type": "markdown", "metadata": {}, "source": texto})
            else:
                celdas.append({"cell_type": "code", "metadata": {}, "execution_count": None,
                               "outputs": [], "source": "\n".join(buf)})
        buf = []

    for l in lineas:
        if l.startswith("# %%"):
            cerrar()
            tipo = "markdown" if "[markdown]" in l else "code"
        elif tipo is not None:
            buf.append(l)
    cerrar()
    return celdas


def main():
    os.makedirs(NOTEBOOKS, exist_ok=True)
    for nombre, (titulo, desc, scripts) in NOTEBOOKS_DEF.items():
        celdas = [{"cell_type": "markdown", "metadata": {},
                   "source": f"# {titulo}\n\n{desc}\n\n"
                             "Servicios: Spark `spark://spark-master:7077`, MinIO `http://minio:9000`, "
                             "catálogo Nessie `http://nessie:19120`."}]
        for s in scripts:
            celdas.extend(a_celdas(os.path.join(JOBS, s)))
        nb = {
            "cells": celdas,
            "metadata": {
                "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                "language_info": {"name": "python"},
            },
            "nbformat": 4,
            "nbformat_minor": 5,
        }
        # el formato nbformat 4.5 pide que cada celda tenga un id
        for i, c in enumerate(nb["cells"]):
            c["id"] = f"celda-{i:02d}"
        destino = os.path.join(NOTEBOOKS, f"{nombre}.ipynb")
        with open(destino, "w", encoding="utf-8") as f:
            json.dump(nb, f, ensure_ascii=False, indent=1)
        print("generado:", destino, f"({len(celdas)} celdas)")


if __name__ == "__main__":
    main()
