"""
Genera los notebooks de respaldo a partir de los scripts de jobs/ (formato "# %%").
Asi el notebook y la tarea del DAG tienen exactamente la misma logica.

Uso (desde la raiz del proyecto):
    python scripts/make_notebooks.py
"""
import json
import os

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOBS = os.path.join(RAIZ, "jobs")
NOTEBOOKS = os.path.join(RAIZ, "notebooks")

# notebook -> (titulo, descripcion, [scripts de jobs que lo componen, en orden])
NOTEBOOKS_DEF = {
    "bronze_ingest": (
        "bronze_ingest",
        "Respaldo manual de la **Tarea 1** del DAG. Parte A: carga manual de los Parquet que no pasan por el "
        "pipeline. Parte B: ingesta con DLT (la que ejecuta Airflow).",
        ["bronze_manual.py", "bronze_ingest.py"],
    ),
    "silver_transform": (
        "silver_transform",
        "Respaldo manual de la **Tarea 2** del DAG. Construye `silver.users_hist` y `silver.post_hist` "
        "(en el DAG solo se ejecuta `users`).",
        ["silver_transform.py"],
    ),
    "gold_agg": (
        "gold_agg",
        "Respaldo manual de la **Tarea 3** del DAG. Genera `gold.cant_post_x_user_hist`.",
        ["gold_agg.py"],
    ),
}


def a_celdas(ruta):
    """Parte un script en celdas usando los marcadores '# %%' y '# %% [markdown]'."""
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
                             "catalogo Nessie `http://nessie:19120`."}]
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
        # nbformat 4.5 exige un id por celda
        for i, c in enumerate(nb["cells"]):
            c["id"] = f"celda-{i:02d}"
        destino = os.path.join(NOTEBOOKS, f"{nombre}.ipynb")
        with open(destino, "w", encoding="utf-8") as f:
            json.dump(nb, f, ensure_ascii=False, indent=1)
        print("generado:", destino, f"({len(celdas)} celdas)")


if __name__ == "__main__":
    main()
