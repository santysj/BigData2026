# BigData2026

Proyecto de Prueba de GitHub Actions y MLFlow

## Nueva práctica integrada en Databricks

La secuencia práctica se encuentra en [`practicas/`](practicas/README.md). La primera clase incluye setup para Databricks Free Edition, generación de datos, ingesta CSV/JSON/Parquet, tablas Bronze en Delta y un desafío de evolución de esquema. Están implementadas las clases 1–4; la clase 5 de MLOps sigue planificada.

La [clase práctica 3 de NoSQL](practicas/clase-03/README.md) reorganiza las tablas Silver según los modelos vistos en teoría: CAP/PACELC, clave-valor, documental, grafos, vectorial y columnar, con notebooks demostrativos y 20 preguntas de comprensión.

La [clase práctica 4 de streaming](practicas/clase-04/README.md) continúa con las dimensiones Silver: Auto Loader, deduplicación, ventanas, watermarks, checkpoints y recuperación, con Job, preguntas y formato de entrega.

Este proyecto fue creado con el objetivo de probar y demostrar cómo configurar y utilizar **GitHub Actions** en un entorno de desarrollo Python.
Ademas como ejemplo para mostrar capacidades de MLflow 

Actions
-------

Los ejemplos siguientes pertenecen al origen del proyecto, dedicado a pruebas de integración continua (CI) y MLflow. El estado actual del repositorio no incluye un workflow bajo `.github/workflows/`; estos ejemplos no son requisitos para ejecutar los TPs de Databricks.


python -m prueba .venv   
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
./prueba/Scripts/activate  
git clone https://github.com/darioabadie/BigData2026.git
git status
git add .
git status
git commit -m "probar actions"
git push origin main


EJECUTAR MLFlow
---------------
1. mlflow server --backend-store-uri sqlite:///mlflow.db --port 5000
MLflow escuchando en http://localhost:5000/

2. Para Docker
Crear dockerfile con:
###################
FROM python:3.10

RUN pip install mlflow

EXPOSE 5000

CMD ["mlflow", "server", "--backend-store-uri", "sqlite:///mlflow.db", "--default-artifact-root", "/mlruns", "--host", "0.0.0.0", "--port", "5000"]
###############
Y ejecutar
    docker build -t mlflow-server .      
    docker run -p 5000:5000 mlflow-server .

3. local: mlflow ui
