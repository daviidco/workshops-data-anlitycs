from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import os
import shutil

local_tz = pendulum.timezone("America/Bogota")

default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

def limpiar_directorios(**kwargs):
    dirs_to_clean = [
        os.path.expanduser("~/projects/streaming/checkpoint"),
        "/tmp/spark_temp"
    ]
    for d in dirs_to_clean:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)
    print("Directorios limpiados")

with DAG(
    dag_id='dag_pipeline_completo',
    default_args=default_args,
    description='Pipeline L-E-T-C con dependencias',
    schedule='0 2 * * *',
    start_date=datetime(2026, 6, 19, tzinfo=local_tz),
    catchup=False,
    tags=['pipeline', 'completo', 'letc'],
) as dag:

    limpiar = PythonOperator(
        task_id='limpiar_directorios',
        python_callable=limpiar_directorios,
    )

    extraer = BashOperator(
        task_id='extraer_delta',
        bash_command="""
            source /home/david/projects/ebd_env/bin/activate && \
            cd /home/david/projects/notebooks && \
            jupyter nbconvert --to notebook --execute 02_etl_delta_intro.ipynb \
            --output 02_executed.ipynb
        """,
    )

    transformar = BashOperator(
        task_id='transformar_postgres',
        bash_command="""
            source /home/david/projects/ebd_env/bin/activate && \
            cd /home/david/projects/notebooks && \
            jupyter nbconvert --to notebook --execute 02_etl_postgres.ipynb \
            --output 02_executed.ipynb
        """,
    )

    cargar = BashOperator(
        task_id='cargar_minio',
        bash_command="""
            source /home/david/projects/ebd_env/bin/activate && \
            cd /home/david/projects/notebooks && \
            jupyter nbconvert --to notebook --execute 04_minio_data_lake.ipynb \
            --output 02_executed.ipynb
        """,
    )

