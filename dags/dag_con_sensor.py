from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import pendulum
import os

local_tz = pendulum.timezone("America/Bogota")

default_args = {
    'owner': 'david',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

INPUT_DIR = os.path.expanduser("~/projects/streaming/input")

with DAG(
    dag_id='dag_con_sensor',
    default_args=default_args,
    description='Espera archivo y ejecuta ETL de streaming',
    #schedule='*/10 * * * *',
    schedule=None,
    start_date=datetime(2026, 6, 19, tzinfo=local_tz),
    catchup=False,
    tags=['sensor', 'streaming', 'trigger'],
) as dag:

    esperar_archivo = FileSensor(
        task_id='esperar_archivo_entrada',
        filepath=os.path.join(INPUT_DIR, "nuevos_datos_*.csv"),
        fs_conn_id='fs_default',
        poke_interval=30,
        timeout=600,
        mode='poke',
    )

    ejecutar_streaming = BashOperator(
        task_id='ejecutar_streaming',
        bash_command="""
            source /home/david/projects/ebd_env/bin/activate && \
            cd /home/david/projects/notebooks && \
            jupyter nbconvert --to notebook --execute 05_structured_streaming.ipynb \
            --output 05_executed.ipynb
        """,
    )

    archivar = BashOperator(
        task_id='archivar_archivo',
        bash_command=f"""
            mkdir -p {INPUT_DIR}/procesados && \
            mv {INPUT_DIR}/nuevos_datos_*.csv {INPUT_DIR}/procesados/ 2>/dev/null || true
        """,
    )
