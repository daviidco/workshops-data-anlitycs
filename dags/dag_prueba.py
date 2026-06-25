from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

# Zona horaria local (ajusta según tu región)
local_tz = pendulum.timezone("America/Bogota")

default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='dag_prueba_funcionamiento',
    default_args=default_args,
    description='DAG de prueba para verificar Airflow 3.2.2',
    schedule=None,  # 'None' en lugar de 'None' (sin comillas) o '@once'
    start_date=datetime(2026, 6, 19, tzinfo=local_tz),  # Fecha fija en lugar de days_ago
    catchup=False,
    tags=['prueba', 'verificacion'],
) as dag:

    tarea_hola = BashOperator(
        task_id='saludar',
        bash_command='echo "Airflow 3.2.2 funcionando correctamente en VM Ubuntu"',
    )

    tarea_fecha = BashOperator(
        task_id='mostrar_fecha',
        bash_command='date',
    )
