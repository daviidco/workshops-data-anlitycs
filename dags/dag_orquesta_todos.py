# ~/airflow/dags/dag_orquesta_todos.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("America/Bogota")

default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def verificar_postgres(**kwargs):
    import psycopg2
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="datamart",
            user="postgres",
            password="123abc"
        )
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print(f"PostgreSQL conectado: {version[0]}")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error PostgreSQL: {e}")
        raise

def verificar_minio(**kwargs):
    import urllib.request
    try:
        response = urllib.request.urlopen("http://localhost:9000/minio/health/live", timeout=5)
        if response.status == 200:
            print("MinIO está saludable")
            return True
    except Exception as e:
        print(f"MinIO no disponible: {e}")
        raise

with DAG(
    dag_id='dag_orquesta_todos',
    default_args=default_args,
    description='Orquesta notebooks 02+03+04 con verificaciones',
    schedule='0 */6 * * *',
    start_date=datetime(2026, 6, 19, tzinfo=local_tz),
    catchup=False,
    tags=['orquestacion', 'multi-tarea', 'completo'],
) as dag:

    check_postgres = PythonOperator(
        task_id='verificar_postgres',
        python_callable=verificar_postgres,
    )

    check_minio = PythonOperator(
        task_id='verificar_minio',
        python_callable=verificar_minio,
    )

    run_02 = BashOperator(
        task_id='ejecutar_notebook_02',
        bash_command="""
            source /home/su_usuario/projects/ebd_env/bin/activate && \
            cd /home/su_usuario/projects/notebooks && \
            jupyter nbconvert --to notebook --execute 02_etl_delta_intro.ipynb \
            --output 02_orquestado.ipynb
        """,
    )

    run_03 = BashOperator(
        task_id='ejecutar_notebook_03',
        bash_command="""
            source /home/su_usuario/projects/ebd_env/bin/activate && \
            cd /home/su_usuario/projects/notebooks && \
            jupyter nbconvert --to notebook --execute 03_etl_postgres.ipynb \
            --output 03_orquestado.ipynb
        """,
    )

    run_04 = BashOperator(
        task_id='ejecutar_notebook_04',
        bash_command="""
            source /home/su_usuario/projects/ebd_env/bin/activate && \
            cd /home/su_usuario/projects/notebooks && \
            jupyter nbconvert --to notebook --execute 04_minio_data_lake.ipynb \
            --output 04_orquestado.ipynb
        """,
    )

    notificar = BashOperator(
        task_id='notificar_completado',
        bash_command='echo "Pipeline completado exitosamente a las $(date)"',
    )
