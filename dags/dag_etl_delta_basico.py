from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("America/Bogota")
default_args = {
	'owner': 'oscar',
	'depends_on_past': False,
	'retries': 1,
	'retry_delay': timedelta(minutes=5),
}

with DAG(
	dag_id='dag_etl_delta_basico',
	default_args=default_args,
	description='Ejecuta notebook 02 de ETL Delta Lake',
	schedule='@daily',
	start_date=datetime(2026, 6, 19, tzinfo=local_tz),
	catchup=False,
	tags=['etl', 'delta', 'basico'],
) as dag:

	ejecutar_notebook = BashOperator(
		task_id='ejecutar_notebook_02',
		bash_command="""
			source /home/david/projects/ebd_env/bin/activate && \
			cd /home/david/projects/notebooks && \
			jupyter nbconvert --to notebook --execute 02_etl_delta_intro.ipynb \
			--output 02_etl_delta_intro_executed.ipynb
		""",
	)
	verificar_salida = BashOperator(
		task_id='verificar_delta_output',
		bash_command="""
			ls -la /home/david/projects/data/*.delta 2>/dev/null || echo "Verificar path de salida"
		""",

	)
