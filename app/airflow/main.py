from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task

default_args = {
    'owner': 'like',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 29),
    'retries': 1,
    'retry_delay': timedelta(seconds=1),
}


# Create a dag
dag = DAG(
    dag_id='cars-sales',
    default_args=default_args,
    description='Cars Sales Data Warehousing',
    schedule_interval=timedelta(days=1),
)

# Create task for pre-processing and staging data
staging = BashOperator(
	task_id = 'staging',
	bash_command='''
		cd ~/Documents/Projects/Youcode/FilRouge/sales-car-analysis/app/stagingArea && \
		python3 main.py
	''',
    dag=dag,
)

# Create task for transformation
transformation = BashOperator(
	task_id = 'transformation',
	bash_command='''
		cd ~/Documents/Projects/Youcode/FilRouge/sales-car-analysis/app/transformation && \
		python3 main.py
	''',
    dag=dag,
)

staging >> transformation
