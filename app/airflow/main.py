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

# Task: Create tables in data warehouse
create_tables = BashOperator(
	task_id = 'create_tables',
	bash_command='''
		cd ~/Documents/Projects/Youcode/FilRouge/sales-car-analysis/app/load && \
		python3 create_tables.py
	''',
    dag=dag,
)

# Task: Insert country dim
country = BashOperator(
	task_id = 'country',
	bash_command='''
		cd ~/Documents/Projects/Youcode/FilRouge/sales-car-analysis/app/load/tables && \
		python3 country.py
	''',
    dag=dag,
)

# Task: Insert maker dim
maker = BashOperator(
	task_id = 'maker',
	bash_command='''
		cd ~/Documents/Projects/Youcode/FilRouge/sales-car-analysis/app/load/tables && \
		python3 maker.py
	''',
    dag=dag,
)

# Task: Insert color dim
color = BashOperator(
	task_id = 'color',
	bash_command='''
		cd ~/Documents/Projects/Youcode/FilRouge/sales-car-analysis/app/load/tables && \
		python3 color.py
	''',
    dag=dag,
)

# Task: Insert gearbox dim
gearbox = BashOperator(
	task_id = 'gearbox',
	bash_command='''
		cd ~/Documents/Projects/Youcode/FilRouge/sales-car-analysis/app/load/tables && \
		python3 gearbox.py
	''',
    dag=dag,
)

# Task: Insert car_status dim
car_status = BashOperator(
	task_id = 'car_status',
	bash_command='''
		cd ~/Documents/Projects/Youcode/FilRouge/sales-car-analysis/app/load/tables && \
		python3 car_status.py
	''',
    dag=dag,
)

# Task: Insert model dim
model = BashOperator(
	task_id = 'model',
	bash_command='''
		cd ~/Documents/Projects/Youcode/FilRouge/sales-car-analysis/app/load/tables && \
		python3 model.py
	''',
    dag=dag,
)

# Task: Insert date dim
date = BashOperator(
	task_id = 'date',
	bash_command='''
		cd ~/Documents/Projects/Youcode/FilRouge/sales-car-analysis/app/load/tables && \
		python3 date.py
	''',
    dag=dag,
)

# Task: Insert fuel_dimension
fuel_dimension = BashOperator(
	task_id = 'fuel_dimension',
	bash_command='''
		cd ~/Documents/Projects/Youcode/FilRouge/sales-car-analysis/app/load/tables && \
		python3 fuel_dimension.py
	''',
    dag=dag,
)

# Task: Insert fuel_subdimension
fuel_subdimension = BashOperator(
	task_id = 'fuel_subdimension',
	bash_command='''
		cd ~/Documents/Projects/Youcode/FilRouge/sales-car-analysis/app/load/tables && \
		python3 fuel_subdimension.py
	''',
    dag=dag,
)

# Task: Insert cars_dimension
cars_dimension = BashOperator(
	task_id = 'cars_dimension',
	bash_command='''
		cd ~/Documents/Projects/Youcode/FilRouge/sales-car-analysis/app/load/tables && \
		python3 cars_dimension.py
	''',
    dag=dag,
)

# Task: Insert fact
fact = BashOperator(
	task_id = 'fact',
	bash_command='''
		cd ~/Documents/Projects/Youcode/FilRouge/sales-car-analysis/app/load/tables && \
		python3 fact.py
	''',
    dag=dag,
)

staging >> transformation >> create_tables

create_tables >> [
    country, 
    maker,
    color,
    gearbox, 
    car_status, 
    model, 
    date, 
    fuel_dimension
]

fuel_dimension >> fuel_subdimension

[maker, model, color, gearbox, fuel_dimension, car_status] >> cars_dimension

cars_dimension >> fact