from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from Extraction import run_extraction
from Transformation import run_transformation
from Loading import run_loading

default_args = {
    'owner' : 'airflow',
    'depends_om_past' : False,
    'start_date' : datetime(2025, 10, 17),
    'email' : 'alanemetochukwu@yahoo.com',
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries' : 1,
    'retries_delay' : timedelta(minutes=1)

}

dag = DAG(
    'zipco_foods_pipeline',
    default_args = default_args,
    description= 'This represents Zipco foods Data Management pipeline'
)

extraction = PythonOperator(
    task_id = 'extraction_layer',
    python_callable = run_extraction,
    dag=dag,
)

transformation = PythonOperator(
    task_id = 'transformation_layer',
    python_callable = run_transformation,
    dag=dag,

)

loading = PythonOperator(
    task_id = 'loading_layer',
    python_callable = run_loading,
    dag=dag,

)

extraction >> transformation >> loading

