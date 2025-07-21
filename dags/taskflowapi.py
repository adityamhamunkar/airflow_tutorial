"""

"""



from airflow import DAG
from airflow.decorators import task
from datetime import datetime 


## Define the DAG

with DAG(
    dag_id = "math_sequence_dag_with_taskflow",
    start_date=datetime(2023,1,1),
    schedule='@once',
    catchup=False,
) as dag:
    


    ## Task 1: Start With the initia number