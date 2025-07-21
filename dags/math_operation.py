from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# Define functions for each task
def start_num(**context):
    ti = context['ti']
    ti.xcom_push(key='current_value', value=10)
    print("Starting number 10")


def add_five(**context):
    ti = context['ti']
    current_value = ti.xcom_pull(key="current_value", task_ids="start_task")
    new_value = current_value + 5
    ti.xcom_push(key='current_value', value=new_value)
    print(f"Add 5: {current_value} + 5 = {new_value}")


def multiply_by_two(**context):
    ti = context['ti']
    current_value = ti.xcom_pull(key="current_value", task_ids="add_five_task")
    new_value = current_value * 2
    ti.xcom_push(key='current_value', value=new_value)
    print(f"Multiply 2: {current_value} * 2 = {new_value}")


def subtract_3(**context):
    ti = context['ti']
    current_value = ti.xcom_pull(key="current_value", task_ids="multiply_two_task")
    new_value = current_value - 3
    ti.xcom_push(key='current_value', value=new_value)
    print(f"Subtract 3: {current_value} - 3 = {new_value}")


def square_2(**context):
    ti = context['ti']
    current_value = ti.xcom_pull(key="current_value", task_ids="subtract_3_task")
    new_value = current_value ** 2
    ti.xcom_push(key='current_value', value=new_value)
    print(f"Square value: {current_value}^2 = {new_value}")


# Define the DAG
with DAG(
    dag_id="math_sequence_dag",
    start_date=datetime(2023, 1, 1),
    schedule='@once',
    catchup=False,
) as dag:

    start_task = PythonOperator(
        task_id='start_task',
        python_callable=start_num
    )

    add_five_task = PythonOperator(
        task_id='add_five_task',
        python_callable=add_five
    )

    multiply_two_task = PythonOperator(
        task_id='multiply_two_task',
        python_callable=multiply_by_two
    )

    subtract_3_task = PythonOperator(
        task_id='subtract_3_task',
        python_callable=subtract_3
    )

    square_2_task = PythonOperator(
        task_id='square_2_task',
        python_callable=square_2
    )

    # Set dependencies
    start_task >> add_five_task >> multiply_two_task >> subtract_3_task >> square_2_task
