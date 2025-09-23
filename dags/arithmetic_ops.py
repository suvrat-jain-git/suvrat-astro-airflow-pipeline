'''
Task 1 : to start with number
Task 2 : add 50 to number 
Task 3 : multiply the result by 2
Task 4 : to divide the result by 10
''' 

from airflow import DAG 
from airflow.operators.python import PythonOperator 
from datetime import datetime 

# Define the function for each task 

def start_number(**context): 
    context["ti"].xcom_push(key='current_value',value=100)
    print("Starting Number is 100") 

def add_fifty(**context): 
    current_value = context["ti"].xcom_pull(key="current_value",task_ids="start_number")
    new_value = current_value + 50 
    context["ti"].xcom_push(key='current_value',value=new_value)
    print(f'add 50 : {current_value}+50 = {new_value}')   

def multiply_two(**context): 
    current_value = context["ti"].xcom_pull(key="current_value",task_ids="add_fifty")
    new_value=current_value*2
    context["ti"].xcom_push(key='current_value',value=new_value)
    print(f'multiply by 2 : {current_value}*2 = {new_value}')  

def divide_ten(**context): 
    current_value = context["ti"].xcom_pull(key="current_value",task_ids="multiply_two")
    new_value=current_value/10
    context["ti"].xcom_push(key='current_value',value=new_value)
    print(f'divide by 10 : {current_value}/10 = {new_value}')   

# define the DAG 

with DAG(
    dag_id="Arithmetic_Operations"
) as dag: 
    start_number = PythonOperator(
        task_id="start_number",
        python_callable = start_number,
    ) 

    add_fifty = PythonOperator(
        task_id="add_fifty",
        python_callable = add_fifty,
    ) 

    multiply_two = PythonOperator(
        task_id="multiply_two",
        python_callable = multiply_two,
    )

    divide_ten = PythonOperator(
        task_id="divide_ten",
        python_callable = divide_ten,
    ) 

    # dependencies 
    start_number >> add_fifty >> multiply_two >> divide_ten 


