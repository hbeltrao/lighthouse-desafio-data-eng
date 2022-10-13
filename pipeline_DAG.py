from datetime import datetime, timedelta
from textwrap import dedent
import os

# airflow imports
from airflow.utils.edgemodifier import Label
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable

# importing all tasks declared in the scripts page
from Job_desafio_modulo_5.scripts.fetch_orders import fetch_orders
from Job_desafio_modulo_5.scripts.fetch_order_details import fetch_order_details
from Job_desafio_modulo_5.scripts.transform_data import transform_data

# Import variable with the absolute path os the project, to  ensure code portability
from Job_desafio_modulo_5.config.definitions import ROOT_DIR


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open(os.path.join(ROOT_DIR, 'outputs', 'count.txt')) as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open(os.path.join(ROOT_DIR, 'outputs', 'final_output.txt'),"w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##

with DAG(
    'desafio_airflow_modulo_5',
    default_args=default_args,
    description='Pipeline de dados da Northwind',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 9),
    catchup=False,
    tags=['desafio-lighthouse'],
) as dag:
    dag.doc_md = """
        Pipeline de extração de dados das tabelas orders e order_details do database Northwind com cálculo
        do total de ordens enviadas para a cidade do Rio de Janeiro.
    """
   
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )

    fetch_order_details_task = PythonOperator(
        task_id='fetch_data_from_order_details_table',
        python_callable=fetch_order_details
    )

    fetch_orders_task = PythonOperator(
        task_id='fetch_data_from_orders_table',
        python_callable=fetch_orders
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    [fetch_order_details_task, fetch_orders_task] >> transform_data_task >> export_final_output

    

