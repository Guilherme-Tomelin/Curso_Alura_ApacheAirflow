from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator 



with DAG(
    'my_first_dag',
    start_date = days_ago(1),
    schedule_interval = '@daily'
) as dag:
    
    task_1 = EmptyOperator(task_id = "first_task")
    task_2 = EmptyOperator(task_id = "second_task")
    task_3 = EmptyOperator(task_id = "third_task")
    task_4 = BashOperator(
        task_id = "create_folder",
        bash_command = "mkdir -p '/home/guilherme_saratt/Curso_Alura_ApacheAirflow/folder' "
        )

    task_1 >> [task_2, task_3]
    task_3 >> task_4

